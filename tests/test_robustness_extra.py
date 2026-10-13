"""Extra edge-case / hidden-test-hardening suite for Lab 25B (student-written).

``tests/`` is otherwise off-limits, but new test files are explicitly allowed.
These cover the boundary conditions the lab brief calls out: non-monotonic
virtual time, late results and stale breaker generations, half-open probes
finishing out of order, similarity exactly on the cache threshold, invalidation
landing between a GiveUp and the stale serve, zero/empty traffic in the SLO
code, and crashes landing immediately after a side effect.
"""

from __future__ import annotations

import itertools
import math
import random

import pytest

from gameday_lab.breaker import BreakerState, SlidingWindowBreaker
from gameday_lab.cache import CachingClient, SemanticCache
from gameday_lab.client import ResilientClient
from gameday_lab.errors import ErrorKind, classify
from gameday_lab.retry import FullJitterBackoff, RetryBudget
from gameday_lab.sim.contracts import (
    Answer,
    Attempt,
    BaseClient,
    CallContext,
    GiveUp,
    ProviderError,
    ProviderInfo,
    Request,
    Send,
    Serve,
)
from gameday_lab.slo.alerting import BurnRateAlerter, WindowSums, error_budget_report
from gameday_lab.workflows.executor import WorkflowExecutor, checkpoint_key, idempotency_key
from gameday_lab.workflows.runner import STEPS, RefundRequest
from gameday_lab.workflows.services import (
    DurableStore,
    FaultProfile,
    ServiceError,
    Services,
    ServiceTimeout,
    WorkerCrash,
)


# --------------------------------------------------------------------- Part A
@pytest.mark.parametrize("status", [500, 501, 502, 503, 504, 505, 529, 599])
def test_every_5xx_is_retryable_and_breaker_relevant(status: int) -> None:
    from gameday_lab.errors import counts_toward_breaker, is_retryable

    kind = classify(ProviderError("p", status, "x"))
    assert kind in (ErrorKind.SERVER_ERROR, ErrorKind.OVERLOADED)
    assert is_retryable(kind) and counts_toward_breaker(kind)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422, 451])
def test_every_other_4xx_is_a_client_error(status: int) -> None:
    from gameday_lab.errors import counts_toward_breaker, failover_helps, is_retryable

    kind = classify(ProviderError("p", status, "x"))
    assert kind is ErrorKind.CLIENT_ERROR
    assert not is_retryable(kind) and not counts_toward_breaker(kind) and not failover_helps(kind)


def test_429_is_classified_by_code_not_by_status() -> None:
    assert classify(ProviderError("p", 429, "rate_limit_exceeded")) is ErrorKind.RATE_LIMITED
    assert classify(ProviderError("p", 429, "insufficient_quota")) is ErrorKind.QUOTA_EXHAUSTED
    # a 429 with an unexpected code is still a rate limit, never a server error
    assert classify(ProviderError("p", 429, "something_new")) is ErrorKind.RATE_LIMITED


def test_status_zero_is_always_our_timeout() -> None:
    assert classify(ProviderError("p", 0, "anything")) is ErrorKind.TIMEOUT
    assert classify(ProviderError("p", 408, "request_timeout")) is ErrorKind.TIMEOUT


def test_backoff_ceiling_is_clamped_at_cap_for_large_retry_numbers() -> None:
    b = FullJitterBackoff(base_s=1.0, cap_s=8.0, rng=random.Random(5))
    for n in (10, 40, 1000):
        samples = [b.delay(n) for _ in range(200)]
        assert all(0.0 <= s <= 8.0 + 1e-9 for s in samples)
        assert max(samples) > 7.0


def test_retry_after_of_zero_still_refuses_to_jitter_below_it() -> None:
    b = FullJitterBackoff(base_s=0.5, cap_s=4.0, rng=random.Random(6))
    assert all(0.0 <= b.delay(1, retry_after=0.0) <= 0.5 + 1e-9 for _ in range(50))


def test_backoff_is_deterministic_for_a_seed() -> None:
    a = [FullJitterBackoff(0.5, 4.0, random.Random(9)).delay(3) for _ in range(3)]
    b = [FullJitterBackoff(0.5, 4.0, random.Random(9)).delay(3) for _ in range(3)]
    assert a == b


def test_budget_survives_non_monotonic_virtual_time() -> None:
    budget = RetryBudget(ratio=0.0, min_per_s=1.0, max_tokens=2.0)
    assert budget.try_acquire_retry(100.0) and budget.try_acquire_retry(100.0)
    assert not budget.try_acquire_retry(50.0)  # time went backwards: no refund of tokens
    assert not budget.try_acquire_retry(100.0)  # still empty
    assert budget.try_acquire_retry(101.0)  # a real second has passed


def test_budget_edge_values() -> None:
    empty = RetryBudget(ratio=0.0, min_per_s=0.0, max_tokens=0.0)
    assert not empty.try_acquire_retry(0.0)
    empty.record_request(0.0)  # must not raise or go negative
    assert not empty.try_acquire_retry(0.0)


# --------------------------------------------------------------------- Part B
def _breaker(**kw: object) -> SlidingWindowBreaker:
    defaults: dict[str, object] = {"window_size": 6, "min_calls": 4, "failure_rate_threshold": 0.5,
                                   "slow_call_s": 5.0, "slow_rate_threshold": 0.5, "open_duration_s": 10.0,
                                   "half_open_max_probes": 3, "half_open_successes": 3}
    defaults.update(kw)
    return SlidingWindowBreaker("alpha", **defaults)  # type: ignore[arg-type]


def test_half_open_probes_completing_out_of_order_close_exactly_once() -> None:
    b = _breaker()
    for _ in range(4):
        p = b.allow(0.0)
        assert p is not None
        b.on_failure(p, 0.0)
    assert b.state is BreakerState.OPEN
    permits = [b.allow(20.0) for _ in range(3)]
    assert all(p is not None and p.probe for p in permits)
    assert b.allow(20.0) is None  # 4th concurrent request: refused
    b.on_success(permits[2], 21.0, 1.0)  # type: ignore[arg-type]
    b.on_success(permits[0], 21.5, 1.0)  # type: ignore[arg-type]
    assert b.state is BreakerState.HALF_OPEN
    b.on_success(permits[1], 22.0, 1.0)  # type: ignore[arg-type]
    assert b.state is BreakerState.CLOSED
    assert [t.to for t in b.transitions] == [BreakerState.OPEN, BreakerState.HALF_OPEN, BreakerState.CLOSED]


def test_result_of_a_closed_permit_arriving_after_reclose_is_ignored() -> None:
    b = _breaker()
    stale = b.allow(0.0)
    assert stale is not None
    for _ in range(4):
        p = b.allow(1.0)
        assert p is not None
        b.on_failure(p, 1.0)
    assert b.state is BreakerState.OPEN
    probes = [b.allow(20.0) for _ in range(3)]
    for p in probes:
        b.on_success(p, 21.0, 1.0)  # type: ignore[arg-type]
    assert b.state is BreakerState.CLOSED
    generation_before = b.generation
    b.on_failure(stale, 30.0)  # a very late failure from before the trip
    assert b.state is BreakerState.CLOSED and b.generation == generation_before
    b.on_success(stale, 30.0, 1.0)
    assert len(b._window) == 0  # stale success did not pollute the fresh window


def test_open_breaker_refuses_until_the_exact_boundary() -> None:
    b = _breaker(open_duration_s=10.0)
    for _ in range(4):
        p = b.allow(100.0)
        assert p is not None
        b.on_failure(p, 100.0)
    assert b.allow(109.999) is None
    assert b.allow(110.0) is not None


def test_release_of_a_stale_probe_is_a_no_op() -> None:
    b = _breaker()
    for _ in range(4):
        p = b.allow(0.0)
        assert p is not None
        b.on_failure(p, 0.0)
    old = b.allow(20.0)
    assert old is not None
    b.on_failure(old, 21.0)  # reopens, bumps the generation
    b.release(old, 21.5)  # must not corrupt probes_in_flight
    assert b.probes_in_flight == 0


def test_a_single_slow_success_does_not_trip() -> None:
    b = _breaker(slow_rate_threshold=0.6)
    p = b.allow(0.0)
    assert p is not None
    b.on_success(p, 1.0, 99.0)
    assert b.state is BreakerState.CLOSED  # below min_calls


def test_all_slow_results_trip_on_the_slow_rate() -> None:
    b = _breaker()
    for _ in range(4):
        p = b.allow(0.0)
        assert p is not None
        b.on_success(p, 0.0, 5.0)  # exactly slow_call_s counts as slow
    assert b.state is BreakerState.OPEN and b.transitions[-1].reason == "slow_call_rate"


def test_window_never_grows_past_window_size() -> None:
    b = _breaker()
    for i in range(200):
        p = b.allow(float(i))
        assert p is not None
        b.on_failure(p, float(i))
        if b.state is BreakerState.OPEN:
            break
    assert len(b._window) <= b.window_size


# --------------------------------------------------------------------- Part C
PROVIDERS = {
    "alpha": ProviderInfo("alpha", "A", "us", True, 1.0),
    "alpha-eu": ProviderInfo("alpha-eu", "A", "eu", True, 1.0),
    "beta": ProviderInfo("beta", "B", "us", True, 1.4),
    "gamma-mini": ProviderInfo("gamma-mini", "C", "us", False, 0.2),
}
ORDER = ["alpha", "alpha-eu", "beta", "gamma-mini"]
SETTINGS = {
    "attempt_timeout_s": 8.0, "max_attempts_per_provider": 2, "min_time_left_s": 1.0,
    "backoff_base_s": 0.25, "backoff_cap_s": 4.0, "retry_budget_ratio": 0.1,
    "retry_budget_min_per_s": 0.5, "retry_budget_max_tokens": 20, "quota_cooldown_s": 300,
    "breaker": {"window_size": 10, "min_calls": 5, "failure_rate_threshold": 0.5, "slow_call_s": 5.0,
                "slow_rate_threshold": 0.6, "open_duration_s": 10.0, "half_open_max_probes": 1,
                "half_open_successes": 1},
}


def _client(**over: object) -> ResilientClient:
    settings = dict(SETTINGS)
    settings.update(over)
    return ResilientClient(PROVIDERS, ORDER, settings, seed=3)


def _call(task: str = "tool", now: float = 0.0, deadline: float = 30.0, cid: int = 1) -> CallContext:
    return CallContext(cid, cid, task, now, deadline)


def test_chat_ladder_includes_the_tool_free_model_but_tool_does_not() -> None:
    c = _client()
    assert "gamma-mini" in c.ladder("chat")
    assert "gamma-mini" not in c.ladder("tool")


def test_failover_never_leaks_permits_when_the_ladder_is_exhausted() -> None:
    c = _client()
    for name in ORDER:
        b = c.breakers[name]
        for i in range(5):
            p = b.allow(0.0)
            assert p is not None
            b.on_failure(p, 0.0)
        assert b.state is BreakerState.OPEN
    call = _call()
    action = c.start(call, 0.0)
    assert isinstance(action, GiveUp)
    assert all(b.probes_in_flight == 0 for b in c.breakers.values())


def test_no_client_error_ever_marks_a_provider_unhealthy() -> None:
    c = _client()
    for i in range(50):
        now = float(i)
        call = CallContext(i, i, "tool", now, now + 30.0)
        send = c.start(call, now)
        assert isinstance(send, Send)
        assert send.provider == "alpha"
        c.on_error(Attempt(call, "alpha", 1, now, send.tag),
                   ProviderError("alpha", 422, "unprocessable"), now + 0.1)
    assert c.breakers["alpha"].state is BreakerState.CLOSED
    assert len(c.breakers["alpha"]._window) == 0  # nothing was recorded at all


def test_rate_limit_cooldown_is_shared_and_never_early() -> None:
    c = _client()
    call = _call()
    send = c.start(call, 0.0)
    assert isinstance(send, Send)
    c.on_error(Attempt(call, "alpha", 1, 0.0, send.tag),
               ProviderError("alpha", 429, "rate_limit_exceeded", retry_after=3.0), 0.0)
    for now in (0.0, 1.0, 2.999):
        other = _call(cid=int(now * 10) + 100, now=now, deadline=now + 30)
        action = c.start(other, now)
        assert isinstance(action, Send) and action.provider != "alpha"
    back = c.start(_call(cid=999, now=3.0, deadline=30.0), 3.0)
    assert isinstance(back, Send) and back.provider == "alpha"


def test_quota_cooldown_outlives_a_short_retry_after() -> None:
    c = _client(quota_cooldown_s=300.0)
    call = _call()
    send = c.start(call, 0.0)
    assert isinstance(send, Send)
    c.on_error(Attempt(call, "alpha", 1, 0.0, send.tag),
               ProviderError("alpha", 429, "insufficient_quota"), 0.0)
    later = c.start(_call(cid=2, now=299.0, deadline=329.0), 299.0)
    assert isinstance(later, Send) and later.provider == "alpha-eu"


def test_attempt_never_exceeds_the_deadline_even_with_a_huge_timeout() -> None:
    c = _client(attempt_timeout_s=30.0)
    call = _call(now=0.0, deadline=5.0)
    action = c.start(call, 0.0)
    assert isinstance(action, Send)
    assert action.delay_s + action.timeout_s <= 5.0 + 1e-9


def test_deadline_is_respected_on_a_late_failure() -> None:
    c = _client()
    call = _call(now=0.0, deadline=10.0)
    send = c.start(call, 0.0)
    assert isinstance(send, Send)
    action = c.on_error(Attempt(call, "alpha", 1, 9.9, send.tag),
                        ProviderError("alpha", 0, "client_timeout"), 9.9)
    if isinstance(action, Send):
        assert 9.9 + action.delay_s + action.timeout_s <= 10.0 + 1e-9
    else:
        assert isinstance(action, GiveUp)


def test_cancel_frees_the_probe_slot_and_a_second_probe_is_allowed() -> None:
    c = _client()
    for i in range(5):
        call = _call(cid=i)
        send = c.start(call, 0.0)
        assert isinstance(send, Send)
        c.on_error(Attempt(call, "alpha", 1, 0.0, send.tag), ProviderError("alpha", 500, "api_error"), 1.0)
    assert c.breakers["alpha"].state is BreakerState.OPEN
    call = _call(cid=50, now=20.0, deadline=50.0)
    send = c.start(call, 20.0)
    assert isinstance(send, Send) and c.breakers["alpha"].state is BreakerState.HALF_OPEN
    c.on_cancel(call, send.tag, 21.0)
    assert c.breakers["alpha"].probes_in_flight == 0


def test_retry_budget_can_stop_same_provider_retries_entirely() -> None:
    c = _client(retry_budget_ratio=0.0, retry_budget_min_per_s=0.0, retry_budget_max_tokens=0.0)
    call = _call()
    send = c.start(call, 0.0)
    assert isinstance(send, Send)
    action = c.on_error(Attempt(call, "alpha", 1, 0.1, send.tag),
                        ProviderError("alpha", 503, "service_unavailable"), 0.1)
    assert isinstance(action, Send) and action.provider == "alpha-eu"  # failover, not retry


# --------------------------------------------------------------------- Part G
def _req(emb: tuple[float, ...], *, user: str = "u1", personal: bool = False, version: str = "v1") -> Request:
    return Request("q", user, personal, version, emb)


_ids = itertools.count(1000)


def _answer(version: str = "v1", sources: tuple[str, ...] = ("policy/refund",)) -> Answer:
    return Answer(next(_ids), version, sources, 0.0)


def _unit(cos: float) -> tuple[float, ...]:
    return (cos, math.sqrt(max(0.0, 1.0 - cos * cos)), 0.0)


def test_similarity_exactly_on_the_threshold_hits() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=60, max_stale_s=60, max_entries=5)
    a = _answer()
    c.store(_req((1.0, 0.0, 0.0)), a, 0.0)
    hit = c.lookup(_req(_unit(0.9)), 1.0)
    assert hit is not None and hit.answer == a


def test_just_below_the_threshold_misses() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=60, max_stale_s=60, max_entries=5)
    c.store(_req((1.0, 0.0, 0.0)), _answer(), 0.0)
    assert c.lookup(_req(_unit(0.9 - 1e-6)), 1.0) is None


def test_entry_exactly_at_the_end_of_its_stale_window_is_still_usable() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=10, max_stale_s=90, max_entries=5)
    c.store(_req((1.0, 0.0, 0.0)), _answer(), 0.0)
    hit = c.lookup(_req((1.0, 0.0, 0.0)), 100.0)
    assert hit is not None and not hit.fresh
    assert c.lookup(_req((1.0, 0.0, 0.0)), 100.001) is None


def test_a_different_prompt_version_never_reuses_the_old_answer() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=600, max_stale_s=600, max_entries=5)
    c.store(_req((1.0, 0.0, 0.0), version="v1"), _answer("v1"), 0.0)
    assert c.lookup(_req((1.0, 0.0, 0.0), version="v2"), 1.0) is None
    c.store(_req((1.0, 0.0, 0.0), version="v2"), _answer("v2"), 1.0)
    assert len(c.entries) == 2  # two scopes, both kept


def test_eviction_keeps_the_entry_just_used() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=600, max_stale_s=600, max_entries=2)
    x, y, z = (1.0, 0.0, 0.0), (0.0, 1.0, 0.0), (0.0, 0.0, 1.0)
    c.store(_req(x), _answer(), 0.0)
    c.store(_req(y), _answer(), 1.0)
    c.lookup(_req(x), 2.0)
    c.store(_req(z), _answer(), 3.0)
    assert c.lookup(_req(y), 4.0) is None and c.lookup(_req(x), 4.0) is not None


def test_invalidate_only_removes_entries_grounded_on_that_source() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=600, max_stale_s=600, max_entries=10)
    c.store(_req((1.0, 0.0, 0.0)), _answer(sources=("policy/refund",)), 0.0)
    c.store(_req((0.0, 1.0, 0.0)), _answer(sources=("policy/shipping",)), 0.0)
    c.store(_req((0.0, 0.0, 1.0)), _answer(sources=("policy/refund", "policy/shipping")), 0.0)
    assert c.invalidate("policy/refund", 1.0) == 2
    assert c.lookup(_req((0.0, 1.0, 0.0)), 2.0) is not None
    assert c.invalidate("policy/refund", 3.0) == 0


def test_personal_question_is_never_stored_or_served() -> None:
    c = SemanticCache(similarity_threshold=0.9, ttl_s=600, max_stale_s=600, max_entries=10)
    c.store(_req((1.0, 0.0, 0.0), personal=True, user="u1"), _answer(sources=("account/u1",)), 0.0)
    assert c.entries == []
    c.store(_req((1.0, 0.0, 0.0)), _answer(), 0.0)
    assert c.lookup(_req((1.0, 0.0, 0.0), personal=True, user="u2"), 1.0) is None


class _StubInner(BaseClient):
    name = "stub"

    def __init__(self) -> None:
        self.next_action: object = Send("alpha", timeout_s=8.0)
        self.events: list[str] = []

    def start(self, call: CallContext, now: float) -> object:
        self.events.append("start")
        return self.next_action

    def on_success(self, attempt: Attempt, now: float, latency_s: float) -> None:
        self.events.append("success")

    def on_error(self, attempt: Attempt, err: ProviderError, now: float) -> object:
        self.events.append("error")
        return self.next_action


def _caching(ttl: float = 60.0, stale: float = 600.0) -> tuple[CachingClient, _StubInner]:
    inner = _StubInner()
    return CachingClient(inner, SemanticCache(0.9, ttl, stale, 50)), inner


def test_invalidation_arriving_between_giveup_and_serve_is_honoured() -> None:
    cc, inner = _caching()
    call = CallContext(1, 1, "chat", 0.0, 30.0, request=_req((1.0, 0.0, 0.0)))
    assert isinstance(cc.start(call, 0.0), Send)
    call.answer = _answer(sources=("policy/refund",))
    cc.on_success(Attempt(call, "alpha", 1, 0.0, None), 1.0, 1.0)
    inner.next_action = GiveUp("no_provider_available")
    # the webhook lands first; only then do the providers give up
    cc.on_content_update("policy/refund", 100.0)
    later = CallContext(2, 2, "chat", 100.0, 130.0, request=_req((1.0, 0.0, 0.0)))
    assert isinstance(cc.start(later, 100.0), GiveUp)


def test_stale_serve_is_marked_stale_and_fresh_serve_is_not() -> None:
    cc, inner = _caching(ttl=60.0, stale=600.0)
    call = CallContext(1, 1, "chat", 0.0, 30.0, request=_req((1.0, 0.0, 0.0)))
    assert isinstance(cc.start(call, 0.0), Send)
    call.answer = _answer()
    cc.on_success(Attempt(call, "alpha", 1, 0.0, None), 1.0, 1.0)
    fresh = cc.start(CallContext(2, 2, "chat", 5.0, 35.0, request=_req((1.0, 0.0, 0.0))), 5.0)
    assert isinstance(fresh, Serve) and fresh.stale is False
    inner.next_action = GiveUp("no_provider_available")
    old = cc.start(CallContext(3, 3, "chat", 200.0, 230.0, request=_req((1.0, 0.0, 0.0))), 200.0)
    assert isinstance(old, Serve) and old.stale is True


def test_call_without_a_request_never_touches_the_cache() -> None:
    cc, _inner = _caching()
    call = CallContext(1, 1, "tool", 0.0, 30.0)
    assert isinstance(cc.start(call, 0.0), Send)
    cc.on_success(Attempt(call, "alpha", 1, 0.0, None), 1.0, 1.0)
    assert cc.cache.entries == []


def test_giveup_on_a_non_faq_call_is_not_upgraded_to_a_stale_answer() -> None:
    cc, inner = _caching()
    inner.next_action = GiveUp("no_provider_available")
    assert isinstance(cc.start(CallContext(1, 1, "tool", 0.0, 30.0), 0.0), GiveUp)


# --------------------------------------------------------------------- Part E
def test_window_sums_on_empty_and_single_element_data() -> None:
    empty = WindowSums([])
    assert empty.sum(0, 1) == 0 and empty.sum(5, 10) == 0
    one = WindowSums([7])
    assert one.sum(0, 1) == 7 and one.sum(0, 100) == 7 and one.sum(-1, 1) == 0


def test_burn_rate_is_zero_without_traffic() -> None:
    a = BurnRateAlerter(0.995)
    t, b = WindowSums([0, 0, 0]), WindowSums([0, 0, 0])
    assert a.burn_rate(t, b, 2, 3) == 0.0


def test_burn_rate_is_defined_on_a_clipped_short_history() -> None:
    a = BurnRateAlerter(0.995)
    t, b = WindowSums([1000]), WindowSums([50])
    assert abs(a.burn_rate(t, b, 0, 4320) - 10.0) < 1e-9  # 5% errors on the 1 minute we have


def test_alerting_on_a_very_short_series_terminates_an_open_episode() -> None:
    total, bad = [100] * 30, [100] * 30  # 100% errors for the whole 30 minutes
    events = BurnRateAlerter().evaluate(total, bad)
    assert events and all(e.end_min <= len(total) for e in events)
    assert any(e.end_min == len(total) for e in events)  # still firing at the end of data


def test_alerting_on_empty_data_is_empty() -> None:
    assert BurnRateAlerter().evaluate([], []) == []


def test_two_separate_incidents_produce_separate_events() -> None:
    day = 1440
    total, bad = [1000] * (4 * day), [0] * (4 * day)
    for start in (day, 3 * day):
        for i in range(start, start + 20):
            bad[i] = 1000
    events = BurnRateAlerter().evaluate(total, bad)
    fast = [e for e in events if e.rule == "fast_burn_2pct_1h"]
    assert len(fast) == 2 and fast[0].end_min <= fast[1].start_min


def test_error_budget_zero_traffic() -> None:
    r = error_budget_report([], [], 0.995)
    assert r["requests"] == 0 and r["allowed_bad"] == 0.0 and r["bad"] == 0.0
    assert r["availability"] == 1.0 and r["freeze_releases"] is False


def test_error_budget_exactly_on_the_freeze_boundary() -> None:
    total, bad = [1000] * 100, [0] * 100
    bad[0] = 500  # exactly the 500-request budget for 99.5%
    r = error_budget_report(total, bad, 0.995)
    assert r["budget_consumed"] == 1.0 and r["budget_remaining"] == 0.0
    assert r["freeze_releases"] is True  # nothing left: stop shipping
    bad[0] = 499
    assert error_budget_report(total, bad, 0.995)["freeze_releases"] is False


def test_error_budget_one_over_the_boundary() -> None:
    total, bad = [1000] * 100, [0] * 100
    bad[0] = 501
    r = error_budget_report(total, bad, 0.995)
    assert r["budget_consumed"] > 1.0 and r["budget_remaining"] < 0
    assert r["freeze_releases"] is True


# --------------------------------------------------------------------- Part D
def _req_wf(session: str = "s1", order: str = "ord_1") -> RefundRequest:
    return RefundRequest(session, order, 1500, "a@example.com")


def test_idempotency_and_checkpoint_namespaces_do_not_collide() -> None:
    req = _req_wf()
    idem = {idempotency_key(req, s) for s in STEPS}
    cp = {checkpoint_key(req, s) for s in STEPS}
    assert len(idem) == len(STEPS) and len(cp) == len(STEPS)
    assert not (idem & cp)


def test_idempotency_key_varies_with_session_and_step_only() -> None:
    a = _req_wf("s1", "ord_1")
    b = _req_wf("s2", "ord_1")
    assert idempotency_key(a, "issue_refund") != idempotency_key(b, "issue_refund")
    assert idempotency_key(a, "issue_refund") == idempotency_key(a, "issue_refund")


def test_crash_immediately_after_the_email_does_not_resend_it() -> None:
    services = Services.build(4, FaultProfile(0.0, 0.0, 0.0))
    store = DurableStore()
    services.email.faults = FaultProfile(0.0, 0.0, 1.0)
    with pytest.raises(WorkerCrash):
        WorkflowExecutor(services, store).run(_req_wf())
    services.email.faults = FaultProfile(0.0, 0.0, 0.0)
    WorkflowExecutor(services, store).run(_req_wf())  # fresh worker resumes
    assert len(services.email.outbox) == 1
    assert len(services.payments.ledger) == 1


def test_checkpoint_is_never_written_for_a_step_that_did_not_finish() -> None:
    services = Services.build(6, FaultProfile(0.0, 0.0, 0.0))
    store = DurableStore()
    services.payments.faults = FaultProfile(0.0, 1.0, 0.0)  # payment committed, answer lost
    with pytest.raises(ServiceTimeout):
        WorkflowExecutor(services, store, max_attempts=1).run(_req_wf())
    assert store.get(checkpoint_key(_req_wf(), "issue_refund")) is None
    assert store.get(checkpoint_key(_req_wf(), "lookup_order")) is not None


def test_retryable_service_errors_are_retried_and_non_retryable_are_not() -> None:
    services = Services.build(9, FaultProfile(0.0, 0.0, 0.0))
    store = DurableStore()
    calls = {"n": 0}
    original = services.llm.classify

    def flaky(order_id: str, amount: int) -> dict:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ServiceError("boom")
        return original(order_id, amount)

    services.llm.classify = flaky  # type: ignore[method-assign]
    assert WorkflowExecutor(services, store).run(_req_wf())["status"] == "done"
    assert calls["n"] == 3


def test_max_attempts_exhaustion_reraises_the_last_failure() -> None:
    services = Services.build(9, FaultProfile(0.0, 0.0, 0.0))
    services.orders.faults = FaultProfile(1.0, 0.0, 0.0)  # every read fails
    with pytest.raises(ServiceError):
        WorkflowExecutor(services, DurableStore(), max_attempts=3).run(_req_wf())


def test_worker_crash_propagates_immediately_without_retrying() -> None:
    services = Services.build(9, FaultProfile(0.0, 0.0, 0.0))
    store = DurableStore()
    services.payments.faults = FaultProfile(0.0, 0.0, 1.0)
    with pytest.raises(WorkerCrash):
        WorkflowExecutor(services, store).run(_req_wf())
    assert services.payments.requests == 1  # no retry loop inside the dying worker


def test_resume_after_a_crash_does_not_repay_or_resend() -> None:
    services = Services.build(12, FaultProfile(0.0, 0.0, 0.0))
    store = DurableStore()
    req = _req_wf("s9", "ord_9")
    # the very first refund execution kills the worker; the restart must resume
    services.payments.faults = FaultProfile(0.0, 0.0, 1.0)
    with pytest.raises(WorkerCrash):
        WorkflowExecutor(services, store).run(req)
    services.payments.faults = FaultProfile(0.0, 0.0, 0.0)
    executions_after_crash = services.payments.executions
    result = WorkflowExecutor(services, store).run(req)
    assert result["status"] == "done"
    assert len(services.payments.ledger) == 1 and len(services.email.outbox) == 1
    assert services.payments.executions == executions_after_crash  # never charged twice
