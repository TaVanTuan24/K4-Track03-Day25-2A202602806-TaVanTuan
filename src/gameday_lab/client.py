"""Part C — ResilientClient: compose classification, retry, breaker and fallback.

The simulator calls ``start`` for every logical LLM call, then ``on_success``
or ``on_error`` for every attempt. You answer with ``Send`` (try a provider,
maybe after a delay) or ``GiveUp`` (user gets a static degraded answer).

Tests: tests/test_client.py, then `make compare` (6 incident tapes).

Per-call scratch space lives in ``call.state`` (a dict). Suggested keys:
    "ladder": list[str]  providers this call may use, in order
    "idx":    int        position of the provider we are currently on
    "tries":  int        attempts already made on that provider

Decision table for ``on_error`` (implement it!):

| ErrorKind        | breaker              | then                                                    |
|------------------|----------------------|---------------------------------------------------------|
| CLIENT_ERROR     | release (not health) | GiveUp("client_error") — never retry, never fail over   |
| QUOTA_EXHAUSTED  | release              | cooldown provider for quota_cooldown_s; fail over now   |
| RATE_LIMITED     | release              | cooldown provider until now + retry_after; fail over now|
| OVERLOADED /     | on_failure           | retry SAME provider after backoff.delay(tries) if       |
| SERVER_ERROR /   |                      |   tries < max_attempts_per_provider AND budget allows   |
| TIMEOUT          |                      |   AND it fits the deadline; otherwise fail over         |

Cooldowns are per provider and shared by all calls (other calls must avoid a
rate-limited provider too). When every provider in the ladder is unusable,
you may wait for a *short* (<= 5 s) rate-limit cooldown if it fits the deadline.
"""

from __future__ import annotations

import random
from typing import Any

from gameday_lab.breaker import BreakerState, Permit, SlidingWindowBreaker
from gameday_lab.errors import (  # noqa: F401
    ErrorKind,
    classify,
    counts_toward_breaker,
    failover_helps,
    is_retryable,
)
from gameday_lab.retry import FullJitterBackoff, RetryBudget
from gameday_lab.sim.contracts import (
    Action,
    Attempt,
    BaseClient,
    CallContext,
    GiveUp,
    ProviderError,
    ProviderInfo,
    Send,
)

RATE_LIMIT_WAIT_MAX_S = 5.0  # only wait for short (rate-limit) cooldowns, never for quota


class ResilientClient(BaseClient):
    name = "resilient"

    def __init__(
        self,
        providers: dict[str, ProviderInfo],
        fallback_order: list[str] | tuple[str, ...],
        settings: dict[str, Any],
        seed: int = 0,
    ) -> None:
        """PROVIDED: reads the `client:` block of configs/gameday.yaml."""
        self.providers = providers
        self.order = list(fallback_order)
        self.timeout_s = float(settings.get("attempt_timeout_s", 8.0))
        self.max_attempts_per_provider = int(settings.get("max_attempts_per_provider", 2))
        self.min_time_left_s = float(settings.get("min_time_left_s", 1.0))
        self.quota_cooldown_s = float(settings.get("quota_cooldown_s", 300.0))
        self.backoff = FullJitterBackoff(
            float(settings.get("backoff_base_s", 0.25)),
            float(settings.get("backoff_cap_s", 4.0)),
            random.Random(seed),
        )
        self.budget = RetryBudget(
            float(settings.get("retry_budget_ratio", 0.1)),
            float(settings.get("retry_budget_min_per_s", 0.5)),
            float(settings.get("retry_budget_max_tokens", 20.0)),
        )
        breaker_cfg = dict(settings.get("breaker", {}))
        self.breakers = {name: SlidingWindowBreaker(name, **breaker_cfg) for name in providers}
        self.cooldown_until: dict[str, float] = {name: 0.0 for name in providers}

    # ------------------------------------------------------------------ provided helper
    def _send(self, call: CallContext, provider: str, now: float, delay: float) -> Action | None:
        """PROVIDED. Build a Send for ``provider`` (after ``delay``), or None if its breaker refuses.

        Takes a breaker permit (carried in ``tag``), and caps the attempt timeout
        so it never runs past the call's deadline. Returns GiveUp("deadline") if
        there is not enough time left for a meaningful attempt.
        """
        permit = self.breakers[provider].allow(now)
        if permit is None:
            return None
        timeout = min(self.timeout_s, call.deadline - (now + delay))
        if timeout < self.min_time_left_s:
            self.breakers[provider].release(permit, now)
            return GiveUp("deadline")
        return Send(provider, timeout_s=timeout, delay_s=delay, tag=permit)

    # ------------------------------------------------------------------ TODO
    def ladder(self, task: str) -> list[str]:
        """``self.order`` filtered by capability.

        A ``"tool"`` call needs a deployment that supports tool calling; one that
        does not still answers with HTTP 200, but the body is prose rather than a
        tool call (the simulator counts that as ``incompatible_routes``).
        ``"chat"`` can use every deployment. The filter is driven entirely by
        ``ProviderInfo.supports_tools`` — no deployment name is baked in here.
        """
        if task == "tool":
            return [name for name in self.order if self.providers[name].supports_tools]
        return list(self.order)

    def _cooldown(self, provider: str, until: float) -> None:
        """Extend a per-provider cooldown; it is shared by every call in flight."""
        self.cooldown_until[provider] = max(self.cooldown_until.get(provider, 0.0), until)

    def _route(self, call: CallContext, now: float, delay: float = 0.0) -> Action:
        """Pick the provider for the next attempt, walking down the ladder.

        Skips a provider that is still cooling down *at send time* (``now + delay``)
        and one whose breaker refuses a permit. A failover goes out immediately, a
        same-provider retry keeps its backoff delay. When the ladder is exhausted we
        may wait out one short rate-limit cooldown; otherwise GiveUp.
        """
        ladder = call.state.get("ladder") or self.ladder(call.task)
        call.state["ladder"] = ladder
        idx = int(call.state.get("idx", 0))

        while idx < len(ladder):
            provider = ladder[idx]
            if self.cooldown_until.get(provider, 0.0) > now + delay:
                idx += 1
                call.state["tries"] = 0
                delay = 0.0
                continue
            action = self._send(call, provider, now, delay)
            call.state["idx"] = idx
            if action is not None:
                return action
            idx += 1
            call.state["tries"] = 0
            delay = 0.0

        call.state["idx"] = idx
        # Every ladder member was either cooling down or refused a permit. A short
        # rate-limit cooldown is worth waiting out, and it is worth waiting out
        # again later in the same call: a call that waited for `beta`, got a second
        # 429 and gave up is a user we dropped for no reason. `_earliest_usable`
        # admits only a wait that still leaves room for a real attempt inside the
        # deadline, so repeats are bounded by the deadline and by how often a
        # provider can be rate-limited — no per-call counter is needed.
        provider, wait = self._earliest_usable(call, ladder, now)
        if provider is not None:
            call.state["idx"] = ladder.index(provider)
            call.state["tries"] = 0
            action = self._send(call, provider, now, wait)
            if action is not None:  # always a Send: _earliest_usable checked the permit
                return action
        return GiveUp("no_provider_available")

    def _earliest_usable(self, call: CallContext, ladder: list[str], now: float) -> tuple[str | None, float]:
        """The ladder member that becomes *sendable* soonest, and how long to wait.

        Only *short* waits are worth taking: a rate limit clears within ~1 s,
        whereas a quota cooldown is minutes and a provider that must still finish
        its own open period is not coming back in time — and a provider whose
        breaker is OPEN is exactly that case, so it is never a wait candidate.
        A tie goes to the earlier (more preferred) deployment.

        ``would_allow`` is required as well as the cooldown, so the provider really
        will hand out a permit at ``now``. Without it, a HALF_OPEN provider whose
        probe slots are all taken would look waitable and then refuse, and the
        caller could spin on it; with it, the ``_send`` that follows cannot fail.
        """
        best: tuple[float, int, str] | None = None
        for pos, provider in enumerate(ladder):
            if self.breakers[provider].state is BreakerState.OPEN:
                continue
            wait = self.cooldown_until.get(provider, 0.0) - now
            if not 0.0 < wait <= RATE_LIMIT_WAIT_MAX_S:
                continue
            if now + wait + self.min_time_left_s > call.deadline:
                continue
            if not self.breakers[provider].would_allow(now):
                continue
            candidate = (wait, pos, provider)
            if best is None or (wait, pos) < (best[0], best[1]):
                best = candidate
        return (best[2], best[0]) if best is not None else (None, 0.0)

    def start(self, call: CallContext, now: float) -> Action:
        """A new logical call begins: pay one token into the retry budget, then route."""
        self.budget.record_request(now)
        call.state["ladder"] = self.ladder(call.task)
        call.state["idx"] = 0
        call.state["tries"] = 0
        return self._route(call, now)

    def _release(self, attempt: Attempt, now: float) -> None:
        """Free a permit whose attempt told us nothing about provider health."""
        permit = attempt.tag
        breaker = self.breakers.get(attempt.provider)
        if breaker is not None and isinstance(permit, Permit):
            breaker.release(permit, now)

    def _failover(self, call: CallContext, now: float) -> Action:
        """Move to the next provider on the ladder, immediately."""
        call.state["tries"] = 0
        call.state["idx"] = int(call.state.get("idx", 0)) + 1
        return self._route(call, now, delay=0.0)

    def on_success(self, attempt: Attempt, now: float, latency_s: float) -> None:
        """Report success + latency to the provider's breaker (``attempt.tag`` is the Permit)."""
        breaker = self.breakers.get(attempt.provider)
        permit = attempt.tag
        if breaker is not None and isinstance(permit, Permit):
            breaker.on_success(permit, now, latency_s)

    def on_error(self, attempt: Attempt, err: ProviderError, now: float) -> Action:
        """Implement the decision table in the module docstring."""
        call = attempt.call
        provider = attempt.provider
        kind = classify(err)

        if kind is ErrorKind.CLIENT_ERROR:
            # Our request is malformed: every provider will reject it. Never retry,
            # never fail over, and never blame the provider for it.
            self._release(attempt, now)
            return GiveUp("client_error")

        if kind is ErrorKind.QUOTA_EXHAUSTED:
            # The account is out of credit: same provider cannot answer, and the
            # cooldown is long because only a human/billing change fixes it.
            self._release(attempt, now)
            self._cooldown(provider, now + self.quota_cooldown_s)
            return self._failover(call, now)

        if kind is ErrorKind.RATE_LIMITED:
            # Our tenant is over the limit, not the provider unhealthy. Respect
            # retry-after exactly (retrying earlier is a retry_after_violation)
            # and let another deployment take the traffic meanwhile.
            self._release(attempt, now)
            retry_after = float(err.retry_after) if err.retry_after is not None else 1.0
            self._cooldown(provider, now + max(0.0, retry_after))
            return self._failover(call, now)

        # OVERLOADED / SERVER_ERROR / TIMEOUT: a real health signal for this provider.
        breaker = self.breakers.get(provider)
        permit = attempt.tag
        if breaker is not None and isinstance(permit, Permit):
            breaker.on_failure(permit, now)

        made = int(call.state.get("tries", 0)) + 1
        call.state["tries"] = made
        if is_retryable(kind) and made < self.max_attempts_per_provider:
            delay = self.backoff.delay(made)
            # Prepare the retry only if it can actually go out at now + delay:
            #   * it fits the caller's deadline;
            #   * the provider is not cooling down — the cooldown is shared, so
            #     another call may have rate-limited this deployment since our
            #     attempt was sent, and retrying into it now would be rejected
            #     again (and would waste the attempt);
            #   * the breaker would still admit it — the failure we just recorded
            #     may have been the one that tripped it;
            #   * the retry budget still holds a token.
            # The token is spent LAST, and only because the send is now certain,
            # so a retry that cannot go out never drains the budget. After these
            # gates `_send` cannot return None or GiveUp("deadline"), because
            # would_allow mirrors allow() and the deadline test below is the same
            # one _send performs.
            if (now + delay + self.min_time_left_s <= call.deadline
                    and self.cooldown_until.get(provider, 0.0) <= now + delay
                    and self.breakers[provider].would_allow(now)
                    and self.budget.try_acquire_retry(now)):
                action = self._send(call, provider, now, delay)
                if action is not None:  # always a Send here; None kept as a guard
                    return action
        if not failover_helps(kind):
            return GiveUp("not_retryable")
        return self._failover(call, now)

    def on_cancel(self, call: CallContext, tag: Any, now: float) -> None:
        """A delayed attempt was dropped (deadline) — release its permit."""
        if isinstance(tag, Permit):
            breaker = self.breakers.get(tag.breaker)
            if breaker is not None:
                breaker.release(tag, now)
