"""Part D — Exactly-once *effects* for an agent workflow.

Two ingredients, and you need both:

1. **Idempotency keys** — a retry (or a re-run after a crash) of a side effect
   must reuse the *same* key, so the payment provider de-duplicates it.
   The key must be derived from *what* you are doing (session + step), never
   random per attempt.
2. **Checkpoints** — after each step succeeds, persist its result in the
   DurableStore. A fresh worker resumes from the last checkpoint instead of
   redoing (and re-paying for) everything.

Think hard about the ORDER of "execute step" and "save checkpoint".
Never catch ``WorkerCrash`` — it means this process is dead.

Tests: tests/test_executor.py, then `make workflows`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from gameday_lab.workflows.runner import STEPS, RefundRequest
from gameday_lab.workflows.services import DurableStore, ServiceError, Services, ServiceTimeout

T = TypeVar("T")


def idempotency_key(req: RefundRequest, step: str) -> str:
    """A key that is identical for every retry / restart of the same side effect
    of the same session, and different across sessions and steps.

    Derived from *what we are doing* (session + step), never random: a retry
    after a lost response, or a fresh worker resuming a crashed one, must present
    the payment provider the exact same key so it de-duplicates the charge.
    """
    return f"refund-wf/{req.session_id}/{step}"


def checkpoint_key(req: RefundRequest, step: str) -> str:
    """DurableStore key under which the result of ``step`` is saved.

    A distinct namespace from the idempotency keys (``checkpoint/`` vs
    ``refund-wf/``) so a step's stored *result* can never be mistaken for a
    provider-side idempotency record, and one session's steps can never collide
    with another session's.
    """
    return f"checkpoint/{req.session_id}/{step}"


class WorkflowExecutor:
    def __init__(self, services: Services, store: DurableStore, max_attempts: int = 5) -> None:
        self.s = services
        self.store = store
        self.max_attempts = max_attempts

    def _retry(self, fn: Callable[[], T]) -> T:
        """Call ``fn()``; retry on ServiceError / ServiceTimeout up to
        ``max_attempts`` times (re-raise the last one).

        Only the two *retryable* faults are caught. ``WorkerCrash`` inherits
        BaseException and is deliberately not caught: this process is dead, and
        the runner will start a fresh worker that must resume from checkpoints.
        """
        last: Exception | None = None
        for _ in range(self.max_attempts):
            try:
                return fn()
            except (ServiceError, ServiceTimeout) as exc:
                last = exc
        assert last is not None
        raise last

    def _step(self, req: RefundRequest, state: dict[str, Any], step: str) -> Any:
        """PROVIDED: execute one step. ``state`` holds results of earlier steps."""
        if step == "classify_request":
            return self._retry(lambda: self.s.llm.classify(req.order_id, req.amount_cents))
        if step == "lookup_order":
            return self._retry(lambda: self.s.orders.get(state["classify_request"]["order_id"]))
        if step == "issue_refund":
            key = idempotency_key(req, step)
            return self._retry(lambda: self.s.payments.refund(req.order_id, req.amount_cents, idempotency_key=key))
        if step == "send_email":
            key = idempotency_key(req, step)
            data = {"refund_id": state["issue_refund"]["refund_id"]}
            return self._retry(lambda: self.s.email.send(req.customer_email, "refund_done", data, idempotency_key=key))
        if step == "write_summary":
            return self._retry(lambda: self.s.llm.summarize(state["issue_refund"]["refund_id"],
                                                            state["send_email"]["message_id"]))
        raise ValueError(step)

    def run(self, req: RefundRequest) -> dict[str, Any]:
        """Run every step of ``STEPS`` for ``req``, exactly once per effect.

        For each step: if a checkpoint exists, a previous (possibly crashed)
        worker already did it — load the stored result and skip. Otherwise run
        the step and checkpoint its result *after* it returned.

        The order is the whole point. Checkpointing *before* the side effect
        would mark a payment as done that never happened (a lost refund); the
        reverse means a crash between the effect and the checkpoint replays the
        step, which is safe only because the step reuses its stable idempotency
        key and the provider de-duplicates it.
        """
        state: dict[str, Any] = {}
        for step in STEPS:
            key = checkpoint_key(req, step)
            done = self.store.get(key)
            if done is not None:
                state[step] = done
                continue
            result = self._step(req, state, step)
            self.store.put(key, result)  # only after the effect really happened
            state[step] = result
        return {"status": "done", **state}
