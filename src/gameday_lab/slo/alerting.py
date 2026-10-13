"""Part E — Multi-window, multi-burn-rate alerting (Google SRE Workbook, ch. 5).

Input: per-minute ``total`` and ``bad`` request counts for a 30-day window.

burn_rate(window) = error_ratio(window) / (1 - SLO)

A rule fires at minute i when BOTH its long and its short window (ending at i,
inclusive) burn at least ``burn_rate``. The long window says "this is
significant"; the short window says "it is still happening" (so the alert
resets quickly after recovery).

Tests: tests/test_alerting.py, then `make slo`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlertRule:
    name: str
    severity: str  # "page" or "ticket"
    long_min: int
    short_min: int
    burn_rate: float


# SRE Workbook defaults for a 30-day SLO window
DEFAULT_RULES: tuple[AlertRule, ...] = (
    AlertRule("fast_burn_2pct_1h", "page", 60, 5, 14.4),
    AlertRule("slow_burn_5pct_6h", "page", 360, 30, 6.0),
    AlertRule("budget_leak_10pct_3d", "ticket", 4320, 360, 1.0),
)


@dataclass(frozen=True)
class AlertEvent:
    rule: str
    severity: str
    start_min: int  # first minute the rule fired
    end_min: int  # first minute it stopped firing (exclusive)


class WindowSums:
    """O(1) sum of any window, via prefix sums. Build once, query many times.

    43,200 minutes x 6 windows: recomputing every window with sum() is far too slow.
    """

    def __init__(self, values: list[int]) -> None:
        """Build the prefix-sum list (``prefix[0] = 0``, ``prefix[i+1] = sum(values[:i+1])``)."""
        prefix = [0]
        total = 0
        for value in values:
            total += value
            prefix.append(total)
        self._prefix = prefix
        self._n = len(values)

    def sum(self, end_inclusive: int, length: int) -> int:
        """Sum of ``values[end_inclusive - length + 1 .. end_inclusive]``, in O(1).

        Clipped at index 0 when the window starts before the data (a 3-day window
        evaluated in the first hour of the dataset only has data from minute 0).
        An empty or fully out-of-range window is 0.
        """
        if length <= 0:
            return 0
        hi = min(int(end_inclusive), self._n - 1)
        if hi < 0:
            return 0
        lo = max(0, int(end_inclusive) - length + 1)
        if lo > hi:
            return 0
        return self._prefix[hi + 1] - self._prefix[lo]


class BurnRateAlerter:
    def __init__(self, slo: float = 0.995, rules: tuple[AlertRule, ...] = DEFAULT_RULES) -> None:
        if not 0 < slo < 1:
            raise ValueError("slo must be in (0, 1)")
        self.slo = slo
        self.rules = rules

    def burn_rate(self, total: WindowSums, bad: WindowSums, end: int, length: int) -> float:
        """``(bad / total over the window) / (1 - slo)``; 0.0 when the window has no traffic.

        With a 99.5% SLO, ``1 - slo`` is the error ratio that exactly spends the
        budget over the whole window, so a burn rate of 14.4 means "the budget is
        gone in ~2 days at this speed".
        """
        requests = total.sum(end, length)
        if requests <= 0:
            return 0.0
        return (bad.sum(end, length) / requests) / (1.0 - self.slo)

    def evaluate(self, total: list[int], bad: list[int]) -> list[AlertEvent]:
        """One AlertEvent per contiguous firing episode of each rule.

        A rule fires at minute i only when BOTH its long and its short window
        burn at least ``rule.burn_rate``. The long window establishes that enough
        budget is being spent to matter; the short window requires that it is
        *still* happening, so an alert clears minutes after recovery instead of
        hours. Contiguous firing minutes become one event with an exclusive end.
        """
        n = len(total)
        t_sums, b_sums = WindowSums(total), WindowSums(bad)
        events: list[AlertEvent] = []
        for rule in self.rules:
            start: int | None = None
            for i in range(n):
                firing = (self.burn_rate(t_sums, b_sums, i, rule.long_min) >= rule.burn_rate
                          and self.burn_rate(t_sums, b_sums, i, rule.short_min) >= rule.burn_rate)
                if firing and start is None:
                    start = i
                elif not firing and start is not None:
                    events.append(AlertEvent(rule.name, rule.severity, start, i))
                    start = None
            if start is not None:
                events.append(AlertEvent(rule.name, rule.severity, start, n))
        events.sort(key=lambda e: e.start_min)
        return events


def error_budget_report(total: list[int], bad: list[int], slo: float = 0.995) -> dict[str, float | bool]:
    """How much of the 30-day error budget is spent?

    ``allowed_bad = (1 - slo) * requests`` is the number of failing requests the
    SLO permits over the whole window; ``budget_consumed`` is the fraction of it
    already spent and ``freeze_releases`` is the policy answer: once nothing is
    left, ship no more changes until the window rolls forward.
    """
    requests = float(sum(total))
    bad_total = float(sum(bad))
    allowed_bad = round((1.0 - slo) * requests, 1)
    if requests > 0:
        availability = round(1.0 - bad_total / requests, 6)
    else:
        availability = 1.0
    if allowed_bad > 0:
        consumed = round(bad_total / allowed_bad, 4)
    else:
        # only reachable with zero traffic (or a 100% SLO): no budget to spend
        consumed = 0.0 if bad_total <= 0 else 1.0
    remaining = round(1.0 - consumed, 4)
    return {
        "requests": requests,
        "allowed_bad": allowed_bad,
        "bad": bad_total,
        "availability": availability,
        "budget_consumed": consumed,
        "budget_remaining": remaining,
        "freeze_releases": bool(remaining <= 0),
    }
