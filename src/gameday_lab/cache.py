"""Part G — a semantic response cache that is safe, fresh, and keeps FAQ answers up in an outage.

* ``SemanticCache``: lookup by embedding similarity *within a scope* (prompt
  version; personal questions never), TTL + a stale window, invalidation by
  source document, bounded size (LRU).
* ``CachingClient``: wraps your ResilientClient. A fresh hit answers without
  calling any provider; a miss goes to the providers and the answer is stored;
  when the providers give up, a *stale* answer beats no answer (fallback
  ladder, rung 3: "cached response").

Tests: tests/test_cache.py, then `make cache` (tape C1).

Lifetime of an entry (age = now - stored_at):

    0 ......... ttl_s ................ ttl_s + max_stale_s ......
    |   fresh   |  stale: only if no provider answers  |  expired: drop it

Only FAQ calls carry ``call.request``; every other call has ``request=None``
and must go straight to the inner client.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from gameday_lab.sim.contracts import (
    Action,
    Answer,
    Attempt,
    BaseClient,
    CallContext,
    GiveUp,
    ProviderError,
    Request,
    Serve,
)


def cosine(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """PROVIDED. Cosine similarity: 1.0 = same direction (high = strict threshold)."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


@dataclass
class Entry:
    embedding: tuple[float, ...]
    answer: Answer
    stored_at: float
    last_used: float


@dataclass(frozen=True)
class CacheHit:
    answer: Answer
    similarity: float
    age_s: float
    fresh: bool  # age_s <= ttl_s; a stale hit may only be served when no provider can answer


class SemanticCache:
    def __init__(self, similarity_threshold: float = 0.9, ttl_s: float = 60.0, max_stale_s: float = 600.0,
                 max_entries: int = 500) -> None:
        """PROVIDED: reads the `cache:` block of configs/gameday.yaml."""
        self.similarity_threshold = similarity_threshold
        self.ttl_s = ttl_s
        self.max_stale_s = max_stale_s
        self.max_entries = max_entries
        self.entries: list[Entry] = []

    def _prune(self, now: float) -> None:
        """Drop entries past the end of their stale window."""
        oldest = now - (self.ttl_s + self.max_stale_s)
        self.entries = [e for e in self.entries if e.stored_at >= oldest]

    def lookup(self, req: Request, now: float) -> CacheHit | None:
        """The best usable entry for ``req``, or None.

        * personal questions are not in the allowlist -> never served from cache
        * expired entries (age > ttl + max_stale) are dropped
        * scope: only entries generated with the same ``prompt_version`` (a new
          system prompt can change every answer, so v1 answers are not reusable)
        * best = highest cosine similarity; a hit needs >= ``similarity_threshold``
        """
        if req.personal:
            return None
        self._prune(now)
        best: Entry | None = None
        best_sim = 0.0
        for entry in self.entries:
            if entry.answer.prompt_version != req.prompt_version:
                continue
            sim = cosine(req.embedding, entry.embedding)
            if sim >= self.similarity_threshold and (best is None or sim > best_sim):
                best, best_sim = entry, sim
        if best is None:
            return None
        best.last_used = now
        age = now - best.stored_at
        return CacheHit(best.answer, best_sim, age, fresh=age <= self.ttl_s)

    def store(self, req: Request, answer: Answer, now: float) -> None:
        """Remember ``answer`` for ``req``.

        * personal answers are never stored (they go stale in minutes and would
          leak one user's order/refund/points to another)
        * an entry in the same scope that is already similar *is* the same
          question: replace it (refresh the TTL) instead of piling up duplicates
        * bounded size: evict the least recently used entry
        """
        if req.personal:
            return
        for entry in self.entries:
            if entry.answer.prompt_version != answer.prompt_version:
                continue
            if cosine(req.embedding, entry.embedding) >= self.similarity_threshold:
                entry.embedding = req.embedding
                entry.answer = answer
                entry.stored_at = now
                entry.last_used = now
                return
        self.entries.append(Entry(req.embedding, answer, now, now))
        if len(self.entries) > self.max_entries:
            lru = min(self.entries, key=lambda e: e.last_used)
            self.entries.remove(lru)

    def invalidate(self, source: str, now: float) -> int:
        """Drop every entry whose ``answer.sources`` contains ``source``; return how many."""
        before = len(self.entries)
        self.entries = [e for e in self.entries if source not in e.answer.sources]
        return before - len(self.entries)


class CachingClient(BaseClient):
    name = "cached"

    def __init__(self, inner: BaseClient, cache: SemanticCache) -> None:
        """PROVIDED. ``inner`` is your ResilientClient (see factory.build_client("cached"))."""
        self.inner = inner
        self.cache = cache

    def on_cancel(self, call: CallContext, tag: Any, now: float) -> None:
        """PROVIDED."""
        self.inner.on_cancel(call, tag, now)

    # ------------------------------------------------------------------ TODO
    def start(self, call: CallContext, now: float) -> Action:
        """A fresh cache hit answers without touching a provider; everything else
        goes to the inner client (and is upgraded to a stale answer on GiveUp)."""
        req = call.request
        if req is not None:
            hit = self.cache.lookup(req, now)
            if hit is not None and hit.fresh:
                return Serve(hit.answer, stale=False)
        return self._or_stale(call, self.inner.start(call, now), now)

    def on_success(self, attempt: Attempt, now: float, latency_s: float) -> None:
        """Tell the inner client first (its breaker needs the result), then remember
        the answer the provider produced for this FAQ request."""
        self.inner.on_success(attempt, now, latency_s)
        call = attempt.call
        if call.request is not None and call.answer is not None:
            self.cache.store(call.request, call.answer, now)

    def on_error(self, attempt: Attempt, err: ProviderError, now: float) -> Action:
        """Delegate to the inner client, then apply stale-if-error."""
        return self._or_stale(attempt.call, self.inner.on_error(attempt, err, now), now)

    def on_content_update(self, source: str, now: float) -> None:
        """A document changed (webhook) — invalidate every answer built on it.

        This is the only defence against a *previously served* answer being wrong
        after an editorial change: the answers themselves carry no version number
        the client could compare, so the entries must be dropped outright.
        """
        self.cache.invalidate(source, now)

    def _or_stale(self, call: CallContext, action: Action, now: float) -> Action:
        """Stale-if-error: rung 3 of the fallback ladder.

        Only when the providers have given up *and* the call is a FAQ call do we
        re-consult the cache — the lookup happens again now, so an invalidation
        that arrived while the providers were failing still wins.
        """
        if isinstance(action, GiveUp) and call.request is not None:
            hit = self.cache.lookup(call.request, now)
            if hit is not None:
                return Serve(hit.answer, stale=not hit.fresh)
        return action
