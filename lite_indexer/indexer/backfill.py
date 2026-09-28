"""Freemium-only backfill queue for ranges the indexer has not indexed.

Policy: the free/keyless RPC pool is reserved for the live tip. Ranges the
indexer skipped itself — a stalled/disabled chain's backlog, or a range a
data-reconciliation rewind flagged as incomplete — are filled by a bounded,
disk-persisted queue that dispatches ONLY keyed/freemium endpoints (see
:mod:`lite_indexer.rpc.freemium`). Freemium accounts carry their own
per-account caps, so backfill can never burn the free pool's rate budget.

Design:

* :func:`detect_gaps` reuses existing signals — the state file's
  ``last_block`` vs the chain tip, and rewind ranges recorded by
  ``MaintenanceMixin._maybe_reconcile_range`` — instead of a parallel
  block-tracking system.
* :class:`BackfillQueue` persists chunk keys ``(chain, from, to)`` as JSON
  under the state dir with the same atomic-write pattern as
  :class:`~lite_indexer.state.StateManager`. Re-enqueueing a done or
  in-flight chunk is a no-op.
* :func:`drain_backfill_queue` runs bounded workers (max depth + max
  concurrency) through an injected async executor. A failed chunk stays in
  the queue (attempts bumped); it is parked after ``BACKFILL_MAX_ATTEMPTS``
  rather than dropped.

The sync-loop sidecar (``BackfillMixin``) lives in ``indexer/_sidecars.py``
and the concrete EVM executor in ``indexer/backfill_executor.py``, keeping
this module focused on the queue itself.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lite_indexer.config import (
    BACKFILL_DONE_RETENTION,
    BACKFILL_MAX_ATTEMPTS,
    BACKFILL_MAX_CHUNKS_PER_CYCLE,
    BACKFILL_MAX_CONCURRENCY,
    BACKFILL_MAX_QUEUE_DEPTH,
    BACKFILL_WINDOW_INFER_S,
    BACKFILL_WINDOW_JITTER_S,
    BACKFILL_WINDOW_MIN_REMAINING,
    STATE_DIR,
)
from lite_indexer.indexer._chunk_runner import ChunkRunner
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.freemium import select_freemium_endpoint_for_chain
from lite_indexer.rpc.ledger import (
    LEDGER,
    ProviderLedger,
    account_id_for_endpoint,
    key_id_for_endpoint,
    provider_id_for_endpoint,
)
from lite_indexer.rpc.provider_caps import provider_concurrency_slot
from lite_indexer.state import atomic_write_json

log = get_logger(__name__)
logger = log

# Executor contract: ``async (chunk, freemium_endpoint) -> success``.
BackfillExecutor = Callable[["BackfillChunk", str], Awaitable[bool]]
EndpointSelector = Callable[[str], str | None]

# Bounded queue constants (chunks), independent of config so tests can pass
# explicit values without monkeypatching the env.
_DEFAULT_DONE_RETENTION = max(1, BACKFILL_DONE_RETENTION)


@dataclass(frozen=True)
class BackfillChunk:
    """One inclusive block range to (re-)fetch for a chain."""

    chain: str
    from_block: int
    to_block: int

    @property
    def key(self) -> str:
        """Stable idempotency key ``chain:from:to``."""
        return f"{self.chain}:{self.from_block}:{self.to_block}"

    @property
    def span(self) -> int:
        """Number of blocks covered (0 for an empty range)."""
        return max(0, self.to_block - self.from_block + 1)

    def to_dict(self) -> dict[str, Any]:
        return {"chain": self.chain, "from_block": self.from_block, "to_block": self.to_block}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackfillChunk | None:
        """Build a chunk from persisted JSON, or None when malformed."""
        try:
            chain = str(data["chain"])
            from_block = int(data["from_block"])
            to_block = int(data["to_block"])
        except (KeyError, TypeError, ValueError):
            return None
        if not chain or to_block < from_block:
            return None
        return cls(chain=chain, from_block=from_block, to_block=to_block)


def chunk_range(chain: str, from_block: int, to_block: int, size: int) -> list[BackfillChunk]:
    """Split ``[from_block, to_block]`` into ``size``-block chunks (clamped)."""
    step = max(1, int(size))
    chunks: list[BackfillChunk] = []
    start = from_block
    while start <= to_block:
        end = min(start + step - 1, to_block)
        chunks.append(BackfillChunk(chain=chain, from_block=start, to_block=end))
        start = end + 1
    return chunks


@dataclass(frozen=True)
class WindowDecision:
    """Quota-window scheduling decision for one backfill chunk dispatch."""

    allowed: bool
    wait_seconds: float = 0.0
    reason: str = "ok"


def _chain_jitter(chain: str, bound: float) -> float:
    """Deterministic per-chain jitter in ``[0, bound]`` seconds.

    Spreads post-reset dispatches so every chain does not fire at the exact
    same window boundary (stable across processes — hashed, not ``hash()``).
    """
    if bound <= 0.0:
        return 0.0
    digest = hashlib.sha256(chain.encode("utf-8")).digest()
    fraction = int.from_bytes(digest[:4], "big") / 0xFFFFFFFF
    return fraction * bound


def window_decision(
    chain: str,
    endpoint: str,
    *,
    now: float | None = None,
    min_remaining_pct: float = BACKFILL_WINDOW_MIN_REMAINING,
    jitter_seconds: float = BACKFILL_WINDOW_JITTER_S,
    infer_window_s: float = BACKFILL_WINDOW_INFER_S,
    ledger: ProviderLedger | None = None,
) -> WindowDecision:
    """Decide whether a backfill chunk may dispatch now, window-aware.

    Reads the target freemium ACCOUNT's quota state from the ledger:

    * no ledger window / unseeded provider → **allow** (absence of data must
      never starve backfill);
    * remaining budget >= ``min_remaining_pct`` → **allow** (after a reset the
      budget is full, so this is the "fire right after the window opens"
      path);
    * remaining budget below the floor → **defer** until the reset plus a
      bounded per-chain jitter, instead of dispatching into a 429.

    Args:
        chain: Chain name (drives the deterministic jitter).
        endpoint: Freemium endpoint whose account budget gates the dispatch.
        now: Epoch override (tests).
        min_remaining_pct: Remaining-budget floor (percent).
        jitter_seconds: Upper bound on the post-reset stagger.
        infer_window_s: Window length used when the ledger has no reset time.
        ledger: Ledger override (tests); defaults to the shared singleton.
    """
    now = time.time() if now is None else now
    active_ledger = ledger if ledger is not None else LEDGER
    provider = provider_id_for_endpoint(endpoint)
    key_id = key_id_for_endpoint(endpoint) or None
    try:
        state = active_ledger.state_for(provider, key_id, now, account_id=account_id_for_endpoint(endpoint))
    except Exception as exc:  # noqa: BLE001 — scheduling must never raise
        logger.debug("backfill_window_state_failed", error=str(exc)[:100])
        return WindowDecision(allowed=True, reason="ledger_error")

    if not state or not state.get("budget_units"):
        return WindowDecision(allowed=True, reason="unknown")

    consumed_pct = float(state.get("consumed_pct", 0.0) or 0.0)
    remaining_pct = 100.0 - consumed_pct
    reset_at = float(state.get("reset_at", 0.0) or 0.0)
    if reset_at <= 0.0:
        # Ledger has a budget but no window: infer a short (60s) window
        # aligned to the next boundary rather than waiting for a reset that
        # will never be reported.
        reset_at = (math.floor(now / infer_window_s) + 1.0) * infer_window_s

    if reset_at <= now:
        return WindowDecision(allowed=True, reason="fresh_window")
    if remaining_pct >= max(0.0, min_remaining_pct):
        return WindowDecision(allowed=True, reason="budget_ok")
    return WindowDecision(
        allowed=False,
        wait_seconds=(reset_at - now) + _chain_jitter(chain, jitter_seconds),
        reason="near_cap",
    )


def detect_gaps(
    chain: str,
    last_block: int,
    tip: int | None,
    rewound_ranges: Sequence[tuple[int, int]],
    chunk_size: int,
    max_chunks: int,
    *,
    include_forward_gap: bool,
) -> list[BackfillChunk]:
    """Detect un-indexed ranges and split them into bounded chunks.

    Two signals, both reused rather than reinvented:

    1. the forward gap ``(last_block, tip]`` — the backlog of a stalled or
       disabled chain (only when ``include_forward_gap`` is set, so an
       actively-advancing chain is left to the live loop);
    2. rewind ranges produced by data reconciliation, which are known to be
       missing.

    Args:
        chain: Chain name.
        last_block: Last block persisted in the state file.
        tip: Current chain tip (None when unknown).
        rewound_ranges: Inclusive ``(from, to)`` ranges flagged by reconciliation.
        chunk_size: Blocks per chunk.
        max_chunks: Upper bound on returned chunks (0/negative = unbounded).
        include_forward_gap: Whether to include the ``(last_block, tip]`` span.

    Returns:
        Deduplicated, ordered chunks (forward gap first, oldest first).
    """
    ranges: list[tuple[int, int]] = []
    if include_forward_gap and tip is not None and tip > last_block:
        ranges.append((last_block + 1, tip))
    ranges.extend((int(f), int(t)) for f, t in rewound_ranges if int(t) >= int(f))

    chunks: list[BackfillChunk] = []
    seen: set[str] = set()
    bound = max_chunks if max_chunks and max_chunks > 0 else None
    for start, end in ranges:
        for chunk in chunk_range(chain, start, end, chunk_size):
            if chunk.key in seen:
                continue
            seen.add(chunk.key)
            chunks.append(chunk)
            if bound is not None and len(chunks) >= bound:
                return chunks
    return chunks


class BackfillQueue:
    """Bounded, persisted, idempotent queue of :class:`BackfillChunk` per chain.

    State file: ``<state_dir>/backfill_queue_<chain>.json``. In-flight chunks
    remain in ``pending`` and are restored as runnable on load (their worker
    died with the process); done keys are retained as the idempotency gate
    and LRU-pruned.
    """

    def __init__(
        self,
        chain: str,
        state_dir: Path | None = None,
        max_depth: int = BACKFILL_MAX_QUEUE_DEPTH,
        max_attempts: int = BACKFILL_MAX_ATTEMPTS,
        done_retention: int = _DEFAULT_DONE_RETENTION,
    ) -> None:
        self.chain = chain
        self._state_dir = Path(state_dir) if state_dir is not None else STATE_DIR
        self._max_depth = max(1, int(max_depth))
        self._max_attempts = max(1, int(max_attempts))
        self._done_retention = max(1, int(done_retention))
        self._pending: dict[str, BackfillChunk] = {}
        self._attempts: dict[str, int] = {}
        self._in_flight: set[str] = set()
        self._done: dict[str, None] = {}
        self.load()

    # ── Persistence ────────────────────────────────────────────────────

    def _path(self) -> Path:
        return self._state_dir / f"backfill_queue_{self.chain}.json"

    def _payload(self) -> dict[str, Any]:
        return {
            "chain": self.chain,
            "pending": [chunk.to_dict() for chunk in self._pending.values()],
            "attempts": dict(self._attempts),
            "done": list(self._done),
        }

    def load(self) -> None:
        """Load persisted queue state (best-effort; corruption → empty)."""
        path = self._path()
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, dict):
                return
            self._pending = {}
            self._attempts = {}
            self._done = {}
            for raw in data.get("pending") or []:
                if isinstance(raw, dict):
                    chunk = BackfillChunk.from_dict(raw)
                    if chunk is not None and chunk.chain == self.chain:
                        self._pending[chunk.key] = chunk
            for key, value in (data.get("attempts") or {}).items():
                try:
                    self._attempts[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            for key in data.get("done") or []:
                self._done[str(key)] = None
            self._prune_done()
            logger.info(
                "backfill_queue_loaded",
                chain=self.chain,
                pending=len(self._pending),
                done=len(self._done),
            )
        except (OSError, ValueError, TypeError) as exc:
            logger.warning("backfill_queue_load_failed", chain=self.chain, error=str(exc)[:80])

    def save(self) -> None:
        """Atomically persist the queue (best-effort)."""
        try:
            atomic_write_json(self._path(), self._payload())
        except OSError as exc:
            logger.warning("backfill_queue_save_failed", chain=self.chain, error=str(exc)[:80])

    # ── Mutation ───────────────────────────────────────────────────────

    def enqueue(self, chunks: Sequence[BackfillChunk]) -> int:
        """Add chunks, skipping done/in-flight/queued duplicates.

        Bounded: stops once the queue reaches ``max_depth``. Returns the
        number of chunks actually added.
        """
        added = 0
        for chunk in chunks:
            if chunk.chain != self.chain or chunk.key in self._done:
                continue
            if chunk.key in self._pending:
                continue
            if self.depth >= self._max_depth:
                logger.warning("backfill_queue_full", chain=self.chain, depth=self.depth)
                break
            self._pending[chunk.key] = chunk
            self._attempts.setdefault(chunk.key, 0)
            added += 1
        if added:
            self.save()
            metrics_incr("backfill_enqueued", float(added))
            logger.info("backfill_enqueued", chain=self.chain, added=added, depth=self.depth)
        return added

    def mark_in_flight(self, key: str) -> None:
        """Mark a pending chunk as dispatched (no-op when unknown)."""
        if key in self._pending:
            self._in_flight.add(key)

    def mark_done(self, key: str) -> None:
        """Mark a chunk complete; it is never enqueued again."""
        self._in_flight.discard(key)
        chunk = self._pending.pop(key, None)
        if chunk is None and key not in self._done:
            return
        self._done[key] = None
        self._attempts.pop(key, None)
        self._prune_done()
        self.save()
        metrics_incr("backfill_completed")
        logger.info("backfill_completed", chain=self.chain, chunk=key)

    def mark_failed(self, key: str) -> None:
        """Return a failed chunk to pending with a bumped attempt count.

        The chunk is NEVER dropped: after ``max_attempts`` it is parked
        (still persisted, excluded from selection) until an operator clears
        it or :meth:`requeue` resets the counter.
        """
        self._in_flight.discard(key)
        if key not in self._pending:
            return
        self._attempts[key] = self._attempts.get(key, 0) + 1
        self.save()
        metrics_incr("backfill_failed")
        logger.warning("backfill_failed", chain=self.chain, chunk=key, attempts=self._attempts[key])

    def requeue(self, key: str | None = None) -> int:
        """Reset attempt counters so parked/failed chunks are retried again."""
        keys = [key] if key is not None else list(self._attempts)
        for candidate in keys:
            if candidate in self._attempts:
                self._attempts[candidate] = 0
        if keys:
            self.save()
        return len(keys)

    def _prune_done(self) -> None:
        while len(self._done) > self._done_retention:
            oldest = next(iter(self._done))
            del self._done[oldest]

    # ── Selection / introspection ──────────────────────────────────────

    def next_pending(self, limit: int) -> list[BackfillChunk]:
        """Up to ``limit`` runnable chunks (not in-flight, below max attempts)."""
        if limit <= 0:
            return []
        runnable = [
            chunk
            for key, chunk in self._pending.items()
            if key not in self._in_flight and self._attempts.get(key, 0) < self._max_attempts
        ]
        return runnable[:limit]

    @property
    def depth(self) -> int:
        """Pending chunk count (in-flight chunks remain in ``pending``)."""
        return len(self._pending)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)

    @property
    def done_count(self) -> int:
        return len(self._done)

    @property
    def parked_count(self) -> int:
        return sum(1 for attempts in self._attempts.values() if attempts >= self._max_attempts)

    def attempts_for(self, key: str) -> int:
        return self._attempts.get(key, 0)

    def contains(self, key: str) -> bool:
        return key in self._pending or key in self._done

    def snapshot(self) -> dict[str, int]:
        """Compact counters for logging / metrics."""
        return {
            "pending": self.pending_count,
            "in_flight": self.in_flight_count,
            "done": self.done_count,
            "parked": self.parked_count,
        }


# ── Worker ──────────────────────────────────────────────────────────────


async def _run_chunk(runner: ChunkRunner, chunk: BackfillChunk, endpoint: str) -> None:
    """Run one chunk under the concurrency bound and record the outcome."""
    async with runner.semaphore:
        runner.queue.mark_in_flight(chunk.key)
        try:
            async with provider_concurrency_slot(provider_id_for_endpoint(endpoint)):
                ok = await runner.executor(chunk, endpoint)
        except Exception as exc:  # noqa: BLE001 — a failed chunk is retried, never dropped
            logger.warning(
                "backfill_exception",
                chain=chunk.chain,
                chunk=chunk.key,
                endpoint=endpoint[:60],
                error=str(exc)[:120],
            )
            ok = False
        if ok:
            runner.queue.mark_done(chunk.key)
            runner.stats["completed"] += 1
        else:
            runner.queue.mark_failed(chunk.key)
            runner.stats["failed"] += 1


def _resolve_runnable(
    chunks: Sequence[BackfillChunk],
    selector: EndpointSelector,
    stats: dict[str, int],
) -> list[tuple[BackfillChunk, str]]:
    """Pair pending chunks with their freemium endpoint (skips when none)."""
    runnable: list[tuple[BackfillChunk, str]] = []
    for chunk in chunks:
        endpoint = selector(chunk.chain)
        if endpoint is None:
            stats["skipped"] += 1
            continue
        runnable.append((chunk, endpoint))
    return runnable


async def drain_backfill_queue(
    queue: BackfillQueue,
    executor: BackfillExecutor,
    *,
    max_concurrency: int = BACKFILL_MAX_CONCURRENCY,
    max_chunks: int = BACKFILL_MAX_CHUNKS_PER_CYCLE,
    memory_ok: bool = True,
    endpoint_selector: EndpointSelector | None = None,
    window_gate: Callable[[str, str], WindowDecision] | None = None,
) -> dict[str, int]:
    """Drain up to ``max_chunks`` pending chunks through ``executor``.

    Bounded by ``max_concurrency`` workers. When ``memory_ok`` is False the
    drain is skipped entirely (chunks stay queued) so the sidecar never
    pushes the daemon toward its recycle ceiling. A chunk with no freemium
    endpoint is skipped (left queued, no attempt consumed). With a
    ``window_gate``, chunks whose target account is near its quota cap are
    deferred (left queued, no attempt consumed) until just after the window
    resets.

    Args:
        queue: The persisted queue.
        executor: ``async (chunk, endpoint) -> success``.
        max_concurrency: Concurrent worker cap (>=1).
        max_chunks: Chunks dispatched this drain.
        memory_ok: False pauses the drain without touching the queue.
        endpoint_selector: Freemium endpoint resolver (defaults to the
            chain-aware live-pool selector).
        window_gate: Optional ``(chain, endpoint) -> WindowDecision`` quota
            window rule. None disables window-aware scheduling.

    Returns:
        Counters: ``dispatched``, ``completed``, ``failed``, ``skipped``,
        ``memory_paused``, ``window_deferred``.
    """
    stats = {"dispatched": 0, "completed": 0, "failed": 0, "skipped": 0, "memory_paused": 0, "window_deferred": 0}
    if not memory_ok:
        stats["memory_paused"] = 1
        metrics_incr("backfill_memory_paused")
        return stats

    chunks = queue.next_pending(max_chunks)
    if not chunks:
        return stats

    selector = endpoint_selector or _default_endpoint_selector
    runnable = _resolve_runnable(chunks, selector, stats)
    if not runnable:
        return stats

    if window_gate is not None:
        allowed: list[tuple[BackfillChunk, str]] = []
        for chunk, endpoint in runnable:
            decision = window_gate(chunk.chain, endpoint)
            if decision.allowed:
                allowed.append((chunk, endpoint))
                continue
            stats["window_deferred"] += 1
            metrics_incr("backfill_window_deferred")
            logger.info(
                "backfill_window_deferred",
                chain=chunk.chain,
                wait_seconds=round(decision.wait_seconds, 1),
                reason=decision.reason,
            )
        runnable = allowed
        if not runnable:
            return stats

    semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
    stats["dispatched"] = len(runnable)
    runner = ChunkRunner(queue=queue, executor=executor, semaphore=semaphore, stats=stats)
    await asyncio.gather(*(_run_chunk(runner, chunk, endpoint) for chunk, endpoint in runnable))
    return stats


def _default_endpoint_selector(chain: str) -> str | None:
    """Backfill endpoint for ``chain`` (EVM: freemium-only; TRON: REST).

    The free/keyless pool is reserved for the EVM live tip, so EVM chunks
    resolve to a freemium (keyed) endpoint. TRON has no freemium
    ``eth_getLogs`` pool at all — ``GET_LOGS_ENDPOINTS`` has no ``tron``
    entry — which made every TRON chunk "skipped" (attempts stayed at 0)
    before the queue could ever drain. TRON's transport is the public
    TronGrid REST endpoint owned by the strategy, so return that so the
    chunk is dispatched (the TRON executor ignores the endpoint value).
    """
    endpoint = select_freemium_endpoint_for_chain(chain)
    if endpoint is not None:
        return endpoint
    if chain == "tron":
        try:
            from lite_indexer.chains.non_evm import TRON_ENDPOINTS

            return TRON_ENDPOINTS[0] if TRON_ENDPOINTS else None
        except Exception as exc:  # noqa: BLE001 — endpoint resolution is best-effort
            logger.debug("backfill_tron_endpoint_failed", error=str(exc)[:100])
    return None


__all__ = [
    "BackfillChunk",
    "BackfillExecutor",
    "BackfillQueue",
    "EndpointSelector",
    "WindowDecision",
    "chunk_range",
    "detect_gaps",
    "drain_backfill_queue",
    "window_decision",
]
