"""Range/chunk planning helpers for the EVM transfer handler.

Extracted from ``strategies.evm.transfer_handler``: adaptive chunk-size
reduction after pool-wide failures, round-robin rotation for all-soft-
banned (rate-limited) pools, wide-range endpoint preference, and the
contiguous-range grouping used by the bloom pre-filter.
"""

from __future__ import annotations

import json
from pathlib import Path

from lite_indexer.chains.helpers import get_max_block_range
from lite_indexer.config import STATE_DIR
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Adaptive chunk-size reduction ─────────────────────────────────────
# After repeated pool-wide failures (RpcFetchFailedError), the effective
# chunk size is halved (floor ADAPTIVE_CHUNK_FLOOR) so restrictive free
# RPCs with small range caps (blastapi=10, blockrazor=25) or small result
# caps can serve the range. A success resets the reduction. This makes
# catch-up degrade gracefully instead of failing the same 50-block chunk
# forever when every endpoint caps below the default chunk size.
_ADAPTIVE_CHUNK_LEVELS: dict[str, int] = {}
_ADAPTIVE_CHUNK_STREAKS: dict[str, int] = {}
_ADAPTIVE_CHUNK_LOADED: set[str] = set()
ADAPTIVE_CHUNK_FLOOR: int = 5
ADAPTIVE_CHUNK_MAX_HALVES: int = 4
# Consecutive successful batches at the current reduced size before the
# level decays ONE step back toward the base chunk size. Persisted so a
# watchdog restart cannot reset the adaptation (2026-09-19).
ADAPTIVE_RECOVERY_SUCCESSES: int = 12


def _adaptive_state_path(chain: str) -> Path:
    """Persistent state file holding one chain's chunk adaptation."""
    return Path(STATE_DIR) / f"adaptive_chunk_{chain}.json"


def _load_adaptive_chunk(chain: str) -> None:
    """Hydrate a chain's persisted adaptation once per process (best effort)."""
    if chain in _ADAPTIVE_CHUNK_LOADED:
        return
    _ADAPTIVE_CHUNK_LOADED.add(chain)
    try:
        data = json.loads(_adaptive_state_path(chain).read_text())
        level = min(int(data.get("level", 0) or 0), ADAPTIVE_CHUNK_MAX_HALVES)
        if level > 0:
            _ADAPTIVE_CHUNK_LEVELS[chain] = level
        _ADAPTIVE_CHUNK_STREAKS[chain] = max(0, int(data.get("successes", 0) or 0))
    except (OSError, ValueError, TypeError, AttributeError):
        logger.debug("adaptive_chunk_state_unreadable", chain=chain)


def _save_adaptive_chunk(chain: str) -> None:
    """Persist the chain's adaptation; deleting the file once fully recovered."""
    try:
        path = _adaptive_state_path(chain)
        level = _ADAPTIVE_CHUNK_LEVELS.get(chain, 0)
        successes = _ADAPTIVE_CHUNK_STREAKS.get(chain, 0)
        if level <= 0 and successes <= 0:
            path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"level": level, "successes": successes}))
    except OSError:
        # Persistence is an optimisation — never break event fetching.
        logger.debug("adaptive_chunk_persist_failed", chain=chain)


def _adaptive_chunk_size(chain: str, base_chunk: int) -> int:
    """Return the effective chunk size for ``chain`` after repeated failures."""
    _load_adaptive_chunk(chain)
    halves: int = min(_ADAPTIVE_CHUNK_LEVELS.get(chain, 0), ADAPTIVE_CHUNK_MAX_HALVES)
    divisor: int = 2**halves
    return max(ADAPTIVE_CHUNK_FLOOR, base_chunk // divisor)


def _bump_adaptive_chunk(chain: str) -> None:
    """Record one pool-wide failure — next fetch uses a smaller chunk."""
    _load_adaptive_chunk(chain)
    _ADAPTIVE_CHUNK_LEVELS[chain] = min(_ADAPTIVE_CHUNK_LEVELS.get(chain, 0) + 1, ADAPTIVE_CHUNK_MAX_HALVES)
    _ADAPTIVE_CHUNK_STREAKS[chain] = 0
    _save_adaptive_chunk(chain)


def _record_adaptive_success(chain: str) -> None:
    """Record a successful batch; decay one level only after sustained success.

    The old "reset to the base size on every success" behaviour re-armed a
    50-block chunk after a single 5-block success, so a range-capped chain
    like base could never settle. Decaying one level at a time, and only
    after ``ADAPTIVE_RECOVERY_SUCCESSES`` clean batches, lets a genuinely
    healthy provider be re-admitted without reintroducing the oscillation.
    """
    _load_adaptive_chunk(chain)
    if _ADAPTIVE_CHUNK_LEVELS.get(chain, 0) <= 0:
        return
    streak = _ADAPTIVE_CHUNK_STREAKS.get(chain, 0) + 1
    if streak < ADAPTIVE_RECOVERY_SUCCESSES:
        _ADAPTIVE_CHUNK_STREAKS[chain] = streak
    else:
        _ADAPTIVE_CHUNK_LEVELS[chain] = max(0, _ADAPTIVE_CHUNK_LEVELS[chain] - 1)
        _ADAPTIVE_CHUNK_STREAKS[chain] = 0
    _save_adaptive_chunk(chain)


def _reset_adaptive_chunk(chain: str) -> None:
    """Reset the adaptive chunk size (tests / explicit recovery)."""
    _load_adaptive_chunk(chain)
    _ADAPTIVE_CHUNK_LEVELS.pop(chain, None)
    _ADAPTIVE_CHUNK_STREAKS.pop(chain, None)
    _save_adaptive_chunk(chain)


# Rate-limited-pool crawl cursor, keyed by chain. When every endpoint is
# soft-banned the pool is rate-limited, NOT down: each fetch starts at the
# next eligible endpoint (round-robin) so the whole pool is retried at a
# bounded pace rather than the same least-banned endpoint being pinned and
# re-banned every cycle (the base/celo deadlock, 2026-09-19).
_RATE_LIMITED_CURSOR: dict[str, int] = {}


def _rate_limited_rotation(chain: str, eligible: list[str]) -> list[str]:
    """Round-robin an all-soft-banned pool so the crawl spreads over peers."""
    if not eligible:
        return []
    idx = _RATE_LIMITED_CURSOR.get(chain, 0) % len(eligible)
    _RATE_LIMITED_CURSOR[chain] = idx + 1
    return eligible[idx:] + eligible[:idx]


def _prefer_wide_range(endpoints: list[str], chunk: int) -> list[str]:
    """Stable partition: endpoints that can serve ``chunk`` blocks first.

    Catch-up throughput: a pool mixing 10-range public RPCs with 100-range
    keyed endpoints (celo infura/drpc) must dial the wide endpoints first,
    otherwise every chunk is throttled down to the narrowest provider cap
    and catch-up never converges.
    """
    wide: list[str] = []
    narrow: list[str] = []
    for ep in endpoints:
        limit = get_max_block_range(ep)
        (wide if limit is None or limit >= chunk else narrow).append(ep)
    return wide + narrow


def _contiguous_ranges(blocks: set[int]) -> list[tuple[int, int]]:
    """Group a set of block numbers into minimal contiguous ranges.

    Used to build the smallest set of ``eth_getLogs`` ranges covering
    only blocks that passed the bloom pre-filter.
    """
    if not blocks:
        return []
    ordered = sorted(blocks)
    ranges: list[tuple[int, int]] = []
    start = prev = ordered[0]
    for block in ordered[1:]:
        if block == prev + 1:
            prev = block
            continue
        ranges.append((start, prev))
        start = prev = block
    ranges.append((start, prev))
    return ranges
