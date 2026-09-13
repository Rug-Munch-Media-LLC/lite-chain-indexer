"""
In-call ``eth_getLogs`` range splitting on provider range/size errors.

Static caps plus cross-cycle halving convert an oversized request into a
*failed cycle*: the logs for that window are skipped and retried later.
Ponder's edge is parsing the provider's error and splitting the range
IN-LINE, recursively, so the window still succeeds this cycle.

This module holds the pure machinery:

  * :func:`is_range_error_message` / :func:`classify_logs_error` — recognise
    range / size / result-limit rejections (and ONLY those; auth, 429 and
    timeouts must still travel the normal retry/throttle path).
  * :func:`split_bounds` / :func:`can_split` — midpoint split under the
    ``GETLOGS_SPLIT_MAX_DEPTH`` / ``GETLOGS_SPLIT_MIN_BLOCKS`` bounds.
  * :func:`merge_logs` — order-preserving merge with ``(block, logIndex)``
    dedupe in case a provider returns overlapping boundary logs.
  * :func:`fetch_logs_split` — async recursive driver over an injected
    single-range fetch callback (mockable in tests).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from lite_indexer.config import GETLOGS_SPLIT_MAX_DEPTH, GETLOGS_SPLIT_MIN_BLOCKS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Result of one single-range fetch:
#   ("ok", logs)     — logs is a list (possibly empty)
#   ("range", None)  — provider rejected the range/size/result limit
#   ("error", None)  — any other failure (auth, 429, timeout, malformed)
FetchResult = tuple[str, list[dict[str, Any]] | None]
FetchOnce = Callable[[int, int], Awaitable[FetchResult]]

# Case-insensitive substrings that mark a range / size / result-limit
# rejection. Deliberately broad (providers phrase these inconsistently)
# but never matches plain auth/rate-limit transports.
RANGE_ERROR_MARKERS: tuple[str, ...] = (
    "range",
    "limit",
    "too many",
    "exceed",
    "query returned",
    "-32602",
    "block range",
    "max",
    "response size",
)

# JSON-RPC error code for "Invalid params" (oversized filter args).
INVALID_PARAMS_CODE = -32602


def is_range_error_message(message: str) -> bool:
    """Whether ``message`` looks like a range/size/result-limit rejection."""
    if not message:
        return False
    lowered = message.lower()
    return any(marker in lowered for marker in RANGE_ERROR_MARKERS)


def classify_logs_error(data: Any) -> str:
    """Classify a JSON-RPC response body: ``"range"`` or ``"error"``.

    A missing/empty error is ``"error"`` (caller checks the result shape
    first); a range/size/result-limit message — or the invalid-params code
    ``-32602`` — is ``"range"``.
    """
    if not isinstance(data, dict):
        return "error"
    err = data.get("error")
    if not err:
        return "error"
    if isinstance(err, dict):
        code = err.get("code")
        message = str(err.get("message", err))
    else:
        code = None
        message = str(err)
    if code == INVALID_PARAMS_CODE:
        return "range"
    return "range" if is_range_error_message(message) else "error"


def split_bounds(from_block: int, to_block: int) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """Midpoint-split ``[from, to]`` into two adjacent half-open ranges.

    Returns ``None`` when the range cannot be split (a single block).
    """
    if to_block <= from_block:
        return None
    mid = (from_block + to_block) // 2
    return (from_block, mid), (mid + 1, to_block)


def can_split(from_block: int, to_block: int, depth: int) -> bool:
    """Whether the range may be split: under the depth cap and above floor."""
    if depth >= GETLOGS_SPLIT_MAX_DEPTH:
        return False
    return (to_block - from_block + 1) > GETLOGS_SPLIT_MIN_BLOCKS


def _log_sort_key(log: dict[str, Any]) -> tuple[int, int]:
    """Numeric sort key from hex ``blockNumber`` / ``logIndex`` (0 default)."""
    try:
        block = int(str(log.get("blockNumber", "0x0")), 16)
    except (TypeError, ValueError):
        block = 0
    try:
        index = int(str(log.get("logIndex", "0x0")), 16)
    except (TypeError, ValueError):
        index = 0
    return block, index


def merge_logs(chunks: list[list[dict[str, Any]] | None]) -> list[dict[str, Any]]:
    """Merge split-range results in order, deduping ``(block, logIndex)``.

    ``None`` chunks (a failed half) are skipped here; the caller fails
    closed before merging when any half failed.
    """
    merged: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for chunk in chunks:
        if not chunk:
            continue
        for log in chunk:
            key = _log_sort_key(log)
            if key in seen:
                continue
            seen.add(key)
            merged.append(log)
    merged.sort(key=_log_sort_key)
    return merged


async def fetch_logs_split(
    fetch_once: FetchOnce,
    from_block: int,
    to_block: int,
    *,
    depth: int = 0,
) -> list[dict[str, Any]] | None:
    """Fetch ``[from, to]``, splitting recursively on a range error.

    On a range/size/result-limit rejection the range is split at the
    midpoint and BOTH halves are fetched recursively within the same call;
    results are merged (order-preserving, deduped). Non-range failures
    (auth, 429, timeout, malformed) return ``None`` immediately so the
    normal retry/throttle path handles them. If either half ultimately
    fails, the whole call fails closed (``None``) — never partial data.

    Args:
        fetch_once: Async callback ``(from_block, to_block) -> (status, logs)``.
        from_block: Inclusive start block.
        to_block: Inclusive end block.
        depth: Current recursion depth (tests/telemetry).

    Returns:
        Merged log list (possibly ``[]``), or ``None`` on failure.
    """
    status, logs = await fetch_once(from_block, to_block)
    if status == "ok":
        return logs if logs is not None else []
    if status != "range":
        return None

    bounds = split_bounds(from_block, to_block)
    if bounds is None or not can_split(from_block, to_block, depth):
        # Range error we cannot split further — fail closed (no partials).
        logger.debug(
            "getlogs_range_unsplittable",
            from_block=from_block,
            to_block=to_block,
            depth=depth,
        )
        return None

    (left_from, left_to), (right_from, right_to) = bounds
    metrics_incr("getlogs_range_splits")
    logger.warning(
        "getlogs_range_split",
        from_block=from_block,
        to_block=to_block,
        depth=depth,
        left=(left_from, left_to),
        right=(right_from, right_to),
    )
    left = await fetch_logs_split(fetch_once, left_from, left_to, depth=depth + 1)
    right = await fetch_logs_split(fetch_once, right_from, right_to, depth=depth + 1)
    if left is None or right is None:
        return None
    return merge_logs([left, right])


__all__ = [
    "INVALID_PARAMS_CODE",
    "RANGE_ERROR_MARKERS",
    "can_split",
    "classify_logs_error",
    "fetch_logs_split",
    "is_range_error_message",
    "merge_logs",
    "split_bounds",
]
