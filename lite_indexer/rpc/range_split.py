"""
Provider-error-driven in-call range splitting (method-agnostic).

Static caps plus cross-cycle halving convert an oversized request into a
*failed cycle*: the records for that window are skipped and retried later.
Ponder's edge is parsing the provider's error and splitting the range
IN-LINE, recursively, so the window still succeeds this cycle.

Originally ``eth_getLogs``-only; the machinery is generic and now serves any
range-parameterized method (``eth_getLogs``, ``trace_filter``, …). Methods
that take a SINGLE block (``eth_getBlockByNumber``, ``eth_getCode``,
``eth_getBlockReceipts``) are NOT range-parameterized and are deliberately
not routed through here.

This module holds the pure machinery:

  * :func:`is_range_error_message` / :func:`classify_logs_error` — recognise
    range / size / result-limit rejections (and ONLY those; auth, 429 and
    timeouts must still travel the normal retry/throttle path).
  * :func:`split_bounds` / :func:`can_split` — midpoint split under the
    ``GETLOGS_SPLIT_MAX_DEPTH`` / ``GETLOGS_SPLIT_MIN_BLOCKS`` bounds.
  * :func:`merge_logs` (log dedupe) / :func:`merge_records` (generic concat).
  * :func:`fetch_range_split` — async recursive driver over an injected
    single-range fetch callback, with an optional BATCH callback so sibling
    sub-ranges are fetched in ONE HTTP request.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Literal

from lite_indexer.config import GETLOGS_SPLIT_MAX_DEPTH, GETLOGS_SPLIT_MIN_BLOCKS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Result of one single-range fetch:
#   ("ok", records)  — records is a list (possibly empty)
#   ("range", None)  — provider rejected the range/size/result limit
#   ("error", None)  — any other failure (auth, 429, timeout, malformed)
FetchResult = tuple[str, list[Any] | None]
FetchOnce = Callable[[int, int], Awaitable[FetchResult]]
FetchMany = Callable[[list[tuple[int, int]]], Awaitable[list[FetchResult]]]
MergeKind = Literal["logs", "records"]

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


# Generic alias (the classifier is not logs-specific).
classify_range_error = classify_logs_error


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


def merge_logs(chunks: list[list[Any] | None]) -> list[Any]:
    """Merge split-range LOG results in order, deduping ``(block, logIndex)``.

    ``None`` chunks (a failed half) are skipped here; the caller fails
    closed before merging when any half failed.
    """
    merged: list[Any] = []
    seen: set[tuple[int, int]] = set()
    for chunk in chunks:
        if not chunk:
            continue
        for log in chunk:
            if not isinstance(log, dict):
                merged.append(log)
                continue
            key = _log_sort_key(log)
            if key in seen:
                continue
            seen.add(key)
            merged.append(log)
    merged.sort(key=lambda item: _log_sort_key(item) if isinstance(item, dict) else (0, 0))
    return merged


def merge_records(chunks: list[list[Any] | None]) -> list[Any]:
    """Merge split-range generic records in order (concatenate, no dedupe)."""
    merged: list[Any] = []
    for chunk in chunks:
        if chunk:
            merged.extend(chunk)
    return merged


def _merge(kind: MergeKind, chunks: list[list[Any] | None]) -> list[Any]:
    return merge_logs(chunks) if kind == "logs" else merge_records(chunks)


async def fetch_range_split(
    fetch_once: FetchOnce,
    from_block: int,
    to_block: int,
    *,
    method: str = "eth_getLogs",
    fetch_many: FetchMany | None = None,
    depth: int = 0,
    merge: MergeKind = "records",
) -> list[Any] | None:
    """Fetch ``[from, to]``, splitting recursively on a range error.

    On a range/size/result-limit rejection the range is split at the
    midpoint and BOTH halves are fetched (in one batched request when
    ``fetch_many`` is supplied, else sequentially) within the same call.
    Non-range failures (auth, 429, timeout, malformed) return ``None`` so
    the normal retry/throttle path handles them. If either half ultimately
    fails, the whole call fails closed (``None``) — never partial data.

    Args:
        fetch_once: Async callback ``(from, to) -> (status, records)``.
        from_block: Inclusive start block.
        to_block: Inclusive end block.
        method: Method name (telemetry only).
        fetch_many: Optional async batch callback that fetches several
            ranges in ONE request; returns one ``(status, records)`` per
            range in order.
        depth: Current recursion depth (tests/telemetry).
        merge: ``"logs"`` (dedupe by block/logIndex, sort) or ``"records"``
            (order-preserving concatenate).

    Returns:
        Merged record list (possibly ``[]``), or ``None`` on failure.
    """
    status, records = await fetch_once(from_block, to_block)
    if status == "ok":
        return records if records is not None else []
    if status != "range":
        return None
    return await _split_range(fetch_once, fetch_many, from_block, to_block, method, depth, merge)


async def _resolve_child(
    child: FetchResult,
    fetch_once: FetchOnce,
    fetch_many: FetchMany | None,
    from_block: int,
    to_block: int,
    method: str,
    depth: int,
    merge: MergeKind,
) -> list[Any] | None:
    """Resolve an already-fetched child (ok / range → split / error → None)."""
    status, records = child
    if status == "ok":
        return records if records is not None else []
    if status != "range":
        return None
    return await _split_range(fetch_once, fetch_many, from_block, to_block, method, depth, merge)


async def _split_range(
    fetch_once: FetchOnce,
    fetch_many: FetchMany | None,
    from_block: int,
    to_block: int,
    method: str,
    depth: int,
    merge: MergeKind,
) -> list[Any] | None:
    """Split a known-bad range and resolve both halves."""
    bounds = split_bounds(from_block, to_block)
    if bounds is None or not can_split(from_block, to_block, depth):
        # Range error we cannot split further — fail closed (no partials).
        logger.debug(
            "range_unsplittable",
            method=method,
            from_block=from_block,
            to_block=to_block,
            depth=depth,
        )
        return None

    (left_from, left_to), (right_from, right_to) = bounds
    metrics_incr("getlogs_range_splits")
    logger.warning(
        "range_split",
        method=method,
        from_block=from_block,
        to_block=to_block,
        depth=depth,
        left=(left_from, left_to),
        right=(right_from, right_to),
    )

    if fetch_many is not None:
        children = await fetch_many([(left_from, left_to), (right_from, right_to)])
    else:
        children = [
            await fetch_once(left_from, left_to),
            await fetch_once(right_from, right_to),
        ]
    left_child = children[0] if len(children) > 0 else ("error", None)
    right_child = children[1] if len(children) > 1 else ("error", None)

    left = await _resolve_child(left_child, fetch_once, fetch_many, left_from, left_to, method, depth + 1, merge)
    right = await _resolve_child(right_child, fetch_once, fetch_many, right_from, right_to, method, depth + 1, merge)
    if left is None or right is None:
        return None
    return _merge(merge, [left, right])


async def fetch_logs_split(
    fetch_once: FetchOnce,
    from_block: int,
    to_block: int,
    *,
    fetch_many: FetchMany | None = None,
    depth: int = 0,
) -> list[dict[str, Any]] | None:
    """Back-compat wrapper: :func:`fetch_range_split` for eth_getLogs."""
    result = await fetch_range_split(
        fetch_once,
        from_block,
        to_block,
        method="eth_getLogs",
        fetch_many=fetch_many,
        depth=depth,
        merge="logs",
    )
    if result is None:
        return None
    return [record for record in result if isinstance(record, dict)]


__all__ = [
    "INVALID_PARAMS_CODE",
    "RANGE_ERROR_MARKERS",
    "FetchMany",
    "FetchOnce",
    "FetchResult",
    "can_split",
    "classify_logs_error",
    "classify_range_error",
    "fetch_logs_split",
    "fetch_range_split",
    "is_range_error_message",
    "merge_logs",
    "merge_records",
    "split_bounds",
]
