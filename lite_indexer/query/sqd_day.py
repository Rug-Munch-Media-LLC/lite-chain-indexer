"""UTC day → block range resolution for the SQD portal.

WHY A BLOCK RANGE AT ALL
    The portal streams BLOCKS; the archive is keyed by DATE. Something has to
    translate one into the other, and it must be exact: a unit whose parquet
    holds rows from outside its date silently corrupts every query over the
    range, and ``write_unit`` cannot catch it (it only compares counts).

WHY BINARY SEARCH
    Block numbers are monotonic in time on every EVM chain the portal serves,
    so ``first_block_at_or_after`` is ``~log2(head)`` header fetches — ≈27 for
    polygon's 97.5M head. A linear scan would be 97M requests. The cost matters
    because a 90-day backfill pays it once per day boundary.

MEASURED 2026-11-18 (polygon-mainnet)
    The portal answers a single-block stream with its header for ANY block it
    holds, including blocks containing no events: blocks 1, 12345678 and
    97530000 all returned ``{"header": {"number": …, "timestamp": …}}``. That
    is what makes the search possible — a header endpoint does not exist
    (``/blocks``, ``/block/{n}``, ``/headers`` all 404), so the stream is the
    only probe.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator

from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

#: Seconds in one UTC day. A day unit is exactly this wide by definition.
DAY_SECONDS = 86_400

#: Header-only field set — the cheapest probe the portal offers.
HEADER_FIELDS: dict[str, object] = {"block": {"number": True, "timestamp": True}}


def day_bounds(day: dt.date) -> tuple[int, int]:
    """Epoch seconds for ``[00:00:00, 24:00:00)`` UTC of ``day``."""
    start = int(dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC).timestamp())
    return start, start + DAY_SECONDS


def iter_days(end: dt.date, days: int) -> Iterator[dt.date]:
    """Yield ``days`` UTC dates ending at ``end`` (inclusive), oldest first."""
    if days < 1:
        raise ValueError(f"days must be >= 1, got {days}")
    for offset in range(days - 1, -1, -1):
        yield end - dt.timedelta(days=offset)


def first_block_at_or_after(
    src: SQDSource, dataset: str, epoch: int, head: int, cache: dict[int, int | None] | None = None
) -> int | None:
    """Lowest block number whose timestamp is ``>= epoch``, or None past head.

    Args:
        src: Portal client (its throttled ``_request`` is the transport).
        dataset: Portal dataset slug, e.g. ``polygon-mainnet``.
        epoch: Unix seconds boundary.
        head: Highest block the dataset holds.
        cache: Optional epoch → result memo. Consecutive days share a boundary
            epoch exactly (day N's end is day N+1's start), so a 90-day run pays
            ~91 searches instead of ~180 without it.

    Returns:
        The boundary block number, or None when every held block predates
        ``epoch``.
    """
    if cache is not None and epoch in cache:
        return cache[epoch]
    lo, hi = 1, head
    boundary: int | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        status, text = src._request(
            "POST",
            f"{src.PORTAL}/datasets/{dataset}/stream",
            {"type": "evm", "fromBlock": mid, "toBlock": mid, "fields": HEADER_FIELDS},
            timeout=60.0,
        )
        if status == 204:
            # Above the dataset height: everything below is older, so move down.
            hi = mid - 1
            continue
        if status != 200:
            raise SQDPortalError(f"header probe HTTP {status} at block {mid}")
        stamp: int | None = None
        for line in text.splitlines():
            if line.strip():
                try:
                    stamp = int((json.loads(line).get("header") or {})["timestamp"])
                except (KeyError, TypeError, ValueError):
                    stamp = None
                break
        if stamp is None:
            raise SQDPortalError(f"no header for block {mid} in {dataset}")
        if stamp >= epoch:
            boundary = mid
            hi = mid - 1
        else:
            lo = mid + 1
    if cache is not None:
        cache[epoch] = boundary
    return boundary


def day_block_range(
    src: SQDSource, dataset: str, day: dt.date, head: int, cache: dict[int, int | None] | None = None
) -> tuple[int, int] | None:
    """Inclusive block range covering one UTC day, or None when it is empty."""
    start_ts, end_ts = day_bounds(day)
    first = first_block_at_or_after(src, dataset, start_ts, head, cache)
    if first is None:
        return None
    after = first_block_at_or_after(src, dataset, end_ts, head, cache)
    last = (after - 1) if after is not None else head
    if last < first:
        return None
    return first, last
