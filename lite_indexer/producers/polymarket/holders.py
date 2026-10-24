"""``pm_holders`` producer — top holders per live market, one snapshot per hour.

The key includes ``asof``, so an hour's snapshot is written once and a replay
inside the same hour replaces itself instead of appending. Markets with no
holder data answer ``null``; that is a normal empty result, not an error.
"""

from __future__ import annotations

import datetime as dt

from lite_indexer.logging import get_logger
from lite_indexer.producers.polymarket.http import DATA_API, ProviderError, fetch_json
from lite_indexer.producers.polymarket.rows import holder_rows

logger = get_logger(__name__)

PER_MARKET = 10
MAX_MARKETS = 20


def collect(http, condition_ids: list[str], asof: dt.datetime, limit: int = PER_MARKET) -> list[tuple]:
    """Holder snapshot rows; a failing market is skipped, the rest of the batch survives."""
    rows: list[tuple] = []
    for condition_id in condition_ids[:MAX_MARKETS]:
        try:
            payload = fetch_json(http, f"{DATA_API}/holders", {"market": condition_id, "limit": limit})
        except ProviderError as exc:
            logger.warning("polymarket_holders_skipped", market=condition_id, error=str(exc)[:200])
            continue
        rows.extend(holder_rows(payload, condition_id, asof))
    logger.info("polymarket_holders_collected", rows=len(rows), markets=len(condition_ids[:MAX_MARKETS]))
    return rows
