"""``pm_markets`` producer — live markets by 24h volume, upserted per ``asof``.

The table is ReplacingMergeTree(asof) keyed on (condition_id, market_id), so
each run writes a fresh image of the same market and the newest ``asof`` wins:
no duplicate market can accumulate, and a replayed run is a no-op.
"""

from __future__ import annotations

import datetime as dt

from lite_indexer.logging import get_logger
from lite_indexer.producers.polymarket.http import GAMMA_API, fetch_json
from lite_indexer.producers.polymarket.rows import market_rows
from lite_indexer.producers.polymarket.store import DB, scalars

logger = get_logger(__name__)

PAGE = 100
MAX_PAGES = 3


def collect(http, asof: dt.datetime, max_pages: int = MAX_PAGES) -> list[tuple]:
    """Market images for this cycle, newest volume ranking first."""
    seen: dict[tuple, tuple] = {}
    for page in range(max_pages):
        payload = fetch_json(
            http,
            f"{GAMMA_API}/markets",
            {
                "limit": PAGE,
                "offset": page * PAGE,
                "active": "true",
                "closed": "false",
                "order": "volume24hr",
                "ascending": "false",
            },
        )
        batch = market_rows(payload, asof)
        for row in batch:
            seen[(row[1], row[0])] = row
        if len(payload or []) < PAGE:
            break
    rows = list(seen.values())
    logger.info("polymarket_markets_collected", rows=len(rows), asof=str(asof))
    return rows


def top_condition_ids(writer, limit: int = 20) -> list[str]:
    """Live markets we already know about, richest first — holders fan-out target."""
    rows = scalars(
        writer,
        f"SELECT condition_id FROM {DB}.pm_markets WHERE closed = 0 ORDER BY volume DESC LIMIT {int(limit)}",
    )
    return [row[0] for row in rows if row and row[0]]
