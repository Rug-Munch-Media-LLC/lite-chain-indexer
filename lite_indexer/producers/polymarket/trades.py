"""``pm_trades`` producer — newest-first ``/trades`` pages, cut at the stored cursor.

The endpoint has no "since" parameter, so the cursor is applied locally: page
backwards until a page's oldest trade is older than ``since``, then drop the
tail. Repeat rows are removed on the table's ORDER BY key before the insert, so
an overlapping replay costs no duplication.
"""

from __future__ import annotations

import datetime as dt

from lite_indexer.logging import get_logger
from lite_indexer.producers.polymarket.http import DATA_API, fetch_json
from lite_indexer.producers.polymarket.rows import trade_rows

logger = get_logger(__name__)

PAGE = 500
MAX_PAGES = 6


def collect(http, since: dt.datetime | None, max_pages: int = MAX_PAGES) -> list[tuple]:
    """Trades to write: every page newer than ``since`` (all pages when None)."""
    seen: dict[tuple, tuple] = {}
    for page in range(max_pages):
        payload = fetch_json(
            http,
            f"{DATA_API}/trades",
            {"limit": PAGE, "offset": page * PAGE, "takerOnly": "true"},
        )
        batch = trade_rows(payload)
        if not batch:
            break
        for row in batch:
            seen[(row[0], row[1], row[2], row[3], row[7])] = row
        oldest = min(row[1] for row in batch)
        if since is not None and oldest <= since:
            break
        if len(batch) < PAGE:
            break
    rows = list(seen.values())
    if since is not None:
        rows = [row for row in rows if row[1] >= since]
    logger.info("polymarket_trades_collected", rows=len(rows), since=str(since))
    return rows
