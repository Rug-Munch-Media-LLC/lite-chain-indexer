"""Pure row builders: upstream JSON -> tuple in the exact ClickHouse column order.

No I/O, no clock: the caller passes ``asof``. Every builder dedupes on the
target table's ORDER BY key (the tables are ReplacingMergeTree), so a replayed
page can never inflate ``count()`` while the merge catches up.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

TRADE_COLUMNS = (
    "condition_id",
    "ts",
    "proxy_wallet",
    "asset",
    "side",
    "outcome",
    "outcome_index",
    "size",
    "price",
    "notional",
    "title",
    "slug",
    "tx_hash",
)
MARKET_COLUMNS = (
    "market_id",
    "condition_id",
    "question",
    "slug",
    "end_date",
    "liquidity",
    "volume",
    "closed",
    "outcomes",
    "outcome_prices",
    "asof",
)
HOLDER_COLUMNS = (
    "asof",
    "condition_id",
    "token",
    "proxy_wallet",
    "amount",
    "outcome_index",
    "name",
    "pseudonym",
)


def _f(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _s(value: Any) -> str:
    return "" if value is None else str(value)


def _json(value: Any) -> str:
    """Gamma returns list fields already JSON-encoded; keep them verbatim."""
    if value is None:
        return ""
    return value if isinstance(value, str) else json.dumps(value)


def _parse_dt(value: Any) -> dt.datetime | None:
    """ISO-8601 -> naive UTC (clickhouse_driver rejects tz-aware DateTime)."""
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed


def trade_rows(payload: Any) -> list[tuple]:
    """pm_trades rows, keyed by (condition_id, ts, proxy_wallet, asset, size)."""
    seen: dict[tuple, tuple] = {}
    for trade in payload or []:
        try:
            ts = dt.datetime.fromtimestamp(int(trade["timestamp"]), dt.UTC).replace(tzinfo=None)
        except (KeyError, TypeError, ValueError):
            continue
        size, price = _f(trade.get("size")), _f(trade.get("price"))
        row = (
            _s(trade.get("conditionId")),
            ts,
            _s(trade.get("proxyWallet")),
            _s(trade.get("asset")),
            _s(trade.get("side")),
            _s(trade.get("outcome")),
            int(trade.get("outcomeIndex") or 0),
            size,
            price,
            round(size * price, 8),
            _s(trade.get("title")),
            _s(trade.get("slug")),
            _s(trade.get("transactionHash")),
        )
        seen[(row[0], row[1], row[2], row[3], row[7])] = row
    return list(seen.values())


def market_rows(payload: Any, asof: dt.datetime) -> list[tuple]:
    """pm_markets rows, keyed by (condition_id, market_id) — later asof wins."""
    seen: dict[tuple, tuple] = {}
    for market in payload or []:
        condition_id = _s(market.get("conditionId"))
        if not condition_id:
            continue
        row = (
            _s(market.get("id")),
            condition_id,
            _s(market.get("question")),
            _s(market.get("slug")),
            _parse_dt(market.get("endDate")),
            _f(market.get("liquidity")),
            _f(market.get("volume")),
            bool(market.get("closed")),
            _json(market.get("outcomes")),
            _json(market.get("outcomePrices")),
            asof,
        )
        seen[(condition_id, row[0])] = row
    return list(seen.values())


def holder_rows(payload: Any, condition_id: str, asof: dt.datetime) -> list[tuple]:
    """pm_holders rows, keyed by (condition_id, token, proxy_wallet, asof)."""
    seen: dict[tuple, tuple] = {}
    for block in payload or []:
        token = _s(block.get("token"))
        for holder in block.get("holders") or []:
            row = (
                asof,
                condition_id,
                token,
                _s(holder.get("proxyWallet")),
                _f(holder.get("amount")),
                int(holder.get("outcomeIndex") or 0),
                _s(holder.get("name")),
                _s(holder.get("pseudonym")),
            )
            seen[(condition_id, token, row[3], asof)] = row
    return list(seen.values())
