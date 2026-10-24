"""Archive specs for the Polymarket tables."""

from __future__ import annotations

from lite_indexer.storage.archive_specs._spec import CURSOR, SNAPSHOT, TableSpec

POLYMARKET_SPECS: dict[str, TableSpec] = {
    "pm_trades": TableSpec(
        "pm_trades",
        "pm_trades",
        "ingested_at",
        CURSOR,
        None,
        "ts",
        0,
        (
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
            "ingested_at",
        ),
        dataset="polymarket",
    ),
    "pm_markets": TableSpec(
        "pm_markets",
        "pm_markets",
        "",
        SNAPSHOT,
        None,
        "",
        0,
        (
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
        ),
        dataset="polymarket",
    ),
    "pm_holders": TableSpec(
        "pm_holders",
        "pm_holders",
        "",
        SNAPSHOT,
        None,
        "",
        0,
        ("asof", "condition_id", "token", "proxy_wallet", "amount", "outcome_index", "name", "pseudonym"),
        dataset="polymarket",
    ),
}
