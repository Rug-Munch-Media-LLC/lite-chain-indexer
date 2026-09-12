"""Price refresh orchestration: token scan, multi-source aggregation, upserts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.config import (
    CH_DEX_TABLE,
    CH_TOKEN_PRICES_TABLE,
    COINGECKO_PLATFORMS,
    PRICING_ENABLED,
    PRICING_MAX_TRACKED_TOKENS,
    PRICING_TOKEN_WINDOW_DAYS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.pricing.cache import _PRICE_CACHE
from lite_indexer.pricing.fetchers import PriceFetcher

logger = get_logger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────

_TOKEN_PRICES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_TOKEN_PRICES_TABLE} (
    chain LowCardinality(String),
    token_address String,
    symbol String DEFAULT '',
    price_usd Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, token_address)
"""


async def ensure_token_prices_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.token_prices`` if it does not exist.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(_TOKEN_PRICES_DDL)  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
        logger.info("token_prices_table_ensured", table=CH_TOKEN_PRICES_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("token_prices_table_skip", error=str(exc)[:80])
        else:
            logger.warning("token_prices_table_failed", error=str(exc)[:100])
        return False


def _collect_tracked_tokens(ch_client: Any, chains: list[str] | None = None) -> list[tuple[str, str]]:
    """Collect (chain, token) pairs recently seen in dex_events.

    Scans the ``token0`` / ``token1`` columns over the recent
    ``PRICING_TOKEN_WINDOW_DAYS`` window, capped at
    ``PRICING_MAX_TRACKED_TOKENS`` distinct tokens.

    Args:
        ch_client: ClickHouse client.
        chains: Optional chain filter.

    Returns:
        List of (chain, token_address) tuples (empty on failure).
    """
    try:
        chain_filter = " AND chain IN %(chains)s" if chains else ""
        params: dict[str, Any] = {
            "days": PRICING_TOKEN_WINDOW_DAYS,
            "limit": PRICING_MAX_TRACKED_TOKENS,
            "chains": chains or [],
        }
        seen: dict[tuple[str, str], None] = {}
        for column in ("token0", "token1"):
            query = (
                f"SELECT chain, {column} FROM {CH_DEX_TABLE} "
                f"WHERE {column} != '' AND block_timestamp >= now() - INTERVAL %(days)s DAY"
                f"{chain_filter} LIMIT %(limit)s"
            )
            for row in ch_client.execute(query, params) or []:
                if len(row) >= 2 and row[1]:
                    seen[(str(row[0]), str(row[1]))] = None
        return list(seen.keys())[:PRICING_MAX_TRACKED_TOKENS]
    except Exception as exc:  # noqa: BLE001 — scan is best-effort
        logger.debug("tracked_tokens_scan_failed", error=str(exc)[:80])
        return []


async def _upsert_prices(ch_client: Any, prices: dict[tuple[str, str], tuple[float, str]]) -> int:
    """Upsert (chain, token) -> (price, symbol) rows into token_prices."""
    rows: list[tuple[Any, ...]] = []
    now = datetime.now(UTC)
    for (chain, addr), (price, symbol) in prices.items():
        rows.append((chain, addr, symbol, price, now))
    if not rows:
        return 0
    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_TOKEN_PRICES_TABLE} (chain, token_address, symbol, price_usd, updated_at) VALUES",
            rows,
        )
        metrics_incr("pricing_upserts")
        return len(rows)
    except Exception as exc:  # noqa: BLE001 — upsert is best-effort
        logger.warning("token_prices_upsert_failed", error=str(exc)[:100])
        return 0


async def refresh_prices(
    ch_client: Any,
    chains: list[str] | None = None,
    tokens: list[tuple[str, str]] | None = None,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Refresh USD prices for tracked tokens and update the cache.

    Primary source is DefiLlama (batched ``chain:address`` keys); CoinGecko
    fills in the tokens DefiLlama misses on supported platforms. Both are
    free-tier, no auth. Results are upserted into ``token_prices`` and the
    in-memory cache. Every failure degrades gracefully — the previous
    cached prices remain usable and no exception propagates.

    Args:
        ch_client: ClickHouse client.
        chains: Optional chain filter for the tracked-token scan.
        tokens: Explicit (chain, address) list (skips the scan).
        http_client: Optional injected HTTP client (testing).

    Returns:
        Stats dict: status, tokens_known, fetched, upserted, failed,
        sources.
    """
    stats: dict[str, Any] = {
        "status": "ok",
        "tokens_known": 0,
        "fetched": 0,
        "upserted": 0,
        "failed": 0,
        "sources": {"defillama": 0, "coingecko": 0},
    }
    if not PRICING_ENABLED:
        stats["status"] = "disabled"
        return stats

    if tokens is None:
        tokens = _collect_tracked_tokens(ch_client, chains=chains)
    if not tokens:
        stats["status"] = "no_tokens"
        return stats
    stats["tokens_known"] = len(tokens)

    if not await ensure_token_prices_table(ch_client):
        stats["status"] = "table_unavailable"
        return stats

    fetcher = PriceFetcher(client=http_client)
    try:
        by_chain: dict[str, list[tuple[str, str]]] = {}
        for chain, addr in tokens:
            by_chain.setdefault(chain, []).append((chain, addr))

        # (chain, token) -> (price, symbol)
        prices: dict[tuple[str, str], tuple[float, str]] = {}

        for chain, pairs in by_chain.items():
            coins = [f"{chain}:{addr}" for _, addr in pairs]
            dl = await fetcher.fetch_defillama(coins)
            for key, info in dl.items():
                _chain, _addr = key.split(":", 1)
                # DefiLlama lowercases EVM keys; keep Solana/Tron as-is.
                addr_norm = _addr.lower() if _chain in COINGECKO_PLATFORMS and _chain != "tron" else _addr
                prices[(_chain, addr_norm)] = (info["price"], info["symbol"])
                stats["sources"]["defillama"] += 1

            # CoinGecko fallback for tokens DefiLlama did not price.
            if COINGECKO_PLATFORMS.get(chain):
                missing = [addr for _, addr in pairs if (chain, addr) not in prices]
                if missing:
                    cg = await fetcher.fetch_coingecko(chain, missing)
                    for addr, usd in cg.items():
                        prices[(chain, addr)] = (usd, "")
                        stats["sources"]["coingecko"] += 1

        stats["fetched"] = len(prices)
        stats["failed"] = max(0, stats["tokens_known"] - len(prices))

        upserted = await _upsert_prices(ch_client, prices)
        stats["upserted"] = upserted

        now = datetime.now(UTC)
        for (chain, addr), (price, _symbol) in prices.items():
            _PRICE_CACHE[(chain, addr)] = (price, now)

        stats["status"] = "partial" if stats["failed"] else "ok"
        metrics_incr("pricing_refreshes")
        return stats
    finally:
        await fetcher.aclose()
