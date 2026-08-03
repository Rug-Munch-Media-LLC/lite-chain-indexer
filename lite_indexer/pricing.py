"""
USD price feed + whale detection for DEX events.

Two related features live here:

1. **Whale detection** — a DEX swap is a *whale* when the USD notional of
   either leg is >= ``WHALE_THRESHOLD_USD`` (requires a price from the
   ``token_prices`` feed), OR the raw token amount of either leg exceeds
   the per-chain ``WHALE_AMOUNT_THRESHOLDS`` heuristic (used when no price
   is available yet). ``detect_whale`` is called at ingestion time by
   :meth:`ClickHouseWriter.write_dex_events` to populate the ``whale``
   column (previously hard-coded to 0).

2. **USD price feed** — fetches prices for tokens recently seen in
   ``dex_events`` from the free tiers of DefiLlama (primary, no auth,
   batched) and CoinGecko (fallback, no auth), caches them in memory and
   mirrors them to ``wallet_memory.token_prices`` (ReplacingMergeTree).
   The indexer refreshes on startup and every ``PRICING_REFRESH_INTERVAL``
   seconds. ``price_usd`` / ``volume_usd`` on DEX rows and the USD-based
   whale check are computed from this cache.

Resilience: every external call is best-effort. A failed provider degrades
to the previous cached price; ingestion never blocks on the price feed.

Decimals: DEX rows store raw base-unit amounts, so USD conversion divides
by ``10^decimals``. The feed does not provide decimals, so a small
``KNOWN_DECIMALS`` table (stablecoins) plus a per-chain default (18 EVM /
9 Solana / 6 Tron) is used — see ``DEFAULT_DECIMALS_BY_CHAIN``.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from lite_indexer.config import (
    CH_DEX_TABLE,
    CH_TOKEN_PRICES_TABLE,
    COINGECKO_BASE_URL,
    COINGECKO_MIN_INTERVAL,
    COINGECKO_PLATFORMS,
    DEFAULT_DECIMALS_BY_CHAIN,
    DEFILLAMA_BASE_URL,
    DEFILLAMA_MIN_INTERVAL,
    PRICING_BATCH_SIZE,
    PRICING_ENABLED,
    PRICING_MAX_TRACKED_TOKENS,
    PRICING_REFRESH_INTERVAL,
    PRICING_TOKEN_WINDOW_DAYS,
    WHALE_AMOUNT_THRESHOLDS,
    WHALE_THRESHOLD_USD,
    WHALE_TOP_QUANTILE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Amounts above this are treated as bogus (0). The old V3 parser stored
# signed int256 values as unsigned, so negative amounts decoded to ~2^256 —
# guarding against those rows (and any other corruption) keeps a single
# malformed amount from flagging every swap as a whale.
_MAX_SANE_AMOUNT = 2**128

# Known token decimals for well-known stablecoins (keyed by lowercased EVM
# address / native Solana mint / Tron base58 address). Everything else uses
# the per-chain default from DEFAULT_DECIMALS_BY_CHAIN.
KNOWN_DECIMALS: dict[str, int] = {
    # USDC
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,  # ethereum
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,  # base
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": 6,  # arbitrum
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": 6,  # polygon
    "0x0b2c639c533813f4aa9d7837caf62653d097ff85": 6,  # optimism
    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": 6,  # avalanche
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 18,  # bsc
    "epjfwdd5aufqsseqm2qn1xzybapc8g4weggkzwytdt1v": 6,  # solana
    # USDT
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,  # ethereum
    "0x55d398326f99059ff775485246999027b3197955": 18,  # bsc
    "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": 6,  # polygon
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": 6,  # arbitrum
    "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": 6,  # optimism
    "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": 6,  # avalanche
    "tr7nhqjekqxgtci8q8zy4pl8otszgjlj6t": 6,  # tron
    # DAI
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # ethereum
    "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": 18,  # polygon
    # BUSD
    "0xe9e7cea3dedca5984780bafc599bd69add087d56": 18,  # bsc
}

# In-memory price cache: (chain, token_address) -> (price_usd, updated_at).
# Populated by refresh_prices; read synchronously by the DEX write path.
_PRICE_CACHE: dict[tuple[str, str], tuple[float, datetime]] = {}

# Per-provider last-request monotonic timestamp (free-tier pacing).
_LAST_REQUEST: dict[str, float] = {}


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
        ch_client.execute(_TOKEN_PRICES_DDL)
        logger.info("token_prices_table_ensured", table=CH_TOKEN_PRICES_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("token_prices_table_skip", error=str(exc)[:80])
        else:
            logger.warning("token_prices_table_failed", error=str(exc)[:100])
        return False


# ── Price lookup helpers (used by the DEX write path) ─────────────────


def get_price(chain: str, token: str) -> float | None:
    """Return the cached USD price for a token, or None.

    Prices older than two refresh intervals are treated as stale and
    returned as None so the caller falls back to raw-amount heuristics.

    Args:
        chain: Chain name (must match the token_prices row key).
        token: Token address (lowercased EVM address / Solana mint / Tron).

    Returns:
        Price in USD, or None when unknown or stale.
    """
    if not token:
        return None
    entry = _PRICE_CACHE.get((chain, token))
    if entry is None:
        return None
    price, updated = entry
    if datetime.now(UTC) - updated > timedelta(seconds=PRICING_REFRESH_INTERVAL * 2 + 60):
        return None
    return price


def get_decimals(chain: str, token: str) -> int:
    """Return the decimals used to convert raw amounts to token units.

    Prefers the known-stablecoin table, then the per-chain default.

    Args:
        chain: Chain name.
        token: Token address.

    Returns:
        Decimals (6/9/18 typically).
    """
    if token in KNOWN_DECIMALS:
        return KNOWN_DECIMALS[token]
    return DEFAULT_DECIMALS_BY_CHAIN.get(chain, 18)


def _sane_amount(amount_raw: Any) -> int:
    """Parse a raw token amount, returning 0 for invalid or bogus values."""
    try:
        amount = int(amount_raw)
    except (ValueError, TypeError):
        return 0
    if amount <= 0 or amount >= _MAX_SANE_AMOUNT:
        return 0
    return amount


def compute_swap_usd(event: dict[str, Any]) -> float:
    """Estimate the USD notional of a swap event (max over both legs).

    Uses the cached price feed. Returns 0.0 when no price is available for
    either leg.

    Args:
        event: Parsed DEX swap event dict (v2 or v3 shape).

    Returns:
        USD notional rounded to 2 decimals (0.0 when unpriced).
    """
    chain = str(event.get("chain", ""))
    max_usd = 0.0
    legs = (
        (event.get("token0", ""), event.get("amount0In", "0") or "0"),
        (event.get("token1", ""), event.get("amount1In", "0") or "0"),
    )
    for token, amount_raw in legs:
        price = get_price(chain, str(token))
        if not price or price <= 0:
            continue
        amount = _sane_amount(amount_raw)
        if amount <= 0:
            continue
        usd = (amount / (10 ** get_decimals(chain, str(token)))) * price
        if usd > max_usd:
            max_usd = usd
    return round(max_usd, 2)


def detect_whale(
    event: dict[str, Any],
    *,
    usd_threshold: float | None = None,
    amount_thresholds: dict[str, int] | None = None,
) -> int:
    """Return 1 when a DEX swap is a whale, 0 otherwise.

    A swap is a whale when either leg satisfies:

    1. **USD notional** — ``amount / 10^decimals * price >=
       usd_threshold`` (Fix 3; needs a cached price), OR
    2. **Raw amount heuristic** — either raw amount >= the per-chain
       ``WHALE_AMOUNT_THRESHOLDS`` fallback (Fix 1 interim; used when no
       price is available).

    Args:
        event: Parsed DEX swap event dict.
        usd_threshold: Override for ``WHALE_THRESHOLD_USD`` (testing).
        amount_thresholds: Override for ``WHALE_AMOUNT_THRESHOLDS``.

    Returns:
        1 (whale) or 0.
    """
    threshold = WHALE_THRESHOLD_USD if usd_threshold is None else usd_threshold
    thresholds = WHALE_AMOUNT_THRESHOLDS if amount_thresholds is None else amount_thresholds
    chain = str(event.get("chain", ""))

    # USD-based detection (Fix 3): amount * price >= WHALE_THRESHOLD_USD.
    legs = (
        (event.get("token0", ""), event.get("amount0In", "0") or "0"),
        (event.get("token1", ""), event.get("amount1In", "0") or "0"),
    )
    for token, amount_raw in legs:
        price = get_price(chain, str(token))
        if price and price > 0:
            amount = _sane_amount(amount_raw)
            if amount > 0:
                usd = (amount / (10 ** get_decimals(chain, str(token)))) * price
                if usd >= threshold:
                    return 1

    # Raw-amount heuristic (Fix 1): either amount exceeds the per-chain
    # threshold. Uses the same legs as the USD check.
    thr = thresholds.get(chain, 0)
    if thr:
        amt0 = _sane_amount(event.get("amount0In", "0") or "0")
        amt1 = _sane_amount(event.get("amount1In", "0") or "0")
        if amt0 >= thr or amt1 >= thr:
            return 1
    return 0


# ── Price fetch (CoinGecko / DefiLlama, free tiers) ───────────────────


class PriceFetcher:
    """Fetches USD prices from DefiLlama + CoinGecko free tiers.

    Both endpoints require no API key. Requests are paced per provider to
    stay inside the free-tier rate limits; failures return empty results
    (callers fall back to the other provider or the previous cache).
    """

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def aclose(self) -> None:
        """Close the HTTP client (only if we created it)."""
        if self._owns_client:
            await self._client.aclose()

    async def _pace(self, provider: str, min_interval: float) -> None:
        """Sleep so consecutive requests to a provider stay >= min_interval."""
        now = time.monotonic()
        wait = min_interval - (now - _LAST_REQUEST.get(provider, 0.0))
        if wait > 0:
            await asyncio.sleep(wait)
        _LAST_REQUEST[provider] = time.monotonic()

    async def fetch_defillama(self, coins: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch prices from DefiLlama for ``chain:address`` keys.

        Args:
            coins: List of ``{chain}:{address}`` coin keys.

        Returns:
            Dict mapping coin key -> {"price": float, "symbol": str,
            "confidence": float | None} for priced coins.
        """
        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(coins), PRICING_BATCH_SIZE):
            batch = coins[i : i + PRICING_BATCH_SIZE]
            url = f"{DEFILLAMA_BASE_URL}/prices/current/{','.join(batch)}"
            await self._pace("defillama", DEFILLAMA_MIN_INTERVAL)
            try:
                resp = await self._client.get(url)
                if resp.status_code == 429:
                    await asyncio.sleep(5)
                    continue
                resp.raise_for_status()
                data = resp.json()
                for key, info in (data.get("coins") or {}).items():
                    price = float(info.get("price") or 0.0)
                    if price > 0:
                        out[key] = {
                            "price": price,
                            "symbol": str(info.get("symbol") or ""),
                            "confidence": info.get("confidence"),
                        }
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                logger.warning("defillama_fetch_failed", error=str(exc)[:80])
        return out

    async def fetch_coingecko(self, chain: str, addresses: list[str]) -> dict[str, float]:
        """Fetch prices from CoinGecko ``simple/token_price``.

        Only works for chains in ``COINGECKO_PLATFORMS`` (EVM + Tron).

        Args:
            chain: Chain name.
            addresses: Contract addresses to price.

        Returns:
            Dict mapping lowercased address -> USD price.
        """
        platform = COINGECKO_PLATFORMS.get(chain)
        if not platform or not addresses:
            return {}
        out: dict[str, float] = {}
        for i in range(0, len(addresses), PRICING_BATCH_SIZE):
            batch = addresses[i : i + PRICING_BATCH_SIZE]
            url = (
                f"{COINGECKO_BASE_URL}/simple/token_price/{platform}"
                f"?contract_addresses={','.join(batch)}&vs_currencies=usd"
            )
            await self._pace("coingecko", COINGECKO_MIN_INTERVAL)
            try:
                resp = await self._client.get(url)
                if resp.status_code == 429:
                    await asyncio.sleep(10)
                    continue
                resp.raise_for_status()
                data = resp.json()
                for addr, prices in data.items():
                    usd = float((prices or {}).get("usd") or 0.0)
                    if usd > 0:
                        out[str(addr).lower()] = usd
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                logger.warning("coingecko_fetch_failed", chain=chain, error=str(exc)[:80])
        return out


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
        ch_client.execute(
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


# ── Backfill ──────────────────────────────────────────────────────────

_MUTATION_BATCH = 20_000


def backfill_whales(
    ch_client: Any,
    *,
    days: int = 90,
    chain: str | None = None,
    limit: int = 500_000,
    dry_run: bool = False,
    usd_threshold: float | None = None,
    amount_thresholds: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Re-scan recent dex_events and mark whale rows retroactively.

    Recomputes the whale flag for rows in the window using:
      1. USD notional from the token_prices table (Fix 3),
      2. The per-chain raw-amount heuristic (Fix 1),
      3. Top ``WHALE_TOP_QUANTILE`` (99.9%) per-pair raw amount over the
         window — the "top 0.1% for that pair" definition.

    Only rows whose flag changes are mutated (``ALTER TABLE ... UPDATE``),
    in batches, so re-runs are idempotent and cheap.

    Args:
        ch_client: ClickHouse client.
        days: How far back to scan (block_timestamp window).
        chain: Restrict to one chain.
        limit: Max rows scanned (bounds memory).
        dry_run: Preview counts without issuing mutations.
        usd_threshold: Override for ``WHALE_THRESHOLD_USD`` (testing).
        amount_thresholds: Override for ``WHALE_AMOUNT_THRESHOLDS``.

    Returns:
        Stats dict: scanned, whale_up (0->1), whale_down (1->0),
        unchanged, dry_run, errors.
    """
    threshold = WHALE_THRESHOLD_USD if usd_threshold is None else usd_threshold
    thresholds = WHALE_AMOUNT_THRESHOLDS if amount_thresholds is None else amount_thresholds
    stats: dict[str, Any] = {
        "scanned": 0,
        "whale_up": 0,
        "whale_down": 0,
        "unchanged": 0,
        "dry_run": dry_run,
        "errors": [],
    }

    chain_clause = " AND chain = %(chain)s" if chain else ""
    where = f"block_timestamp >= now() - INTERVAL %(days)s DAY{chain_clause}"
    params: dict[str, Any] = {"days": days, "chain": chain or "", "limit": limit}

    try:
        # Per-pair top-quantile raw amounts (the "top 0.1%" definition).
        pair_q: dict[str, float] = {}
        try:
            quantile_query = (
                f"SELECT contract_address, quantile({WHALE_TOP_QUANTILE})(greatest(amount0, amount1)) "
                f"FROM {CH_DEX_TABLE} WHERE {where} GROUP BY contract_address"
            )
            for row in ch_client.execute(quantile_query, params) or []:
                if len(row) >= 2 and row[1]:
                    pair_q[str(row[0])] = float(row[1])
        except Exception as exc:  # noqa: BLE001 — quantile is best-effort
            logger.debug("whale_quantile_failed", error=str(exc)[:80])

        rows = (
            ch_client.execute(
                "SELECT chain, tx_hash, log_index, token0, token1, amount0, amount1, "
                "contract_address, version, whale "
                f"FROM {CH_DEX_TABLE} WHERE {where} "
                "ORDER BY block_timestamp DESC LIMIT %(limit)s",
                params,
            )
            or []
        )
        stats["scanned"] = len(rows)

        # Load prices for the tokens in the scanned rows.
        tokens = {(str(r[0]), str(r[3])) for r in rows if len(r) > 3}
        tokens |= {(str(r[0]), str(r[4])) for r in rows if len(r) > 4}
        prices: dict[tuple[str, str], float] = {}
        if tokens:
            try:
                price_rows = ch_client.execute(
                    f"SELECT chain, token_address, price_usd FROM {CH_TOKEN_PRICES_TABLE} "
                    "WHERE (chain, token_address) IN %(tokens)s",
                    {"tokens": list(tokens)},
                )
                for pr in price_rows or []:
                    if len(pr) >= 3 and pr[2]:
                        prices[(str(pr[0]), str(pr[1]))] = float(pr[2])
            except Exception as exc:  # noqa: BLE001 — pricing is best-effort
                stats["errors"].append(f"price_lookup: {str(exc)[:60]}")

        set_1: list[tuple[Any, ...]] = []
        set_0: list[tuple[Any, ...]] = []

        for r in rows:
            rchain, tx_hash, log_index = str(r[0]), str(r[1]), int(r[2])
            token0, token1 = str(r[3]), str(r[4])
            amount0 = _sane_amount(r[5])
            amount1 = _sane_amount(r[6])
            pair = str(r[7])
            current_whale = int(r[9]) if len(r) > 9 else 0

            new_whale = 0
            # 1. USD notional.
            for token, amount in ((token0, amount0), (token1, amount1)):
                price = prices.get((rchain, token))
                if price and price > 0 and amount > 0:
                    usd = (amount / (10 ** get_decimals(rchain, token))) * price
                    if usd >= threshold:
                        new_whale = 1
                        break
            # 2. Per-chain raw heuristic.
            if not new_whale:
                thr = thresholds.get(rchain, 0)
                if thr and (amount0 >= thr or amount1 >= thr):
                    new_whale = 1
            # 3. Top 0.1% for the pair.
            if not new_whale:
                q = pair_q.get(pair, 0.0)
                if q > 0 and max(amount0, amount1) >= q:
                    new_whale = 1

            key = (rchain, tx_hash, log_index)
            if new_whale != current_whale:
                (set_1 if new_whale else set_0).append(key)

        stats["whale_up"] = len(set_1)
        stats["whale_down"] = len(set_0)
        stats["unchanged"] = stats["scanned"] - stats["whale_up"] - stats["whale_down"]

        if dry_run:
            return stats

        for keys, whale in ((set_1, 1), (set_0, 0)):
            for i in range(0, len(keys), _MUTATION_BATCH):
                batch = keys[i : i + _MUTATION_BATCH]
                ch_client.execute(
                    f"ALTER TABLE {CH_DEX_TABLE} UPDATE whale = %(whale)s "
                    "WHERE (chain, tx_hash, log_index) IN %(keys)s",
                    {"whale": whale, "keys": batch},
                )
        metrics_incr("whale_backfills")
        return stats
    except Exception as exc:  # noqa: BLE001 — CLI-facing, surface the error
        stats["errors"].append(str(exc)[:120])
        logger.error("whale_backfill_failed", error=str(exc)[:120])
        return stats


__all__ = [
    "PriceFetcher",
    "backfill_whales",
    "compute_swap_usd",
    "detect_whale",
    "ensure_token_prices_table",
    "get_decimals",
    "get_price",
    "refresh_prices",
]
