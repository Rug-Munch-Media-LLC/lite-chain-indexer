"""
Tests for whale detection + USD price normalization (pricing.py).

Covers:
  1. detect_whale — USD notional (Fix 3), raw-amount heuristic (Fix 1),
     stablecoin decimals, fallback behavior
  2. compute_swap_usd / get_price / get_decimals cache helpers
  3. refresh_prices — DefiLlama primary, CoinGecko fallback, resilience
  4. V3 signed int256 decode (negative amounts must not read as ~2^256)
  5. ClickHouseWriter.write_dex_events — whale=1 rows and USD columns
  6. backfill_whales — retroactive marking, idempotent mutations

Uses a fake ClickHouse client (same pattern as test_wallet_features.py)
so the suite is hermetic — no CH_HOST required.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from lite_indexer.pricing import (
    _PRICE_CACHE,
    backfill_whales,
    compute_swap_usd,
    detect_whale,
    ensure_token_prices_table,
    get_decimals,
    get_price,
    refresh_prices,
)


class FakeClient:
    """Minimal ClickHouse client double for pricing tests."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params: list[Any] = []
        self.prices: dict[tuple[str, str], float] = {}
        self.price_symbols: dict[tuple[str, str], str] = {}
        self.dex_rows: list[tuple[Any, ...]] = []
        self.pair_quantiles: dict[str, float] = {}
        self.mutations: list[tuple[int, list[tuple[Any, ...]]]] = []
        self.token_prices_inserts: list[tuple[Any, ...]] = []
        self.tracked_tokens: list[tuple[str, str]] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        self.params.append(params)

        if query.startswith("INSERT INTO wallet_memory.token_prices"):
            for row in params or []:
                self.prices[(row[0], row[1])] = float(row[3])
                self.price_symbols[(row[0], row[1])] = str(row[2])
                self.token_prices_inserts.append(row)
            return len(params or [])
        if query.startswith("SELECT chain, token_address, price_usd FROM"):
            wanted = set(params.get("tokens") or []) if isinstance(params, dict) else set()
            return [(c, t, p) for (c, t), p in self.prices.items() if not wanted or (c, t) in wanted]
        if query.startswith("SELECT chain, token0 FROM wallet_memory.dex_events"):
            return self.tracked_tokens
        if query.startswith("SELECT chain, token1 FROM wallet_memory.dex_events"):
            return self.tracked_tokens
        if "quantile(" in query:
            return [(pair, q) for pair, q in self.pair_quantiles.items()]
        if query.startswith("SELECT chain, tx_hash, log_index, token0, token1, amount0, amount1"):
            return self.dex_rows
        if query.startswith("ALTER TABLE wallet_memory.dex_events UPDATE whale"):
            whale = int(params.get("whale") or 0) if isinstance(params, dict) else 0
            keys = list(params.get("keys") or []) if isinstance(params, dict) else []
            self.mutations.append((whale, keys))
            return 1
        if query.startswith("SELECT name FROM system.columns"):
            return []
        return []


@pytest.fixture(autouse=True)
def _clean_price_cache() -> None:
    """Every test starts with an empty in-memory price cache."""
    _PRICE_CACHE.clear()


def _swap(
    *,
    chain: str = "ethereum",
    token0: str = "0xtoken0",
    token1: str = "0xtoken1",
    amount0: str = "0",
    amount1: str = "0",
    version: str = "v2",
) -> dict[str, Any]:
    """Build a parsed DEX swap event dict matching the pipeline shape."""
    return {
        "type": "swap",
        "dex_type": version,
        "dex": "uniswap_v2" if version == "v2" else "uniswap_v3",
        "chain": chain,
        "blockNumber": 100,
        "blockTimestamp": "2026-08-03T00:00:00Z",
        "transactionHash": "0xtx",
        "logIndex": 0,
        "sender": "0xsender",
        "recipient": "0xrecipient",
        "token0": token0,
        "token1": token1,
        "amount0In": amount0,
        "amount1In": amount1,
        "amount0Out": "0",
        "amount1Out": "0",
        "pairAddress": "0xpair",
        "price_usd": 0.0,
        "volume_usd": 0.0,
        "source": "lite_indexer",
    }


def _seed_price(chain: str, token: str, price: float, age_seconds: int = 0) -> None:
    """Seed the in-memory price cache (age_seconds in the past)."""
    _PRICE_CACHE[(chain, token)] = (price, datetime.now(UTC) - timedelta(seconds=age_seconds))


# ── DDL ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ensure_token_prices_table_ddl() -> None:
    client = FakeClient()
    ok = await ensure_token_prices_table(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE TABLE IF NOT EXISTS wallet_memory.token_prices" in ddl
    assert "ReplacingMergeTree()" in ddl
    assert "ORDER BY (chain, token_address)" in ddl
    assert "price_usd Float64" in ddl


@pytest.mark.asyncio
async def test_ensure_token_prices_table_graceful_on_failure() -> None:
    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("database wallet_memory doesn't exist")

    ok = await ensure_token_prices_table(BrokenClient())
    assert ok is False  # non-fatal: degrades to a log, never raises


# ── Whale detection ───────────────────────────────────────────────────


def test_detect_whale_usd_threshold() -> None:
    """amount * price >= $100K marks a swap as a whale (Fix 3)."""
    # Token priced at $2, 18 decimals. 50_000 tokens * $2 = $100_000 → whale.
    _seed_price("ethereum", "0xtoken0", 2.0)
    whale = detect_whale(_swap(amount0=str(50_000 * 10**18)))
    assert whale == 1
    # 49_999 tokens * $2 = $99_998 → not a whale.
    whale = detect_whale(_swap(amount0=str(49_999 * 10**18)))
    assert whale == 0


def test_detect_whale_stablecoin_decimals() -> None:
    """USDC (6 decimals) at $1: 100_000 USDC → whale, 100 USDC → not."""
    usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    _seed_price("ethereum", usdc, 1.0)
    assert detect_whale(_swap(token0=usdc, amount0=str(100_000 * 10**6))) == 1
    assert detect_whale(_swap(token0=usdc, amount0=str(100 * 10**6))) == 0


def test_detect_whale_uses_max_of_both_legs() -> None:
    """A whale-sized amount on either leg flags the swap."""
    _seed_price("ethereum", "0xtoken0", 1.0)
    _seed_price("ethereum", "0xtoken1", 1.0)
    # Leg 0 tiny, leg 1 whale-sized (both 18 decimals).
    event = _swap(amount0=str(1_000 * 10**18), amount1=str(500_000 * 10**18))
    assert detect_whale(event) == 1


def test_detect_whale_raw_amount_fallback() -> None:
    """No price → per-chain raw-amount heuristic (Fix 1) still flags whales."""
    # Ethereum default threshold is 1e23 raw. 2e23 → whale, 1e21 → not.
    assert detect_whale(_swap(amount0=str(2 * 10**23)), amount_thresholds={"ethereum": 10**23}) == 1
    assert detect_whale(_swap(amount0=str(10**21)), amount_thresholds={"ethereum": 10**23}) == 0


def test_detect_whale_ignores_bogus_amounts() -> None:
    """~2^256 amounts (old unsigned V3 negatives) must NOT flag a whale."""
    bogus = str(2**256 - 1000)
    assert detect_whale(_swap(amount0=bogus), amount_thresholds={"ethereum": 10**23}) == 0
    assert detect_whale(_swap(amount0="not-a-number")) == 0


def test_compute_swap_usd_max_leg() -> None:
    """USD notional is the max over both legs, 0.0 when unpriced."""
    _seed_price("ethereum", "0xtoken0", 2.0)
    _seed_price("ethereum", "0xtoken1", 1.0)
    event = _swap(amount0=str(10_000 * 10**18), amount1=str(100_000 * 10**18))
    # leg0 = 10_000*2 = $20K, leg1 = 100_000*1 = $100K → max $100_000
    assert compute_swap_usd(event) == 100_000.0
    assert compute_swap_usd(_swap()) == 0.0  # unpriced


def test_get_price_and_decimals() -> None:
    _seed_price("ethereum", "0xtoken0", 1.5)
    assert get_price("ethereum", "0xtoken0") == 1.5
    assert get_price("ethereum", "0xmissing") is None

    assert get_decimals("ethereum", "0xwhatever") == 18  # EVM default
    assert get_decimals("solana", "0xwhatever") == 9  # Solana default
    assert get_decimals("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") == 6  # USDC


def test_get_price_stale_returns_none() -> None:
    _seed_price("ethereum", "0xtoken0", 1.5, age_seconds=3600)  # 1h old
    assert get_price("ethereum", "0xtoken0") is None  # stale → fall back


# ── V3 signed int256 decode ───────────────────────────────────────────


def test_v3_parser_decodes_negative_amount_magnitude() -> None:
    """A negative V3 amount must decode to its magnitude, not ~2^256."""
    from lite_indexer.dex.v3 import parse_swap_v3

    positive = (2**255).to_bytes(32, "big").hex()
    negative = (2**256 - 123_456).to_bytes(32, "big").hex()
    log = {
        "topics": [
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
            "0x" + "00" * 12 + "aa" * 20,
            "0x" + "00" * 12 + "bb" * 20,
        ],
        "data": "0x" + positive + negative + "00" * 32 + "00" * 32 + "00" * 32,
        "blockNumber": "0x64",
        "logIndex": "0x0",
        "transactionHash": "0xtx",
        "address": "0xpool",
    }
    event = parse_swap_v3(log, "ethereum", 0, 200, {"0xpool": ("0xt0", "0xt1")})
    assert event is not None
    assert int(event["amount0In"]) == 2**255
    assert int(event["amount1In"]) == 123_456  # magnitude, not 2^256 - 123456


# ── refresh_prices ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_prices_defillama_upserts_and_caches() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "coins.llama.fi" in str(request.url)
        return httpx.Response(
            200,
            json={
                "coins": {
                    "ethereum:0xtoken0": {"price": 1.5, "symbol": "TOK", "timestamp": 1, "confidence": 0.99},
                    "ethereum:0xtoken1": {"price": 0.5, "symbol": "X", "timestamp": 1, "confidence": 0.5},
                }
            },
        )

    client = FakeClient()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        stats = await refresh_prices(
            client,
            tokens=[("ethereum", "0xtoken0"), ("ethereum", "0xtoken1")],
            http_client=http,
        )
    finally:
        await http.aclose()

    assert stats["status"] == "ok"
    assert stats["fetched"] == 2
    assert stats["upserted"] == 2
    assert stats["sources"]["defillama"] == 2
    # Mirror in ClickHouse + cache.
    assert client.prices == {("ethereum", "0xtoken0"): 1.5, ("ethereum", "0xtoken1"): 0.5}
    assert client.price_symbols[("ethereum", "0xtoken0")] == "TOK"
    assert _PRICE_CACHE[("ethereum", "0xtoken0")][0] == 1.5
    # Whale detection now works off the refreshed cache.
    assert detect_whale(_swap(token0="0xtoken0", amount0=str(100_000 * 10**18))) == 1


@pytest.mark.asyncio
async def test_refresh_prices_coingecko_fallback() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "coins.llama.fi" in str(request.url):
            return httpx.Response(200, json={"coins": {}})  # DefiLlama misses it
        return httpx.Response(200, json={"0xtoken0": {"usd": 2.0}})

    client = FakeClient()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        stats = await refresh_prices(client, tokens=[("ethereum", "0xtoken0")], http_client=http)
    finally:
        await http.aclose()

    assert stats["sources"]["coingecko"] == 1
    assert stats["fetched"] == 1
    assert client.prices[("ethereum", "0xtoken0")] == 2.0


@pytest.mark.asyncio
async def test_refresh_prices_provider_down_is_resilient() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    client = FakeClient()
    _seed_price("ethereum", "0xtoken0", 9.0)  # previous cached price
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        stats = await refresh_prices(client, tokens=[("ethereum", "0xtoken0")], http_client=http)
    finally:
        await http.aclose()

    assert stats["status"] == "partial"
    assert stats["fetched"] == 0
    assert stats["failed"] == 1
    # Previous cache is preserved; no exception propagates.
    assert _PRICE_CACHE[("ethereum", "0xtoken0")][0] == 9.0
    assert client.token_prices_inserts == []


@pytest.mark.asyncio
async def test_refresh_prices_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    import lite_indexer.pricing as pricing_mod

    monkeypatch.setattr(pricing_mod, "PRICING_ENABLED", False)
    stats = await refresh_prices(FakeClient(), tokens=[("ethereum", "0xtoken0")])
    assert stats["status"] == "disabled"


@pytest.mark.asyncio
async def test_refresh_prices_collects_tracked_tokens() -> None:
    """Without explicit tokens, recent dex_events token0/token1 are scanned."""
    client = FakeClient()
    client.tracked_tokens = [("ethereum", "0xtoken0"), ("base", "0xtoken1")]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"coins": {"ethereum:0xtoken0": {"price": 1.0, "symbol": "A"}}})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        stats = await refresh_prices(client, http_client=http)
    finally:
        await http.aclose()

    assert stats["tokens_known"] == 2
    assert stats["fetched"] == 1  # base:0xtoken1 unpriced
    assert stats["status"] == "partial"


# ── ClickHouseWriter.write_dex_events ─────────────────────────────────


def test_write_dex_events_writes_whale_and_usd_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """DEX ingestion writes whale=1, price_usd, volume_usd instead of 0."""
    import lite_indexer.storage.clickhouse as chmod
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    monkeypatch.setattr(chmod, "CH_TTL_ENABLED", False)

    _seed_price("ethereum", "0xtoken0", 2.0)
    _seed_price("ethereum", "0xtoken1", 1.0)

    writer = ClickHouseWriter()
    client = FakeClient()
    writer._client = client

    whale_event = _swap(
        token0="0xtoken0",
        token1="0xtoken1",
        amount0=str(100_000 * 10**18),  # 100K * $2 = $200K → whale
        amount1=str(1 * 10**18),
    )
    small_event = _swap(
        token0="0xtoken0",
        token1="0xtoken1",
        amount0=str(10 * 10**18),  # 10 * $2 = $20 → not a whale
        amount1=str(1 * 10**18),
    )

    inserted = writer.write_dex_events("ethereum", [whale_event, small_event])
    assert inserted == 2

    insert_q = [q for q in client.queries if q.startswith("INSERT INTO wallet_memory.dex_events")]
    assert len(insert_q) == 1
    assert "price_usd" in insert_q[0]
    assert "volume_usd" in insert_q[0]

    rows = client.params[-1]
    assert len(rows) == 2
    whale_row = rows[0]
    small_row = rows[1]

    # whale column (index 19), price_usd (20), volume_usd (21)
    assert whale_row[19] == 1
    assert whale_row[20] == 2.0  # token0 price
    assert whale_row[21] == 200_000.0  # max leg notional
    assert small_row[19] == 0
    assert small_row[21] == 20.0


def test_write_dex_events_graceful_without_prices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unpriced swaps insert whale=0 and USD 0.0 without raising."""
    import lite_indexer.storage.clickhouse as chmod
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    monkeypatch.setattr(chmod, "CH_TTL_ENABLED", False)

    writer = ClickHouseWriter()
    client = FakeClient()
    writer._client = client

    inserted = writer.write_dex_events("ethereum", [_swap(amount0=str(2 * 10**23))])
    assert inserted == 1
    row = client.params[-1][0]
    # raw threshold (1e23 ethereum) still flags the whale without a price
    assert row[19] == 1
    assert row[20] == 0.0
    assert row[21] == 0.0


# ── backfill_whales ───────────────────────────────────────────────────


def test_backfill_whales_marks_and_unmarks() -> None:
    client = FakeClient()
    client.prices = {
        ("ethereum", "0xtoken0"): 1.0,
        ("ethereum", "0xtoken1"): 1.0,
    }
    client.dex_rows = [
        # whale currently 0 but 200K USDC notional → should flip to 1
        (
            "ethereum",
            "0xtx1",
            0,
            "0xtoken0",
            "0xtoken1",
            str(200_000 * 10**6),
            str(200_000 * 10**18),
            "0xpair",
            "v2",
            0,
        ),
        # whale currently 1 but small notional → should flip to 0
        ("ethereum", "0xtx2", 1, "0xtoken0", "0xtoken1", str(100 * 10**6), str(100 * 10**18), "0xpair", "v2", 1),
        # already correct → unchanged
        ("ethereum", "0xtx3", 2, "0xtoken0", "0xtoken1", str(5 * 10**6), str(5 * 10**18), "0xpair", "v2", 0),
    ]

    stats = backfill_whales(client, days=90, limit=1000)
    assert stats["scanned"] == 3
    assert stats["whale_up"] == 1
    assert stats["whale_down"] == 1
    assert stats["unchanged"] == 1

    # Two mutations: whale=1 for the flipped-up key, whale=0 for the flipped-down.
    assert len(client.mutations) == 2
    set_1 = next(m for m in client.mutations if m[0] == 1)
    set_0 = next(m for m in client.mutations if m[0] == 0)
    assert ("ethereum", "0xtx1", 0) in set_1[1]
    assert ("ethereum", "0xtx2", 1) in set_0[1]


def test_backfill_whales_dry_run_issues_no_mutations() -> None:
    client = FakeClient()
    client.dex_rows = [
        ("ethereum", "0xtx1", 0, "0xtoken0", "0xtoken1", str(200_000 * 10**18), "0", "0xpair", "v2", 0),
    ]
    stats = backfill_whales(client, days=90, dry_run=True)
    assert stats["whale_up"] == 1
    assert client.mutations == []  # dry run never mutates


def test_backfill_whales_top_quantile_marks() -> None:
    """Rows at/above the pair's 99.9th percentile are marked as whales."""
    client = FakeClient()
    client.pair_quantiles = {"0xpair": 10**24}  # p99.9 of max leg amount
    client.dex_rows = [
        ("ethereum", "0xtx1", 0, "0xtoken0", "0xtoken1", str(2 * 10**24), "0", "0xpair", "v2", 0),  # above → whale
        ("ethereum", "0xtx2", 1, "0xtoken0", "0xtoken1", str(10**21), "0", "0xpair", "v2", 0),  # below → not
    ]
    stats = backfill_whales(client, days=90, limit=1000)
    assert stats["whale_up"] == 1
    set_1 = next(m for m in client.mutations if m[0] == 1)
    assert ("ethereum", "0xtx1", 0) in set_1[1]
