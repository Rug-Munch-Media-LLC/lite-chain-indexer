"""Coverage tests for ``lite_indexer.pricing.cache`` pure helpers.

Hermetic — pure in-memory cache + arithmetic, no network, no ClickHouse.
Complements ``tests/test_pricing.py`` (which drives the write path and
backfills) by pinning the cache staleness boundary, the decimals lookup
precedence, the bogus-amount guard, and the max-leg USD computation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from lite_indexer.config import DEFAULT_DECIMALS_BY_CHAIN, PRICING_REFRESH_INTERVAL
from lite_indexer.pricing.cache import (
    KNOWN_DECIMALS,
    _MAX_SANE_AMOUNT,
    _PRICE_CACHE,
    _sane_amount,
    compute_swap_usd,
    get_decimals,
    get_price,
)

USDC_ETH = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT_TRON = "tr7nhqjekqxgtci8q8zy4pl8otszgjlj6t"


def _seed(chain: str, token: str, price: float, *, age_seconds: float = 0.0) -> None:
    _PRICE_CACHE[(chain, token)] = (price, datetime.now(UTC) - timedelta(seconds=age_seconds))


# ── get_price ───────────────────────────────────────────────────────────


def test_get_price_returns_seeded_value() -> None:
    _seed("ethereum", "0xabc", 1.5)
    assert get_price("ethereum", "0xabc") == 1.5


def test_get_price_unknown_token_returns_none() -> None:
    assert get_price("ethereum", "0xnever-seen") is None


def test_get_price_empty_token_returns_none() -> None:
    assert get_price("ethereum", "") is None


def test_get_price_stale_entry_returns_none() -> None:
    # Older than 2 refresh intervals + 60s → treated as stale.
    _seed("ethereum", "0xstale", 2.0, age_seconds=PRICING_REFRESH_INTERVAL * 2 + 120)
    assert get_price("ethereum", "0xstale") is None


def test_get_price_fresh_entry_inside_window() -> None:
    _seed("ethereum", "0xfresh", 3.0, age_seconds=PRICING_REFRESH_INTERVAL)
    assert get_price("ethereum", "0xfresh") == 3.0


# ── get_decimals ────────────────────────────────────────────────────────


def test_get_decimals_prefers_known_stablecoin_table() -> None:
    assert get_decimals("ethereum", USDC_ETH) == 6
    assert get_decimals("tron", USDT_TRON) == 6


def test_get_decimals_falls_back_to_chain_default() -> None:
    expected = DEFAULT_DECIMALS_BY_CHAIN.get("ethereum", 18)
    assert get_decimals("ethereum", "0xunknown-token") == expected


def test_get_decimals_unknown_chain_defaults_to_18() -> None:
    assert get_decimals("no-such-chain", "0xunknown") == 18


def test_known_decimals_table_is_lowercase_keyed() -> None:
    assert all(key == key.lower() for key in KNOWN_DECIMALS)


# ── _sane_amount ────────────────────────────────────────────────────────


def test_sane_amount_parses_positive_int() -> None:
    assert _sane_amount("1000") == 1000
    assert _sane_amount(42) == 42


def test_sane_amount_rejects_non_numeric() -> None:
    assert _sane_amount("not-a-number") == 0
    assert _sane_amount(None) == 0


def test_sane_amount_rejects_zero_and_negative() -> None:
    assert _sane_amount(0) == 0
    assert _sane_amount(-5) == 0


def test_sane_amount_rejects_overflow_magnitude() -> None:
    # The old V3 parser stored signed int256 as unsigned (~2^256) — guard.
    assert _sane_amount(_MAX_SANE_AMOUNT) == 0
    assert _sane_amount(2**256) == 0


# ── compute_swap_usd ────────────────────────────────────────────────────


def test_compute_swap_usd_returns_zero_when_unpriced() -> None:
    event = {"chain": "ethereum", "token0": "0xunpriced", "amount0In": "1000"}
    assert compute_swap_usd(event) == 0.0


def test_compute_swap_usd_uses_max_leg() -> None:
    _seed("ethereum", USDC_ETH, 1.0)
    event = {
        "chain": "ethereum",
        "token0": USDC_ETH,
        "amount0In": str(2_000_000),  # 2 USDC
        "token1": "0xunpriced",
        "amount1In": "0",
    }
    assert compute_swap_usd(event) == 2.0


def test_compute_swap_usd_ignores_bogus_amount() -> None:
    _seed("ethereum", USDC_ETH, 1.0)
    event = {"chain": "ethereum", "token0": USDC_ETH, "amount0In": str(2**256)}
    assert compute_swap_usd(event) == 0.0


def test_compute_swap_usd_handles_missing_amount_keys() -> None:
    _seed("ethereum", USDC_ETH, 1.0)
    event = {"chain": "ethereum", "token0": USDC_ETH}
    assert compute_swap_usd(event) == 0.0
