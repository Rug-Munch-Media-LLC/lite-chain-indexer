"""In-memory price cache + USD conversion helpers (used by the DEX write path)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from lite_indexer.config import (
    DEFAULT_DECIMALS_BY_CHAIN,
    PRICING_REFRESH_INTERVAL,
    WHALE_AMOUNT_THRESHOLDS,
    WHALE_THRESHOLD_USD,
)

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
