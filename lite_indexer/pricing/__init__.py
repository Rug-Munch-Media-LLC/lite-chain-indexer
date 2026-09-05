"""USD price feed + whale detection for DEX events.

Split into sub-modules:

- ``cache`` — in-memory price cache, decimals, USD conversion, whale detection
- ``fetchers`` — DefiLlama / CoinGecko free-tier HTTP fetchers
- ``aggregators`` — refresh orchestration (token scan, upserts, cache fill)
- ``helpers`` — retroactive backfill jobs (whales, zero-priced rows)

Resilience: every external call is best-effort. A failed provider degrades
to the previous cached price; ingestion never blocks on the price feed.
"""

from __future__ import annotations

from lite_indexer.pricing.aggregators import (
    ensure_token_prices_table,
    refresh_prices,
)
from lite_indexer.pricing.cache import (
    _PRICE_CACHE,
    compute_swap_usd,
    detect_whale,
    get_decimals,
    get_price,
)
from lite_indexer.pricing.fetchers import PriceFetcher
from lite_indexer.pricing.helpers import backfill_dex_prices, backfill_whales

__all__ = [
    "PriceFetcher",
    "_PRICE_CACHE",
    "backfill_dex_prices",
    "backfill_whales",
    "compute_swap_usd",
    "detect_whale",
    "ensure_token_prices_table",
    "get_decimals",
    "get_price",
    "refresh_prices",
]
