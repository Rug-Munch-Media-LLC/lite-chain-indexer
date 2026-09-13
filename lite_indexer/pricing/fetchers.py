"""Price fetching from CoinGecko / DefiLlama free tiers (paced per provider)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from lite_indexer.config import (
    COINGECKO_BASE_URL,
    COINGECKO_MIN_INTERVAL,
    COINGECKO_PLATFORMS,
    DEFILLAMA_BASE_URL,
    DEFILLAMA_MIN_INTERVAL,
    PRICING_BATCH_SIZE,
)
from lite_indexer.logging import get_logger
from lite_indexer.util.http import http_limits

logger = get_logger(__name__)

# Per-provider last-request monotonic timestamp (free-tier pacing).
_LAST_REQUEST: dict[str, float] = {}


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
            limits=http_limits(max_connections=10, max_keepalive_connections=5),
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
