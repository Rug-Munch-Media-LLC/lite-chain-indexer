"""Periodic daemon loops (RPC discovery/usage + price refresh).

Split out of cli/_daemon.py (structure ratchet); each loop is a
best-effort background task started by the daemon.
"""
from __future__ import annotations

import asyncio

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.storage.clickhouse import ClickHouseWriter

logger = get_logger(__name__)


async def _discovery_and_usage_loop(chains: list[str]) -> None:
    """Periodically log RPC usage and auto-discover new free endpoints.

    Scans chainlist.org for new free RPCs for each EVM chain at
    ``RPC_DISCOVERY_INTERVAL`` (default: daily) and appends healthy
    endpoints to the in-memory pool. Also emits a periodic usage/cost
    summary so operators can see which endpoints actually serve data.

    Args:
        chains: Chains to keep discovering for.
    """
    from lite_indexer.config import RPC_DISCOVERY_INTERVAL
    from lite_indexer.rpc.discovery import auto_discover
    from lite_indexer.rpc.routing import log_rpc_usage

    interval = max(300, RPC_DISCOVERY_INTERVAL)
    evm_chains = [c for c in chains if c != "solana" and c != "tron"]

    while True:
        try:
            log_rpc_usage()
            if interval and evm_chains:
                for chain in evm_chains:
                    try:
                        await auto_discover(chain, add_to_pool=True, validate=True)
                    except (
                        httpx.HTTPError,
                        ConnectionError,
                        OSError,
                        TimeoutError,
                        ValueError,
                        KeyError,
                        TypeError,
                    ) as exc:
                        logger.warning("rpc_discovery_failed", chain=chain, error=str(exc)[:80])
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            logger.warning("discovery_loop_error", error=str(exc)[:100])
        await asyncio.sleep(interval)


async def _price_refresh_loop(chains: list[str]) -> None:
    """Periodically refresh USD prices for tracked DEX tokens.

    Refreshes the ``token_prices`` feed (CoinGecko + DefiLlama free tiers)
    on startup and every ``PRICING_REFRESH_INTERVAL`` seconds (min 5 min).
    The in-memory price cache feeds the whale flag + USD columns written
    on DEX rows. Best-effort: provider failures keep the previous cache.

    Args:
        chains: Chains to refresh prices for (tracked-token scan filter).
    """
    from lite_indexer.config import PRICING_REFRESH_INTERVAL
    from lite_indexer.pricing import refresh_prices

    interval = max(300, PRICING_REFRESH_INTERVAL)
    while True:
        try:
            writer = ClickHouseWriter()
            ch_client = writer.client()
            if ch_client is not None:
                stats = await refresh_prices(ch_client, chains=chains)
                logger.info("prices_refreshed", stats=stats)
            else:
                logger.warning("price_refresh_skipped_no_ch")
        except Exception as exc:  # noqa: BLE001 — refresh is best-effort
            logger.warning("price_refresh_failed", error=str(exc)[:100])
        await asyncio.sleep(interval)
