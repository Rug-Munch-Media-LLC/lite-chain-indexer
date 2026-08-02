"""
Tron Strategy - RPC Client
==========================
TronGrid REST client helpers with endpoint failover.

Tron uses a REST API (TronGrid) rather than JSON-RPC:
  - GET /wallet/getnowblock via POST /wallet/getnowblock
  - GET /v1/contracts/{address}/events?event_name=Transfer
  - Block timestamps from block_header.raw_data.timestamp
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.chains.non_evm import TRON_ENDPOINTS
from lite_indexer.logging import get_logger

from .models import TRON_PAGE_SIZE

logger = get_logger(__name__)


async def tron_get(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Make a GET request to TronGrid with endpoint failover.

    Args:
        client: Shared httpx AsyncClient.
        path: API path (e.g., '/wallet/getnowblock').
        params: Optional query parameters.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    last_error: str | None = None

    for base_url in TRON_ENDPOINTS:
        url = f"{base_url}{path}"
        try:
            resp = await client.get(url, params=params, timeout=30.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return data
                last_error = "non_dict_response"
            elif resp.status_code == 429:
                await asyncio.sleep(1)
                continue
            else:
                last_error = f"http_{resp.status_code}"
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_error = str(exc)[:60]
            continue
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
            last_error = str(exc)[:60]
            logger.warning("tron_get_failed", url=url[:80], error=last_error)
            continue

    logger.debug("tron_request_failed", path=path, error=last_error)
    return None


async def tron_post(
    client: httpx.AsyncClient,
    path: str,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Make a POST request to TronGrid with endpoint failover.

    Args:
        client: Shared httpx AsyncClient.
        path: API path (e.g., '/wallet/getnowblock').
        json_body: Optional JSON body.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    last_error: str | None = None

    for base_url in TRON_ENDPOINTS:
        url = f"{base_url}{path}"
        try:
            resp = await client.post(url, json=json_body or {}, timeout=30.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return data
                last_error = "non_dict_response"
            elif resp.status_code == 429:
                await asyncio.sleep(1)
                continue
            else:
                last_error = f"http_{resp.status_code}"
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_error = str(exc)[:60]
            continue
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
            last_error = str(exc)[:60]
            logger.warning("tron_post_failed", url=url[:80], error=last_error)
            continue

    logger.debug("tron_request_failed", path=path, error=last_error)
    return None


async def tron_get_with_range(
    client: httpx.AsyncClient,
    path: str,
    from_block: int,
    to_block: int,
    cursor: str | None = None,
    *,
    min_block_timestamp: int | None = None,
    max_block_timestamp: int | None = None,
) -> dict[str, Any] | None:
    """Make a GET request to TronGrid filtered by block timestamp range.

    TronGrid's /v1/contracts/*/events API does NOT accept
    from_block_number/to_block_number — it filters on
    min_block_timestamp/max_block_timestamp (epoch milliseconds).
    We derive those timestamps from the requested block range in the
    strategy and pass them here. only_confirmed=true excludes
    unconfirmed (pending) events that would otherwise be ingested as
    garbage.

    Args:
        client: Shared httpx AsyncClient.
        path: API path.
        from_block: Start block (kept for logging/context).
        to_block: End block (kept for logging/context).
        cursor: Pagination cursor (fingerprint).
        min_block_timestamp: Inclusive start timestamp (ms).
        max_block_timestamp: Inclusive end timestamp (ms).

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    query_parts = [
        "event_name=Transfer",
        f"limit={TRON_PAGE_SIZE}",
        "order_by=block_timestamp,desc",
    ]
    if min_block_timestamp is not None:
        query_parts.append(f"min_block_timestamp={min_block_timestamp}")
    if max_block_timestamp is not None:
        query_parts.append(f"max_block_timestamp={max_block_timestamp}")
    query_parts.append("only_confirmed=true")
    if cursor:
        query_parts.append(f"fingerprint={cursor}")

    query_string = "&".join(query_parts)
    full_url = f"?{query_string}"

    last_error: str | None = None

    for base_url in TRON_ENDPOINTS:
        url = f"{base_url}{path}{full_url}"
        try:
            resp = await client.get(url, timeout=30.0)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return data
                last_error = "non_dict_response"
            elif resp.status_code == 429:
                await asyncio.sleep(1)
                continue
            else:
                last_error = f"http_{resp.status_code}"
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_error = str(exc)[:60]
            continue
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
            last_error = str(exc)[:60]
            logger.warning("tron_get_range_failed", url=url[:80], error=last_error)
            continue

    logger.debug("tron_get_range_failed", path=path[:50], error=last_error)
    return None
