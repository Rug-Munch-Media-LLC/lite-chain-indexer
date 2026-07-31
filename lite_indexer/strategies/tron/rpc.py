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

from lite_indexer.logging import get_logger

from .models import TRON_PAGE_SIZE

logger = get_logger(__name__)

# ── Tron RPC Endpoints (tiered fallback) ─────────────────────────────
# T1 — Official TronGrid (only endpoint that supports contract events API)
#           /v1/contracts/{address}/events?event_name=Transfer
# T2 — Verified free endpoints (block queries only, no events API)
# T3 — Best-effort fallbacks
#
# All endpoints tested 2026-07-30: /wallet/getnowblock works.
# Only TronGrid (api.trongrid.io) supports the events API for TRC20 transfers.
#
TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
    # T3 — Fallback
    "https://rpc.tronlink.org",
]


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
        except Exception as exc:
            last_error = str(exc)[:60]
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
        except Exception as exc:
            last_error = str(exc)[:60]
            continue

    logger.debug("tron_request_failed", path=path, error=last_error)
    return None


async def tron_get_with_range(
    client: httpx.AsyncClient,
    path: str,
    from_block: int,
    to_block: int,
    cursor: str | None = None,
) -> dict[str, Any] | None:
    """Make a GET request to TronGrid with block number range.

    TronGrid uses min_block_number and max_block_number for range queries.
    We build the query string manually for custom parameter formatting.

    Args:
        client: Shared httpx AsyncClient.
        path: API path.
        from_block: Start block.
        to_block: End block.
        cursor: Pagination cursor.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    query_parts = [
        "event_name=Transfer",
        f"from_block_number={from_block}",
        f"to_block_number={to_block}",
        f"limit={TRON_PAGE_SIZE}",
        "order_by=block_timestamp,desc",
    ]
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
        except Exception as exc:
            last_error = str(exc)[:60]
            continue

    logger.debug("tron_get_range_failed", path=path[:50], error=last_error)
    return None
