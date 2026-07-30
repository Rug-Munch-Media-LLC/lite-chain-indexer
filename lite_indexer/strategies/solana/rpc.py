"""
Solana Strategy - RPC Client
=============================
Standalone JSON-RPC client for Solana with endpoint rotation.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Solana RPC Endpoints (tiered fallback) ───────────────────────────
SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC (most reliable, rate-limited per IP)
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    # T3 — Fallbacks (may have lower rate limits or require API keys)
    "https://rpc.ankr.com/solana",
    "https://solana.drpc.org",
]


async def rpc_call(
    client: httpx.AsyncClient,
    method: str,
    params: list[Any] | None = None,
    endpoint: str | None = None,
) -> dict[str, Any] | None:
    """Make a JSON-RPC call to a Solana RPC endpoint.

    Args:
        client: httpx AsyncClient to use for the request.
        method: Solana RPC method name.
        params: Method parameters.
        endpoint: Specific endpoint to use (default: rotate through list).

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }

    endpoints = list(SOLANA_ENDPOINTS)
    if endpoint:
        endpoints = [endpoint]

    last_error: str | None = None
    for url in endpoints:
        try:
            resp = await client.post(url, json=payload, timeout=30.0)
            if resp.status_code == 429:
                await asyncio.sleep(1)
                continue
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "result" in data:
                    return data
                if isinstance(data, dict) and "error" in data:
                    last_error = str(data["error"])
                    continue
            last_error = f"http_{resp.status_code}"
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_error = str(exc)[:60]
            continue

    logger.debug("solana_rpc_failed", method=method, error=last_error)
    return None
