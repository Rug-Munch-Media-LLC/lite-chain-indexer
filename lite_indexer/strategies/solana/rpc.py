"""
Solana Strategy - RPC Client
=============================
Standalone JSON-RPC client for Solana with endpoint rotation.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


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

    # 2026-09-01: skip endpoints that 429'd recently (60s cooldown) — the
    # old code slept 1s per 429 then tried the next endpoint sequentially,
    # so a batch of 100 calls all hit mainnet-beta first, slept, and burned
    # the window on retries. A module-level cooldown map makes the pool
    # actually rotate under rate-limiting.
    live = [u for u in endpoints if _endpoint_cooldown.get(u, 0) < time.monotonic()]
    if not live:
        live = endpoints  # all cooling down — try anyway rather than fail

    last_error: str | None = None
    for url in live:
        try:
            resp = await client.post(url, json=payload, timeout=30.0)
            if resp.status_code == 429:
                _endpoint_cooldown[url] = time.monotonic() + 60.0
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


#: Per-endpoint 429 cooldown (url -> monotonic time when it becomes usable)
_endpoint_cooldown: dict[str, float] = {}
