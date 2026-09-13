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


async def get_transactions_for_address(
    client: httpx.AsyncClient,
    address: str,
    *,
    token_accounts: str = "all",
    transaction_details: str = "signatures",
    sort_order: str = "asc",
    limit: int = 1000,
    block_time_gte: int | None = None,
    block_time_lt: int | None = None,
    status: str | None = None,
) -> dict[str, Any] | None:
    """Recover transaction history for an address — INCLUDING closed accounts.

    ``getSignaturesForAddress`` returns ``[]`` for closed/deleted accounts
    because the live signature index drops them. SolanaProvider's
    ``getTransactionsForAddress`` with ``filters.tokenAccounts`` indexes
    token-account activity, so it retains closed-account history.

    Enhanced-history providers expose a verified closed-account capability
    matrix; configure one via the environment/overlay.

    Args:
        client: httpx AsyncClient.
        address: Base-58 wallet or token-account address.
        token_accounts: ``"none"`` | ``"balanceChanged"`` | ``"all"``
            (default). ``"all"`` recovers closed accounts and token accounts.
        transaction_details: ``"signatures"`` (cheap) or ``"full"`` (rich).
        sort_order: ``"asc"`` (oldest first) or ``"desc"``.
        limit: Max txs (up to 1000).
        block_time_gte / block_time_lt: Unix-second window bounds.
        status: ``"succeeded"`` | ``"failed"`` | ``"any"`` | None.

    Returns:
        Parsed JSON-RPC response dict (``{"result": {"data": [...]}}``),
        or None on failure. The response also carries a ``paginationToken``
        (format ``"slot:position"``) for keyset pagination.
    """
    params: dict[str, Any] = {
        "transactionDetails": transaction_details,
        "sortOrder": sort_order,
        "limit": limit,
        "maxSupportedTransactionVersion": 0,
        "filters": {"tokenAccounts": token_accounts},
    }
    if block_time_gte is not None or block_time_lt is not None:
        bt: dict[str, int] = {}
        if block_time_gte is not None:
            bt["gte"] = block_time_gte
        if block_time_lt is not None:
            bt["lt"] = block_time_lt
        params["filters"]["blockTime"] = bt
    if status is not None:
        params["filters"]["status"] = status

    return await rpc_call(client, "getTransactionsForAddress", [address, params])


#: Per-endpoint 429 cooldown (url -> monotonic time when it becomes usable)
_endpoint_cooldown: dict[str, float] = {}
