"""Last-resort endpoint fallback for the EVM transfer handler.

Extracted from ``strategies.evm.transfer_handler._fetch_range``: when
every endpoint in the active pool failed, try the chain's known-good
last-resort endpoints (official RPCs / gateway.org) so a total pool wipe
still has a shot at serving the range before the data-loss guard raises.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from lite_indexer.chains import last_resort_endpoints
from lite_indexer.config import RPC_TIMEOUT
from lite_indexer.dex import parse_transfer_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    apply_endpoint_backoff,
    ban_endpoint,
    handle_rate_limit,
    provider_slot,
    record_endpoint_failure,
    record_provider_response,
)
from lite_indexer.rpc.gates import provider_gates
from lite_indexer.rpc.pool import apply_endpoint_backoff as _noop  # noqa: F401  (placeholder removed below)
from lite_indexer.rpc.provider_caps import account_key_for_endpoint

logger = get_logger(__name__)


@dataclass
class LastResortAttempt:
    """Inputs for one last-resort attempt (keeps the call site readable)."""

    chain: str
    body: dict[str, Any]
    client: httpx.AsyncClient
    rotated: list[str]
    from_block: int
    to_block: int
    best_result: list[dict[str, Any]] | None
    last_error: str | None


async def try_last_resort(attempt: LastResortAttempt) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Try the chain's last-resort endpoints; return (best_result, last_error)."""
    chain = attempt.chain
    body = attempt.body
    client = attempt.client
    rotated = attempt.rotated
    from_block = attempt.from_block
    to_block = attempt.to_block
    best_result = attempt.best_result
    last_error = attempt.last_error
    last_resort = [ep for ep in last_resort_endpoints(chain) if ep not in rotated]
    if not last_resort:
        return best_result, last_error
    logger.warning(
        "last_resort_endpoints_tried",
        chain=chain,
        count=len(last_resort),
        from_block=from_block,
        to_block=to_block,
    )
    for url in last_resort:
        await apply_endpoint_backoff(url)
        # Same gate chain as RpcClient.call — last-resort endpoints are
        # shared provider accounts too; bypassing the gates 429-bans them.
        await provider_gates(url, "eth_getLogs")
        try:
            async with provider_slot(account_key_for_endpoint(url)):
                resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
            record_provider_response(url, resp.status_code, resp.headers)
            if resp.status_code == 200 and resp.text and len(resp.text) >= 10:
                data = resp.json()
                if isinstance(data, dict) and isinstance(data.get("result"), list):
                    logs = data["result"]
                    parsed = parse_transfer_logs(logs, chain, from_block, to_block)
                    if best_result is None or len(parsed) > len(best_result):
                        best_result = parsed
                        if len(parsed) == len(logs):
                            break
                elif isinstance(data, dict) and data.get("result") is None:
                    if best_result is None:
                        best_result = []
                    break
            elif resp.status_code == 429:
                await handle_rate_limit(url)
            elif resp.status_code == 403:
                record_endpoint_failure(url)
                ban_endpoint(url, reason="http_403")
        except (
            httpx.HTTPError,
            ConnectionError,
            OSError,
            TimeoutError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:80]}"
            logger.warning(
                "last_resort_fetch_failed",
                chain=chain,
                endpoint=url[:50],
                error_type=type(exc).__name__,
                error=str(exc)[:60] or repr(exc)[:120],
            )
            record_endpoint_failure(url)
            continue
    return best_result, last_error
