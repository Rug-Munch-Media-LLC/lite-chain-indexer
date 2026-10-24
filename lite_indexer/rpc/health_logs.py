"""eth_getLogs health — probe the capability the indexer actually needs.

``eth_blockNumber`` answers on every public BSC endpoint, including ones
that are pruned (``header not found`` for anything older than their
retention), range-capped (25 blocks), rate-limited (429) or behind a
monthly quota. A pool gauge built on liveness alone therefore reported
8/13 "healthy" on 2026-10-23 while ZERO endpoints could serve the range
the bsc loop was asking for.

This module owns the getLogs-level verdict: one span, over the exact
pending range, with the same headers as the real call. A verdict of
``False`` means "this endpoint cannot fetch logs for the range we need",
which is the only definition of healthy that matters to the sync loop.
"""

from __future__ import annotations

import os

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc import user_agent

logger = get_logger(__name__)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
#: Blocks per probe. Small on purpose: a 5-block span is enough to prove the
#: endpoint holds the history we need, and it does not burn a free-tier quota.
PROBE_SPAN_BLOCKS = int(os.environ.get("LOGS_PROBE_SPAN_BLOCKS", "5"))

_logs_ok: dict[str, bool] = {}
_logs_reason: dict[str, str] = {}


def logs_probe_verdict(endpoint: str) -> bool | None:
    """Return the last getLogs verdict for ``endpoint`` (None = never probed)."""
    return _logs_ok.get(endpoint)


def logs_probe_reason(endpoint: str) -> str:
    """Return the human-readable reason behind the last getLogs verdict."""
    return _logs_reason.get(endpoint, "not_probed")


def clear_logs_verdicts() -> None:
    """Drop every recorded verdict (tests / shutdown)."""
    _logs_ok.clear()
    _logs_reason.clear()


def _record(endpoint: str, ok: bool, reason: str, *, chain: str = "", span_blocks: int = PROBE_SPAN_BLOCKS) -> None:
    """Store the verdict, then persist it as a capability record.

    Persisting here is the point: a probe that passed must leave a record
    (``logs_capable`` + a measured span), otherwise the pool gauge and the
    range planner see "unknown capability" and treat a working endpoint as
    unusable — the keyed-provider failure mode of 2026-10-23.
    """
    _logs_ok[endpoint] = ok
    _logs_reason[endpoint] = reason
    try:
        from lite_indexer.rpc.logs_capability_persist import note_logs_probe

        note_logs_probe(endpoint, chain, span_blocks, ok, reason)
    except Exception:  # noqa: BLE001 — recording must never break probing
        logger.debug("logs_capability_record_failed", endpoint=endpoint[:60])


async def probe_logs_range(
    endpoint: str,
    from_block: int,
    to_block: int,
    timeout: float = 8.0,
    chain: str = "",
) -> tuple[bool, str]:
    """Fetch one tiny eth_getLogs span and classify the answer.

    Unlike a liveness probe this fails on capability errors that a
    ``eth_blockNumber`` call never sees: pruned history
    (``header not found``), range ceilings (``exceed maximum block range``),
    plan refusals (``Archive requests require a personal token``) and quota
    exhaustion (HTTP 429). An empty log list is a PASS — the endpoint
    served the span, it simply had no transfers in it.

    Args:
        endpoint: RPC endpoint URL.
        from_block: First block of the span (the real pending range).
        to_block: Last block of the span.
        timeout: Per-probe HTTP timeout.
        chain: Chain the endpoint serves (recorded on the capability record).

    Returns:
        ``(ok, reason)``. Reason is ``"ok"`` or the provider's rejection.
    """
    span_blocks = max(1, int(to_block) - int(from_block) + 1)
    verdict_kw = {"chain": chain, "span_blocks": span_blocks}
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{"fromBlock": hex(from_block), "toBlock": hex(to_block), "topics": [TRANSFER_TOPIC]}],
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), headers=user_agent.rpc_headers()) as client:
            resp = await client.post(endpoint, json=body)
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError) as exc:
        reason = f"transport: {type(exc).__name__}"
        _record(endpoint, False, reason, **verdict_kw)
        return False, reason

    if resp.status_code != 200:
        reason = f"http {resp.status_code}"
        _record(endpoint, False, reason, **verdict_kw)
        return False, reason
    try:
        payload = resp.json()
    except ValueError:
        _record(endpoint, False, "non-json body", **verdict_kw)
        return False, "non-json body"

    error = payload.get("error") if isinstance(payload, dict) else None
    if error:
        reason = f"rpc {error.get('code')}: {str(error.get('message'))[:90]}"
        _record(endpoint, False, reason, **verdict_kw)
        return False, reason
    if not isinstance(payload.get("result"), list):
        _record(endpoint, False, "no result list", **verdict_kw)
        return False, "no result list"

    _record(endpoint, True, "ok", **verdict_kw)
    return True, "ok"
