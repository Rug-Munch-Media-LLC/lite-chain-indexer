"""Single-shot broad eth_getLogs capability probes.

Split out of capabilities.py (2026-10-24). Request shapes and classification
are unchanged — the tip-range shape must stay the production shape.
"""

from __future__ import annotations

from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc.capability_classify import (
    CAPABILITY_HTTP_STATUSES,
    GetLogsCapability,
    _GetLogsProbeStatus,
    classify_logs_capability,
)
from lite_indexer.rpc.capability_constants import CAPABILITY_PROBE_TIMEOUT, TRANSFER_TOPIC
from lite_indexer.rpc.gates import provider_gates

logger = get_logger(__name__)


# ── Raw eth_getLogs probe helper ──────────────────────────────────────


async def _probe_getlogs_once(
    endpoint: str,
    from_block: int,
    to_block: int,
    topic0: str,
    timeout: float = CAPABILITY_PROBE_TIMEOUT,
) -> _GetLogsProbeStatus:
    """Issue a single topic-only eth_getLogs request over ``[from, to]``.

    Uses a fresh short-timeout httpx client (mirrors
    ``lite_indexer.rpc.health.verify_endpoint``). A client-side timeout is
    classified as a :data:`_GetLogsProbeStatus.RANGE_ERROR` — an endpoint
    that cannot answer a range in time is effectively range-limited at
    that size for the purposes of chunk sizing.

    Returns:
        OK for a valid list/null result, RANGE_ERROR for range/timeout
        failures, OTHER_ERROR for anything else (method not found etc.).
    """
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [topic0],
            }
        ],
    }
    try:
        # Startup-probe burst gate: probes POST to real provider accounts,
        # so they honor the same throttle/budget chain as RpcClient.call.
        await provider_gates(endpoint, "eth_getLogs")
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(endpoint, json=payload)
            if resp.status_code in CAPABILITY_HTTP_STATUSES:
                # Auth/challenge/method refusal — not a range limitation.
                return _GetLogsProbeStatus.OTHER_ERROR
            data = resp.json()
    except httpx.TimeoutException:
        logger.debug(
            "capability_getlogs_timeout",
            endpoint=endpoint[:60],
            from_block=from_block,
            to_block=to_block,
        )
        return _GetLogsProbeStatus.RANGE_ERROR
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.debug(
            "capability_getlogs_transport_error",
            endpoint=endpoint[:60],
            error=str(exc)[:80],
        )
        return _GetLogsProbeStatus.OTHER_ERROR

    return _classify_probe_payload(data)


def _classify_probe_payload(data: Any) -> _GetLogsProbeStatus:
    """Classify one eth_getLogs response body into a probe status."""
    if not isinstance(data, dict):
        return _GetLogsProbeStatus.OTHER_ERROR
    if "result" in data and not data.get("error"):
        return _GetLogsProbeStatus.OK

    err = data.get("error") or {}
    err_msg = str(err.get("message", err)) if isinstance(err, dict) else str(err)
    lowered = err_msg.lower()
    if any(token in lowered for token in ("range", "limit", "too large", "exceeds", "timeout", "max")):
        return _GetLogsProbeStatus.RANGE_ERROR
    return _GetLogsProbeStatus.OTHER_ERROR


# ── Probes ─────────────────────────────────────────────────────────────


def _decode_json_body(resp: httpx.Response) -> Any:
    """Decoded body, or raw text when not JSON (challenge pages must classify)."""
    try:
        return resp.json()
    except ValueError:
        return getattr(resp, "text", "") or ""


async def probe_logs_capability(
    endpoint: str,
    tip: int | None,
    timeout: float = CAPABILITY_PROBE_TIMEOUT,
) -> GetLogsCapability:
    """Probe broad, topic-only ``eth_getLogs`` over a real tip range.

    Issues exactly the request shape the indexer uses in production
    (``fromBlock = tip - 2``, ``toBlock = "latest"``, topic-only) and
    requires a ``result`` **list**. Endpoints that answer eth_blockNumber
    but reject this shape (publicnode's -32701, HTTP 405, auth/tenant
    refusals, malformed-range refusals) classify as
    :data:`GetLogsCapability.CANNOT_SERVE_LOGS`; transport/timeout/5xx
    classify as TRANSIENT so a flaky endpoint is not permanently excluded.
    When ``tip`` is unknown the probe cannot anchor a real range and returns
    TRANSIENT (never exclude on missing data).

    Args:
        endpoint: RPC endpoint URL.
        tip: Chain tip block number to anchor the probe range (None when
            unresolvable).
        timeout: Per-request timeout in seconds.

    Returns:
        The capability classification for this endpoint.
    """
    if tip is None:
        logger.debug("capability_logs_probe_no_tip", endpoint=endpoint[:60])
        return GetLogsCapability.TRANSIENT
    from_block = hex(max(0, int(tip) - 2))
    # Topic-filtered: this MUST be the production request shape. The
    # unfiltered tip-range probe was rejected by providers whose getLogs
    # limit is a RESULT cap, not a block cap (Ankr bsc: -32062 "Block range
    # is too large" for 3 unfiltered blocks in 0.27s, while the same span
    # topic-filtered answers 200). Those endpoints classified TRANSIENT ->
    # logs_capable=None -> never recorded usable, never excluded from the
    # getLogs rotation, pool metrics read 0/0 for the chain (bsc 2026-10-23).
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{"fromBlock": from_block, "toBlock": "latest", "topics": [TRANSFER_TOPIC]}],
    }
    status_code: int | None = None
    try:
        # Startup-probe burst gate (same chain as RpcClient.call).
        await provider_gates(endpoint, "eth_getLogs")
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(endpoint, json=payload)
            status_code = resp.status_code
            # HTTP auth/challenge/method-refusal statuses are capability
            # refusals regardless of the body — classify BEFORE json() so an
            # HTML Cloudflare body cannot raise into TRANSIENT.
            if status_code in CAPABILITY_HTTP_STATUSES:
                logger.debug(
                    "capability_logs_probe_http_refusal",
                    endpoint=endpoint[:60],
                    http_status=status_code,
                )
                return GetLogsCapability.CANNOT_SERVE_LOGS
            data = _decode_json_body(resp)
    except httpx.TimeoutException:
        logger.debug("capability_logs_probe_timeout", endpoint=endpoint[:60], from_block=from_block)
        return GetLogsCapability.TRANSIENT
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.debug(
            "capability_logs_probe_transient",
            endpoint=endpoint[:60],
            error=str(exc)[:80],
        )
        return GetLogsCapability.TRANSIENT

    outcome = classify_logs_capability(status_code, data)
    logger.debug(
        "capability_logs_probe",
        endpoint=endpoint[:60],
        http_status=status_code,
        outcome=outcome.value,
    )
    return outcome
