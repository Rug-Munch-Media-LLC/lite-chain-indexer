"""RPC response triage — decode/malformed handling + telemetry.

Split out of :mod:`lite_indexer.rpc.client_call` (2026-09-22) so the
call path stays under the per-file size ratchet. Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time.
"""

from __future__ import annotations

import json
from typing import Any, cast

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.logging import get_logger
from lite_indexer.rpc import status_recorder
from lite_indexer.rpc.jsonparse import decode_response_async

logger = get_logger(__name__)


async def _decode_rpc_response(
    endpoint: str,
    method: str,
    resp: httpx.Response,
    bytes_received: int,
) -> tuple[bool, dict[str, Any] | None]:
    """Empty/malformed-JSON triage. Returns ``(transient, data)``."""
    body_text = getattr(resp, "text", "")
    if not body_text or len(body_text) < 5:
        logger.debug("rpc_empty_response", endpoint=endpoint[:50], method=method)
        _facade.record_rpc_usage(endpoint, method, ok=False, error="empty_response")
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        return (True, None)
    resp.raise_for_status()
    # Malformed JSON protection (msgspec fast path when present)
    try:
        data = await decode_response_async(resp)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.debug(
            "rpc_malformed_json",
            endpoint=endpoint[:50],
            method=method,
            error=str(exc)[:60],
        )
        _facade.record_rpc_usage(endpoint, method, ok=False, error="malformed_json", bytes_received=bytes_received)
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        return (False, None)
    if not isinstance(data, dict):
        # Some RPCs return a bare JSON string/array — treat as invalid
        # instead of crashing on data.get(...) (2026-09-20).
        logger.warning("rpc_malformed_response", endpoint=endpoint[:50], method=method)
        _facade.record_rpc_usage(endpoint, method, ok=False, error="malformed_response")
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        return (False, None)
    return (False, data)


async def _process_call_response(
    endpoint: str,
    method: str,
    resp: httpx.Response,
    started: float,
) -> tuple[bool, dict[str, Any] | None]:
    """Telemetry + response handling for one completed HTTP attempt.

    Returns ``(transient, data)``: ``(True, None)`` retries the loop,
    ``(False, None)`` fails the call, ``(False, data)`` resolves it
    (JSON-RPC error payloads are returned verbatim in their slot).
    """
    from lite_indexer.rpc.autotune import record_call as autotune_record
    from lite_indexer.rpc.circuit import record_circuit_success
    from lite_indexer.rpc.throttle import record_rpc_response

    bytes_received = len(getattr(resp, "content", b"") or b"")
    record_rpc_response(endpoint, method, resp.status_code, resp.headers)
    # Every completed attempt's status, noted once here. The pool walk reads
    # it back to tell a 200-with-a-pruned-block from a real refusal, which
    # is the one distinction the whole 2026-09-26 fix turns on.
    _facade._safe_record(status_recorder.note_response_status, endpoint, resp.status_code)
    _facade._safe_record(_facade.record_provider_response, endpoint, resp.status_code, resp.headers)
    _facade.record_budget_response(endpoint, resp.status_code)
    transient, data = await _decode_rpc_response(endpoint, method, resp, bytes_received)
    if transient or data is None:
        return (transient, data)
    ok = bool("error" not in data or data.get("error") is None)
    _facade._safe_record(autotune_record, endpoint, ok=ok)
    if ok:
        _facade.record_rpc_usage(endpoint, method, ok=True, bytes_received=bytes_received)
        _facade.record_endpoint_success(endpoint)
        _facade.record_budget_success(endpoint)
        await record_circuit_success(endpoint)
    else:
        _facade.record_rpc_usage(
            endpoint,
            method,
            ok=False,
            error=str(data.get("error", ""))[:120],
            bytes_received=bytes_received,
        )
    _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=ok)
    return (False, cast(dict[str, Any], data))


def _handle_call_exception(endpoint: str, method: str, exc: BaseException) -> bool:
    """Classify one failed attempt (isinstance order mirrors the original
    except chain). Returns True when the failure is transient."""
    if isinstance(exc, httpx.TimeoutException):
        logger.debug("rpc_timeout", endpoint=endpoint[:50], method=method)
        _facade.record_rpc_usage(endpoint, method, ok=False, error="timeout")
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        return True
    if isinstance(exc, (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError)):
        logger.debug("rpc_connection_error", endpoint=endpoint[:50], method=method, error=str(exc)[:60])
        _facade.record_rpc_usage(endpoint, method, ok=False, error="connection_error")
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        logger.debug("rpc_http_error", endpoint=endpoint[:50], method=method, status=status)
        _facade.record_rpc_usage(endpoint, method, ok=False, error=f"http_{status}")
        # 429 = rate-limit: backoff only, no circuit trip/hard-ban.
        if status == 429:
            _facade.record_rate_limit(endpoint)
        # 403 = provider blacklist: hard-ban the endpoint for an
        # exponential window so we stop hammering it.
        elif status == 403:
            _facade.ban_endpoint(endpoint, reason="http_403")
        _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
        # 5xx = upstream hiccup: transient, worth a retry.
        return status >= 500
    logger.warning("rpc_call_failed", endpoint=endpoint[:50], method=method, error=str(exc)[:80])
    _facade.record_rpc_usage(endpoint, method, ok=False, error=str(exc)[:120])
    _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
    return True
