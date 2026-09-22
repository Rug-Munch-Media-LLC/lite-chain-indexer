"""eth_getLogs outcome handling — 429 waits, success triage, errors.

Split out of :mod:`lite_indexer.rpc.client_logrange` (2026-09-22) so
the range path stays under the per-file size ratchet. Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import RATE_LIMIT_WAIT_CAP
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.jsonparse import decode_response_async

logger = get_logger(__name__)


@dataclass(slots=True)
class _LogsRangeCtx:
    """One eth_getLogs range dispatch (keeps helper signatures ≤5 params)."""

    endpoint: str
    payload: dict[str, Any]
    proxy_spec: Any
    account_key: str
    max_retries: int


async def _logs_429(endpoint: str, resp: httpx.Response, attempt: int) -> None:
    """Respect an explicit Retry-After EXACTLY; otherwise use full-jitter
    exponential backoff. Rate limits trigger backoff ONLY — never the
    circuit breaker or a hard ban."""
    from lite_indexer.rpc.backoff import exponential_backoff
    from lite_indexer.rpc.throttle import RATE_TRACKER, extract_rate_limit

    retry_after = extract_rate_limit(resp.headers or {}).get("retry_after")
    if retry_after is not None and retry_after > 0:
        state = RATE_TRACKER.state_for(endpoint)
        backoff_until = state.get("backoff_until", 0.0)
        wait = max(0.0, backoff_until - time.time()) if backoff_until else float(retry_after)
    else:
        wait = exponential_backoff(attempt, base=1.0, cap=float(RATE_LIMIT_WAIT_CAP))
    logger.debug(
        "rate_limited_retry",
        endpoint=endpoint[:40],
        attempt=attempt,
        wait=round(wait, 1),
    )
    _facade.record_rate_limit(endpoint)
    _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
    await asyncio.sleep(wait)


async def _logs_success(
    endpoint: str, resp: httpx.Response
) -> tuple[str, list[dict[str, Any]] | None]:
    """raise_for_status + decode + classify a non-429 getLogs response.

    Returns ``("ok", logs)`` for a list/null result and
    ``("range"/"error", None)`` classifications otherwise.
    """
    from lite_indexer.rpc.range_split import classify_logs_error

    resp.raise_for_status()
    data = await decode_response_async(resp)
    if isinstance(data, dict) and "result" in data:
        result = data["result"]
        if isinstance(result, list):
            _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=True)
            _facade.record_budget_success(endpoint)
            return ("ok", result)
        # result is null — endpoint works but no logs in range
        if result is None:
            _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=True)
            _facade.record_budget_success(endpoint)
            return ("ok", [])
    kind = classify_logs_error(data)
    _facade._safe_record(_facade.record_rpc_outcome, endpoint, ok=False)
    return (kind, None)


async def _logs_exc(
    ctx: _LogsRangeCtx,
    exc: BaseException,
    attempt: int,
) -> tuple[str, list[dict[str, Any]] | None]:
    """Classify a failed getLogs attempt; sleeps 1s when retrying.

    Returns ``("retry", None)`` to re-loop, ``("range", None)`` when the
    provider rejected the range over HTTP (the splitter re-chunks
    in-line), and ``("error", None)`` for every other final failure.
    """
    from lite_indexer.rpc.range_split import is_range_error_message

    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        logger.debug(
            "rpc_http_error",
            endpoint=ctx.endpoint[:40],
            status=status,
        )
        if is_range_error_message(getattr(exc.response, "text", "") or ""):
            _facade._safe_record(_facade.record_rpc_outcome, ctx.endpoint, ok=False)
            return ("range", None)
        _facade.record_endpoint_failure(ctx.endpoint)
        _facade._safe_record(_facade.record_rpc_outcome, ctx.endpoint, ok=False)
    elif isinstance(
        exc, (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError)
    ):
        logger.debug(
            "rpc_connection_error",
            endpoint=ctx.endpoint[:40],
            error=str(exc)[:60],
        )
        _facade.record_endpoint_failure(ctx.endpoint)
        metrics_incr("rpc_errors")
        _facade._safe_record(_facade.record_rpc_outcome, ctx.endpoint, ok=False)
    else:
        logger.warning(
            "rpc_unexpected_error",
            endpoint=ctx.endpoint[:40],
            error=str(exc)[:60],
        )
        _facade._safe_record(_facade.record_rpc_outcome, ctx.endpoint, ok=False)
    if attempt < ctx.max_retries - 1:
        await asyncio.sleep(1)
        return ("retry", None)
    return ("error", None)
