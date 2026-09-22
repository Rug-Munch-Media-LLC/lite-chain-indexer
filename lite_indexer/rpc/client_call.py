"""RPC single-call path — gates, pacing, retry loop, response triage.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import (
    RPC_CALL_TIMEOUT,
    RPC_RETRY_ATTEMPTS,
    RPC_RETRY_BASE_DELAY_S,
    RPC_RETRY_MAX_DELAY_S,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.client_response import _handle_call_exception, _process_call_response

logger = get_logger(__name__)


@dataclass(slots=True)
class _CallCtx:
    """Everything one dispatch attempt needs (keeps helpers ≤5 params)."""

    endpoint: str
    method: str
    payload: dict[str, Any]
    timeout: float
    account_key: str
    started: float


async def _call_gates(endpoint: str, method: str) -> bool:
    """Quota-degrade, archive fail-closed and circuit gates.

    Returns True when the call is blocked; every block is logged/metered
    here exactly as the inline gates were (None is the failure shape).
    """
    reason = _facade._defer_reason_for(endpoint, method)
    if reason:
        _facade._warn_deferred(endpoint, method, reason)
        return True
    from lite_indexer.rpc.capabilities import archive_guard_reason

    archive_reason = archive_guard_reason(endpoint, method)
    if archive_reason:
        metrics_incr("rpc_archive_fail_closed")
        logger.warning(
            "rpc_archive_fail_closed",
            endpoint=endpoint[:60],
            method=method,
            reason=archive_reason,
        )
        return True
    from lite_indexer.rpc.circuit import circuit_open

    if await circuit_open(endpoint):
        logger.debug("rpc_circuit_skip", endpoint=endpoint[:50], method=method)
        return True
    return False


async def _pace_call(endpoint: str, method: str) -> None:
    """Throttles: per-method pacing, provider gate, auto-tune, budget.

    Provider (account) gate runs ONCE before the retry loop — retries
    don't re-pace.
    """
    from lite_indexer.rpc.autotune import autotune_wait
    from lite_indexer.rpc.throttle import throttle_before

    await throttle_before(endpoint, method)
    await _facade.provider_throttle_before(endpoint)
    await autotune_wait(endpoint)
    await _facade._budget_before(endpoint)


async def _call_attempt(ctx: _CallCtx) -> tuple[bool, dict[str, Any] | None]:
    """One dispatch attempt: telemetry in/out, POST, response triage."""
    _facade._safe_record(_facade.record_endpoint_call, ctx.endpoint, ctx.method)
    _facade._safe_record(_facade.incr_in_flight, ctx.endpoint)
    try:
        async with _facade.budget_slot(ctx.endpoint):
            async with _facade.provider_concurrency_slot(ctx.account_key):
                resp = await _facade._post_json(ctx.endpoint, ctx.payload, ctx.timeout)
        _facade._safe_record(
            _facade.record_rpc_latency, ctx.endpoint, (time.perf_counter() - ctx.started) * 1000.0
        )
        return await _process_call_response(ctx.endpoint, ctx.method, resp, ctx.started)
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        return (_handle_call_exception(ctx.endpoint, ctx.method, exc), None)
    finally:
        _facade._safe_record(_facade.decr_in_flight, ctx.endpoint)


class CallMixin:
    """Single-call JSON-RPC path with gates, pacing and transient retry."""

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = RPC_CALL_TIMEOUT,
    ) -> dict[str, Any] | None:
        """Make a JSON-RPC call to an endpoint.

        Records per-endpoint usage/cost in the shared RPC usage ledger
        (which endpoints actually serve data vs. error).

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method (e.g., 'eth_blockNumber').
            params: Method parameters.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        if await _call_gates(endpoint, method):
            return None
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        # Per-account (provider) cap: queue when the provider's concurrency /
        # requests-per-second budget is exhausted, and honor the account-level
        # Retry-After window — providers ban the ACCOUNT, not one URL.
        ctx = _CallCtx(
            endpoint=endpoint,
            method=method,
            payload=payload,
            timeout=timeout,
            account_key=_facade.account_key_for_endpoint(endpoint),
            started=time.perf_counter(),
        )
        await _pace_call(endpoint, method)

        # Exponential backoff retry for TRANSIENT failures only (timeouts,
        # connection errors, 5xx, empty responses): 0.1s → 0.2s → 0.4s,
        # max RPC_RETRY_ATTEMPTS attempts. Non-transient outcomes return
        # immediately. All attempts failing returns None — the existing
        # failure shape — and hedging keeps doing its job above.
        max_attempts = RPC_RETRY_ATTEMPTS
        for attempt in range(1, max_attempts + 1):
            transient, data = await _call_attempt(ctx)
            if not transient:
                return data
            from lite_indexer.rpc.circuit import record_circuit_failure

            await record_circuit_failure(endpoint)
            if attempt < max_attempts:
                from lite_indexer.rpc.backoff import exponential_backoff

                # AWS full jitter: uniform(0, min(cap, base*2**attempt)).
                # Explicit Retry-After windows (429) stay exact above.
                delay = exponential_backoff(
                    attempt - 1,
                    base=RPC_RETRY_BASE_DELAY_S,
                    cap=RPC_RETRY_MAX_DELAY_S,
                )
                logger.debug(
                    "rpc_retry",
                    endpoint=endpoint[:50],
                    method=method,
                    attempt=attempt,
                    next_delay_s=round(delay, 4),
                )
                await asyncio.sleep(delay)
        return None
