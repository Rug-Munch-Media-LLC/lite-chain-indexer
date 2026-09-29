"""Single eth_getLogs range fetch — pacing, 429 handling, error triage.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import RPC_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.rpc import proxy as rpc_proxy
from lite_indexer.rpc.client_logexc import (
    _logs_429,
    _logs_exc,
    _logs_success,
    _LogsRangeCtx,
)
from lite_indexer.rpc.client_mixin_api import MixinApi

logger = get_logger(__name__)


async def _logs_dispatch(writer: Any, ctx: _LogsRangeCtx) -> httpx.Response:
    """Telemetry-wrapped POST for one getLogs range attempt.

    The direct client keeps its own pool; ``_post_direct`` adds the
    fallback-only proxy retry. Proxy-primary mode routes through
    ``_post_json``.
    """
    from lite_indexer.rpc.throttle import record_rpc_response

    endpoint = ctx.endpoint
    _facade._safe_record(_facade.record_endpoint_call, endpoint, "eth_getLogs")
    started = time.perf_counter()
    _facade._safe_record(_facade.incr_in_flight, endpoint)
    try:
        async with _facade.budget_slot(endpoint):
            async with _facade.provider_concurrency_slot(ctx.account_key):
                if ctx.proxy_spec is None:
                    client = await writer._get_client()
                    resp = await _facade._post_direct(client, endpoint, ctx.payload, RPC_TIMEOUT)
                else:
                    resp = await _facade._post_json(endpoint, ctx.payload, RPC_TIMEOUT, proxy=ctx.proxy_spec)
    finally:
        _facade._safe_record(_facade.decr_in_flight, endpoint)
    _facade._safe_record(_facade.record_rpc_latency, endpoint, (time.perf_counter() - started) * 1000.0)
    record_rpc_response(endpoint, "eth_getLogs", resp.status_code, resp.headers)
    _facade._safe_record(_facade.record_provider_response, endpoint, resp.status_code, resp.headers)
    _facade.record_budget_response(endpoint, resp.status_code)
    return resp


async def _logs_attempt(
    writer: Any,
    ctx: _LogsRangeCtx,
    attempt: int,
) -> tuple[str, list[dict[str, Any]] | None]:
    """One getLogs range attempt; returns a (kind, logs) outcome.

    ``("retry", None)`` means the loop should re-dispatch (the wait, if
    any, already happened inside this attempt).
    """
    try:
        resp = await _logs_dispatch(writer, ctx)
        if resp.status_code == 429:
            await _logs_429(ctx.endpoint, resp, attempt)
            return ("retry", None)
        return await _logs_success(ctx.endpoint, resp)
    except (
        httpx.HTTPError,
        ConnectionError,
        OSError,
        TimeoutError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        return await _logs_exc(ctx, exc, attempt)


class LogRangeMixin:
    """Single-range eth_getLogs fetch used by the range splitter."""

    async def _fetch_logs_range(
        self: MixinApi,
        endpoint: str,
        from_block: int,
        to_block: int,
        topic: str,
        address: str | None,
        max_retries: int,
    ) -> tuple[str, list[dict[str, Any]] | None]:
        """Fetch ONE ``eth_getLogs`` range and classify the outcome.

        Returns ``("ok", logs)`` for a list/null result, ``("range", None)``
        when the provider rejected the range/size/result limit (the caller
        splits and retries in-line), and ``("error", None)`` for every
        other failure (timeout/429/auth/malformed).
        """
        from lite_indexer.rpc.throttle import throttle_before

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [self._logs_filter(from_block, to_block, topic, address)],
        }
        ctx = _LogsRangeCtx(
            endpoint=endpoint,
            payload=payload,
            proxy_spec=rpc_proxy.proxy_for_endpoint(endpoint),
            account_key=_facade.account_key_for_endpoint(endpoint),
            max_retries=max_retries,
        )
        # Identity budget + proxy: recorded once per range (retries below do
        # not re-count against the budget).
        await _facade._budget_before(endpoint)
        for attempt in range(max_retries):
            # Apply provider rate limiting before request
            await _facade.apply_provider_rate_limit(endpoint)
            # Intelligent throttle: Retry-After / budget / per-method pacing.
            await throttle_before(endpoint, "eth_getLogs")
            # Provider (account) gate: account-level Retry-After / rps budget.
            await _facade.provider_throttle_before(endpoint)
            outcome = await _logs_attempt(self, ctx, attempt)
            if outcome[0] != "retry":
                return outcome
        return ("error", None)
