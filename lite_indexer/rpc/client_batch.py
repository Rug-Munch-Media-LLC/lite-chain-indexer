"""RPC batch path — JSON-RPC array POSTs with per-element fallback.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import RPC_CALL_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.batch_chunks import batch_chunks
from lite_indexer.rpc.jsonparse import decode_response_async

logger = get_logger(__name__)


async def _batch_sendable(
    endpoint: str,
    start: int,
    chunk: list[tuple[str, list[Any] | None]],
) -> list[tuple[int, str, list[Any] | None]]:
    """Filter one chunk to dispatchable elements (defer/archive gates).

    Guards are per element: deferred / non-archive items stay None.
    """
    from lite_indexer.rpc.capabilities import archive_guard_reason

    sendable: list[tuple[int, str, list[Any] | None]] = []
    for offset, (method, params) in enumerate(chunk):
        idx = start + offset
        if _facade._defer_reason_for(endpoint, method):
            continue
        if archive_guard_reason(endpoint, method):
            metrics_incr("rpc_archive_fail_closed")
            continue
        sendable.append((idx, method, params))
    return sendable


async def _match_batch(
    endpoint: str,
    sendable: list[tuple[int, str, list[Any] | None]],
    data: list[Any],
    results: list[dict[str, Any] | None],
) -> int:
    """Map batch responses back into ``results``; returns matched count."""
    by_id = {item.get("id"): item for item in data if isinstance(item, dict)}
    matched = 0
    for i, (idx, method, _p) in enumerate(sendable):
        item = by_id.get(i + 1)
        if isinstance(item, dict):
            results[idx] = item
            matched += 1
        _facade._safe_record(_facade.record_endpoint_call, endpoint, method)
        _facade._safe_record(
            _facade.record_rpc_outcome, endpoint, ok=bool(item and item.get("error") is None)
        )
    metrics_incr("rpc_batch_calls")
    return matched


class BatchMixin:
    """Batched JSON-RPC dispatch with transparent per-element fallback."""

    async def call_batch(
        self,
        endpoint: str,
        calls: list[tuple[str, list[Any] | None]],
        *,
        timeout: float = RPC_CALL_TIMEOUT,
        fallback: bool = True,
        enabled: bool | None = None,
    ) -> list[dict[str, Any] | None]:
        """Send N same-shape calls as ONE JSON-RPC array POST.

        Collapsing independent calls into a batch multiplies throughput: a
        batch counts as ONE HTTP request against the caps (paced once);
        ledger cost is still recorded per element. Elements return in
        input order. With ``fallback`` True (default), any batch-level
        failure transparently re-issues unresolved elements as individual
        :meth:`call` invocations — never lose data. Callers that cannot
        afford a per-element fallback storm (non-critical timestamp phase)
        pass ``fallback=False`` and get None slots. Per-element JSON-RPC
        errors are always returned verbatim in their slot.

        Args:
            endpoint: RPC endpoint URL.
            calls: ``(method, params)`` pairs, max ``RPC_BATCH_MAX`` per POST.
            timeout: Per-request timeout in seconds.
            fallback: When False, a failed batch yields None slots.
            enabled: Whether batching is on. ``None`` uses the configured
                ``RPC_BATCH_ENABLED`` (the production path).

        Returns:
            One raw JSON-RPC response dict per input call, or None for an
            element that could not be resolved.
        """
        if not calls:
            return []
        results: list[dict[str, Any] | None] = [None] * len(calls)
        if not (_facade.RPC_BATCH_ENABLED if enabled is None else enabled):
            if not fallback:
                return results
            return await self._call_batch_fallback(endpoint, calls, timeout)

        from lite_indexer.rpc.circuit import circuit_open

        if await circuit_open(endpoint):
            logger.debug("rpc_batch_circuit_skip", endpoint=endpoint[:50])
            return results

        for start, chunk in batch_chunks(calls, _facade.RPC_BATCH_MAX):
            sendable = await _batch_sendable(endpoint, start, chunk)
            if not sendable:
                continue
            pace_method = sendable[0][1]
            payload = [
                {"jsonrpc": "2.0", "id": i + 1, "method": m, "params": p or []}
                for i, (_idx, m, p) in enumerate(sendable)
            ]
            try:
                data = await self._post_batch(endpoint, payload, pace_method, timeout)
            except Exception as exc:  # noqa: BLE001 — batch must degrade, not fail
                logger.warning("rpc_batch_failed", endpoint=endpoint[:50], error=str(exc)[:80])
                metrics_incr("rpc_batch_fallback")
                if fallback:
                    results = await self._call_batch_fallback(endpoint, calls, timeout, results)
                continue
            if not isinstance(data, list):
                metrics_incr("rpc_batch_fallback")
                if fallback:
                    results = await self._call_batch_fallback(endpoint, calls, timeout, results)
                continue
            matched = await _match_batch(endpoint, sendable, data, results)
            if matched == 0:
                metrics_incr("rpc_batch_fallback")
                if fallback:
                    results = await self._call_batch_fallback(endpoint, calls, timeout, results)
        return results

    async def _post_batch(
        self,
        endpoint: str,
        payload: list[dict[str, Any]],
        method: str,
        timeout: float,
    ) -> Any:
        """POST one JSON-RPC batch array with throttle/cap accounting."""
        from lite_indexer.rpc.throttle import record_rpc_response, throttle_before

        account_key = _facade.account_key_for_endpoint(endpoint)
        await throttle_before(endpoint, method, max_wait=timeout)
        await _facade.provider_throttle_before(endpoint)
        await _facade._budget_before(endpoint)
        async with _facade.budget_slot(endpoint):
            async with _facade.provider_concurrency_slot(account_key):
                resp = await _facade._post_json(endpoint, payload, timeout)
        record_rpc_response(endpoint, method, resp.status_code, resp.headers)
        _facade._safe_record(
            _facade.record_provider_response, endpoint, resp.status_code, resp.headers
        )
        _facade.record_budget_response(endpoint, resp.status_code)
        if resp.status_code == 429:
            _facade.record_rate_limit(endpoint)
            raise httpx.HTTPStatusError(
                "batch rate limited", request=httpx.Request("POST", endpoint), response=resp
            )
        resp.raise_for_status()
        return await decode_response_async(resp)

    async def _call_batch_fallback(
        self,
        endpoint: str,
        calls: list[tuple[str, list[Any] | None]],
        timeout: float,
        existing: list[dict[str, Any] | None] | None = None,
    ) -> list[dict[str, Any] | None]:
        """Re-issue unresolved batch elements as individual calls.

        Elements already resolved in ``existing`` are left untouched, so a
        partial batch only pays for the missing elements.
        """
        out: list[dict[str, Any] | None] = (
            list(existing) if existing is not None else [None] * len(calls)
        )
        for i, (method, params) in enumerate(calls):
            if out[i] is None:
                out[i] = await self.call(endpoint, method, params, timeout=timeout)
        return out
