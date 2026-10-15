"""RPC transport glue — safe telemetry, budget pacing, proxy-aware POST.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22) so the request
path stays under the per-file size ratchet. Patchable collaborators are
resolved through the ``lite_indexer.rpc.client`` facade module at call
time, so tests patching facade attributes keep working unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import RPC_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import proxy as rpc_proxy
from lite_indexer.rpc import user_agent
from lite_indexer.util.http import http_limits

logger = get_logger(__name__)


class TransportMixin:
    """Shared httpx client lifecycle for :class:`RpcClient`."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=http_limits(),
                **user_agent.client_kwargs(),
            )
        return self._client

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent)."""
        client, self._client = self._client, None
        if client is not None and not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()


_TELEMETRY_FAILS: list[float] = []
_TELEMETRY_WARN_EVERY_S = 60.0


def _safe_record(recorder: Any, *args: Any, **kwargs: Any) -> None:
    """Run an additive ledger/routing telemetry call without breaking RPC.

    The quota ledger (``rpc/ledger.py``) and EWMA/staleness trackers
    (``rpc/routing.py``) are best-effort observability layers: if a
    recorder ever raises (e.g. a corrupt ledger file), the error is
    logged and the RPC result is returned untouched.

    WHY THIS IS NO LONGER logger.debug (2026-10-15): it was, and that hid a
    real breakage. The quota ledger on this host held 172 accounts with only
    ONE recording any usage while the RPC access logs clearly showed calls to
    Alchemy/dRPC/Infura — the recorder was failing and logging at DEBUG, so
    the ledger looked merely "unused" instead of broken. A quota ledger that
    silently records nothing cannot warn before a monthly quota is exhausted;
    it is worse than absent, because it looks like a working control.

    A telemetry failure is expected to be rare, so warn (rate-limited to once
    per _TELEMETRY_WARN_EVERY_S) rather than debug, and count every failure so
    a caller/health check can assert the recorder is actually working.
    """
    try:
        recorder(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — telemetry must never raise into the RPC path
        import time as _t

        now = _t.time()
        if not _TELEMETRY_FAILS or (now - _TELEMETRY_FAILS[-1]) >= _TELEMETRY_WARN_EVERY_S:
            logger.warning(
                "rpc_telemetry_skipped",
                error=str(exc)[:120],
                recorder=getattr(recorder, "__name__", str(recorder)),
                total_failures=len(_TELEMETRY_FAILS) + 1,
            )
        _TELEMETRY_FAILS.append(now)


async def _budget_before(endpoint: str) -> None:
    """Honor the identity budget: pace to rps, then record the dispatch."""
    wait = _facade.budget_wait_seconds(endpoint)
    if wait > 0:
        logger.debug("rpc_budget_wait", endpoint=endpoint[:50], wait_s=round(wait, 2))
        await asyncio.sleep(wait)
    _facade.record_budget_dispatch(endpoint)


async def _post_direct(
    client: httpx.AsyncClient,
    endpoint: str,
    payload: Any,
    timeout: float,
) -> httpx.Response:
    """POST directly, retrying ONCE via a proxy after a transport failure.

    Only active in ``RPC_PROXY_FALLBACK_ONLY`` mode (``fallback_proxy_for``
    returns None otherwise). The passed client carries its timeout.
    """
    try:
        return await client.post(endpoint, json=payload)
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        fallback = rpc_proxy.fallback_proxy_for(endpoint)
        if fallback is None:
            raise
        logger.warning(
            "rpc_proxy_fallback_used",
            endpoint=endpoint[:50],
            proxy=fallback.label,
            error=str(exc)[:80],
        )
        metrics_incr("rpc_proxy_fallback_used")
        proxy_client = await _facade._get_call_client(timeout, proxy=fallback)
        return await proxy_client.post(endpoint, json=payload)
    except RuntimeError as exc:
        if not _facade._is_closed_client_error(exc):
            raise
        # Self-heal: the pooled client was closed under us (LRU eviction /
        # shutdown race). Drop it, recreate once, retry the same request.
        logger.warning("rpc_client_recreated", endpoint=endpoint[:50])
        metrics_incr("rpc_client_recreated")
        _facade._drop_call_client(timeout, None)
        return await (await _facade._get_call_client(timeout)).post(endpoint, json=payload)


async def _post_json(
    endpoint: str,
    payload: Any,
    timeout: float,
    *,
    proxy: rpc_proxy.ProxySpec | None = None,
) -> httpx.Response:
    """POST a JSON body, routing through the residency proxy when enabled.

    Selected by ``RPC_PROXY_FALLBACK_ONLY``: proxy-primary (default, ONE
    direct retry on proxy failure — ``rpc_proxy_fallback_direct``) or
    direct-primary / fallback-only (ONE proxy retry —
    ``rpc_proxy_fallback_used``), the quota-safe default for the scheduler.
    """
    spec = proxy if proxy is not None else rpc_proxy.proxy_for_endpoint(endpoint)
    if spec is None:
        return await _facade._post_direct(await _facade._get_call_client(timeout), endpoint, payload, timeout)
    try:
        client = await _facade._get_call_client(timeout, proxy=spec)
        return await client.post(endpoint, json=payload)
    except (httpx.ProxyError, RuntimeError, httpx.TransportError) as exc:
        if isinstance(exc, httpx.ProxyError):
            logger.warning(
                "rpc_proxy_fallback_direct",
                endpoint=endpoint[:50],
                proxy=spec.label,
                error=str(exc)[:80],
            )
            rpc_proxy.mark_proxy_failed(endpoint, spec)
        elif _facade._is_closed_client_error(exc):
            # Self-heal: pooled proxy client closed under us (LRU eviction /
            # TLS teardown). Drop it, recreate once, retry the same request.
            logger.warning("rpc_client_recreated", endpoint=endpoint[:50], proxy=spec.label)
            metrics_incr("rpc_client_recreated")
            _facade._drop_call_client(timeout, spec)
            return await (await _facade._get_call_client(timeout, proxy=spec)).post(endpoint, json=payload)
        else:
            raise
        client = await _facade._get_call_client(timeout)
        return await client.post(endpoint, json=payload)
