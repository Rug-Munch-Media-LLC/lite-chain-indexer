"""
Pooled HTTP clients for RPC calls.

Extracted from :mod:`lite_indexer.rpc.client` (2026-09-20) so the request
logic and the connection pool live in separate modules. ``client``
re-exports every name here, so existing importers stay stable.
"""

from __future__ import annotations

import contextlib
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.util.http import http_limits

if TYPE_CHECKING:
    from lite_indexer.rpc import proxy as rpc_proxy

logger = get_logger(__name__)


# ── Pooled HTTP Clients ────────────────────────────────────────────────
# Reuse a small set of AsyncClients across all RpcClient instances, keyed
# by timeout (and proxy) so callers don't fight over one value. Creation
# is synchronous — no await between lookup and assignment.
#
# BOUNDED (2026-09-13): capped at RPC_CALL_CLIENTS_MAX with LRU eviction;
# an evicted client is closed. A client closed under an in-flight request
# self-heals via _is_closed_client_error → drop + recreate + retry.
_call_clients: OrderedDict[tuple[float, str | None], httpx.AsyncClient] = OrderedDict()


def _is_closed_client_error(exc: BaseException) -> bool:
    """Whether ``exc`` means the pooled client/transport was closed under us.

    Covers httpx's "client has been closed" RuntimeError, anyio's
    ClosedResourceError (proxy TLS teardown) and httpx transport closes.
    """
    if isinstance(exc, httpx.TransportError):
        return True
    name = type(exc).__name__
    if name in ("ClosedResourceError", "BrokenResourceError"):
        return True
    return isinstance(exc, RuntimeError) and "client has been closed" in str(exc).lower()


def _drop_call_client(timeout: float, proxy: rpc_proxy.ProxySpec | None) -> None:
    """Evict a (possibly closed/broken) pooled client so the next call recreates it."""
    with contextlib.suppress(KeyError):
        del _call_clients[(timeout, proxy.url if proxy is not None else None)]


async def _get_call_client(timeout: float, proxy: rpc_proxy.ProxySpec | None = None) -> httpx.AsyncClient:
    """Get a pooled module-level httpx client for the given timeout/proxy.

    Capped at :data:`RPC_CALL_CLIENTS_MAX` with LRU eviction (evicted
    clients are closed). Explicit connection limits keep sockets bounded.
    A proxy whose optional dependency is missing degrades to a direct
    client (logged) rather than failing the call.
    """
    # Read the cap from ``client`` at call time: it is re-exported there and
    # tests patch it on that module, keeping the historical knob import-stable
    # without a module-level import cycle (client imports this module).
    from lite_indexer.rpc.client import RPC_CALL_CLIENTS_MAX

    key = (timeout, proxy.url if proxy is not None else None)
    client = _call_clients.get(key)
    if client is not None and not client.is_closed:
        _call_clients.move_to_end(key)
        return client
    kwargs: dict[str, Any] = {"timeout": httpx.Timeout(timeout), "limits": http_limits()}
    if proxy is not None:
        kwargs["proxy"] = proxy.url
    try:
        client = httpx.AsyncClient(**kwargs)
    except (ImportError, ValueError) as exc:
        # e.g. a socks5 proxy without the optional socks transport installed.
        logger.warning("rpc_proxy_client_unavailable", proxy=proxy.label if proxy else "", error=str(exc)[:80])
        kwargs.pop("proxy", None)
        client = httpx.AsyncClient(**kwargs)
    _call_clients[key] = client
    _call_clients.move_to_end(key)
    while len(_call_clients) > RPC_CALL_CLIENTS_MAX:
        _, evicted = _call_clients.popitem(last=False)
        with contextlib.suppress(Exception):
            await evicted.aclose()
    return client


async def aclose_call_clients() -> None:
    """Close and clear every pooled call client (shutdown / tests)."""
    clients = list(_call_clients.values())
    _call_clients.clear()
    for client in clients:
        if not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()
