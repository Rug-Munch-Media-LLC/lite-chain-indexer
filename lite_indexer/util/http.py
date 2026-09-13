"""Shared httpx connection-pool limits for every long-lived client.

Every per-instance ``httpx.AsyncClient`` in this repo used to fall back
to httpx's default pool (100 connections) or an ad-hoc value, and none
carried an explicit, centrally tunable ceiling. On 2026-09-13 the daemon
held 150 open sockets. Routing all client construction through
:func:`http_limits` bounds the pool per client and keeps the knobs in
one place (env-tunable via the HTTP_* config constants).
"""

from __future__ import annotations

import httpx

from lite_indexer.config import (
    HTTP_KEEPALIVE_EXPIRY,
    HTTP_MAX_CONNECTIONS,
    HTTP_MAX_KEEPALIVE_CONNECTIONS,
)


def http_limits(
    *,
    max_connections: int | None = None,
    max_keepalive_connections: int | None = None,
) -> httpx.Limits:
    """Build explicit, env-tunable HTTP connection-pool limits.

    Args:
        max_connections: Override for the total pool size.
        max_keepalive_connections: Override for idle keep-alive sockets.

    Returns:
        ``httpx.Limits`` with both ceilings set.
    """
    return httpx.Limits(
        max_connections=HTTP_MAX_CONNECTIONS if max_connections is None else max_connections,
        max_keepalive_connections=(
            HTTP_MAX_KEEPALIVE_CONNECTIONS if max_keepalive_connections is None else max_keepalive_connections
        ),
        keepalive_expiry=HTTP_KEEPALIVE_EXPIRY,
    )
