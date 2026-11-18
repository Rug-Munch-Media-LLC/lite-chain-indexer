"""The assembled durable cache store.

Composes the cohesive cache modules into the single
:class:`ResponseCacheDB` type the RPC layer talks to:

* :class:`~lite_indexer.rpc.cache_conn.CacheConn` — connection/lifecycle
* :class:`~lite_indexer.rpc.cache_rows.CacheRowsMixin` — payload rows
* :class:`~lite_indexer.rpc.cache_maint.CacheMaintMixin` — invalidation/stats
* :class:`~lite_indexer.rpc.cache_gating.GatingStateMixin` — gating state
"""

from __future__ import annotations

from lite_indexer.rpc.cache_gating import GatingStateMixin
from lite_indexer.rpc.cache_maint import CacheMaintMixin
from lite_indexer.rpc.cache_rows import CacheRowsMixin


class ResponseCacheDB(CacheRowsMixin, CacheMaintMixin, GatingStateMixin):
    """Durable SQLite cache store. All methods are exception-safe."""


__all__ = ["ResponseCacheDB"]
