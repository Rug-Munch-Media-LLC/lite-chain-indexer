"""ClickHouse writer connection concern — lazy, thread-local clients.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) so the
writer stays under the per-file size ratchet.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class ConnectMixin:
    """Lazy ClickHouse connection lifecycle for :class:`ClickHouseWriter`."""

    def __init__(self) -> None:
        # Legacy/shared client slot: set by tests (fake injection) and by
        # the main thread on first connect. Production worker threads use
        # ``_local`` below so no two threads share one connection.
        self._client: Any = None
        # Thread-local ClickHouse client — clickhouse_driver connections are
        # NOT safe for concurrent use, so each write-pool worker keeps its
        # own (bounded by CH_WRITE_THREADS).
        self._local = threading.local()
        # Wallet-access DDL is applied once per process, only after the
        # corresponding ensure_*() call reports success (so a table that
        # appears later is picked up on the next cycle).
        self._wallet_projection_ready = False
        self._token_meta_ready = False
        self._balance_cache_ready = False
        self._holder_mv_ready = False
        self._trace_table_ready = False
        # Schema probe cache for dex_events amount columns (None = unknown).
        # See _dex_amounts_numeric / write_dex_events (2026-08-19 incident).
        self._dex_numeric_amounts: bool | None = None

    def _connect(self) -> Any | None:
        """Lazy-init a ClickHouse native protocol client for this thread.

        A shared ``self._client`` (test-injected fake, or the main thread's
        connection) wins when present; otherwise each write-pool worker
        thread gets its own client via thread-local storage. Returns None
        when ClickHouse is unreachable.
        """
        if self._client is not None:
            return self._client
        local_client = getattr(self._local, "client", None)
        if local_client is not None:
            return local_client

        ch_host = os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
        ch_port_raw = os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"
        ch_port = int(ch_port_raw)
        ch_user = os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default")
        ch_pass = os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
        ch_db = os.getenv("CH_DATABASE", "wallet_memory")

        try:
            from clickhouse_driver import Client as CHClient

            client = CHClient(
                host=ch_host,
                port=ch_port,
                user=ch_user,
                password=ch_pass,
                database=ch_db,
                connect_timeout=5,
                send_receive_timeout=30,
            )
            client.execute("SELECT 1")
            self._local.client = client
            logger.info("clickhouse_connected", host=ch_host, database=ch_db)
            return client
        except Exception as exc:
            logger.warning("clickhouse_unavailable", error=str(exc)[:100])
            return None

    def client(self) -> Any | None:
        """Return the underlying ClickHouse client (lazily connected).

        Exposes the raw client for modules that need direct SQL access
        (reconciliation, reorg staging). Returns None when ClickHouse is
        unavailable.

        Returns:
            ClickHouse client, or None when unavailable.
        """
        return self._connect()
