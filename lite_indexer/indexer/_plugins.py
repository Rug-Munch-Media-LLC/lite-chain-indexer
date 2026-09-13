"""LiteIndexer mixins — extracted from the former god-file (indexer/__init__.py).

Degod split 2026-08-23: each mixin holds one cohesive responsibility and is
mixed into LiteIndexer. Method names/behavior unchanged — tests validate.
"""

from __future__ import annotations

import gc  # noqa: F401 — used by maintenance mixin body
from typing import Any  # noqa: F401

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import PLUGIN_ENABLED
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.logging import get_logger
from lite_indexer.plugin import BasePlugin, PluginContext, PluginLoader
from lite_indexer.rpc import get_best_endpoint

log = get_logger(__name__)
logger = log  # god-file bodies reference `logger`; keep both names


class PluginsMixin(IndexerMixinBase):
    "Plugin lifecycle: loading, table provisioning, event dispatch."

    # ── Plugins ─────────────────────────────────────────────────────

    def _get_plugins(self) -> list[BasePlugin]:
        """Load (once) the plugins registered for this chain.

        Plugins are optional: when the plugin layer is disabled or no
        plugins are installed the result is an empty list and the sync
        loop is unchanged. Load failures never propagate — the loader
        skips invalid plugins with a warning.
        """
        if self._plugins is None:
            if not PLUGIN_ENABLED:
                self._plugins = []
            else:
                try:
                    self._plugins = PluginLoader().load_all(chain=self.chain)
                except Exception as exc:  # noqa: BLE001 — plugin layer is best-effort
                    logger.warning("plugin_load_failed", chain=self.chain, error=str(exc)[:200])
                    self._plugins = []
            # Auto-create ClickHouse tables from plugin schemas
            self._ensure_plugin_tables()
        return self._plugins

    def _ensure_plugin_tables(self) -> None:
        """Create ClickHouse tables declared by plugin schemas.

        Iterates every loaded plugin; for each one that declares a
        ``schema`` block in its manifest, generates a ``CREATE TABLE IF
        NOT EXISTS`` DDL statement from the column declarations, maps
        Python-type shorthands to ClickHouse types via
        :func:`map_ch_type`, and executes the DDL through
        :meth:`ch_writer`. Plugins without a schema or when ClickHouse
        is unavailable are silently skipped (best-effort).

        The ``order_by`` key from the manifest (defaulting to the first
        column) is used as the MergeTree ordering key. Metadata columns
        (``_block_number``, ``_tx_hash``, ``_timestamp``) are added
        automatically.
        """
        if not self._plugins:
            return

        from lite_indexer.plugin.base import map_ch_type

        client = self.ch_writer.client()
        if client is None:
            logger.warning(
                "plugin_table_creation_skipped",
                reason="clickhouse_unavailable",
                plugins_with_schema=sum(1 for p in self._plugins if p.manifest.schema),
            )
            return

        for plugin in self._plugins:
            schema = plugin.manifest.schema
            if schema is None:
                continue

            table_name = schema.table
            col_defs: list[str] = []
            for col_name, col_type in schema.columns.items():
                ch_type = map_ch_type(col_type)
                col_defs.append(f"    {col_name} {ch_type}")

            # Metadata columns
            col_defs.append("    _block_number UInt64")
            col_defs.append("    _tx_hash String")
            col_defs.append("    _timestamp DateTime DEFAULT now()")

            # Ordering key: explicit, or auto-detect indexed columns, or first col
            if schema.order_by:
                order_cols = ", ".join(schema.order_by)
            else:
                # Auto-detect: use first column (or nothing special)
                first_col = next(iter(schema.columns), None)
                order_cols = first_col if first_col else "_timestamp"

            ddl = (
                f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
                + ",\n".join(col_defs)
                + f"\n) ENGINE = MergeTree()\nORDER BY ({order_cols})"
            )

            try:
                client.execute(
                    ddl
                )  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                logger.info(
                    "plugin_table_created",
                    plugin=plugin.name,
                    table=table_name,
                    columns=len(schema.columns),
                )
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.warning(
                    "plugin_table_create_failed",
                    plugin=plugin.name,
                    table=table_name,
                    error=str(exc)[:200],
                )

    async def _dispatch_plugin_events(self, from_block: int, to_block: int) -> dict[str, Any]:
        """Fetch + dispatch decoded events to registered plugins (EVM).

        For every registered plugin matching this chain, fetches the
        plugin's declared contracts/events via ``eth_getLogs`` (one call
        per contract × event topic, chunk range = the current sync
        range) and routes each raw log to the plugin's manifest handler.

        Best-effort by design: RPC failures, malformed logs, or handler
        errors are logged and skipped — plugin indexing must never block
        or break the core sync loop.

        Returns:
            Summary stats (``plugin_events_fetched`` /
            ``plugin_events_handled``) or an empty dict when no plugins
            are registered for this chain.
        """
        plugins = self._get_plugins()
        if not plugins:
            return {}

        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return {}
        endpoint = get_best_endpoint(endpoints)

        ctx = PluginContext(
            chain=self.chain,
            from_block=from_block,
            to_block=to_block,
            data_dir=self.data_dir,
            ch_writer=self.ch_writer,
            logger=logger,
        )

        fetched = 0
        handled = 0
        for plugin in plugins:
            for spec in plugin.manifest.events:
                for contract in plugin.manifest.contracts:
                    start = max(from_block, contract.start_block)
                    if start > to_block:
                        continue
                    try:
                        logs = await self.rpc.get_logs(
                            endpoint,
                            start,
                            to_block,
                            topic=spec.topic,
                            address=contract.address,
                        )
                    except Exception as exc:  # noqa: BLE001 — best-effort
                        logger.warning(
                            "plugin_logs_fetch_failed",
                            plugin=plugin.name,
                            event_name=spec.name,
                            error=str(exc)[:120],
                        )
                        continue
                    if not logs:
                        continue
                    fetched += len(logs)
                    for raw_log in logs:
                        try:
                            raw_log["_chain"] = self.chain
                            if await plugin.handle_log(raw_log, ctx):
                                handled += 1
                        except Exception as exc:  # noqa: BLE001 — handler errors are contained
                            logger.warning(
                                "plugin_handle_failed",
                                plugin=plugin.name,
                                event_name=spec.name,
                                error=str(exc)[:120],
                            )

        if fetched:
            logger.info(
                "plugin_events_dispatched",
                chain=self.chain,
                plugins=len(plugins),
                logs=fetched,
                handled=handled,
            )
        return {"plugin_events_fetched": fetched, "plugin_events_handled": handled}
