"""Schema management mixin for the ClickHouse writer.

Split from storage/clickhouse.py (de-godding 2026-08-23). All methods
operate on ``self`` (client access via ``self._connect()`` /
``self.client()``), initialized by :class:`ClickHouseWriter.__init__`.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class SchemaMixin:
    """Idempotent column/projection/TTL DDL for writer tables."""

    def _ensure_source_column(self, client: Any, table: str) -> None:
        """Ensure source column exists in ClickHouse table."""
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s AND name = 'source'",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source String DEFAULT ''")
            logger.info("source_column_added", table=table)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("source_column_skip", error=str(exc)[:80])

    def _ensure_raw_payload_column(self, client: Any, table: str) -> None:
        """Ensure raw_payload exists so writers can name it in INSERTs.

        Older deployments predate the column, so add it idempotently with
        ``DEFAULT ''`` — historical rows stay valid and every new row
        carries the serialized raw RPC payload when the parser emitted one.
        Mirrors ``_ensure_source_column`` (migrations/010 adds the same
        column to ``wallet_memory.transactions`` for operator provisioning).
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s AND name = 'raw_payload'",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS raw_payload String DEFAULT ''")
            logger.info("raw_payload_column_added", table=table)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("raw_payload_column_skip", error=str(exc)[:80])

    def _ensure_dex_usd_columns(self, client: Any) -> None:
        """Ensure dex_events has price_usd/volume_usd columns.

        Whale detection + USD normalization (pricing.py) write the USD
        price and notional onto every DEX row at ingestion time. Older
        deployments predate the columns, so add them idempotently with
        ``DEFAULT 0`` — historical rows stay valid and every new row
        carries the USD enrichment.
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s "
                "AND name IN ('price_usd', 'volume_usd')",
                {
                    "database": CH_DEX_TABLE.split(".")[0],
                    "table": CH_DEX_TABLE.split(".")[-1],
                },
            )
            existing = {r[0] for r in result}
            for column, after in (("price_usd", "whale"), ("volume_usd", "price_usd")):
                if column not in existing:
                    client.execute(
                        f"ALTER TABLE {CH_DEX_TABLE} ADD COLUMN IF NOT EXISTS {column} Float64 DEFAULT 0 AFTER {after}"
                    )
                    logger.info("dex_usd_column_added", table=CH_DEX_TABLE, column=column)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("dex_usd_columns_skip", error=str(exc)[:80])

    def _ensure_deployer_columns(self, client: Any) -> None:
        """Ensure deployer_events has token0/token1/pair_address/deployer_eoa columns.

        F0 data-quality fix: deployer_events must store the resolved EOA
        (deployer / deployer_eoa) plus the pair's token0/token1 and the
        pair/pool address itself (pair_address) so the table no longer
        mislabels factory→deployer and pair→token_address.
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns WHERE database = %(database)s AND table = %(table)s",
                {
                    "database": CH_DEPLOYER_TABLE.split(".")[0],
                    "table": CH_DEPLOYER_TABLE.split(".")[-1],
                },
            )
            existing = {r[0] for r in result}
            for column, after in (
                ("token0", "token_address"),
                ("token1", "token0"),
                ("pair_address", "token1"),
                ("deployer_eoa", "deployer"),
            ):
                if column not in existing:
                    client.execute(
                        f"ALTER TABLE {CH_DEPLOYER_TABLE} "
                        f"ADD COLUMN IF NOT EXISTS {column} String DEFAULT '' AFTER {after}"
                    )
                    logger.info("deployer_column_added", table=CH_DEPLOYER_TABLE, column=column)
        except Exception as exc:
            if "privileges" in str(exc).lower():
                return
            logger.debug("deployer_columns_skip", error=str(exc)[:80])

    def _ensure_solana_owner_column(self, client: Any) -> None:
        """Ensure solana_spl_balance_changes has the owner column.

        F1 wallet resolution: the SPL token account's on-chain ``owner``
        (the wallet that holds the token) is captured from the pre/post
        token balances of each transaction. Old deployments predate the
        column, so add it idempotently with ``DEFAULT ''`` — historical
        rows stay valid (empty owner) and every new row carries the real
        wallet address.
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns WHERE database = %(database)s AND table = %(table)s AND name = 'owner'",
                {
                    "database": SOLANA_CH_TABLE.split(".")[0],
                    "table": SOLANA_CH_TABLE.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(f"ALTER TABLE {SOLANA_CH_TABLE} ADD COLUMN IF NOT EXISTS owner String DEFAULT '' AFTER mint")
            logger.info("solana_owner_column_added", table=SOLANA_CH_TABLE)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("solana_owner_column_skip", error=str(exc)[:80])

    # ── Column Pruning (TTL) ─────────────────────────────────────────

    def _column_types(self, client: Any, table: str) -> dict[str, str]:
        """Return {column: ClickHouse type} for a table from system.columns.

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.

        Returns:
            Dict of column name → type string.
        """
        try:
            result = client.execute(
                "SELECT name, type FROM system.columns WHERE database = %(database)s AND table = %(table)s",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            return {r[0]: r[1] for r in result}
        except Exception as exc:
            logger.debug("column_types_failed", table=table, error=str(exc)[:80])
            return {}

    def _degrade_column_ttl(self, client: Any, table: str, column: str, days: int) -> None:
        """Set a column-level TTL so the value resets to its default.

        When the TTL lapses, ClickHouse replaces the column value with
        the default value of its data type ('' for String, 0 for numerics),
        which is exactly the "degrade to summary" behavior.

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            column: Column name to degrade.
            days: Retention days (TTL interval).
        """
        try:
            types = self._column_types(client, table)
            col_type = types.get(column)
            if col_type is None:
                return
            client.execute(
                f"ALTER TABLE {table} "
                f"MODIFY COLUMN IF EXISTS {column} {col_type} "
                f"TTL block_timestamp + INTERVAL {days} DAY"
            )
            logger.info("column_ttl_set", table=table, column=column, days=days)
        except Exception as exc:
            if "privileges" in str(exc).lower():
                return
            logger.debug("column_ttl_skip", table=table, column=column, error=str(exc)[:80])

    def _ensure_summary_projection(self, client: Any, table: str, summary_columns: tuple[str, ...]) -> None:
        """Add a summary projection over the columns scanners actually use.

        The projection materializes a compact, well-ordered copy of the
        summary columns so aggregation queries can skip the heavy detail
        columns entirely.

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            summary_columns: Columns to include in the projection.
        """
        if not summary_columns:
            return
        try:
            self._column_types(client, table)  # ensure table/column introspection works
            projection_name = f"{table.split('.')[-1]}_summary"
            select = ", ".join(summary_columns)
            client.execute(
                f"ALTER TABLE {table} "
                f"ADD PROJECTION IF NOT EXISTS {projection_name} "
                f"(SELECT {select} ORDER BY block_timestamp)"
            )
            logger.info("summary_projection_ensured", table=table, projection=projection_name)
        except Exception as exc:
            if "privileges" in str(exc).lower():
                return
            logger.debug("summary_projection_skip", table=table, error=str(exc)[:80])

    def ensure_ttl_policies(self, client: Any) -> None:
        """Apply retention + column-degradation policies to CH tables.

        Idempotent and non-fatal: every DDL is wrapped so a missing
        table, column, or lacking privileges degrades to a debug log.
        Called on first write of each cycle when ``CH_TTL_ENABLED``.

        Args:
            client: ClickHouse client.
        """
        # evm_transfers: full precision for 30 days, then degrade
        # token_symbol/token_name (heavy String columns) to defaults.
        days = RETENTION_DAYS.get("evm_transfers", 30)
        for column in ("token_symbol", "token_name"):
            self._degrade_column_ttl(client, CH_TABLE, column, days)
        self._ensure_summary_projection(client, CH_TABLE, EVT_SUMMARY_COLUMNS)

        # dex_events: keep swap details for 90 days, then degrade the
        # detail columns (sender/recipient/amounts) keeping identity.
        dex_days = RETENTION_DAYS.get("dex_events", 90)
        for column in (
            "sender",
            "recipient",
            "amount0_in",
            "amount1_in",
            "amount0_out",
            "amount1_out",
            "amount0",
            "amount1",
            "whale",
            "ingested_at",
            "source",
        ):
            self._degrade_column_ttl(client, CH_DEX_TABLE, column, dex_days)
        self._ensure_summary_projection(client, CH_DEX_TABLE, DEX_SUMMARY_COLUMNS)
