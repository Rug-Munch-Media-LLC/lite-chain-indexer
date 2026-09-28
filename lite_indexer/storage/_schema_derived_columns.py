"""Derived + auxiliary column DDL (dex-USD, deployer, solana owner, TTL).

Extracted from ``storage/_schema.py`` (shrink-only size ratchet). Composed
into :class:`lite_indexer.storage._schema.SchemaMixin` by plain multiple
inheritance — the halves declare no base and never import each other.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import CH_DEPLOYER_TABLE, CH_DEX_TABLE, SOLANA_CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.storage._ddl_memo import _ddl_memo

logger = get_logger(__name__)


class _SchemaDerivedColumnMixin:
    @_ddl_memo
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
                    client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                        f"ALTER TABLE {CH_DEX_TABLE} ADD COLUMN IF NOT EXISTS {column} Float64 DEFAULT 0 AFTER {after}"
                    )
                    logger.info("dex_usd_column_added", table=CH_DEX_TABLE, column=column)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("dex_usd_columns_skip", error=str(exc)[:80])

    @_ddl_memo
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

    @_ddl_memo
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
            client.execute(
                f"ALTER TABLE {SOLANA_CH_TABLE} ADD COLUMN IF NOT EXISTS owner String DEFAULT '' AFTER mint"
            )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
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
