"""
ClickHouse writer — inserts events via ClickHouse native protocol.

Handles lazy connections, column existence checks, and batch inserts
for transfer events, DEX swaps, and deployer events.

Deployer events resolve the real deployer EOA (the transaction `from`
address) via eth_getTransactionByHash before inserting — the PairCreated/
PoolCreated log itself only exposes the factory address, not the EOA that
submitted the transaction.

Column pruning (Improvement 3): old rows keep their summary columns
(from/to/token/value/block_ts for transfers, swap identity for DEX)
while heavy detail columns degrade to type defaults via column-level
TTL — cutting storage 30-50% while keeping every scanner query
answerable.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import (
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_TABLE,
    CH_TTL_ENABLED,
    DEX_SUMMARY_COLUMNS,
    EVT_SUMMARY_COLUMNS,
    RETENTION_DAYS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Max concurrent eth_getTransactionByHash calls while resolving deployer
# EOAs (keeps RPC overhead low while batching unique tx hashes).
_DEPLOYER_RPC_CONCURRENCY = 5


async def _resolve_deployer_eoas(chain: str, events: list[dict[str, Any]]) -> dict[str, str]:
    """Resolve the deployer EOA (tx `from`) for each unique tx hash.

    Groups deployer events by unique ``transactionHash`` so each tx is
    queried exactly once via ``eth_getTransactionByHash``, then maps
    tx_hash → from address. Returns {} if resolution is unavailable.

    Args:
        chain: Chain name (used to pick RPC endpoints).
        events: Deployer event dicts (each with a ``transactionHash``).

    Returns:
        Dict mapping tx_hash → lowercased `from` address (EOA).
    """
    tx_hashes = {e.get("transactionHash", "") for e in events if isinstance(e, dict) and e.get("transactionHash")}
    if not tx_hashes:
        return {}

    # Deferred imports avoid any import-cycle risk with the storage package.
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.rpc import RpcClient, get_best_endpoint

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.warning("deployer_eoa_no_endpoints", chain=chain)
        return {}

    rpc = RpcClient()
    endpoint = get_best_endpoint(endpoints)
    semaphore = asyncio.Semaphore(_DEPLOYER_RPC_CONCURRENCY)

    async def _get_from(tx_hash: str) -> tuple[str, str]:
        async with semaphore:
            try:
                result = await rpc.call(endpoint, "eth_getTransactionByHash", [tx_hash])
                if result and isinstance(result.get("result"), dict):
                    from_addr = result["result"].get("from")
                    if from_addr:
                        return tx_hash, str(from_addr).lower()
            except Exception as exc:  # noqa: BLE001 — resolution is best-effort
                logger.debug("deployer_eoa_fetch_failed", tx=tx_hash[:16], error=str(exc)[:60])
            return tx_hash, ""

    results = await asyncio.gather(*(_get_from(h) for h in tx_hashes), return_exceptions=True)

    mapping: dict[str, str] = {}
    for res in results:
        if isinstance(res, tuple) and res[1]:
            mapping[res[0]] = res[1]
    return mapping


class ClickHouseWriter:
    """Insert events into ClickHouse tables.

    Connects lazily on first write. Handles column existence checks.
    """

    def __init__(self) -> None:
        self._client: Any = None

    def _connect(self) -> Any | None:
        """Lazy-init ClickHouse native protocol client."""
        if self._client is not None:
            return self._client

        ch_host = os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
        ch_port = int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT", "9000"))
        ch_user = os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default")
        ch_pass = os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
        ch_db = os.getenv("CH_DATABASE", "wallet_memory")

        try:
            from clickhouse_driver import Client as CHClient  # type: ignore[import-untyped]

            self._client = CHClient(
                host=ch_host,
                port=ch_port,
                user=ch_user,
                password=ch_pass,
                database=ch_db,
                connect_timeout=5,
                send_receive_timeout=30,
            )
            self._client.execute("SELECT 1")
            logger.info("clickhouse_connected", host=ch_host, database=ch_db)
            return self._client
        except Exception as exc:
            logger.warning("clickhouse_unavailable", error=str(exc)[:100])
            return None

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

    def _ensure_deployer_columns(self, client: Any) -> None:
        """Ensure deployer_events has token0/token1/deployer_eoa columns.

        F0 data-quality fix: deployer_events must store the resolved EOA
        (deployer / deployer_eoa) plus the pair's token0/token1 so the
        table no longer mislabels factory→deployer and pair→token_address.
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
            for column, after in (("token0", "token_address"), ("token1", "token0"), ("deployer_eoa", "deployer")):
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

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert transfer events into ClickHouse. Returns rows inserted."""
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_TABLE)
        if CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        now = datetime.now(UTC)
        rows: list[tuple] = []

        for event in events:
            ts_str = event.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            rows.append(
                (
                    chain,
                    event["blockNumber"],
                    event["txHash"],
                    event["from"],
                    event["to"],
                    event["tokenAddress"],
                    event["value"],
                    ts,
                    event["logIndex"],
                    event.get("tokenSymbol"),
                    event.get("tokenName"),
                    now,
                    event.get("source", "lite_indexer"),
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_TABLE} "
                "(chain, block_number, tx_hash, from_address, to_address, "
                "token_address, value, block_timestamp, log_index, "
                "token_symbol, token_name, ingested_at, source) VALUES",
                rows,
            )
            logger.info("clickhouse_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    def write_dex_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert DEX swap events into ClickHouse. Returns rows inserted."""
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("dex_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEX_TABLE)
        if CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        now = datetime.now(UTC)
        rows: list[tuple] = []

        for event in events:
            ts_str = event.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            version = event.get("dex_type", "v2")
            amt0 = event.get("amount0In", "0") or "0"
            amt1 = event.get("amount1In", "0") or "0"

            rows.append(
                (
                    "swap",
                    chain,
                    event["blockNumber"],
                    ts,
                    event["transactionHash"],
                    event["logIndex"],
                    event.get("pairAddress", ""),
                    event.get("dex", ""),
                    version,
                    event.get("token0", ""),
                    event.get("token1", ""),
                    event.get("sender", ""),
                    event.get("recipient", ""),
                    event.get("amount0In", "0"),
                    event.get("amount1In", "0"),
                    event.get("amount0Out", "0"),
                    event.get("amount1Out", "0"),
                    amt0,
                    amt1,
                    0,  # whale — UInt8, not detected yet
                    now,
                    event.get("source", "lite_indexer"),
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEX_TABLE} "
                "(type, chain, block_number, block_timestamp, tx_hash, log_index, "
                "contract_address, dex, version, token0, token1, sender, recipient, "
                "amount0_in, amount1_in, amount0_out, amount1_out, "
                "amount0, amount1, whale, ingested_at, source) VALUES",
                rows,
            )
            logger.info("dex_clickhouse_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("dex_clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    async def write_deployer_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert deployer events into ClickHouse. Returns rows inserted.

        F0 data-quality fix — resolves the real deployer EOA via
        ``eth_getTransactionByHash`` (batched per unique tx hash) and maps:

          deployer       ← tx `from` (the EOA)      [was: factory]
          deployer_eoa   ← tx `from` (the EOA)      [new column]
          token_address  ← token0 (first token)     [was: pair_address]
          token0         ← token0                    [new column]
          token1         ← token1                    [new column]
          factory        ← factory                   [unchanged]

        If EOA resolution fails the deployer fields stay empty rather than
        being mislabeled with the factory address.
        """
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("deployer_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)
        self._ensure_deployer_columns(client)

        # Resolve real deployer EOA per unique tx hash (batched RPC calls).
        try:
            eoa_map = await _resolve_deployer_eoas(chain, events)
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.warning("deployer_eoa_resolution_failed", chain=chain, error=str(exc)[:100])
            eoa_map = {}

        now = datetime.now(UTC)
        rows: list[tuple] = []

        for e in events:
            ts_str = e.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            version = e.get("version", "v2")
            pool_type = f"{version}_pool"
            factory = e.get("factory", "")

            # Correct mapping: deployer ← tx `from` (EOA); token_address ← token0
            tx_hash = e.get("transactionHash", "")
            deployer = eoa_map.get(tx_hash, "")
            token0 = e.get("token0", "") or ""
            token1 = e.get("token1", "") or ""
            token_address = token0 or e.get("pair_address", "")

            rows.append(
                (
                    deployer,
                    deployer,
                    token_address,
                    token0,
                    token1,
                    chain,
                    e.get("blockNumber", 0),
                    ts,
                    tx_hash,
                    pool_type,
                    factory,
                    now,
                    "lite_indexer",
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEPLOYER_TABLE} "
                "(deployer, deployer_eoa, token_address, token0, token1, "
                "chain, block_number, block_timestamp, tx_hash, type, "
                "factory, ingested_at, source) VALUES",
                rows,
            )
            logger.info("deployer_events_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0
