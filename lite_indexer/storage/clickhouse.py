"""
ClickHouse writer — inserts events via ClickHouse native protocol.

Handles lazy connections, column existence checks, and batch inserts
for transfer events, DEX swaps, and deployer events.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_DEPLOYER_TABLE, CH_DEX_TABLE, CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.disk import check_write_ok

logger = get_logger(__name__)


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

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert transfer events into ClickHouse. Returns rows inserted."""
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_TABLE)

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

    def write_deployer_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert deployer events into ClickHouse. Returns rows inserted."""
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("deployer_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)

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

            rows.append(
                (
                    factory,
                    e.get("pair_address", ""),
                    chain,
                    e.get("blockNumber", 0),
                    ts,
                    e.get("transactionHash", ""),
                    pool_type,
                    factory,
                    now,
                    "lite_indexer",
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEPLOYER_TABLE} "
                "(deployer, token_address, chain, block_number, block_timestamp, "
                "tx_hash, type, factory, ingested_at, source) VALUES",
                rows,
            )
            logger.info("deployer_events_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0
