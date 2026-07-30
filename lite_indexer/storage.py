"""
Storage writers — NDJSON and ClickHouse output.

Provides:
  - NDJSONWriter: Appends events as JSONL files organized by chain/date.
  - ClickHouseWriter: Inserts events via ClickHouse native protocol.

Disk cap and free-space checks are included before writes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lite_indexer.config import (
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_TABLE,
    DATA_DIR,
    DISK_CAP_BYTES,
    MIN_FREE_DISK_BYTES,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Prometheus-style metrics counters ─────────────────────────────────
_metrics: dict[str, int] = {
    "transfers_written": 0,
    "dex_swaps_written": 0,
    "deployers_written": 0,
    "clickhouse_inserts": 0,
    "clickhouse_errors": 0,
}


def get_metrics() -> dict[str, int]:
    """Get and reset metrics counters."""
    result = dict(_metrics)
    for k in _metrics:
        _metrics[k] = 0
    return result


def check_disk_space(min_free_bytes: int = MIN_FREE_DISK_BYTES) -> bool:
    """Check if there's enough disk space to continue.

    Returns True if OK, False if dangerously low.
    """
    try:
        # Check the parent of DATA_DIR or /srv/data if DATA_DIR is relative
        check_path = DATA_DIR.parent if DATA_DIR.is_absolute() else Path("/srv/data")
        usage = shutil.disk_usage(check_path)
        free = usage.free
        if free < min_free_bytes:
            logger.error(
                "disk_full",
                free_gb=round(free / 1e9, 2),
                min_gb=round(min_free_bytes / 1e9, 2),
            )
            return False
        return True
    except OSError as exc:
        logger.warning("disk_space_check_failed", error=str(exc)[:80])
        return True  # Fail open


def check_disk_cap(data_dir: Path = DATA_DIR, cap_bytes: int = DISK_CAP_BYTES) -> bool:
    """Check if data directory is under the disk cap.

    Returns True if we can write, False if cap exceeded.
    """
    if not data_dir.exists():
        return True

    try:
        result = subprocess.run(
            ["/usr/bin/du", "-sb", str(data_dir)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0:
            total_bytes = int(result.stdout.split()[0])
            if total_bytes >= cap_bytes:
                logger.error(
                    "disk_cap_exceeded",
                    usage_gb=round(total_bytes / 1024**3, 2),
                    cap_gb=round(cap_bytes / 1024**3, 2),
                )
                return False
            return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Fallback
    try:
        stat = os.statvfs(str(data_dir))
        free_bytes = stat.f_frsize * stat.f_bavail
        return free_bytes > 1024**3  # At least 1GB free
    except (OSError, AttributeError):
        return True  # Fail open


def check_write_ok(label: str = "", data_dir: Path = DATA_DIR, cap_bytes: int = DISK_CAP_BYTES) -> bool:
    """Combined disk cap + free space check before a write operation."""
    if not check_disk_cap(data_dir, cap_bytes):
        logger.error("write_aborted_disk_cap", label=label)
        return False
    if not check_disk_space():
        logger.error("write_aborted_disk_space", label=label)
        return False
    return True


# ── NDJSON Writer ─────────────────────────────────────────────────────


class NDJSONWriter:
    """Append events as NDJSON (JSONL) files.

    File path: {data_dir}/{chain}/{date}/events.jsonl
    DEX path:  {data_dir}/{chain}/dex/{date}/events.jsonl
    """

    def __init__(self, data_dir: Path = DATA_DIR) -> None:
        self.data_dir = data_dir

    def write_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Append transfer events to NDJSON. Returns bytes written."""
        if not events:
            return 0
        if not check_write_ok("ndjson"):
            return 0

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        out_dir = self.data_dir / chain / today
        out_path = out_dir / "events.jsonl"

        return self._write_lines(events, out_path)

    def write_dex_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Append DEX swap events to NDJSON. Returns bytes written."""
        if not events:
            return 0
        if not check_write_ok("dex_ndjson"):
            return 0

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        out_dir = self.data_dir / chain / "dex" / today
        out_path = out_dir / "events.jsonl"

        return self._write_lines(events, out_path)

    def _write_lines(self, events: list[dict[str, Any]], out_path: Path) -> int:
        """Write NDJSON lines to a file in one batch. Returns bytes written."""
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [json.dumps(e, ensure_ascii=False) + "\n" for e in events]
            bytes_written = 0
            with open(out_path, "a") as f:
                for line in lines:
                    f.write(line)
                    bytes_written += len(line.encode("utf-8"))

            logger.info(
                "ndjson_written",
                path=str(out_path),
                events=len(events),
                bytes_written=bytes_written,
            )
            return bytes_written
        except OSError as exc:
            logger.error("ndjson_write_failed", path=str(out_path), error=str(exc))
            return 0


# ── ClickHouse Writer ─────────────────────────────────────────────────


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

            rows.append((
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
            ))

        try:
            client.execute(
                f"INSERT INTO {CH_TABLE} "
                "(chain, block_number, tx_hash, from_address, to_address, "
                "token_address, value, block_timestamp, log_index, "
                "token_symbol, token_name, ingested_at, source) VALUES",
                rows,
            )
            logger.info("clickhouse_inserted", chain=chain, rows=len(rows))
            _metrics["clickhouse_inserts"] += 1
            return len(rows)
        except Exception as exc:
            _metrics["clickhouse_errors"] += 1
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

            rows.append((
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
            ))

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
            _metrics["clickhouse_inserts"] += 1
            return len(rows)
        except Exception as exc:
            _metrics["clickhouse_errors"] += 1
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

            rows.append((
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
            ))

        try:
            client.execute(
                f"INSERT INTO {CH_DEPLOYER_TABLE} "
                "(deployer, token_address, chain, block_number, block_timestamp, "
                "tx_hash, type, factory, ingested_at, source) VALUES",
                rows,
            )
            logger.info("deployer_events_inserted", chain=chain, rows=len(rows))
            _metrics["deployers_written"] += len(rows)
            _metrics["clickhouse_inserts"] += 1
            return len(rows)
        except Exception as exc:
            _metrics["clickhouse_errors"] += 1
            logger.error("deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0
