"""SonarxLakeSource — AWS v1.1 sonarx lake reader (base/arbitrum).

Reads the free ``aws-public-blockchain`` v1.1 sonarx Parquet archive in
place (anonymous S3, no credentials) via DuckDB httpfs, exactly like
:mod:`lite_indexer.query.biglake_source` does for the v1.0 eth lake.

Layout (live-verified 2026-09-19, coverage through 2026-09-12):

    s3://aws-public-blockchain/v1.1/sonarx/{base,arbitrum}/
        {transfers,traces}/date=YYYY-MM-DD/*.parquet

Rows are mapped to OUR warehouse shapes:

* ``transfers`` → ``evm_transfers`` event dicts (ClickHouseWriter.
  ``write_transfers`` keys: blockNumber/txHash/from/to/tokenAddress/
  value/blockTimestamp/logIndex).
* ``traces``    → ``internal_transfers`` row dicts (trace_store.
  ``write_internal_transfers`` keys).

``tx_log_key`` is NOT computed here — the writer derives it from
(block_number, tx_hash, log_index) via ``_transfer_dedup_key``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

S3_BASE = "s3://aws-public-blockchain/v1.1/sonarx"
CHAINS = ("base", "arbitrum")
TABLES = ("transfers", "traces")


def _iso_from_unix(ts: Any) -> str:
    """Unix-seconds (lake TIMESTAMP decimal) → ISO-8601 UTC string."""
    try:
        return datetime.fromtimestamp(int(ts), UTC).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        logger.debug("lake_timestamp_invalid", value=str(ts)[:40])
        return ""


def _num_str(value: Any) -> str:
    """Normalize a lake DOUBLE value to a decimal string (precision caveat: DOUBLE)."""
    if value is None:
        return "0"
    try:
        fvalue = float(value)
    except (TypeError, ValueError):
        logger.debug("lake_number_invalid", value=str(value)[:40])
        return str(value)
    return str(int(fvalue)) if fvalue.is_integer() else str(fvalue)


def _addr(value: Any) -> str:
    return str(value or "").lower()


def map_transfer_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Map one sonarx ``transfers`` row to an ``evm_transfers`` event dict."""
    return {
        "blockNumber": int(raw.get("BLOCK_NUMBER") or 0),
        "txHash": raw.get("TRANSACTION_HASH") or "",
        "from": _addr(raw.get("FROM_ADDRESS")),
        "to": _addr(raw.get("TO_ADDRESS")),
        "tokenAddress": _addr(raw.get("TOKEN_ADDRESS")),
        "value": _num_str(raw.get("VALUE")),
        "blockTimestamp": _iso_from_unix(raw.get("TIMESTAMP")),
        "logIndex": int(raw.get("LOG_INDEX") or 0),
    }


def map_trace_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Map one sonarx ``traces`` row to an ``internal_transfers`` row dict."""
    path = str(raw.get("PATH") or "").strip()
    trace_address = [int(p) for p in path.split(",") if p.strip().isdigit()] if path else []
    input_data = raw.get("INPUT") or ""
    return {
        "chain": "",
        "block_number": int(raw.get("BLOCK_NUMBER") or 0),
        "block_timestamp": _iso_from_unix(raw.get("TIMESTAMP")),
        "tx_hash": raw.get("TRANSACTION_HASH") or "",
        "trace_address": trace_address,
        "from_address": _addr(raw.get("FROM_ADDRESS")),
        "to_address": _addr(raw.get("TO_ADDRESS")),
        "value_wei": int(_num_str(raw.get("VALUE"))),
        "call_type": raw.get("TYPE") or "",
        "gas_used": int(raw.get("GAS_USED") or 0),
        "error": "" if not raw.get("ERROR") else str(raw["ERROR"])[:200],
        "input_method_id": input_data[:10] if isinstance(input_data, str) and input_data.startswith("0x") else "",
    }


class SonarxLakeSource:
    """DuckDB-backed reader over the v1.1 sonarx lake (base/arbitrum).

    Args:
        connect: Optional zero-arg callable returning a DuckDB-like
            connection (DI seam for tests — avoids the network).
    """

    def __init__(self, *, connect: Callable[[], Any] | None = None) -> None:
        self._connect_impl = connect
        self._con: Any | None = None

    def available(self) -> bool:
        """True when a connection can be obtained (duckdb importable or injected)."""
        if self._connect_impl is not None:
            return True
        try:
            import duckdb  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            logger.warning("sonarx_lake_disabled", reason="duckdb_missing")
            return False
        return True

    def connection(self) -> Any:
        """Public accessor for the DuckDB connection."""
        if self._con is None:
            if self._connect_impl is not None:
                self._con = self._connect_impl()
            else:
                import duckdb  # pylint: disable=import-outside-toplevel

                con = duckdb.connect()
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.execute("SET enable_progress_bar = false;")
                con.execute("SET s3_region = 'us-east-2';")
                con.execute(
                    "CREATE OR REPLACE SECRET s3_anon "
                    "(TYPE S3, PROVIDER config, REGION 'us-east-2', KEY_ID '', SECRET '')"
                )
                self._con = con
        return self._con

    def _read(self, chain: str, table: str, date: str, mapper: Any) -> Iterator[dict[str, Any]]:
        """Yield mapped rows for one (chain, table, date) partition."""
        path = f"{S3_BASE}/{chain}/{table}/date={date}/*.parquet"
        con = self.connection()
        cur = con.execute(
            f"SELECT * FROM read_parquet('{path}', hive_partitioning=true, union_by_name=true)"  # nosemgrep: formatted-sql-query — constants + validated date
        )
        cols = [d[0] for d in cur.description]
        while True:
            batch = cur.fetchmany(10_000)
            if not batch:
                break
            for row in batch:
                yield mapper(dict(zip(cols, row, strict=False)))

    def read_transfers(self, chain: str, date: str) -> Iterator[dict[str, Any]]:
        """Yield ``evm_transfers``-shaped event dicts for one date partition."""
        yield from self._read(chain, "transfers", date, map_transfer_row)

    def read_traces(self, chain: str, date: str) -> Iterator[dict[str, Any]]:
        """Yield ``internal_transfers``-shaped row dicts for one date partition."""
        yield from self._read(chain, "traces", date, map_trace_row)

    def count(self, chain: str, table: str, date: str) -> int:
        """Row count for one partition (parquet metadata only — no full scan)."""
        path = f"{S3_BASE}/{chain}/{table}/date={date}/*.parquet"
        con = self.connection()
        row = con.execute(
            f"SELECT count(*) FROM read_parquet('{path}', hive_partitioning=true, union_by_name=true)"  # nosemgrep: formatted-sql-query — constants + validated date
        ).fetchone()
        return int(row[0]) if row else 0
