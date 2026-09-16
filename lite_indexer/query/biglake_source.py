"""BigLakeSource — on-demand Ethereum history from the AWS Open Data lake.

The fourth tier of the "no-full-indexing-needed" architecture: reads the
free, daily-refreshed ``aws-public-blockchain`` Parquet archive in place
(anonymous S3, no credentials required) via DuckDB. Complements
ClickHouse (hot) and the B2 cold store for ranges that were never indexed
locally — including traces, which are expensive to self-index.

Coverage (v1): ``ethereum`` only.

* ``token_transfers`` → ``evm_transfers`` row shape
* ``traces``          → ``internal_transfers`` row shape

``btc/`` exists in the lake but is not mapped yet.

Scope: reads are bounded by ``date=`` partitions. Pass explicit
``dates`` (``["2026-09-12", ...]``) for exact days, or rely on
``BIGLAKE_SCOPE`` (defaults to the current month, e.g. ``2026-09-*``).
Never glob ``date=*`` — full-history listing is slow.

CLI:

    python -m lite_indexer.query.biglake_source transfers 0xADDR --dates 2026-09-10 2026-09-12
    python -m lite_indexer.query.biglake_source transfers 0xADDR --traces
    python -m lite_indexer.query.biglake_source backfill --table token_transfers --dates 2026-09-12 [--to-b2]

Env:
    BIGLAKE_ENABLED   set to 0/false to disable entirely (default enabled)
    BIGLAKE_SCOPE     month glob for default reads (default: current month)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _num_str(value: Any) -> str:
    """Normalize a numeric lake value to a decimal string.

    NOTE: the lake stores value columns as DOUBLE, so very large integers
    can lose precision beyond 2**53. Acceptable for v1 (display/filtering
    use); do not use for exact accounting.
    """
    if value is None:
        return "0"
    try:
        fvalue = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(fvalue)) if fvalue.is_integer() else str(fvalue)


class BigLakeSource:
    """DuckDB-backed reader over the AWS public blockchain dataset.

    Args:
        enabled: Override ``BIGLAKE_ENABLED`` (default enabled).
        scope: Month glob for reads without explicit ``dates``
            (``"2026-09-*"``); defaults to ``BIGLAKE_SCOPE`` or the
            current month.
        connect: Optional zero-arg callable returning a DuckDB-like
            connection (used by tests to avoid the network).
    """

    S3_BASE = "s3://aws-public-blockchain/v1.0/eth"
    TABLE_MAP: dict[str, str] = {
        "evm_transfers": "token_transfers",
        "internal_transfers": "traces",
    }

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        scope: str | None = None,
        connect: Callable[[], Any] | None = None,
    ) -> None:
        self._enabled = _env_flag("BIGLAKE_ENABLED", True) if enabled is None else enabled
        self._scope = scope or os.getenv("BIGLAKE_SCOPE") or f"{datetime.now(UTC):%Y-%m}-*"
        self._connect_impl = connect
        self._con: Any | None = None
        self._duckdb_missing = False

    # ── Availability ─────────────────────────────────────────────────

    def available(self) -> bool:
        """True when the tier can serve queries (enabled + DuckDB importable)."""
        if not self._enabled or self._duckdb_missing:
            return False
        if self._connect_impl is not None:
            return True
        try:
            import duckdb  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            self._duckdb_missing = True
            logger.warning("biglake_disabled", reason="duckdb_missing")
            return False
        return True

    def connection(self) -> Any:
        """Public accessor for the DuckDB connection (CLI/backfill use)."""
        return self._connection()

    def _connection(self) -> Any:
        if self._con is None:
            if self._connect_impl is not None:
                self._con = self._connect_impl()
            else:
                import duckdb  # pylint: disable=import-outside-toplevel

                con = duckdb.connect()
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.execute("SET enable_progress_bar = false;")
                con.execute("SET s3_region = 'us-east-1';")
                con.execute(
                    "CREATE OR REPLACE SECRET s3_anon "
                    "(TYPE S3, PROVIDER config, REGION 'us-east-1', KEY_ID '', SECRET '')"
                )
                self._con = con
        return self._con

    # ── Querying ─────────────────────────────────────────────────────

    @staticmethod
    def _validate_dates(dates: Sequence[str] | None) -> list[str] | None:
        if not dates:
            return None
        for d in dates:
            if not _DATE_RE.match(str(d)):
                raise ValueError(f"invalid date for biglake query: {d!r} (expected YYYY-MM-DD)")
        return [str(d) for d in dates]

    def _paths(self, biglake_table: str, dates: Sequence[str] | None) -> list[str]:
        if dates:
            return [f"{self.S3_BASE}/{biglake_table}/date={d}/*.parquet" for d in dates]
        return [f"{self.S3_BASE}/{biglake_table}/date={self._scope}/*.parquet"]

    def query(
        self,
        table: str,
        chain: str = "ethereum",
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        address: str | None = None,
        dates: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Read rows from the lake in the warehouse row shape.

        Args:
            table: Logical table name (``evm_transfers`` or
                ``internal_transfers``).
            chain: Only ``ethereum`` is supported (v1).
            from_block/to_block: Inclusive ``block_number`` bounds.
            address: Match ``from_address OR to_address`` (case-insensitive).
            dates: Exact ``YYYY-MM-DD`` partitions; defaults to the scope month.
            limit: Row cap (1..1000).

        Returns:
            Rows normalized to the warehouse column shape; empty list when
            the tier is disabled, the chain/table is unsupported, or the
            read fails.
        """
        if chain != "ethereum" or table not in self.TABLE_MAP:
            return []
        if not self.available():
            return []
        dates = self._validate_dates(dates)
        biglake_table = self.TABLE_MAP[table]

        where: list[str] = []
        params: list[Any] = []
        if from_block is not None:
            where.append("block_number >= ?")
            params.append(int(from_block))
        if to_block is not None:
            where.append("block_number <= ?")
            params.append(int(to_block))
        if address:
            addr = str(address).strip().lower()
            where.append("(lower(from_address) = ? OR lower(to_address) = ?)")
            params.extend([addr, addr])

        paths = self._paths(biglake_table, dates)
        path_literals = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
        sql = (
            f"SELECT * FROM read_parquet({path_literals}, hive_partitioning=true, union_by_name=true)"
            + (f" WHERE {' AND '.join(where)}" if where else "")
            + " ORDER BY block_number DESC LIMIT ?"
        )
        params.append(max(1, min(int(limit), 1000)))

        try:
            con = self._connection()
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001 - tier failures must not break routing
            logger.warning("biglake_query_failed", error=str(exc)[:120])
            return []
        return [self._map_row(table, dict(zip(cols, row, strict=False))) for row in raw_rows]

    # ── Row mapping ──────────────────────────────────────────────────

    def _map_row(self, table: str, raw: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        if table == "evm_transfers":
            return {
                "chain": "ethereum",
                "block_number": int(raw.get("block_number") or 0),
                "tx_hash": raw.get("transaction_hash") or "",
                "from_address": str(raw.get("from_address") or "").lower(),
                "to_address": str(raw.get("to_address") or "").lower(),
                "token_address": str(raw.get("token_address") or "").lower(),
                "value": _num_str(raw.get("value")),
                "block_timestamp": raw.get("block_timestamp"),
                "log_index": int(raw.get("log_index") or 0),
                "ingested_at": now,
            }
        trace_address = raw.get("trace_address")
        if isinstance(trace_address, (list, tuple)):
            trace_str = ".".join(str(x) for x in trace_address)
        else:
            trace_str = "" if trace_address is None else str(trace_address)
        input_data = raw.get("input") or ""
        error = raw.get("error")
        return {
            "chain": "ethereum",
            "block_number": int(raw.get("block_number") or 0),
            "block_timestamp": raw.get("block_timestamp"),
            "tx_hash": raw.get("transaction_hash") or "",
            "trace_address": trace_str,
            "from_address": str(raw.get("from_address") or "").lower(),
            "to_address": str(raw.get("to_address") or "").lower(),
            "value_wei": _num_str(raw.get("value")),
            "call_type": raw.get("call_type") or "",
            "gas_used": int(raw.get("gas_used") or 0),
            "error": "" if not error else str(error)[:200],
            "input_method_id": input_data[:10]
            if isinstance(input_data, str) and input_data.startswith("0x")
            else "",
            "ingested_at": now,
        }


# ── CLI ──────────────────────────────────────────────────────────────


def _cmd_transfers(args: argparse.Namespace) -> int:
    src = BigLakeSource()
    if not src.available():
        print("biglake unavailable (duckdb missing or BIGLAKE_ENABLED=0)", file=sys.stderr)  # noqa: T201
        return 1
    out: dict[str, Any] = {
        "evm_transfers": src.query(
            "evm_transfers",
            address=args.address,
            dates=args.dates,
            limit=args.limit,
        ),
    }
    if args.traces:
        out["internal_transfers"] = src.query(
            "internal_transfers",
            address=args.address,
            dates=args.dates,
            limit=args.limit,
        )
    print(json.dumps(out, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_backfill(args: argparse.Namespace) -> int:
    """Export full date partitions to Parquet; optionally mirror to B2.

    Target prefix: ``b2-crypt:chain-data/biglake/eth/<table>/date=<d>/data.parquet``
    """
    src = BigLakeSource()
    if not src.available():
        print("biglake unavailable", file=sys.stderr)  # noqa: T201
        return 1
    tables = [t.strip() for t in args.table.split(",") if t.strip()]
    dates = BigLakeSource._validate_dates(args.dates) or []
    if not dates:
        print("--dates is required (YYYY-MM-DD ...)", file=sys.stderr)  # noqa: T201
        return 2
    rc = 0
    con = src.connection()
    for table in tables:
        biglake_table = BigLakeSource.TABLE_MAP.get(table)
        if biglake_table is None and table in BigLakeSource.TABLE_MAP.values():
            biglake_table = table  # raw lake name (token_transfers/traces) also accepted
        if not biglake_table:
            print(f"skip unsupported table: {table}", file=sys.stderr)  # noqa: T201
            rc = 2
            continue
        for d in dates:
            outdir = f"/tmp/biglake/eth/{biglake_table}/date={d}"
            os.makedirs(outdir, exist_ok=True)
            outfile = f"{outdir}/data.parquet"
            print(f"export {biglake_table} {d} → {outfile}")  # noqa: T201
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{src.S3_BASE}/{biglake_table}/"  # nosemgrep: formatted-sql-query — constants + validated dates
                f"date={d}/*.parquet', hive_partitioning=true, union_by_name=true)) "
                f"TO '{outfile}' (FORMAT PARQUET)"
            )
            size_mb = os.path.getsize(outfile) / 1e6
            print(f"  written {size_mb:.1f} MB")  # noqa: T201
            if args.to_b2:
                dest = f"b2-crypt:chain-data/biglake/eth/{biglake_table}/date={d}/data.parquet"
                proc = subprocess.run(
                    ["rclone", "copyto", outfile, dest],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                ok = proc.returncode == 0
                print(f"  rclone → {dest}: {'OK' if ok else 'FAILED ' + proc.stderr[:200]}")  # noqa: T201
                if not ok:
                    rc = 1
    return rc


def _cli() -> int:
    parser = argparse.ArgumentParser(prog="biglake_source", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_tr = sub.add_parser(
        "transfers", help="wallet token transfers (+ optional traces) from the lake"
    )
    p_tr.add_argument("address")
    p_tr.add_argument(
        "--dates", nargs="*", default=None, help="YYYY-MM-DD partitions (default: scope month)"
    )
    p_tr.add_argument("--limit", type=int, default=100)
    p_tr.add_argument(
        "--traces", action="store_true", help="also query internal_transfers (traces)"
    )
    p_tr.set_defaults(fn=_cmd_transfers)

    p_bf = sub.add_parser("backfill", help="export partitions to Parquet (+ optional B2 mirror)")
    p_bf.add_argument(
        "--table",
        default="token_transfers",
        help="evm_transfers|internal_transfers (raw: token_transfers|traces), comma-separated",
    )
    p_bf.add_argument("--dates", nargs="*", required=True)
    p_bf.add_argument(
        "--to-b2", action="store_true", help="mirror to b2-crypt:chain-data/biglake/..."
    )
    p_bf.set_defaults(fn=_cmd_backfill)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(_cli())
