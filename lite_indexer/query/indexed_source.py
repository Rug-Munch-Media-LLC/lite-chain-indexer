"""IndexedXyzSource — additional coverage via the Indexed.xyz R2 lake.

CC0 dataset (Goldsky alliance) on Cloudflare R2 with published public-read
credentials: raw blocks/transactions/logs (+decoded logs for ethereum) for
ethereum, base, gnosis, linea, optimism, zksync, zora (also sui/pgn/arweave)
— daily-fresh, date-partitioned (``dt=YYYY-MM-DD``), versioned (``v2.0.0``).

This module maps ERC-20 ``Transfer`` logs to the warehouse
``evm_transfers`` row shape, giving an independent third path for the chains
the local index covers thinly (base, gnosis, linea, optimism, zksync) next
to Envio HyperRPC and the hot index.

Log format notes (verified 2026-09-16): ``topics`` is a single VARCHAR of
comma-joined 32-byte hex topics (no JSON brackets); ``data`` is the raw hex
payload; ``block_timestamp`` is epoch seconds.

Env:
    INDEXED_XYZ_ENDPOINT / INDEXED_XYZ_ACCESS_KEY / INDEXED_XYZ_SECRET_KEY
        public-read R2 credentials (published by the project)
    INDEXED_XYZ_VERSION   dataset version directory (default ``v2.0.0``)
    INDEXED_XYZ_ENABLED   set 0/false to disable entirely

CLI:
    python -m lite_indexer.query.indexed_source transfers base 0xADDR --dates 2026-09-15
    python -m lite_indexer.query.indexed_source transfers base 0xADDR --from-block 51000000 --to-block 51050000
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: ERC-20 ``Transfer`` event topic.
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


class IndexedXyzSource:
    """DuckDB-backed reader over the Indexed.xyz R2 dataset.

    Args:
        endpoint/access_key/secret_key: R2 credentials; default to the
            ``INDEXED_XYZ_*`` env vars.
        version: Dataset version directory (default ``INDEXED_XYZ_VERSION``
            or ``v2.0.0``).
        enabled: Override ``INDEXED_XYZ_ENABLED`` (default enabled when
            credentials are present).
        connect: Optional zero-arg callable returning a DuckDB-like
            connection (tests inject a fake; no credentials needed).
    """

    BUCKET = "indexed-xyz-wnam"

    #: Chains with the raw/logs layout this module maps.
    CHAINS: frozenset[str] = frozenset(
        {"ethereum", "base", "gnosis", "linea", "optimism", "zksync", "zora"}
    )

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        version: str | None = None,
        enabled: bool | None = None,
        connect: Any | None = None,
    ) -> None:
        self._endpoint = (
            endpoint if endpoint is not None else os.getenv("INDEXED_XYZ_ENDPOINT", "")
        ).strip()
        self._access_key = (
            access_key if access_key is not None else os.getenv("INDEXED_XYZ_ACCESS_KEY", "")
        ).strip()
        self._secret_key = (
            secret_key if secret_key is not None else os.getenv("INDEXED_XYZ_SECRET_KEY", "")
        ).strip()
        self._version = (
            version if version is not None else os.getenv("INDEXED_XYZ_VERSION", "v2.0.0")
        ).strip()
        self._enabled = _env_flag("INDEXED_XYZ_ENABLED", True) if enabled is None else enabled
        self._connect_impl = connect
        self._con: Any | None = None
        self._duckdb_missing = False

    # ── Availability ─────────────────────────────────────────────────

    def available(self) -> bool:
        """True when creds are present and DuckDB is importable (or injected)."""
        if not self._enabled or self._duckdb_missing:
            return False
        if self._connect_impl is not None:
            return True
        if not (self._endpoint and self._access_key and self._secret_key):
            return False
        try:
            import duckdb  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            self._duckdb_missing = True
            logger.warning("indexed_xyz_disabled", reason="duckdb_missing")
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
                # DuckDB wants the bare host (no scheme); the env value may
                # carry https:// (e.g. from the project's docs).
                endpoint_host = self._endpoint
                for scheme in ("https://", "http://"):
                    if endpoint_host.startswith(scheme):
                        endpoint_host = endpoint_host[len(scheme) :]
                endpoint_host = endpoint_host.rstrip("/")
                con.execute(f"SET s3_endpoint = '{endpoint_host}'")
                con.execute("SET s3_use_ssl = true")
                con.execute("SET s3_region = 'auto'")
                con.execute(f"SET s3_access_key_id = '{self._access_key}'")
                con.execute(f"SET s3_secret_access_key = '{self._secret_key}'")
                self._con = con
        return self._con

    # ── Querying ─────────────────────────────────────────────────────

    @staticmethod
    def _validate_dates(dates: Any) -> list[str] | None:
        if not dates:
            return None
        for d in dates:
            if not _DATE_RE.match(str(d)):
                raise ValueError(f"invalid date for indexed.xyz query: {d!r} (expected YYYY-MM-DD)")
        return [str(d) for d in dates]

    def _paths(self, chain: str, dates: list[str] | None) -> list[str]:
        if not dates:
            return []
        return [
            f"s3://{self.BUCKET}/{chain}/raw/logs/{self._version}/dt={d}/*.parquet" for d in dates
        ]

    def query(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        address: str | None = None,
        dates: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Read ERC-20 transfers from the lake in the warehouse row shape.

        Args:
            table: Only ``evm_transfers`` is mapped (from raw logs).
            chain: One of :attr:`CHAINS`.
            from_block/to_block: Inclusive ``block_number`` bounds.
            address: Wallet filter (``from OR to``), case-insensitive.
            dates: Explicit ``dt=`` partitions (required — the caller wants
                real dates; the router derives them from block timestamps).
            limit: Row cap (1..1000).

        Returns:
            Rows in the ``evm_transfers`` shape; empty list when disabled,
            unsupported, or unreadable.
        """
        if table != "evm_transfers" or chain not in self.CHAINS or not self.available():
            return []
        dates = self._validate_dates(dates)
        paths = self._paths(chain, dates)
        if not paths:
            return []

        wallet40 = str(address or "").strip().lower()
        if wallet40.startswith("0x"):
            wallet40 = wallet40[2:]

        path_literals = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
        sql = (
            "SELECT address, log_index, transaction_hash, topics, data, block_number, block_timestamp "
            f"FROM read_parquet({path_literals}, hive_partitioning=true, union_by_name=true) "
            f"WHERE topics LIKE '{ERC20_TRANSFER_TOPIC},%'"
        )
        params: list[Any] = []
        if from_block is not None:
            sql += " AND block_number >= ?"
            params.append(int(from_block))
        if to_block is not None:
            sql += " AND block_number <= ?"
            params.append(int(to_block))
        if wallet40:
            sql += " AND (right(split_part(topics, ',', 2), 40) = ? OR right(split_part(topics, ',', 3), 40) = ?)"
            params.extend([wallet40, wallet40])
        sql += " ORDER BY block_number DESC, log_index DESC LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))

        try:
            con = self._connection()
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
            logger.warning("indexed_xyz_query_failed", error=str(exc)[:120])
            return []
        return [self._map_row(chain, dict(zip(cols, row, strict=False))) for row in raw_rows]

    # ── Row mapping ──────────────────────────────────────────────────

    def _map_row(self, chain: str, raw: dict[str, Any]) -> dict[str, Any]:
        topics = str(raw.get("topics") or "").split(",")
        from_addr = ("0x" + topics[1][-40:]) if len(topics) > 1 else ""
        to_addr = ("0x" + topics[2][-40:]) if len(topics) > 2 else ""
        data = raw.get("data") or "0x0"
        try:
            value = str(int(data, 16))
        except (TypeError, ValueError):
            value = "0"
        timestamp = raw.get("block_timestamp")
        try:
            block_ts = datetime.fromtimestamp(int(timestamp), tz=UTC) if timestamp else None
        except (TypeError, ValueError, OSError):
            block_ts = None
        return {
            "chain": chain,
            "block_number": int(raw.get("block_number") or 0),
            "tx_hash": raw.get("transaction_hash") or "",
            "from_address": from_addr.lower(),
            "to_address": to_addr.lower(),
            "token_address": str(raw.get("address") or "").lower(),
            "value": value,
            "block_timestamp": block_ts,
            "log_index": int(raw.get("log_index") or 0),
            "ingested_at": datetime.now(UTC),
        }


# ── CLI ──────────────────────────────────────────────────────────────


def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only); logs go to stderr."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        pass


def _derive_dates(from_block: int, to_block: int) -> list[str]:
    """Derive ``dt=`` partitions from a block range via Envio block lookups."""
    from lite_indexer.query.envio_source import EnvioSource

    envio = EnvioSource()
    if not envio.available():
        raise SystemExit("--dates required (ENVIO_RPC_TOKEN missing for block→date derivation)")
    lo = envio.call("ethereum", "eth_getBlockByNumber", [hex(int(from_block)), False])
    hi = envio.call("ethereum", "eth_getBlockByNumber", [hex(int(to_block)), False])
    if not lo or not hi:
        raise SystemExit("could not resolve block timestamps via Envio")
    start = datetime.fromtimestamp(int(lo["timestamp"], 16), tz=UTC).date()
    end = datetime.fromtimestamp(int(hi["timestamp"], 16), tz=UTC).date()
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def _cmd_transfers(args: argparse.Namespace) -> int:
    src = IndexedXyzSource()
    if not src.available():
        print("indexed.xyz unavailable (credentials or duckdb missing)", file=sys.stderr)  # noqa: T201
        return 1
    dates = args.dates
    if not dates and args.from_block and args.to_block:
        dates = _derive_dates(int(args.from_block, 0), int(args.to_block, 0))
    rows = src.query(
        "evm_transfers",
        args.chain,
        from_block=int(args.from_block, 0) if args.from_block else None,
        to_block=int(args.to_block, 0) if args.to_block else None,
        address=args.address,
        dates=dates,
        limit=args.limit,
    )
    print(json.dumps(rows[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cli() -> int:
    _silence_console_logs()
    parser = argparse.ArgumentParser(prog="indexed_source", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_tr = sub.add_parser("transfers", help="wallet ERC-20 transfers from Indexed.xyz")
    p_tr.add_argument("chain", choices=sorted(IndexedXyzSource.CHAINS))
    p_tr.add_argument("address")
    p_tr.add_argument("--dates", nargs="*", default=None, help="dt partitions (YYYY-MM-DD)")
    p_tr.add_argument("--from-block", default=None)
    p_tr.add_argument("--to-block", default=None)
    p_tr.add_argument("--limit", type=int, default=100)
    p_tr.set_defaults(fn=_cmd_transfers)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(_cli())
