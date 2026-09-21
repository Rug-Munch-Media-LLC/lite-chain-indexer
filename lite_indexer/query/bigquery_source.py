"""BigQuerySource — on-demand Ethereum history from ``bigquery-public-data``.

Adds the BigQuery public ``crypto_ethereum`` dataset as a first-class lake
provider (1 TiB/month free query allowance). It is a multi-day snapshot, so
it answers historical backfills that the live index, B2 cold store and
anonymous S3 lake may not cover.

Credentials: a GCP service-account JSON (``GOOGLE_APPLICATION_CREDENTIALS``
or the fleet default ``/srv/secrets/gcp-biglake-sa.json``). Available only
when BOTH the file exists AND the feature flag is on — default enabled when
credentials exist, disabled otherwise.

Backend: the ``bq`` CLI, shelled out through an injectable ``runner``
(``(sql, params) -> rows``); no ``google-cloud-bigquery`` import, no install.

Public surface (R2): :class:`BigQuerySource` is primary; the two module-level
helpers are pure and keep the class thin (the ONE accepted exception).

Env:
    BIGQUERY_ENABLED                 0/false disables (default: creds present)
    GOOGLE_APPLICATION_CREDENTIALS   service-account JSON path
    BIGQUERY_BQ_BIN                  ``bq`` binary (default ``/usr/bin/bq``)
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_tables import (
    _ADDR_COLS,
    _SOLANA_TABLES,
    _TABLE_SELECT,
    build_query,
    run_bq,
)
from lite_indexer.query.lake_row_utils import _norm_addr, _num_str  # re-exported for tests

logger = get_logger(__name__)

_DEFAULT_CREDS = "/srv/secrets/gcp-biglake-sa.json"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _ms_to_dt(value: Any) -> datetime | None:
    """Convert an epoch-millisecond value to an aware UTC datetime."""
    if value is None or value == "":
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
    except (TypeError, ValueError):
        return None


#: Lake table names -> BigQuery ``crypto_ethereum`` table (None = no BQ tier).
_BIGQUERY_TABLE_MAP: dict[str, str | None] = {
    "evm_transfers": "token_transfers",
    "internal_transfers": None,  # crypto_ethereum has no trace table
}


def bigquery_table_for(table: str) -> str | None:
    """Resolve a lake table to a BigQuery table, or None when unmapped.

    Accepts logical warehouse names (``evm_transfers``) as well as native
    BigQuery names (``token_transfers``, ``transactions``, ...).
    """
    if table in _BIGQUERY_TABLE_MAP:
        return _BIGQUERY_TABLE_MAP[table]
    return table if table in _TABLE_SELECT else None


class BigQuerySource:
    """Thin reader over the BigQuery public ``crypto_ethereum`` dataset.

    Args:
        enabled: Override ``BIGQUERY_ENABLED`` (default: credentials present).
        credentials_path: Service-account JSON path; defaults to
            ``GOOGLE_APPLICATION_CREDENTIALS`` or the fleet path.
        runner: Optional ``(sql, params) -> rows`` callable; defaults to the
            ``bq`` CLI. Tests inject a fake to avoid the network.
        bq_bin: ``bq`` executable; defaults to ``BIGQUERY_BQ_BIN``.
        router: Injected :class:`LakeProviderRouter` (tests).
    """

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        credentials_path: str | None = None,
        runner: Callable[[str, list[Any]], list[dict[str, Any]]] | None = None,
        bq_bin: str | None = None,
        router: Any | None = None,
    ) -> None:
        self._credentials = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or _DEFAULT_CREDS
        if enabled is None:
            enabled = _env_flag("BIGQUERY_ENABLED", os.path.exists(self._credentials))
        self._enabled = enabled
        self._runner = runner
        self._bq_bin = bq_bin or os.getenv("BIGQUERY_BQ_BIN") or "/usr/bin/bq"
        self._router_impl = router
        self._router_obj: Any | None = None

    def _router(self) -> Any:
        """Injected router (tests) or a lazily-built default LakeProviderRouter."""
        if self._router_impl is not None:
            return self._router_impl
        if self._router_obj is None:
            from lite_indexer.lakes.router import LakeProviderRouter

            self._router_obj = LakeProviderRouter()
        return self._router_obj

    # ── Availability ─────────────────────────────────────────────────

    def available(self) -> bool:
        """True when enabled, credentials exist, and a backend is runnable."""
        if not self._enabled:
            return False
        if not os.path.exists(self._credentials):
            logger.warning("bigquery_disabled", reason="credentials_missing")
            return False
        if self._runner is not None:
            return True
        if shutil.which(self._bq_bin) is None:
            logger.warning("bigquery_disabled", reason="bq_cli_missing")
            return False
        return True

    # ── Querying ─────────────────────────────────────────────────────

    def query(
        self,
        table: str,
        chain: str = "eth",
        *,
        address: str | None = None,
        from_block: int | None = None,
        to_block: int | None = None,
        dates: list[str] | None = None,
        limit: int = 100,
        mints: list[str] | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read ``crypto_ethereum.<table>`` rows in the warehouse row shape.

        Reads this backend directly (the rotation dispatcher owns provider
        selection). Returns an empty list when the tier is disabled, the table
        has no mapping, an address filter is unsupported for the table, or the
        read fails.
        """
        if chain == "solana":
            return self._query_solana(
                table,
                address=address,
                from_block=from_block,
                to_block=to_block,
                dates=dates,
                limit=limit,
                mints=mints,
                offset=offset,
            )
        if chain != "eth" or table not in _TABLE_SELECT:
            return []
        if not self.available():
            return []
        if address and not _ADDR_COLS.get(table):
            return []
        sql, params = build_query(
            table, address=address, from_block=from_block, to_block=to_block, dates=dates, limit=limit
        )
        cols, raw_rows = self._read_raw(sql, params)
        if not raw_rows:
            return []
        return [self._map_row(table, dict(zip(cols, row, strict=False))) for row in raw_rows]

    def _query_solana(
        self,
        table: str,
        *,
        address: str | None,
        from_block: int | None,
        to_block: int | None,
        dates: list[str] | None,
        limit: int,
        mints: list[str] | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read ``crypto_solana_mainnet_us`` rows in the solana row shape."""
        if table not in _SOLANA_TABLES:
            return []
        if not self.available():
            return []
        sql, params = build_query(
            table,
            chain="solana",
            address=address,
            from_block=from_block,
            to_block=to_block,
            dates=dates,
            limit=limit,
            mints=mints,
            offset=offset,
        )
        rows = self._runner(sql, params) if self._runner is not None else run_bq(self._bq_bin, sql, params)
        if not rows:
            return []
        return [self._map_row(table, r, chain="solana") for r in rows]

    def _read_raw(self, sql: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute via the injected runner (or ``bq``) and return (cols, rows)."""
        rows = self._runner(sql, params) if self._runner is not None else run_bq(self._bq_bin, sql, params)
        if not rows:
            return [], []
        cols = list(rows[0].keys())
        return cols, [tuple(r.get(c) for c in cols) for r in rows]

    # ── Row mapping ──────────────────────────────────────────────────

    def _map_row(self, table: str, raw: dict[str, Any], chain: str = "eth") -> dict[str, Any]:
        if chain == "solana":
            return self._map_solana_row(table, raw)
        return {
            "chain": "eth",
            "block_number": int(raw.get("block_number") or 0),
            "block_timestamp": _ms_to_dt(raw.get("block_timestamp")),
            "tx_hash": raw.get("tx_hash") or "",
            "from_address": _norm_addr(raw.get("from_address")),
            "to_address": _norm_addr(raw.get("to_address")),
            "token_address": _norm_addr(raw.get("token_address")),
            "value": _num_str(raw.get("value")),
            "log_index": int(raw.get("log_index") or 0),
            "ingested_at": datetime.now(UTC),
        }

    def _map_solana_row(self, table: str, raw: dict[str, Any]) -> dict[str, Any]:
        """Map a BigQuery solana row to the indexer's solana row shapes.

        ``token_transfers`` follows the ``solana_transfers`` shape used by
        :mod:`lite_indexer.query.sqd_solana` (``pre_amount`` is unknown in the
        public dataset, so it is ``"0"`` and ``delta == post_amount``);
        ``instructions`` follows the owner-activity instruction shape.
        """
        ts = _ms_to_dt(raw.get("block_timestamp"))
        ingested = datetime.now(UTC)
        if table == "instructions":
            accounts = raw.get("accounts") or []
            return {
                "chain": "solana",
                "slot": int(raw.get("slot") or 0),
                "block_timestamp": ts,
                "tx_hash": raw.get("tx_hash") or "",
                "transaction_index": raw.get("transaction_index"),
                "program_id": raw.get("program_id") or "",
                "accounts": list(accounts),
                "account_count": len(accounts),
                "data": raw.get("data") or "",
                "owner": "",
                "ingested_at": ingested,
            }
        value = str(raw.get("value") or "0")
        return {
            "chain": "solana",
            "slot": int(raw.get("slot") or 0),
            "block_timestamp": ts,
            "tx_hash": raw.get("tx_hash") or "",
            "token_account": "",
            "mint": raw.get("mint") or "",
            "owner": raw.get("owner") or "",
            "pre_amount": "0",
            "post_amount": value,
            "delta": value,
            "decimals": int(float(raw.get("decimals") or 0)),
            "token_symbol": "",
            "token_name": "",
            "source": raw.get("source") or "",
            "destination": raw.get("destination") or "",
            "transfer_type": raw.get("transfer_type") or "",
            "ingested_at": ingested,
        }

# ── CLI validation probe ─────────────────────────────────────────────


def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only); logs go to stderr."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        pass


def _cli() -> int:
    """``python -m lite_indexer.query.bigquery_source <chain> <table> ...``.

    Low-cost validation probe: always pass ``--dates`` (DAY-partition filter)
    and a small ``--limit``. Prints mapped rows as JSON to stdout.
    """
    import argparse
    import json as _json
    import sys as _sys

    _silence_console_logs()
    parser = argparse.ArgumentParser(prog="bigquery_source", description=__doc__)
    parser.add_argument("chain", choices=["eth", "solana"])
    parser.add_argument("table")
    parser.add_argument("--dates", nargs="*", default=None, help="YYYY-MM-DD partitions")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--address", default=None)
    args = parser.parse_args()
    src = BigQuerySource()
    if not src.available():
        print("bigquery source unavailable (credentials or bq CLI missing)", file=_sys.stderr)  # noqa: T201
        return 1
    rows = src.query(
        args.table,
        chain=args.chain,
        address=args.address,
        dates=args.dates,
        limit=args.limit,
    )
    print(_json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
