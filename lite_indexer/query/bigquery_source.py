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
from lite_indexer.query.bigquery_common import env_flag
from lite_indexer.query.bigquery_common import ms_to_dt as _ms_to_dt
from lite_indexer.query.bigquery_solana import map_solana_row, query_solana
from lite_indexer.query.bigquery_spec import BigQuerySpec
from lite_indexer.query.bigquery_tables import (
    _ADDR_COLS,
    _TABLE_SELECT,
    build_query,
    run_bq,
)
from lite_indexer.query.lake_row_utils import _norm_addr, _num_str  # re-exported for tests

logger = get_logger(__name__)

_DEFAULT_CREDS = "/srv/secrets/gcp-biglake-sa.json"


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
    """Thin reader over the BigQuery public datasets (eth + solana)."""

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
            enabled = env_flag("BIGQUERY_ENABLED", os.path.exists(self._credentials))
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


    def query(self, spec: BigQuerySpec) -> list[dict[str, Any]]:
        """Read warehouse rows in the mapped row shape for ``spec.table``.

        Reads this backend directly (the rotation dispatcher owns provider
        selection). Returns an empty list when the tier is disabled, the table
        has no mapping, an address filter is unsupported for the table, or the
        read fails.
        """
        if spec.chain == "solana":
            return query_solana(self, spec)
        if spec.chain != "eth" or spec.table not in _TABLE_SELECT:
            return []
        if not self.available():
            return []
        if spec.address and not _ADDR_COLS.get(spec.table):
            return []
        sql, params = build_query(spec)
        cols, raw_rows = self._read_raw(sql, params)
        if not raw_rows:
            return []
        return [self._map_row(spec.table, dict(zip(cols, row, strict=False))) for row in raw_rows]

    def _read_raw(self, sql: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute via the injected runner (or ``bq``) and return (cols, rows)."""
        rows = self._runner(sql, params) if self._runner is not None else run_bq(self._bq_bin, sql, params)
        if not rows:
            return [], []
        cols = list(rows[0].keys())
        return cols, [tuple(r.get(c) for c in cols) for r in rows]


    def _map_row(self, table: str, raw: dict[str, Any], chain: str = "eth") -> dict[str, Any]:
        if chain == "solana":
            return map_solana_row(table, raw)
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


if __name__ == "__main__":
    from lite_indexer.query.bigquery_cli import main as _cli_main

    raise SystemExit(_cli_main())
