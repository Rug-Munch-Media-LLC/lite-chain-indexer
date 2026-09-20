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
from lite_indexer.query.bigquery_tables import _ADDR_COLS, _TABLE_SELECT, build_query, run_bq
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
    ) -> list[dict[str, Any]]:
        """Read ``crypto_ethereum.<table>`` rows in the warehouse row shape.

        Returns an empty list when the tier is disabled, the table has no
        mapping, an address filter is unsupported for the table, or the read
        fails.
        """
        if chain != "eth" or table not in _TABLE_SELECT:
            return []
        if not self.available():
            return []
        if address and not _ADDR_COLS.get(table):
            return []
        sql, params = build_query(
            table, address=address, from_block=from_block, to_block=to_block, dates=dates, limit=limit
        )
        from lite_indexer.query.biglake_rotation import read_with_rotation

        return read_with_rotation(self, sql=sql, params=params, table=table, biglake_table=table)

    def _read_raw(self, sql: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute via the injected runner (or ``bq``) and return (cols, rows)."""
        rows = self._runner(sql, params) if self._runner is not None else run_bq(self._bq_bin, sql, params)
        if not rows:
            return [], []
        cols = list(rows[0].keys())
        return cols, [tuple(r.get(c) for c in cols) for r in rows]

    # ── Row mapping ──────────────────────────────────────────────────

    def _map_row(self, table: str, raw: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002 — uniform shape
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
