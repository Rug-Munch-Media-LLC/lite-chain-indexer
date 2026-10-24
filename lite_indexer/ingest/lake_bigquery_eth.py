"""Fill ClickHouse ethereum history from ``bigquery-public-data.crypto_ethereum``.

Streams ONE BigQuery job per (day, table) straight into the live ClickHouse
writers — no local Parquet staging (a 90-day slice would need ~41 GB of disk).

Idempotency is enforced twice:

* day-level — a day already present in ClickHouse is skipped unless ``force``;
* key-level — ``evm_transfers`` is ReplacingMergeTree keyed
  ``(chain, block_number, log_index, tx_log_key)`` (the writer derives
  ``tx_log_key`` from ``(block_number, tx_hash, log_index)``) and
  ``internal_transfers`` is keyed ``(chain, block_number, tx_hash,
  trace_address)``. A replayed row collapses at merge time, and the writer's
  existence check skips it immediately.

A NULL ``value`` is NEVER coerced to ``"0"`` (the known sonarx defect):
token transfers with a NULL value are counted and SKIPPED (reported as
``null_values``); trace rows with a NULL value are genuinely zero-value calls,
so they are counted separately and written as 0.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, NamedTuple

from lite_indexer.ingest.lake_bigquery_rows import (  # noqa: F401 — re-exported
    _CHAIN,
    _Sink,
    map_trace,
    map_transfer,
)
from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_common import BigQueryQuotaError
from lite_indexer.query.bigquery_rest import stream_query
from lite_indexer.query.bigquery_tables import _TABLE_SELECT
from lite_indexer.storage.clickhouse import ClickHouseUnavailable

logger = get_logger(__name__)

FILLABLE_TABLES: tuple[str, ...] = ("token_transfers", "traces")
_TABLE_OF = {"token_transfers": "evm_transfers", "traces": "internal_transfers"}
#: Measured day-partition scan (dry-run, 2026-10-20): 113 MB / 252 MB.
_DAY_GIB = {"token_transfers": 0.12, "traces": 0.25}


@dataclass
class EthFillRequest:
    """One ethereum BigQuery fill run: dates, tables, batching, guards."""

    dates: list[str]
    tables: tuple[str, ...] = ("token_transfers",)
    dry_run: bool = False
    batch_rows: int = 5000
    force: bool = False
    max_scan_gib: float = 50.0
    force_budget: bool = False


@dataclass
class EthFillResult:
    """Rows streamed / skipped and the scan cost of the run."""

    counts: dict[str, int] = field(default_factory=dict)
    null_values: dict[str, int] = field(default_factory=dict)
    rows: int = 0
    scan_gib: float = 0.0


def day_sql(table: str, day: str) -> str:
    """Date-bounded SELECT with no LIMIT (a full day exceeds any sane cap)."""
    start = datetime.fromisoformat(day)
    end = start + timedelta(days=1)
    cols = _TABLE_SELECT[table]
    lo = start.strftime("%Y-%m-%d 00:00:00")
    hi = end.strftime("%Y-%m-%d 00:00:00")
    return (
        f"SELECT {cols} FROM `bigquery-public-data.crypto_ethereum.{table}` "  # nosemgrep: formatted-sql-query — allowlisted table + date-derived bounds
        f'WHERE block_timestamp >= TIMESTAMP "{lo}" AND block_timestamp < TIMESTAMP "{hi}"'
    )


def _existing_day_rows(client: Any, table: str, day: str) -> int | None:
    """Rows already in ClickHouse for ``day`` (None = check unavailable)."""
    sql = (
        f"SELECT count() FROM wallet_memory.{_TABLE_OF[table]} "  # nosemgrep: formatted-sql-query — allowlisted table, day is a bound parameter
        "WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s"
    )
    try:
        rows = client.execute(sql, {"chain": _CHAIN, "day": day})
    except Exception as exc:  # noqa: BLE001 — the guard is best-effort
        logger.debug("eth_fill_day_count_failed", day=day, table=table, error=str(exc)[:100])
        return None
    return int(rows[0][0]) if rows else 0


def _scan_estimate(req: EthFillRequest) -> float:
    return sum(len(req.dates) * _DAY_GIB[t] for t in req.tables)


class _FillCtx(NamedTuple):
    """Immutable per-run fill context (request + resolved BigQuery account)."""

    req: EthFillRequest
    project: str
    creds: str


def backfill_eth_from_bigquery(
    req: EthFillRequest,
    *,
    ch: Any | None = None,
    project: str | None = None,
    creds: str | None = None,
) -> EthFillResult:
    """Stream ``req`` from BigQuery into ClickHouse; returns counts + scan cost."""
    bad = [t for t in req.tables if t not in FILLABLE_TABLES]
    if bad:
        raise ValueError(f"unsupported eth fill table(s): {bad} (fillable: {FILLABLE_TABLES})")
    est = _scan_estimate(req)
    if est > req.max_scan_gib and not req.force_budget:
        raise ValueError(
            f"estimated scan {est:.1f} GiB exceeds --max-scan-gib {req.max_scan_gib:.0f} "
            "(free tier = 1 TiB/month). Narrow the range or pass --force-budget."
        )
    from lite_indexer.query.bigquery_cli import bq_accounts

    if project is None or creds is None:
        project, creds = bq_accounts()[0]
    if ch is None:
        from lite_indexer.storage.clickhouse import ClickHouseWriter

        ch = ClickHouseWriter()
    out = EthFillResult(scan_gib=est)
    for day in req.dates:
        for table in req.tables:
            _fill_partition(ch, table, day, _FillCtx(req, project, creds), out)
    return out


def _fill_partition(ch: Any, table: str, day: str, ctx: _FillCtx, out: EthFillResult) -> None:
    """Fill one (day, table) partition and record its counts on ``out``."""
    key = f"eth/{day}/{table}"
    client = ch.client()
    if client is None:
        raise ClickHouseUnavailable(f"ClickHouse unreachable (chain={_CHAIN})")
    try:
        if _skip_existing(client, table, day, ctx, out):
            return
        sink = _Sink(ch, table, ctx.req.batch_rows)
        _stream_partition(sink, table, day, ctx)
        sink.flush()
        _record_partition(out, key, table, day, sink)
    except ClickHouseUnavailable:
        logger.error("eth_fill_aborted_clickhouse_unavailable", date=day, table=table)
        raise
    except BigQueryQuotaError:
        logger.error("eth_fill_aborted_quota", date=day, table=table)
        raise
    except Exception as exc:  # noqa: BLE001 - one bad day must not abort the run
        logger.warning("eth_fill_partition_failed", date=day, table=table, error=str(exc)[:160])
        out.counts[key] = 0


def _skip_existing(client: Any, table: str, day: str, ctx: _FillCtx, out: EthFillResult) -> bool:
    """True when the day is already in ClickHouse and ``--force`` is absent."""
    existing = _existing_day_rows(client, table, day)
    if existing and not ctx.req.force:
        logger.warning("eth_fill_day_already_present", date=day, table=table, rows=existing)
        out.counts[f"eth/{day}/{table}"] = 0
        return True
    return False


def _stream_partition(sink: _Sink, table: str, day: str, ctx: _FillCtx) -> None:
    """Stream one BigQuery day-partition page by page into ``sink``."""
    for page in stream_query(day_sql(table, day), [], project=ctx.project, creds_path=ctx.creds):
        if ctx.req.dry_run:
            sink.rows += len(page)
            continue
        for raw in page:
            sink.add(raw)


def _record_partition(out: EthFillResult, key: str, table: str, day: str, sink: _Sink) -> None:
    """Fold one finished partition's counts into the run result."""
    out.counts[key] = sink.rows
    if sink.nulls:
        out.null_values[key] = sink.nulls
        logger.warning("eth_fill_null_value", date=day, table=table, rows=sink.nulls)
    out.rows += sink.rows
    logger.info("eth_fill_progress", date=day, table=table, rows=sink.rows, total=out.rows)
