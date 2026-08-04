"""
Storage writers — NDJSON, ClickHouse, and Parquet cold-tier output.

Provides:
  - NDJSONWriter: Appends events as JSONL files organized by chain/date.
  - ClickHouseWriter: Inserts events via ClickHouse native protocol
    (with column-level TTL pruning for old rows).
  - ParquetExporter: Rolls old ClickHouse rows into Parquet, uploads to
    B2, and deletes them from ClickHouse to reclaim disk.
  - Wallet access path (competitive features): address-first projections
    (projections), token metadata reference table (token_meta), and
    per-token incremental balance caches (balance_cache).
  - Disk space management: check_disk_cap, check_disk_space, check_write_ok.

Disk cap and free-space checks are included before writes.
"""

from __future__ import annotations

from lite_indexer.metrics import get_metrics
from lite_indexer.storage.balance_cache import (
    ensure_balance_cache_table,
    ensure_holder_balances_mv,
    get_auto_holders,
    get_token_holders,
    update_holder_balance,
)
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.storage.disk import check_disk_cap, check_disk_space, check_write_ok
from lite_indexer.storage.ndjson import NDJSONWriter
from lite_indexer.storage.parquet import ExportSummary, ParquetExporter
from lite_indexer.storage.projections import ensure_wallet_projection
from lite_indexer.storage.reconciliation import (
    ensure_reconciliation_table,
    run_reconciliation,
    select_random_wallets,
    verify_transfer_completeness,
)
from lite_indexer.storage.token_meta import (
    ensure_token_meta_table,
    resolve_token_meta,
    upsert_token_meta,
)
from lite_indexer.storage.trace_store import (
    ensure_internal_transfers_table,
    write_internal_transfers,
)

__all__ = [
    "ClickHouseWriter",
    "NDJSONWriter",
    "ParquetExporter",
    "ExportSummary",
    "check_disk_cap",
    "check_disk_space",
    "check_write_ok",
    "get_metrics",
    # Wallet access path (competitive features)
    "ensure_wallet_projection",
    "ensure_token_meta_table",
    "upsert_token_meta",
    "resolve_token_meta",
    "ensure_balance_cache_table",
    "update_holder_balance",
    "get_token_holders",
    "ensure_holder_balances_mv",
    "get_auto_holders",
    # Balance reconciliation (TrueBlocks)
    "ensure_reconciliation_table",
    "select_random_wallets",
    "verify_transfer_completeness",
    "run_reconciliation",
    # Internal-transfer traces
    "ensure_internal_transfers_table",
    "write_internal_transfers",
]
