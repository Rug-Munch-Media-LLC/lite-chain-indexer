"""Schema definitions and constants for the parquet cold-storage tier."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config import CH_PARQUET_STATE_TABLE

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]
    PYARROW_AVAILABLE = False

# ClickHouse lightweight-delete maximum mutation size guard (rows per day).
_MAX_ROWS_PER_DAY = 5_000_000

# Statuses recorded in the parquet_export_state table.
_STATUS_PENDING = "pending"
_STATUS_EXPORTING = "exporting"
_STATUS_UPLOADED = "uploaded"
_STATUS_DELETED = "deleted"
_STATUS_FAILED = "failed"

# ClickHouse state table DDL (must stay in sync with
# CH_PARQUET_STATE_TABLE and the README). ReplacingMergeTree keeps the
# last-inserted row per (chain, date), so plain INSERTs upsert state.
_STATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_PARQUET_STATE_TABLE} (
    chain LowCardinality(String),
    date Date,
    status LowCardinality(String),
    evm_transfers_rows UInt64 DEFAULT 0,
    dex_events_rows UInt64 DEFAULT 0,
    parquet_bytes UInt64 DEFAULT 0,
    uploaded_at Nullable(DateTime),
    deleted_at Nullable(DateTime),
    error_message String DEFAULT ''
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, date)
"""


@dataclass
class ExportSummary:
    """Outcome of a parquet export run for one chain."""

    chain: str
    dates_attempted: int = 0
    dates_exported: int = 0
    transfers_rows: int = 0
    dex_rows: int = 0
    bytes_written: int = 0
    uploaded: int = 0
    deleted_rows: int = 0
    skipped_existing: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary for CLI output."""
        return {
            "chain": self.chain,
            "dates_attempted": self.dates_attempted,
            "dates_exported": self.dates_exported,
            "transfers_rows": self.transfers_rows,
            "dex_rows": self.dex_rows,
            "bytes_written": self.bytes_written,
            "files_uploaded": self.uploaded,
            "rows_deleted": self.deleted_rows,
            "skipped_existing": self.skipped_existing,
            "errors": self.errors,
        }


__all__ = [
    "ExportSummary",
    "PYARROW_AVAILABLE",
    "pa",
    "pq",
    "_MAX_ROWS_PER_DAY",
    "_STATUS_PENDING",
    "_STATUS_EXPORTING",
    "_STATUS_UPLOADED",
    "_STATUS_DELETED",
    "_STATUS_FAILED",
    "_STATE_TABLE_DDL",
]
