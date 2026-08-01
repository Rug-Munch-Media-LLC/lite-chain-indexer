"""
Storage writers — NDJSON and ClickHouse output.

Provides:
  - NDJSONWriter: Appends events as JSONL files organized by chain/date.
  - ClickHouseWriter: Inserts events via ClickHouse native protocol.
  - Disk space management: check_disk_cap, check_disk_space, check_write_ok.

Disk cap and free-space checks are included before writes.
"""

from __future__ import annotations

from lite_indexer.metrics import get_metrics
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.storage.disk import check_disk_cap, check_disk_space, check_write_ok
from lite_indexer.storage.ndjson import NDJSONWriter

__all__ = [
    "ClickHouseWriter",
    "NDJSONWriter",
    "check_disk_cap",
    "check_disk_space",
    "check_write_ok",
    "get_metrics",
]
