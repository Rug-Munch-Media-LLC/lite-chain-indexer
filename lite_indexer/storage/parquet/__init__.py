# DEPRECATED: Canonical export is now /root/scripts/lite-indexer-parquet-daily.sh
# which uses ClickHouse-native FORMAT Parquet (not pyarrow).
# This module retained for dry-run/testing. Do not schedule.
"""Parquet cold storage tier — split into sub-modules.

- :mod:`lite_indexer.storage.parquet.schema` — ExportSummary, constants, DDL
- :mod:`lite_indexer.storage.parquet.helpers` — path/marker helpers
- :mod:`lite_indexer.storage.parquet.reader` — day-row fetch + file verify
- :mod:`lite_indexer.storage.parquet.writer` — ParquetExporter orchestration
"""

from __future__ import annotations


from lite_indexer.storage.parquet.helpers import (
    _block_range,
    _local_dir,
    _local_path,
    _remote_dir,
    _remote_path,
    _state_marker,
)
from lite_indexer.storage.parquet.reader import _fetch_day_rows, _verify_parquet
from lite_indexer.storage.parquet.schema import (
    _MAX_ROWS_PER_DAY,
    _STATE_TABLE_DDL,
    _STATUS_DELETED,
    _STATUS_EXPORTING,
    _STATUS_FAILED,
    _STATUS_PENDING,
    _STATUS_UPLOADED,
    PYARROW_AVAILABLE,
    ExportSummary,
)
from lite_indexer.storage.parquet.writer import ParquetExporter

__all__ = [
    "ParquetExporter",
    "ExportSummary",
    "PYARROW_AVAILABLE",
    "_MAX_ROWS_PER_DAY",
    "_STATE_TABLE_DDL",
    "_STATUS_PENDING",
    "_STATUS_EXPORTING",
    "_STATUS_UPLOADED",
    "_STATUS_DELETED",
    "_STATUS_FAILED",
    "_local_dir",
    "_local_path",
    "_remote_dir",
    "_remote_path",
    "_state_marker",
    "_block_range",
    "_fetch_day_rows",
    "_verify_parquet",
    "shutil",
    "subprocess",
]
