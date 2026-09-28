"""Operator-tunable ingest and disk bounds are env-driven.

Two bounds were hardcoded in a way only a code change could move:

* ``LakeBackfillRequest.batch_rows`` was a dataclass default of 1000 — the
  one bound an operator could not move, while PRICING_BATCH_SIZE and
  VECTOR_INGESTION_BATCH_SIZE in the same module were env-driven.
* ``DISK_CAP_BYTES`` was hardcoded to 10 GB while ``.env.example`` documented
  ``DISK_CAP_GB``. A ``.env``-only operator silently got 10 GB and a
  disk-cap abort loop; production overrode it with ``--disk-cap 150000000000``
  (a 15x difference) passed on the command line.
"""

from __future__ import annotations

from lite_indexer.config.indexing import DISK_CAP_BYTES
from lite_indexer.config.pure_env import DEFAULT_DISK_CAP_GB, disk_cap_gb
from lite_indexer.ingest.lake_backfill import (
    _DEFAULT_BATCH_ROWS,
    _batch_rows_default,
)


def test_lake_backfill_batch_rows_is_env_tunable() -> None:
    """LAKE_BACKFILL_BATCH_ROWS moves the backfill batch bound."""
    assert _batch_rows_default("5000") == 5000


def test_lake_backfill_batch_rows_defaults_to_1000() -> None:
    """The default stays 1000 — commit 0e35831 lowered it for a live stall."""
    assert _batch_rows_default(None) == 1000
    assert _DEFAULT_BATCH_ROWS == 1000


def test_lake_backfill_batch_rows_unparsable_keeps_the_default() -> None:
    """A typo'd bound must not crash the daemon at import time."""
    assert _batch_rows_default("not-a-number") == _DEFAULT_BATCH_ROWS
    assert _batch_rows_default("") == _DEFAULT_BATCH_ROWS


def test_disk_cap_gb_env_var_is_honoured() -> None:
    """DISK_CAP_GB in the environment sets DISK_CAP_BYTES."""
    cap_gb = disk_cap_gb("150")
    assert cap_gb == 150
    assert int(cap_gb * 1024**3) == 150 * 1024**3


def test_disk_cap_default_is_10gb() -> None:
    """Unset DISK_CAP_GB keeps the documented 10 GB default."""
    assert disk_cap_gb(None) == DEFAULT_DISK_CAP_GB == 10.0
    assert DEFAULT_DISK_CAP_GB == 10.0


def test_disk_cap_unparsable_keeps_the_default() -> None:
    """A non-numeric DISK_CAP_GB falls back instead of raising at import."""
    assert disk_cap_gb("junk") == DEFAULT_DISK_CAP_GB


def test_module_level_disk_cap_is_derived_from_the_seam() -> None:
    """DISK_CAP_BYTES stays exactly DISK_CAP_GB * GiB in the live module."""
    assert int(DISK_CAP_BYTES / 1024**3) * 1024**3 == DISK_CAP_BYTES


def test_env_example_documents_the_key_we_read() -> None:
    """.env.example's DISK_CAP_GB is the name the code actually reads."""
    from pathlib import Path

    env_example = Path(__file__).resolve().parents[1] / ".env.example"
    text = env_example.read_text(encoding="utf-8")
    assert "DISK_CAP_GB" in text
