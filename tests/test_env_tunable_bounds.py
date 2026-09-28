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

import importlib

import pytest

from lite_indexer.config import indexing as indexing_config
from lite_indexer.ingest import lake_backfill


def test_lake_backfill_batch_rows_is_env_tunable(monkeypatch: pytest.MonkeyPatch) -> None:
    """LAKE_BACKFILL_BATCH_ROWS moves the backfill batch bound."""
    monkeypatch.setenv("LAKE_BACKFILL_BATCH_ROWS", "5000")
    reloaded = importlib.reload(lake_backfill)
    try:
        req = reloaded.LakeBackfillRequest(chain="base", dates=[])
        assert req.batch_rows == 5000
    finally:
        monkeypatch.delenv("LAKE_BACKFILL_BATCH_ROWS", raising=False)
        importlib.reload(lake_backfill)


def test_lake_backfill_batch_rows_defaults_to_1000(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default stays 1000 — commit 0e35831 lowered it for a live stall."""
    monkeypatch.delenv("LAKE_BACKFILL_BATCH_ROWS", raising=False)
    reloaded = importlib.reload(lake_backfill)
    try:
        assert reloaded.LakeBackfillRequest(chain="base", dates=[]).batch_rows == 1000
    finally:
        importlib.reload(lake_backfill)


def test_disk_cap_gb_env_var_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    """DISK_CAP_GB in the environment sets DISK_CAP_BYTES."""
    monkeypatch.setenv("DISK_CAP_GB", "150")
    reloaded = importlib.reload(indexing_config)
    try:
        assert reloaded.DISK_CAP_GB == 150
        assert reloaded.DISK_CAP_BYTES == 150 * 1024**3
    finally:
        monkeypatch.delenv("DISK_CAP_GB", raising=False)
        importlib.reload(indexing_config)


def test_disk_cap_default_is_10gb(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset DISK_CAP_GB keeps the documented 10 GB default."""
    monkeypatch.delenv("DISK_CAP_GB", raising=False)
    reloaded = importlib.reload(indexing_config)
    try:
        assert reloaded.DISK_CAP_BYTES == 10 * 1024**3
    finally:
        importlib.reload(indexing_config)


def test_env_example_documents_the_key_we_read() -> None:
    """.env.example's DISK_CAP_GB is the name the code actually reads."""
    from pathlib import Path

    env_example = Path(__file__).resolve().parents[1] / ".env.example"
    text = env_example.read_text(encoding="utf-8")
    assert "DISK_CAP_GB" in text
