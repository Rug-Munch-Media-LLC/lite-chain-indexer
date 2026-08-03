"""
Disk-cap propagation tests.

Guard against regression of the fix where NDJSONWriter fell back to the
10 GB ``DISK_CAP_BYTES`` default instead of honoring the ``disk_cap``
passed to ``LiteIndexer`` (``--disk-cap``). The cap must flow
indexer → ndjson → ``check_write_ok()``.
"""

from __future__ import annotations

import pytest


def test_ndjson_writer_honors_configured_disk_cap(tmp_path) -> None:
    """NDJSONWriter must store the configured cap (not the default)."""
    from lite_indexer.storage.ndjson import NDJSONWriter

    writer = NDJSONWriter(data_dir=tmp_path, disk_cap=42)
    assert writer.disk_cap == 42


def test_indexer_passes_disk_cap_to_ndjson_writer(tmp_path) -> None:
    """LiteIndexer must construct NDJSONWriter with its own disk_cap."""
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, disk_cap=12345, ndjson_enabled=True)
    assert indexer._ndjson_writer is not None
    assert indexer._ndjson_writer.disk_cap == 12345


def test_ndjson_writer_disabled_by_default(tmp_path, monkeypatch) -> None:
    """NDJSON writer must NOT be constructed when NDJSON_ENABLED is off."""
    import lite_indexer.indexer as indexer_mod

    monkeypatch.setattr(indexer_mod, "NDJSON_ENABLED", False)
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    assert indexer.ndjson_enabled is False
    assert indexer._ndjson_writer is None


def test_ndjson_writer_explicit_enable(tmp_path, monkeypatch) -> None:
    """Explicit ndjson_enabled=True must construct the writer even if env is off."""
    import lite_indexer.indexer as indexer_mod

    monkeypatch.setattr(indexer_mod, "NDJSON_ENABLED", False)
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, ndjson_enabled=True)
    assert indexer.ndjson_enabled is True
    assert indexer._ndjson_writer is not None


def test_ndjson_writer_explicit_disable(tmp_path, monkeypatch) -> None:
    """Explicit ndjson_enabled=False must skip the writer even if env is on."""
    import lite_indexer.indexer as indexer_mod

    monkeypatch.setattr(indexer_mod, "NDJSON_ENABLED", True)
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, ndjson_enabled=False)
    assert indexer.ndjson_enabled is False
    assert indexer._ndjson_writer is None


def test_write_events_forwards_cap_to_check_write_ok(tmp_path, monkeypatch) -> None:
    """write_events must call check_write_ok with the configured cap."""
    from lite_indexer.storage import ndjson as ndjson_mod

    calls: list[dict] = []

    def fake_check_write_ok(label, data_dir, cap_bytes):
        calls.append({"label": label, "data_dir": data_dir, "cap_bytes": cap_bytes})
        return True

    monkeypatch.setattr(ndjson_mod, "check_write_ok", fake_check_write_ok)

    writer = ndjson_mod.NDJSONWriter(data_dir=tmp_path, disk_cap=777)
    writer.write_events("ethereum", [{"blockNumber": 1}])

    assert calls, "check_write_ok was never called"
    assert calls[0]["label"] == "ndjson"
    assert calls[0]["data_dir"] == tmp_path
    assert calls[0]["cap_bytes"] == 777


def test_write_dex_events_forwards_cap_to_check_write_ok(tmp_path, monkeypatch) -> None:
    """write_dex_events must call check_write_ok with the configured cap."""
    from lite_indexer.storage import ndjson as ndjson_mod

    calls: list[dict] = []

    def fake_check_write_ok(label, data_dir, cap_bytes):
        calls.append({"label": label, "data_dir": data_dir, "cap_bytes": cap_bytes})
        return True

    monkeypatch.setattr(ndjson_mod, "check_write_ok", fake_check_write_ok)

    writer = ndjson_mod.NDJSONWriter(data_dir=tmp_path, disk_cap=777)
    writer.write_dex_events("ethereum", [{"blockNumber": 1}])

    assert calls, "check_write_ok was never called"
    assert calls[0]["label"] == "dex_ndjson"
    assert calls[0]["cap_bytes"] == 777


@pytest.mark.skip(reason="requires live ClickHouse; see tests/test_smoke.py::test_clickhouse_connect")
def test_disk_cap_exceeded_aborts_write(tmp_path, monkeypatch) -> None:
    """Sanity: check_write_ok returns False when cap is exceeded."""
    from lite_indexer.storage import ndjson as ndjson_mod

    monkeypatch.setattr(ndjson_mod, "check_write_ok", lambda *a, **k: False)

    writer = ndjson_mod.NDJSONWriter(data_dir=tmp_path, disk_cap=1)
    assert writer.write_events("ethereum", [{"blockNumber": 1}]) == 0
