"""Tests for the provider-agnostic external flat-file bootstrap source.

Covers the contract:

* local-dir dataset with a ``manifest.json`` + parquet chunk → import
  produces the expected rows, tables and high-water advance;
* missing manifest (and no glob files) → clear error;
* dry-run builds the plan but imports nothing;
* the high-water guard skips an already-indexed dataset (and partial
  overlap imports only chunks above the mark);
* an http(s) source with a mocked fetch;
* manifest-less glob mode with best-effort block-range inference;
* CLI parsing for ``bootstrap --source`` / ``--force``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lite_indexer.cli import parse_args
from lite_indexer.ingest.flatfile_seams import set_url_fetcher
from lite_indexer.ingest.flatfile import (
    ExternalFlatfileSource,
    FlatfileDataset,
    FlatfileSourceError,
)
from lite_indexer.snapshot_manifest import MANIFEST_NAME

# ── fixtures ─────────────────────────────────────────────────────────


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a minimal parquet chunk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _manifest_dict(
    chain: str = "ethereum",
    start_block: int = 0,
    end_block: int = 200,
    tables: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if tables is None:
        tables = {
            "evm_transfers": {
                "rows": 3,
                "size_bytes": 512,
                "parquet_files": ["part-0.parquet"],
            }
        }
    return {
        "chain": chain,
        "date": "2026-01-01",
        "start_block": start_block,
        "end_block": end_block,
        "tables": tables,
    }


class FakeClickHouse:
    """ClickHouse client double: counts rows on SELECT count(), echoes inserts."""

    def __init__(self, counts: dict[str, int] | None = None, import_rows: int = 0) -> None:
        self.counts = counts or {}
        self.import_rows = import_rows
        self.queries: list[str] = []
        self.inserts: list[str] = []

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        self.queries.append(query)
        stripped = query.lstrip()
        if stripped.startswith("INSERT INTO"):
            self.inserts.append(query)
            return self.import_rows
        if stripped.startswith("CREATE TABLE"):
            return []
        if "SELECT count()" in query:
            target = stripped.split("FROM", 1)[1].strip().split(" ", 1)[0]
            table = target.rsplit(".", 1)[-1]
            return [(self.counts.get(table, 0),)]
        return []


class FakeSource(ExternalFlatfileSource):
    """ExternalFlatfileSource with a stubbed ClickHouse connection."""

    def __init__(self, client: FakeClickHouse, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client

    def _connect(self) -> Any | None:
        return self._client


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    patched = tmp_path / "state"
    patched.mkdir(parents=True, exist_ok=True)
    return patched


def _local_dataset(tmp_path: Path, manifest: dict[str, Any] | None = None) -> Path:
    """Create a local flat-file dataset dir with a parquet chunk + manifest."""
    base = tmp_path / "dataset"
    _write_parquet(
        base / "part-0.parquet",
        [{"block_number": 10, "tx_hash": "0x1"}, {"block_number": 200, "tx_hash": "0x2"}],
    )
    if manifest is not None:
        (base / MANIFEST_NAME).write_text(json.dumps(manifest))
    return base


# ═══════════════════════════════════════════════════════════════════════
# local directory + manifest
# ═══════════════════════════════════════════════════════════════════════


class TestLocalManifest:
    async def test_import_chain_local_manifest(self, tmp_path: Path, state_dir: Path) -> None:
        """Manifest import stages the chunk, reuses build_import_sql + advances state."""
        base = _local_dataset(tmp_path, _manifest_dict())
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        result = await src.import_chain("ethereum")

        assert result["status"] == "ok"
        assert result["rows_imported"] == 3
        assert result["block_range"] == [0, 200]
        assert result["tables"] == {"evm_transfers": 3}
        assert any("INSERT INTO wallet_memory.evm_transfers" in q for q in client.inserts)
        state_file = state_dir / "lite_ethereum.json"
        assert state_file.exists()
        assert json.loads(state_file.read_text())["last_block"] == 200

    async def test_import_stages_chunk_under_date_chain(self, tmp_path: Path, state_dir: Path) -> None:
        """build_import_sql paths resolve to the staged {date}/{chain} layout."""
        base = _local_dataset(tmp_path, _manifest_dict())
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        await src.import_chain("ethereum")

        assert "2026-01-01/ethereum/part-0.parquet" in client.inserts[0]

    async def test_missing_manifest_clear_error(self, tmp_path: Path, state_dir: Path) -> None:
        """No manifest + no glob files → a clear FlatfileSourceError."""
        base = tmp_path / "empty"
        base.mkdir()
        src = ExternalFlatfileSource(source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)
        with pytest.raises(FlatfileSourceError, match="manifest"):
            src.resolve("ethereum")
        result = await src.import_chain("ethereum")
        assert result["status"] == "error"
        assert "manifest" in result["errors"][0]

    async def test_dry_run_imports_nothing(self, tmp_path: Path, state_dir: Path) -> None:
        """Dry-run returns a plan, performs no CH writes and no state change."""
        base = _local_dataset(tmp_path, _manifest_dict())
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        result = await src.import_chain("ethereum", dry_run=True)

        assert result["status"] == "dry_run"
        assert result["plan"] and "INSERT INTO" in result["plan"][0]
        assert client.queries == []
        assert not (state_dir / "lite_ethereum.json").exists()


# ═══════════════════════════════════════════════════════════════════════
# high-water guard
# ═══════════════════════════════════════════════════════════════════════


class TestHighWaterGuard:
    async def test_skips_already_indexed(self, tmp_path: Path, state_dir: Path) -> None:
        """A high-water past the dataset end skips the import entirely."""
        base = _local_dataset(tmp_path, _manifest_dict(end_block=200))
        (state_dir / "highwater_ethereum.json").write_text(json.dumps({"last_block": 500}))
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        result = await src.import_chain("ethereum")

        assert result["status"] == "skipped"
        assert client.queries == []

    async def test_force_overrides_guard(self, tmp_path: Path, state_dir: Path) -> None:
        """--force imports despite being below the high-water."""
        base = _local_dataset(tmp_path, _manifest_dict(end_block=200))
        (state_dir / "highwater_ethereum.json").write_text(json.dumps({"last_block": 500}))
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(
            client,
            source=str(base),
            force=True,
            stage_dir=tmp_path / "stage",
            state_dir=state_dir,
        )

        result = await src.import_chain("ethereum")

        assert result["status"] == "ok"
        assert client.inserts

    async def test_partial_overlap_imports_only_newer_chunks(self, tmp_path: Path, state_dir: Path) -> None:
        """With chunk ranges, only chunks above the high-water are imported."""
        manifest = _manifest_dict(
            start_block=0,
            end_block=300,
            tables={
                "evm_transfers": {
                    "rows": 4,
                    "parquet_files": ["part-0.parquet", "part-1.parquet"],
                    "chunks": [
                        {"file": "part-0.parquet", "rows": 2, "start_block": 0, "end_block": 100},
                        {"file": "part-1.parquet", "rows": 2, "start_block": 101, "end_block": 300},
                    ],
                }
            },
        )
        base = tmp_path / "dataset"
        _write_parquet(base / "part-0.parquet", [{"block_number": 1}])
        _write_parquet(base / "part-1.parquet", [{"block_number": 200}])
        (base / MANIFEST_NAME).write_text(json.dumps(manifest))
        (state_dir / "highwater_ethereum.json").write_text(json.dumps({"last_block": 100}))

        client = FakeClickHouse(counts={"evm_transfers": 2}, import_rows=2)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        result = await src.import_chain("ethereum")

        assert result["status"] == "ok"
        assert len(client.inserts) == 1
        assert "part-1.parquet" in client.inserts[0]
        assert "part-0.parquet" not in client.inserts[0]


# ═══════════════════════════════════════════════════════════════════════
# http source (mocked fetch)
# ═══════════════════════════════════════════════════════════════════════


class TestHttpSource:
    async def test_http_import_with_mocked_fetch(
        self,
        tmp_path: Path,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An http(s) base URL imports via a mocked fetch (no real network)."""
        manifest = json.dumps(_manifest_dict())
        fetched: list[str] = []

        def fake_fetch(url: str) -> bytes:
            fetched.append(url)
            if url.endswith(MANIFEST_NAME):
                return manifest.encode()
            return b"PARQUET"

        set_url_fetcher(fake_fetch)
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(
            client,
            source="https://example.com/dataset",
            stage_dir=tmp_path / "stage",
            state_dir=state_dir,
        )

        assert src.scheme == "http"
        datasets = await src.list()
        assert datasets[0]["chain"] == "ethereum"
        result = await src.import_chain("ethereum")

        assert result["status"] == "ok"
        assert result["rows_imported"] == 3
        assert any(u.endswith("part-0.parquet") for u in fetched)

    async def test_http_list_without_manifest_errors(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A base URL cannot be enumerated without a manifest."""

        def fake_fetch(url: str) -> bytes:
            raise FlatfileSourceError(f"cannot fetch {url!r}: HTTP Error 404: Not Found")

        set_url_fetcher(fake_fetch)
        src = ExternalFlatfileSource(source="https://example.com/dataset", stage_dir=tmp_path / "stage")
        with pytest.raises(FlatfileSourceError, match="cannot be listed"):
            await src.list()


# ═══════════════════════════════════════════════════════════════════════
# glob mode (no manifest)
# ═══════════════════════════════════════════════════════════════════════


class TestGlobMode:
    async def test_glob_import_infers_block_range(self, tmp_path: Path, state_dir: Path) -> None:
        """Manifest-less hive layout imports with inferred block range."""
        base = tmp_path / "dataset"
        _write_parquet(
            base / "chain=ethereum" / "part-0.parquet",
            [{"block_number": 5, "tx_hash": "0x1"}, {"block_number": 42, "tx_hash": "0x2"}],
        )
        client = FakeClickHouse(import_rows=2)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        datasets = await src.list()
        assert datasets[0]["chain"] == "ethereum"
        assert datasets[0]["block_range"] == [5, 42]
        assert datasets[0]["manifest"] is False

        result = await src.import_chain("ethereum")
        assert result["status"] == "ok"
        assert result["imported_block_range"] == [5, 42]
        assert "evm_transfers" in result["tables"]

    async def test_glob_missing_chain_errors(self, tmp_path: Path, state_dir: Path) -> None:
        base = tmp_path / "dataset"
        (base / "chain=base").mkdir(parents=True)
        src = ExternalFlatfileSource(source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)
        with pytest.raises(FlatfileSourceError, match="chain=ethereum"):
            src.resolve("ethereum")


# ═══════════════════════════════════════════════════════════════════════
# dataset helpers
# ═══════════════════════════════════════════════════════════════════════


class TestDatasetHelpers:
    def test_above_excludes_unknown_and_lower_chunks(self) -> None:
        from lite_indexer.ingest.flatfile import FlatfileChunk, FlatfileTable

        dataset = FlatfileDataset(
            chain="ethereum",
            date="2026-01-01",
            start_block=0,
            end_block=300,
            tables={
                "evm_transfers": FlatfileTable(
                    name="evm_transfers",
                    chunks=(
                        FlatfileChunk("a.parquet", end_block=100),
                        FlatfileChunk("b.parquet", start_block=101, end_block=300),
                        FlatfileChunk("c.parquet"),
                    ),
                )
            },
        )
        filtered = dataset.above(100)
        assert filtered is not None
        assert [c.name for c in filtered.tables["evm_transfers"].chunks] == ["b.parquet"]
        assert filtered.above(300) is None

    async def test_import_all_aggregates(self, tmp_path: Path, state_dir: Path) -> None:
        base = _local_dataset(tmp_path, _manifest_dict())
        client = FakeClickHouse(counts={"evm_transfers": 3}, import_rows=3)
        src = FakeSource(client, source=str(base), stage_dir=tmp_path / "stage", state_dir=state_dir)

        result = await src.import_all()

        assert result["status"] == "ok"
        assert result["chains"]["ethereum"]["status"] == "ok"


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════


class TestCli:
    def test_parse_source_and_force(self) -> None:
        args = parse_args(["bootstrap", "--source", "/data/parquet", "--force"])
        assert args.source == "/data/parquet"
        assert args.force is True

    def test_parse_source_defaults(self) -> None:
        args = parse_args(["bootstrap", "--chain", "ethereum"])
        assert args.source is None
        assert args.force is False
