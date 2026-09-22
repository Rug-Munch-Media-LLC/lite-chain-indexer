"""Tests for the snapshot bootstrap system (B2 parquet → ClickHouse).

Covers the spec contract:

* manifest format: build/parse/validate round-trip, remote path helpers
* ClickHouse import statement generation
  (``INSERT INTO ... SELECT * FROM file('*.parquet')``)
* row-count verification against the manifest
* state high-water mark setting (forward-only)
* ``list_snapshots`` via mocked rclone
* full ``bootstrap_chain`` flow with mocked rclone + ClickHouse
* CLI parsing for the ``bootstrap`` subcommand
* snapshot manifest publishing from the parquet export pipeline
"""

from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from lite_indexer.bootstrap import SnapshotBootstrap, build_import_sql
from lite_indexer.cli import parse_args
from lite_indexer.snapshot_manifest import (
    MANIFEST_NAME,
    SnapshotManifest,
    build_manifest,
    manifest_json,
    parse_manifest,
)
from lite_indexer.storage.parquet import ParquetExporter, _block_range, _local_dir

# ── fixtures ─────────────────────────────────────────────────────────


def _manifest_dict(
    chain: str = "ethereum",
    date_str: str = "2026-08-01",
    start_block: int = 100,
    end_block: int = 25_400_000,
    tables: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if tables is None:
        tables = {
            "evm_transfers": {
                "rows": 359_000_000,
                "size_bytes": 21_474_836_480,
                "parquet_files": ["ethereum_2026-08-01.parquet"],
            },
            "dex_events": {
                "rows": 42_000_000,
                "size_bytes": 5_000_000_000,
                "parquet_files": ["ethereum_dex_2026-08-01.parquet"],
            },
        }
    return {
        "chain": chain,
        "date": date_str,
        "start_block": start_block,
        "end_block": end_block,
        "tables": tables,
    }


def _manifest() -> SnapshotManifest:
    return parse_manifest(_manifest_dict())


class FakeClickHouse:
    """Minimal ClickHouse client double for bootstrap tests."""

    def __init__(self, counts: dict[str, int] | None = None, import_rows: int = 123) -> None:
        self.counts = counts or {}
        self.import_rows = import_rows
        self.queries: list[str] = []
        self.inserts: list[str] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        stripped = query.lstrip()
        if stripped.startswith("INSERT INTO"):
            self.inserts.append(query)
            return self.import_rows
        if stripped.startswith("CREATE TABLE"):
            return []
        if "SELECT count()" in query:
            # Parse the table name from: SELECT count() FROM {db}.{table} WHERE ...
            target = stripped.split("FROM", 1)[1].strip().split(" ", 1)[0]
            table = target.rsplit(".", 1)[-1]
            return [(self.counts.get(table, 0),)]
        return []


class FakeBootstrap(SnapshotBootstrap):
    """SnapshotBootstrap with a stubbed ClickHouse connection."""

    def __init__(self, client: FakeClickHouse, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client

    def _connect(self) -> Any | None:
        return self._client


def _patch_rclone(
    monkeypatch: pytest.MonkeyPatch,
    *,
    lsf_output: str = "",
    cat_map: dict[str, str] | None = None,
    copy_ok: bool = True,
) -> list[list[str]]:
    """Patch subprocess.run to emulate rclone lsf/cat/copy; record calls."""
    calls: list[list[str]] = []
    cat_map = cat_map or {}

    def fake_run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        args = list(cmd)
        calls.append(args)
        action = args[1] if len(args) > 1 else ""
        if action == "lsf":
            return subprocess.CompletedProcess(args=args, returncode=0, stdout=lsf_output, stderr="")
        if action == "cat":
            remote = args[2]
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout=cat_map.get(remote, ""),
                stderr="",
            )
        if action == "copy":
            rc = 0 if copy_ok else 1
            return subprocess.CompletedProcess(args=args, returncode=rc, stdout="", stderr="" if copy_ok else "boom")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("lite_indexer.bootstrap.subprocess.run", fake_run)
    monkeypatch.setattr("lite_indexer.bootstrap.shutil.which", lambda _bin: "/usr/bin/rclone")
    return calls


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    """Patch STATE_DIR to a temp dir for the duration of the test."""
    patched = tmp_path / "state"
    patched.mkdir(parents=True, exist_ok=True)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("lite_indexer.state.STATE_DIR", patched)
        yield patched


# ═══════════════════════════════════════════════════════════════════════
# manifest format
# ═══════════════════════════════════════════════════════════════════════


class TestManifest:
    def test_roundtrip(self) -> None:
        """build_manifest → to_dict → parse_manifest preserves everything."""
        manifest = build_manifest(
            "ethereum",
            "2026-08-01",
            100,
            25_400_000,
            {
                "evm_transfers": {"rows": 10, "size_bytes": 2048, "parquet_files": ["a.parquet"]},
                "dex_events": {"rows": 5, "size_bytes": 1024, "parquet_files": ["b.parquet"]},
            },
        )
        parsed = parse_manifest(manifest.to_dict())
        assert parsed.chain == "ethereum"
        assert parsed.date == "2026-08-01"
        assert parsed.start_block == 100
        assert parsed.end_block == 25_400_000
        assert parsed.tables["evm_transfers"].rows == 10
        assert parsed.tables["dex_events"].parquet_files == ("b.parquet",)

    def test_parse_from_json_string(self) -> None:
        """parse_manifest accepts a JSON string."""
        manifest = parse_manifest(json.dumps(_manifest_dict()))
        assert manifest.chain == "ethereum"
        assert manifest.total_rows() == 401_000_000

    def test_missing_keys_raise(self) -> None:
        """Missing required keys are rejected."""
        bad = _manifest_dict()
        del bad["tables"]
        with pytest.raises(ValueError, match="tables"):
            parse_manifest(bad)

    def test_bad_blocks_raise(self) -> None:
        """end_block below start_block is rejected."""
        bad = _manifest_dict(start_block=100, end_block=50)
        with pytest.raises(ValueError, match="end_block"):
            parse_manifest(bad)

    def test_invalid_json_raises(self) -> None:
        """Non-JSON input is rejected."""
        with pytest.raises(ValueError, match="not valid JSON"):
            parse_manifest("{not json")

    def test_totals_and_block_range(self) -> None:
        """Aggregate helpers compute totals + block range."""
        manifest = _manifest()
        assert manifest.total_rows() == 401_000_000
        assert manifest.total_files() == 2
        assert manifest.total_size_bytes() == 26_474_836_480
        assert manifest.block_range() == (100, 25_400_000)

    def test_remote_paths(self) -> None:
        """Snapshot dir + manifest path match the documented layout."""
        manifest = _manifest()
        assert manifest.snapshot_dir() == "b2-crypt:chain-data/snapshots/2026-08-01/ethereum"
        assert manifest.manifest_path() == ("b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json")
        # custom bucket + path prefix
        assert manifest.snapshot_dir("b2-crypt:other", "archives") == ("b2-crypt:other/archives/2026-08-01/ethereum")

    def test_manifest_json_serializes(self) -> None:
        """manifest_json produces parseable JSON with the spec schema."""
        raw = json.loads(manifest_json(_manifest()))
        assert set(raw) == {"chain", "date", "start_block", "end_block", "tables"}
        assert "rows" in raw["tables"]["evm_transfers"]


# ═══════════════════════════════════════════════════════════════════════
# ClickHouse import statement generation
# ═══════════════════════════════════════════════════════════════════════


class TestImportSql:
    def test_build_import_sql_per_table(self, tmp_path: Path) -> None:
        """One zero-copy file() import per manifest table."""
        manifest = _manifest()
        statements = build_import_sql(manifest, tmp_path, "wallet_memory")
        assert len(statements) == 2
        assert statements[0] == (
            "INSERT INTO wallet_memory.evm_transfers SELECT * FROM file("
            f"'{tmp_path}/2026-08-01/ethereum/ethereum_2026-08-01.parquet')"
        )
        assert statements[1] == (
            "INSERT INTO wallet_memory.dex_events SELECT * FROM file("
            f"'{tmp_path}/2026-08-01/ethereum/ethereum_dex_2026-08-01.parquet')"
        )

    def test_build_import_sql_multiple_files(self, tmp_path: Path) -> None:
        """Multiple parquet files for one table are all listed."""
        manifest = parse_manifest(
            _manifest_dict(
                tables={
                    "evm_transfers": {
                        "rows": 10,
                        "size_bytes": 2048,
                        "parquet_files": ["a.parquet", "b.parquet"],
                    }
                }
            )
        )
        statements = build_import_sql(manifest, tmp_path, "wallet_memory")
        assert len(statements) == 1
        assert "a.parquet', '" in statements[0]
        assert "b.parquet'" in statements[0]

    def test_build_import_sql_ch_file_root(self, tmp_path: Path) -> None:
        """ch_file_root remaps file() paths for containerized ClickHouse."""
        manifest = _manifest()
        statements = build_import_sql(manifest, tmp_path, "wallet_memory", ch_file_root="/var/lib/clickhouse/bootstrap")
        assert statements[0].startswith(
            "INSERT INTO wallet_memory.evm_transfers SELECT * FROM file("
            "'/var/lib/clickhouse/bootstrap/2026-08-01/ethereum/"
        )

    def test_build_import_sql_legacy_glob(self, tmp_path: Path) -> None:
        """Manifest without parquet_files falls back to a *.parquet glob."""
        manifest = parse_manifest(_manifest_dict(tables={"evm_transfers": {"rows": 10, "size_bytes": 2048}}))
        statements = build_import_sql(manifest, tmp_path, "wallet_memory")
        assert statements == [
            f"INSERT INTO wallet_memory.evm_transfers SELECT * FROM file('{tmp_path}/2026-08-01/ethereum/*.parquet')"
        ]

    def test_target_db_is_respected(self, tmp_path: Path) -> None:
        """A custom target database is used in the fully-qualified table."""
        statements = build_import_sql(_manifest(), tmp_path, "analytics")
        assert all(s.startswith("INSERT INTO analytics.") for s in statements)


# ═══════════════════════════════════════════════════════════════════════
# row-count verification + high-water mark
# ═══════════════════════════════════════════════════════════════════════


class TestVerifyAndState:
    def test_verify_row_counts_ok(self) -> None:
        """Matching counts report ok per table."""
        manifest = _manifest()
        client = FakeClickHouse(counts={"evm_transfers": 359_000_000, "dex_events": 42_000_000})
        results = SnapshotBootstrap()._verify_row_counts(client, manifest, "wallet_memory")
        assert results["evm_transfers"]["expected"] == 359_000_000
        assert results["evm_transfers"]["actual"] == 359_000_000
        assert results["dex_events"]["actual"] == 42_000_000

    def test_verify_row_counts_mismatch(self) -> None:
        """A count mismatch is surfaced, not silently accepted."""
        manifest = _manifest()
        client = FakeClickHouse(counts={"evm_transfers": 1_000_000, "dex_events": 42_000_000})
        results = SnapshotBootstrap()._verify_row_counts(client, manifest, "wallet_memory")
        assert results["evm_transfers"]["expected"] != results["evm_transfers"]["actual"]

    def test_high_water_set_forward(self, state_dir: Path, tmp_path: Path) -> None:
        """Bootstrap writes state + high-water mark to the snapshot end block."""
        boot = FakeBootstrap(FakeClickHouse(), state_dir=state_dir)
        out = boot._set_high_water("ethereum", 25_400_000, 401_000_000)
        assert out["advanced"] is True
        assert out["state_after"] == 25_400_000
        state_file = state_dir / "lite_ethereum.json"
        assert state_file.exists()
        assert json.loads(state_file.read_text())["last_block"] == 25_400_000
        assert json.loads(state_file.read_text())["source"] == "bootstrap"
        assert json.loads((state_dir / "highwater_ethereum.json").read_text())["last_block"] == 25_400_000

    def test_high_water_never_rewinds(self, state_dir: Path) -> None:
        """An existing state ahead of the snapshot is preserved (additive)."""
        boot = FakeBootstrap(FakeClickHouse(), state_dir=state_dir)
        boot._set_high_water("ethereum", 30_000_000, 1)
        out = boot._set_high_water("ethereum", 25_400_000, 1)
        assert out["advanced"] is False
        assert out["state_after"] == 30_000_000
        assert json.loads((state_dir / "lite_ethereum.json").read_text())["last_block"] == 30_000_000


# ═══════════════════════════════════════════════════════════════════════
# listing + bootstrap flow (mocked rclone)
# ═══════════════════════════════════════════════════════════════════════


class TestListSnapshots:
    async def test_list_snapshots_parses_manifests(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """rclone lsf + cat produce parsed snapshot summaries, newest first."""
        manifest_eth = json.dumps(_manifest_dict(chain="ethereum", date_str="2026-08-01"))
        manifest_base = json.dumps(
            _manifest_dict(
                chain="base",
                date_str="2026-08-02",
                start_block=10_000_000,
                end_block=12_000_000,
            )
        )
        lsf = (
            "2026-08-01/ethereum/manifest.json\n"
            "2026-08-02/base/manifest.json\n"
            "2026-08-01/ethereum/ethereum_2026-08-01.parquet\n"
        )
        cat_map = {
            "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json": manifest_eth,
            "b2-crypt:chain-data/snapshots/2026-08-02/base/manifest.json": manifest_base,
        }
        _patch_rclone(monkeypatch, lsf_output=lsf, cat_map=cat_map)

        boot = SnapshotBootstrap(bucket="b2-crypt:chain-data", snapshots_path="snapshots")
        snapshots = await boot.list_snapshots()

        assert len(snapshots) == 2
        # newest date first
        assert snapshots[0]["chain"] == "base"
        assert snapshots[0]["date"] == "2026-08-02"
        assert snapshots[0]["block_range"] == [10_000_000, 12_000_000]
        assert snapshots[0]["file_count"] == 2
        assert snapshots[1]["chain"] == "ethereum"
        assert snapshots[1]["manifest_url"] == ("b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json")

    async def test_list_snapshots_filters_by_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        lsf = "2026-08-01/ethereum/manifest.json\n2026-08-01/base/manifest.json\n"
        cat_map = {
            "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json": json.dumps(
                _manifest_dict(chain="ethereum")
            ),
            "b2-crypt:chain-data/snapshots/2026-08-01/base/manifest.json": json.dumps(
                _manifest_dict(chain="base", start_block=1, end_block=2)
            ),
        }
        _patch_rclone(monkeypatch, lsf_output=lsf, cat_map=cat_map)
        snapshots = await SnapshotBootstrap().list_snapshots(chain="ethereum")
        assert [s["chain"] for s in snapshots] == ["ethereum"]

    async def test_list_snapshots_no_manifests(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_rclone(monkeypatch, lsf_output="")
        assert await SnapshotBootstrap().list_snapshots() == []

    async def test_list_snapshots_rclone_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("lite_indexer.bootstrap.shutil.which", lambda _bin: None)
        assert await SnapshotBootstrap(rclone_bin="rclone").list_snapshots() == []


class TestBootstrapChain:
    async def test_bootstrap_chain_happy_path(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Full flow: download → import → verify → advance state."""
        manifest = _manifest()
        manifest_remote = "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json"
        lsf = "2026-08-01/ethereum/manifest.json\n"
        cat_map = {manifest_remote: json.dumps(manifest.to_dict())}
        calls = _patch_rclone(monkeypatch, lsf_output=lsf, cat_map=cat_map)

        client = FakeClickHouse(
            counts={"evm_transfers": 359_000_000, "dex_events": 42_000_000},
            import_rows=401_000_000,
        )
        state_dir_path = tmp_path / "state"
        boot = FakeBootstrap(
            client,
            bucket="b2-crypt:chain-data",
            snapshots_path="snapshots",
            local_dir=tmp_path / "bootstrap",
            state_dir=state_dir_path,
        )

        result = await boot.bootstrap_chain("ethereum", snapshot_date="latest")

        assert result["status"] == "ok"
        assert result["chain"] == "ethereum"
        assert result["snapshot_date"] == "2026-08-01"
        assert result["block_range"] == [100, 25_400_000]
        assert result["blocks"] == 25_399_901
        assert result["rows_imported"] == 802_000_000
        assert result["duration_seconds"] >= 0
        # rclone copy was used to download the snapshot dir
        assert any(c[0:2] == ["rclone", "copy"] for c in calls)
        # import SQL was generated + executed
        assert any("INSERT INTO wallet_memory.evm_transfers" in q for q in client.inserts)
        # state + high-water mark advanced
        state_file = state_dir_path / "lite_ethereum.json"
        assert state_file.exists()
        assert json.loads(state_file.read_text())["last_block"] == 25_400_000

    async def test_bootstrap_chain_explicit_date(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """An explicit --from-snapshot date skips the listing."""
        manifest = _manifest()
        manifest_remote = "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json"
        _patch_rclone(
            monkeypatch,
            lsf_output="",
            cat_map={manifest_remote: json.dumps(manifest.to_dict())},
        )
        client = FakeClickHouse(counts={"evm_transfers": 359_000_000, "dex_events": 42_000_000})
        boot = FakeBootstrap(client, state_dir=tmp_path / "state")

        result = await boot.bootstrap_chain("ethereum", snapshot_date="2026-08-01")

        assert result["status"] == "ok"
        assert result["snapshot_date"] == "2026-08-01"

    async def test_bootstrap_chain_no_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No manifest for the chain/date → no_snapshot, no CH writes."""
        _patch_rclone(monkeypatch, lsf_output="")
        client = FakeClickHouse()
        boot = FakeBootstrap(client)
        result = await boot.bootstrap_chain("ethereum", snapshot_date="latest")
        assert result["status"] == "no_snapshot"
        assert client.queries == []

    async def test_bootstrap_chain_clickhouse_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ClickHouse down → explicit failure, state untouched."""
        manifest = _manifest()
        manifest_remote = "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json"
        _patch_rclone(
            monkeypatch,
            lsf_output="2026-08-01/ethereum/manifest.json\n",
            cat_map={manifest_remote: json.dumps(manifest.to_dict())},
        )

        class NoCh(FakeBootstrap):
            def _connect(self) -> Any | None:
                return None

        boot = NoCh(FakeClickHouse())
        result = await boot.bootstrap_chain("ethereum")
        assert result["status"] == "clickhouse_unavailable"
        assert "clickhouse unavailable" in result["errors"][0]

    async def test_bootstrap_chain_verify_mismatch(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Row-count mismatch → status error, no state advance."""
        manifest = _manifest()
        manifest_remote = "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json"
        _patch_rclone(
            monkeypatch,
            lsf_output="2026-08-01/ethereum/manifest.json\n",
            cat_map={manifest_remote: json.dumps(manifest.to_dict())},
        )
        client = FakeClickHouse(counts={"evm_transfers": 1_000_000, "dex_events": 42_000_000})
        state_dir_path = tmp_path / "state"
        boot = FakeBootstrap(client, state_dir=state_dir_path)

        result = await boot.bootstrap_chain("ethereum")

        assert result["status"] == "error"
        assert any("row count mismatch" in e for e in result["errors"])
        assert not (state_dir_path / "lite_ethereum.json").exists()

    async def test_bootstrap_chain_dry_run(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Dry-run previews the plan without downloading or importing."""
        manifest = _manifest()
        manifest_remote = "b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json"
        calls = _patch_rclone(
            monkeypatch,
            lsf_output="2026-08-01/ethereum/manifest.json\n",
            cat_map={manifest_remote: json.dumps(manifest.to_dict())},
        )
        client = FakeClickHouse()
        boot = FakeBootstrap(client, local_dir=tmp_path / "bootstrap", state_dir=tmp_path / "state")

        result = await boot.bootstrap_chain("ethereum", dry_run=True)

        assert result["status"] == "dry_run"
        assert any("would download" in e for e in result["errors"])
        # no rclone copy, no CH writes, no state
        assert not any(c[0:2] == ["rclone", "copy"] for c in calls)
        assert client.queries == []
        assert not (tmp_path / "state" / "lite_ethereum.json").exists()

    async def test_bootstrap_all_aggregates(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """bootstrap_all returns per-chain results and a status."""
        manifest_eth = json.dumps(_manifest_dict(chain="ethereum"))
        lsf = "2026-08-01/ethereum/manifest.json\n"
        _patch_rclone(
            monkeypatch,
            lsf_output=lsf,
            cat_map={"b2-crypt:chain-data/snapshots/2026-08-01/ethereum/manifest.json": manifest_eth},
        )
        client = FakeClickHouse(counts={"evm_transfers": 359_000_000, "dex_events": 42_000_000})
        boot = FakeBootstrap(client, state_dir=tmp_path / "state")

        result = await boot.bootstrap_all(chains=["ethereum"], snapshot_date="2026-08-01")

        assert result["status"] == "ok"
        assert result["chains"]["ethereum"]["status"] == "ok"


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════


class TestCli:
    def test_bootstrap_parse_defaults(self) -> None:
        args = parse_args(["bootstrap", "--chain", "ethereum"])
        assert args.command == "bootstrap"
        assert args.chain == "ethereum"
        assert args.from_snapshot == "latest"
        assert args.list is False
        assert args.all is False

    def test_bootstrap_parse_list(self) -> None:
        args = parse_args(["bootstrap", "--list"])
        assert args.list is True

    def test_bootstrap_parse_all(self) -> None:
        args = parse_args(["bootstrap", "--all", "--from-snapshot", "2026-08-01"])
        assert args.all is True
        assert args.from_snapshot == "2026-08-01"

    def test_bootstrap_parse_invalid_chain(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["bootstrap", "--chain", "not-a-chain"])

    async def test_bootstrap_list_all_conflict(self, capsys: pytest.CaptureFixture[str]) -> None:
        """--list with --all is a usage error, not a run."""
        from lite_indexer.cli import run_bootstrap_cli

        args = parse_args(["bootstrap", "--list", "--all"])
        rc = await run_bootstrap_cli(args)
        assert rc == 2
        assert "mutually exclusive" in capsys.readouterr().err


# ═══════════════════════════════════════════════════════════════════════
# export pipeline → snapshot publishing
# ═══════════════════════════════════════════════════════════════════════


class TestExportPublish:
    def test_block_range_helper(self) -> None:
        """_block_range computes the min/max span over exported rows."""
        transfers = [
            {"block_number": 1000},
            {"block_number": 500},
            {"block_number": None},
        ]
        dex = [{"block_number": 900}]
        assert _block_range(transfers, dex) == (500, 1000)

    def test_block_range_empty(self) -> None:
        assert _block_range([], []) == (0, 0)

    def test_publish_snapshot_writes_manifest_and_uploads(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """publish_snapshot writes manifest.json + uploads + verifies."""

        day = date(2026, 8, 1)
        local = _local_dir(tmp_path / "parquet", "ethereum", day)
        local.mkdir(parents=True)
        (local / "ethereum_2026-08-01.parquet").write_bytes(b"PARQUET")

        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
            calls.append(list(cmd))
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        from lite_indexer.storage.parquet import tooling as parquet_tooling

        parquet_tooling.set_tool_seams(which=lambda _bin: "/usr/bin/rclone", run=fake_run)

        exporter = ParquetExporter(data_dir=tmp_path / "parquet")
        ok = exporter.publish_snapshot(
            "ethereum",
            day,
            transfers_rows=10,
            start_block=100,
            end_block=500,
        )

        assert ok is True
        manifest_file = local / MANIFEST_NAME
        assert manifest_file.exists()
        raw = json.loads(manifest_file.read_text())
        assert raw["chain"] == "ethereum"
        assert raw["start_block"] == 100
        assert raw["end_block"] == 500
        assert raw["tables"]["evm_transfers"]["rows"] == 10
        assert raw["tables"]["evm_transfers"]["parquet_files"] == ["ethereum_2026-08-01.parquet"]
        # rclone copy + rclone check to the snapshot remote
        assert calls[0][0:2] == ["rclone", "copy"]
        assert calls[0][3] == "b2-crypt:chain-data/snapshots/2026-08-01/ethereum"
        assert calls[1][0:2] == ["rclone", "check"]

    def test_publish_snapshot_skips_empty_dir(self, tmp_path: Path) -> None:
        """No exported files → nothing is published."""
        day = date(2026, 8, 1)
        exporter = ParquetExporter(data_dir=tmp_path / "parquet")
        assert exporter.publish_snapshot("ethereum", day) is False
        assert not (tmp_path / "parquet" / "ethereum" / "2026-08-01").exists()
