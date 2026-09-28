"""Tests for the Parquet cold-storage export pipeline.

Covers the spec contract:

* layout: ``{parquet_dir}/{chain}/{date}/`` + remote ``{chain}/{date}/``
* parquet verification (row count + schema)
* rclone directory upload (``rclone copy`` + ``rclone check``)
* ClickHouse ``parquet_export_state`` tracking
* delete only after a verified upload
* CLI flags (--date/--upload/--delete-after-upload/--dry-run/--remote/--chain all)
"""

from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from lite_indexer.cli import parse_args, run_export_parquet
from lite_indexer.storage.parquet import (
    ParquetExporter,
    _local_dir,
    _local_path,
    _remote_dir,
    _remote_path,
)
from lite_indexer.storage.parquet import tooling as parquet_tooling

TRANSFER_COLS = [
    "chain",
    "block_number",
    "tx_hash",
    "from_address",
    "to_address",
    "token_address",
    "value",
    "block_timestamp",
    "log_index",
    "token_symbol",
    "token_name",
    "ingested_at",
    "source",
]
DEX_COLS = [
    "type",
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "contract_address",
    "dex",
    "version",
    "token0",
    "token1",
    "sender",
    "recipient",
    "amount0_in",
    "amount1_in",
    "amount0_out",
    "amount1_out",
    "amount0",
    "amount1",
    "whale",
    "ingested_at",
    "source",
]


class FakeClickHouse:
    """Minimal ClickHouse client double for export tests."""

    def __init__(self, transfers: list[dict[str, Any]] | None = None, dex: list[dict[str, Any]] | None = None) -> None:
        self.transfers = transfers or []
        self.dex = dex or []
        self.state_inserts: list[list[tuple[Any, ...]]] = []
        self.deletes: list[str] = []
        self.queries: list[str] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        if query.startswith("CREATE TABLE IF NOT EXISTS wallet_memory.parquet_export_state"):
            return []
        if query.startswith("INSERT INTO wallet_memory.parquet_export_state"):
            self.state_inserts.append(params)
            return 1
        if (
            "ALTER TABLE wallet_memory.evm_transfers DELETE" in query
            or "ALTER TABLE wallet_memory.dex_events DELETE" in query
        ):
            self.deletes.append(query)
            return 1
        if "FROM wallet_memory.evm_transfers" in query:
            return self._serve(self.transfers, TRANSFER_COLS, (query, params, with_column_types))
        if "FROM wallet_memory.dex_events" in query:
            return self._serve(self.dex, DEX_COLS, (query, params, with_column_types))
        return []

    def _serve(self, src: list[dict[str, Any]], cols_spec: tuple[str, ...], call: tuple[str, Any, bool]) -> Any:
        """Serve rows (optionally block-window filtered) or day aggregates."""
        query, params, with_column_types = call
        if "count()" in query:
            blocks = [r["block_number"] for r in src]
            return [[len(src), min(blocks) if blocks else None, max(blocks) if blocks else None]]
        rows_src = src
        if isinstance(params, dict) and "block_from" in params:
            rows_src = [r for r in rows_src if r["block_number"] >= params["block_from"]]
        if isinstance(params, dict) and "block_to" in params:
            rows_src = [r for r in rows_src if r["block_number"] <= params["block_to"]]
        rows = [[r[c] for c in cols_spec] for r in rows_src]
        cols = [(c, "String") for c in cols_spec]
        return (rows, cols) if with_column_types else rows

    # -- helpers ------------------------------------------------------

    def statuses(self) -> list[str]:
        return [row[2] for batch in self.state_inserts for row in batch]

    def last_status(self) -> str:
        return self.statuses()[-1]


class FakeExporter(ParquetExporter):
    """ParquetExporter with a stubbed ClickHouse connection."""

    def __init__(self, client: FakeClickHouse, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client

    def _connect(self) -> Any | None:
        return self._client


def _transfer_row(chain: str = "ethereum", value: str = "1000") -> dict[str, Any]:
    return {
        "chain": chain,
        "block_number": 100,
        "tx_hash": "0xabc",
        "from_address": "0x111",
        "to_address": "0x222",
        "token_address": "0x333",
        "value": value,
        "block_timestamp": "2026-07-30 10:00:00",
        "log_index": 1,
        "token_symbol": "TEST",
        "token_name": "Test Token",
        "ingested_at": "2026-07-30 12:00:00",
        "source": "lite_indexer",
    }


def _dex_row(chain: str = "ethereum") -> dict[str, Any]:
    return {
        "type": "swap",
        "chain": chain,
        "block_number": 100,
        "block_timestamp": "2026-07-30 10:00:00",
        "tx_hash": "0xabc",
        "log_index": 1,
        "contract_address": "0xpair",
        "dex": "uniswap_v2",
        "version": "v2",
        "token0": "0xt0",
        "token1": "0xt1",
        "sender": "0x111",
        "recipient": "0x222",
        "amount0_in": "1",
        "amount1_in": "2",
        "amount0_out": "3",
        "amount1_out": "4",
        "amount0": "1",
        "amount1": "2",
        "whale": 0,
        "ingested_at": "2026-07-30 12:00:00",
        "source": "lite_indexer",
    }


DAY = date(2026, 7, 30)


@pytest.fixture(autouse=True)
def _isolated_state_dir(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """Run every test against a private state dir, not the live one.

    ``ParquetExporter.__init__`` binds ``state_dir`` from the module-global
    ``writer.PARQUET_STATE_DIR`` at CALL time (not as a def-time default), and
    the CLI builds its own exporter, so this is the one seam that redirects
    both. Without it the suite reads — and can write — the production marker
    directory at ``/var/data/chain-indexer/state/parquet``.

    That is a real order- and host-dependence: on a host that had already
    exported a date, the marker made ``export_chain`` drop the date and
    ``test_cli_dry_run`` saw ``dates_attempted == 0`` instead of 1 and
    failed, while a clean host passed. The hardcoded 2026-07-30 was never
    the problem — the live marker was.

    Seam hygiene (real tooling + config cap) is restored on the way out too.
    """
    from lite_indexer.storage.parquet import writer as parquet_writer
    from lite_indexer.storage.parquet.reader import set_max_rows_per_day

    original = parquet_writer.PARQUET_STATE_DIR
    private = tmp_path_factory.mktemp("parquet-state")
    parquet_writer.PARQUET_STATE_DIR = private
    parquet_tooling.set_tool_seams(None, None)
    set_max_rows_per_day(None)
    yield private
    parquet_writer.PARQUET_STATE_DIR = original
    parquet_tooling.set_tool_seams(None, None)
    set_max_rows_per_day(None)


# ── layout ──────────────────────────────────────────────────────────


def test_layout_matches_spec(tmp_path: Path) -> None:
    """Local and remote layouts are {chain}/{YYYY-MM-DD}/ per the spec."""
    data_dir = tmp_path / "parquet"
    local = _local_dir(data_dir, "ethereum", DAY)
    assert local == data_dir / "ethereum" / "2026-07-30"

    transfers = _local_path(data_dir, "ethereum", DAY, "transfers")
    dex = _local_path(data_dir, "ethereum", DAY, "dex")
    assert transfers == data_dir / "ethereum" / "2026-07-30" / "ethereum_2026-07-30.parquet"
    assert dex == data_dir / "ethereum" / "2026-07-30" / "ethereum_dex_2026-07-30.parquet"

    assert _remote_dir("b2-crypt:lite-indexer-parquet", "ethereum", DAY) == (
        "b2-crypt:lite-indexer-parquet/ethereum/2026-07-30"
    )
    assert _remote_path("b2-crypt:lite-indexer-parquet", "ethereum", DAY, "transfers") == (
        "b2-crypt:lite-indexer-parquet/ethereum/2026-07-30/ethereum_2026-07-30.parquet"
    )


# ── verification ────────────────────────────────────────────────────


def test_verify_parquet_ok(tmp_path: Path) -> None:
    """A written parquet file verifies on row count + schema."""
    from lite_indexer.storage.parquet import PYARROW_AVAILABLE

    if not PYARROW_AVAILABLE:
        pytest.skip("pyarrow not installed")

    path = _local_path(tmp_path, "ethereum", DAY, "transfers")
    path.parent.mkdir(parents=True)
    rows = [_transfer_row() for _ in range(3)]
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)
    assert ParquetExporter._verify_parquet(path, 3, set(rows[0].keys())) is True


def test_verify_parquet_row_mismatch(tmp_path: Path) -> None:
    """Row-count mismatch fails verification."""
    from lite_indexer.storage.parquet import PYARROW_AVAILABLE

    if not PYARROW_AVAILABLE:
        pytest.skip("pyarrow not installed")

    path = _local_path(tmp_path, "ethereum", DAY, "transfers")
    path.parent.mkdir(parents=True)
    rows = [_transfer_row() for _ in range(3)]
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)
    assert ParquetExporter._verify_parquet(path, 5, set(rows[0].keys())) is False


def test_verify_parquet_schema_missing(tmp_path: Path) -> None:
    """Missing column fails verification."""
    from lite_indexer.storage.parquet import PYARROW_AVAILABLE

    if not PYARROW_AVAILABLE:
        pytest.skip("pyarrow not installed")

    path = _local_path(tmp_path, "ethereum", DAY, "transfers")
    path.parent.mkdir(parents=True)
    rows = [_transfer_row() for _ in range(2)]
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)
    assert ParquetExporter._verify_parquet(path, 2, {"chain", "tx_hash", "missing_col"}) is False


# ── export flow ─────────────────────────────────────────────────────


def test_export_empty_date_marks_done(tmp_path: Path) -> None:
    """A date with no rows is archived empty (no files, no deletes)."""
    client = FakeClickHouse(transfers=[], dex=[])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    summary = exporter.export_chain("ethereum", dates=[DAY])

    assert summary.dates_exported == 1
    assert summary.transfers_rows == 0
    assert summary.bytes_written == 0
    assert (tmp_path / "state" / "ethereum" / "2026-07-30.done").exists()
    # pending -> exporting -> uploaded (terminal)
    assert client.last_status() == "uploaded"
    assert client.deletes == []


def test_export_writes_and_verifies_parquet(tmp_path: Path) -> None:
    """Non-empty date writes verified parquet files and records state."""
    client = FakeClickHouse(transfers=[_transfer_row() for _ in range(2)], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    summary = exporter.export_chain("ethereum", dates=[DAY])

    assert summary.dates_exported == 1
    assert summary.transfers_rows == 2
    assert summary.dex_rows == 1
    assert summary.bytes_written > 0
    transfers = _local_path(tmp_path / "parquet", "ethereum", DAY, "transfers")
    dex = _local_path(tmp_path / "parquet", "ethereum", DAY, "dex")
    assert transfers.exists()
    assert dex.exists()
    assert client.last_status() == "uploaded"
    assert client.deletes == []
    assert (tmp_path / "state" / "ethereum" / "2026-07-30.done").exists()


def test_upload_verified_then_delete(tmp_path: Path) -> None:
    """delete-after-upload deletes rows only after the upload verifies."""
    client = FakeClickHouse(transfers=[_transfer_row()], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    parquet_tooling.set_tool_seams(which=lambda _bin: "/usr/bin/rclone", run=fake_run)

    summary = exporter.export_chain("ethereum", dates=[DAY], upload=True, delete=True)

    assert summary.uploaded == 1
    assert summary.deleted_rows == 2  # both tables mutated
    assert len(calls) == 2  # rclone copy + rclone check
    assert calls[0][0:2] == ["rclone", "copy"]
    assert calls[1][0:2] == ["rclone", "check"]
    assert calls[0][2] == str(_local_dir(tmp_path / "parquet", "ethereum", DAY))
    assert calls[0][3] == "b2-crypt-v2:chain-data/parquet/ethereum/2026-07-30"
    assert len(client.deletes) == 2
    assert client.last_status() == "deleted"


def test_upload_failure_skips_delete(tmp_path: Path) -> None:
    """When the upload is not verified, rows are NOT deleted."""
    client = FakeClickHouse(transfers=[_transfer_row()], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    def fake_run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="boom")

    parquet_tooling.set_tool_seams(which=lambda _bin: "/usr/bin/rclone", run=fake_run)

    summary = exporter.export_chain("ethereum", dates=[DAY], upload=True, delete=True)

    assert summary.uploaded == 0
    assert summary.deleted_rows == 0
    assert summary.errors and "upload not verified" in summary.errors[0]
    assert client.deletes == []
    assert client.last_status() == "failed"
    # no state marker → retried next run
    assert not (tmp_path / "state" / "ethereum" / "2026-07-30.done").exists()


def test_delete_without_upload_is_skipped(tmp_path: Path) -> None:
    """Defense-in-depth: delete=True without upload never deletes rows."""
    client = FakeClickHouse(transfers=[_transfer_row()], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    summary = exporter.export_chain("ethereum", dates=[DAY], upload=False, delete=True)

    assert summary.deleted_rows == 0
    assert client.deletes == []
    assert summary.dates_exported == 1  # local archive still succeeds
    assert client.last_status() == "uploaded"


# ── smoke: dry-run + full pipeline safety ───────────────────────────


def test_parquet_export_dry_run(tmp_path: Path) -> None:
    """Smoke: dry-run with mocked ClickHouse + B2 touches nothing.

    Even with --upload/--delete requested, a dry run must not: connect to
    ClickHouse, write parquet files, invoke rclone, mark state, or
    delete rows — it only previews the plan.
    """
    client = FakeClickHouse(transfers=[_transfer_row() for _ in range(3)], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    rclone_calls: list[list[str]] = []
    parquet_tooling.set_tool_seams(
        which=lambda _bin: "/usr/bin/rclone",
        run=lambda cmd, **_: (
            rclone_calls.append(list(cmd)) or subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
        ),
    )

    summary = exporter.export_chain("ethereum", dates=[DAY], upload=True, delete=True, dry_run=True)

    # Summary reports the planned dates without executing.
    assert summary.dates_attempted == 1
    assert summary.dates_exported == 1
    assert summary.bytes_written == 0
    assert summary.uploaded == 0
    assert summary.deleted_rows == 0
    assert summary.errors == []
    # Nothing touched: no CH queries, no rclone, no files, no marker.
    assert client.queries == []
    assert rclone_calls == []
    assert not (tmp_path / "parquet" / "ethereum" / "2026-07-30").exists()
    assert not (tmp_path / "state" / "ethereum" / "2026-07-30.done").exists()


def test_parquet_export_skips_existing_markers(tmp_path: Path) -> None:
    """A (chain, date) already exported is filtered out — never re-exported."""
    marker = tmp_path / "state" / "ethereum" / "2026-07-30.done"
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({"day": "2026-07-30"}))

    client = FakeClickHouse(transfers=[_transfer_row()], dex=[_dex_row()])
    exporter = FakeExporter(client, data_dir=tmp_path / "parquet", state_dir=tmp_path / "state")

    summary = exporter.export_chain("ethereum", dates=[DAY])

    # The marked date is excluded before the run: nothing to attempt.
    assert summary.dates_attempted == 0
    assert summary.dates_exported == 0
    assert client.queries == []  # never even connected to CH
    assert client.state_inserts == []  # no state written


def test_parquet_export_over_cap_fails_safely(tmp_path: Any) -> None:
    """A day whose SINGLE block window exceeds the cap still fails safely.

    (Normal oversized days now export in block-window parts; only a window
    that cannot be split below the cap — e.g. every row in one block — must
    still refuse, so a truncated export is never treated as complete.)
    """
    from lite_indexer.storage.parquet.reader import set_max_rows_per_day

    rows = []
    for _ in range(6):
        row = _transfer_row()
        row["block_number"] = 7  # every row in the same block: unsplittable
        rows.append(row)
    set_max_rows_per_day(5)
    client = FakeClickHouse(transfers=rows, dex=[])
    exporter = FakeExporter(client, data_dir=tmp_path, state_dir=tmp_path / "state", remote="r")
    summary = exporter.export_chain("ethereum", dates=[DAY])
    assert summary.errors and "exceeds export cap" in summary.errors[0]


def test_cli_dry_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--dry-run previews without touching ClickHouse (spec verification)."""
    args = parse_args(
        ["export-parquet", "--chain", "ethereum", "--date", "2026-07-30", "--dry-run", "--data-dir", str(tmp_path)]
    )
    rc = run_export_parquet(args)
    out_lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    out = json.loads(out_lines[-1])  # last line is the JSON summary (logs go first)
    assert rc == 0
    assert out["chain"] == "ethereum"
    assert out["dates_attempted"] == 1
    assert out["dates_exported"] == 1


def test_cli_chain_all_dry_run(tmp_path: Path) -> None:
    """--chain all expands to every supported chain."""
    args = parse_args(
        ["export-parquet", "--chain", "all", "--date", "2026-07-30", "--dry-run", "--data-dir", str(tmp_path)]
    )
    assert args.chain == "all"


def test_cli_delete_requires_upload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--delete-after-upload without --upload is rejected."""
    args = parse_args(["export-parquet", "--chain", "ethereum", "--date", "2026-07-30", "--delete-after-upload"])
    rc = run_export_parquet(args)
    assert rc == 2
    assert "requires --upload" in capsys.readouterr().err


def test_cli_date_days_mutually_exclusive(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--date and --days are mutually exclusive."""
    args = parse_args(["export-parquet", "--chain", "ethereum", "--date", "2026-07-30", "--days", "7"])
    rc = run_export_parquet(args)
    assert rc == 2
    assert "mutually exclusive" in capsys.readouterr().err


def test_cli_invalid_date(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A malformed --date is a usage error, not a crash."""
    args = parse_args(["export-parquet", "--chain", "ethereum", "--date", "not-a-date"])
    rc = run_export_parquet(args)
    assert rc == 2
    assert "expected YYYY-MM-DD" in capsys.readouterr().err


def test_export_chain_chunks_oversized_days(tmp_path: Any) -> None:
    """A day above the row cap exports in block-window parts, memory-safe."""
    from lite_indexer.storage.parquet.helpers import _local_dir
    from lite_indexer.storage.parquet.reader import set_max_rows_per_day

    rows = []
    for i in range(1, 13):
        row = _transfer_row()
        row["block_number"] = i
        rows.append(row)
    set_max_rows_per_day(5)  # force chunking: 12 rows / 60% of 5 => 4 windows
    client = FakeClickHouse(transfers=rows, dex=[])
    exporter = FakeExporter(client, data_dir=tmp_path, state_dir=tmp_path / "state", remote="r")
    summary = exporter.export_chain("ethereum", dates=[DAY])
    assert summary.transfers_rows == 12
    parts = sorted(p.name for p in _local_dir(tmp_path, "ethereum", DAY).glob("ethereum_*-part*.parquet"))
    assert len(parts) >= 3
    assert not list(_local_dir(tmp_path, "ethereum", DAY).glob("ethereum_2026-07-30.parquet"))


def test_export_chain_single_file_under_cap(tmp_path: Any) -> None:
    """Normal days keep the unsuffixed single-file layout."""
    from lite_indexer.storage.parquet.helpers import _local_dir

    client = FakeClickHouse(transfers=[_transfer_row() for _ in range(2)], dex=[])
    exporter = FakeExporter(client, data_dir=tmp_path, state_dir=tmp_path / "state", remote="r")
    summary = exporter.export_chain("ethereum", dates=[DAY])
    assert summary.transfers_rows == 2
    assert (_local_dir(tmp_path, "ethereum", DAY) / "ethereum_2026-07-30.parquet").exists()
