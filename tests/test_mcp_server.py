"""Tests for the read-only MCP server (``lite_indexer.mcp_server``)."""

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
import os

import pytest

from lite_indexer import attestation as att
from lite_indexer import mcp_server

TOOLS = {"lci_status", "lci_chain_head", "lci_recent_transfers", "lci_rpc_health", "lci_attest_verify"}


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


@pytest.fixture
def state(tmp_path: Path) -> Path:
    """Point the server at an isolated state dir + throttle/ledger files."""
    with (
        patch.object(mcp_server.cfg, "STATE_DIR", tmp_path),
        patch.object(mcp_server.cfg, "DATA_DIR", tmp_path),
        patch.object(mcp_server.cfg, "RPC_THROTTLE_FILE", str(tmp_path / "rpc_throttle.json")),
        patch.dict(os.environ, {"QUOTA_LEDGER_FILE": str(tmp_path / "quota_ledger.json")}),
    ):
        yield tmp_path


class FakeCH:
    def __init__(self, rows: list[tuple], columns: list[tuple[str, str]]) -> None:
        self.rows, self.columns = rows, columns
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query: str, params: dict, *, with_column_types: bool = False):  # noqa: ANN201
        self.calls.append((query, params))
        return self.rows, self.columns


def test_tools_registered() -> None:
    tools = asyncio.run(mcp_server.mcp.list_tools())
    assert {t.name for t in tools} == TOOLS


def test_status_shape_and_progress(state: Path) -> None:
    _write(state / "lite_ethereum.json", {"last_block": 100, "last_timestamp": datetime.now(UTC).isoformat()})
    _write(state / "highwater_ethereum.json", {"last_block": 120})
    _write(state / "lite_base.json", {"last_block": 5})
    _write(
        state / "quota_ledger.json",
        {"acct-a": {"consumed_pct": 92.0, "parked": False, "degrade_level": 2}, "acct-b": {"consumed_pct": 10.0}},
    )
    result = mcp_server.lci_status()
    assert result["status"] == "ok"
    chains = {c["chain"] for c in result["chains"]}
    assert chains == {"ethereum", "base"}
    assert result["quota"] == {
        "accounts": 2,
        "parked": 0,
        "max_degrade_level": 2,
        "max_consumed_pct": 92.0,
        "over_80pct": 1,
    }
    assert result["recent_errors"] == []


def test_chain_head_unknown_chain(state: Path) -> None:
    out = mcp_server.lci_chain_head("dogecoin")
    assert out["status"] == "error"
    assert "unknown chain" in out["error"]


def test_chain_head_reports_time_since_advance(state: Path) -> None:
    stamp = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    _write(state / "lite_base.json", {"last_block": 42, "last_timestamp": stamp})
    _write(state / "highwater_base.json", {"last_block": 50})
    out = mcp_server.lci_chain_head("base")
    assert out["status"] == "ok"
    assert out["last_block"] == 42
    assert out["highwater_block"] == 50
    assert 25 <= out["seconds_since_advance"] <= 60


def test_recent_transfers_evm_shape_and_clamp(state: Path) -> None:
    _write(state / "lite_ethereum.json", {"last_block": 1})
    fake = FakeCH(
        rows=[("ethereum", 1, "0xdead", "0x" + "a" * 40, "0x" + "b" * 40, "0xtoken", 7, "2026-01-01 00:00:00", 3)],
        columns=[
            (name, "String")
            for name in (
                "chain",
                "block_number",
                "tx_hash",
                "from_address",
                "to_address",
                "token_address",
                "value",
                "block_timestamp",
                "log_index",
            )
        ],
    )
    with patch.object(mcp_server, "_client", lambda: fake):
        address = "0x" + "a" * 40
        out = mcp_server.lci_recent_transfers("ethereum", address=address, limit=5000)
    assert out["status"] == "ok"
    assert out["limit"] == 100  # clamped
    assert out["rows"][0]["tx_hash"] == "0xdead"
    query, params = fake.calls[0]
    assert "wallet_memory.evm_transfers" in query
    assert "ORDER BY block_number DESC, log_index DESC" in query
    assert params == {"lim": 100, "chain": "ethereum", "addr": address}


def test_recent_transfers_solana_uses_solana_table(state: Path) -> None:
    _write(state / "lite_solana.json", {"last_block": 9})
    fake = FakeCH(rows=[], columns=[(name, "String") for name in ("slot", "signature")])
    with patch.object(mcp_server, "_client", lambda: fake):
        out = mcp_server.lci_recent_transfers("solana")
    assert out["status"] == "ok"
    query, params = fake.calls[0]
    assert "solana_transfers" in query
    assert "chain = %(chain)s" not in query
    assert params == {"lim": 20}


def test_recent_transfers_rejects_bad_input(state: Path) -> None:
    _write(state / "lite_ethereum.json", {"last_block": 1})
    assert mcp_server.lci_recent_transfers("dogecoin")["status"] == "error"
    assert mcp_server.lci_recent_transfers("ethereum", address="nope")["status"] == "error"


def test_recent_transfers_unreachable_clickhouse_masks_secret(state: Path) -> None:
    _write(state / "lite_ethereum.json", {"last_block": 1})

    def boom() -> FakeCH:
        raise RuntimeError("connect failed https://rpc.example.org/v2/SUPERSECRETKEY123456789?apikey=SECRETVALUE")

    with patch.object(mcp_server, "_client", boom):
        out = mcp_server.lci_recent_transfers("ethereum")
    assert out["status"] == "unavailable"
    assert out["rows"] == []
    dumped = json.dumps(out)
    assert "SUPERSECRETKEY123456789" not in dumped
    assert "SECRETVALUE" not in dumped


def test_rpc_health_summary_masks_endpoint_keys(state: Path) -> None:
    endpoint = "https://rpc.example.org/v2/ABCDEFGHIJKLMNOPQRSTUVWX?apikey=SECRETVALUE1234567890"
    _write(state / "rpc_throttle.json", {endpoint: {"last_status": 429, "rate_limit_hits": 4, "in_backoff": True}})
    out = mcp_server.lci_rpc_health()
    assert out["status"] == "ok"
    assert out["endpoints"] == 1 and out["in_backoff"] == 1 and out["rate_limited"] == 1 and out["healthy"] == 0
    dumped = json.dumps(out)
    assert "SECRETVALUE1234567890" not in dumped
    assert "ABCDEFGHIJKLMNOPQRSTUVWX" not in dumped
    assert "apikey=***" in dumped


def test_rpc_health_without_throttle_file_is_unavailable(state: Path) -> None:
    out = mcp_server.lci_rpc_health()
    assert out["status"] == "unavailable"
    assert out["endpoints"] == 0


def test_attest_verify_disabled(state: Path) -> None:
    with patch.object(mcp_server.cfg, "ATTESTATION_ENABLED", False):
        assert mcp_server.lci_attest_verify("whatever.json")["status"] == "disabled"


def test_attest_verify_rejects_path_outside_root(state: Path) -> None:
    outside = _write(state / "evil.json", {})
    with (
        patch.object(mcp_server.cfg, "ATTESTATION_ENABLED", True),
        patch.object(mcp_server.cfg, "ATTESTATION_DIR", state / "attestations"),
    ):
        assert mcp_server.lci_attest_verify(str(outside))["status"] == "error"


def test_attest_verify_valid_manifest(state: Path) -> None:
    root = state / "attestations"
    with (
        patch.object(mcp_server.cfg, "ATTESTATION_ENABLED", True),
        patch.object(mcp_server.cfg, "ATTESTATION_DIR", root),
        patch.object(att, "ATTESTATION_ENABLED", True),
    ):
        written = att.record_range_attestation(
            "ethereum", 1, 10, [{"tx_hash": "0xabc"}], ["0x" + "a" * 64], state_dir=root
        )
        assert written is not None
        out = mcp_server.lci_attest_verify(str(root / "ethereum-1-10.json"))
    assert out["status"] == "ok"
    assert out["chain"] == "ethereum"
    assert out["from_block"] == 1 and out["to_block"] == 10


def test_import_guard_without_fastmcp() -> None:
    repo = Path(__file__).resolve().parents[1]
    code = "import sys; sys.modules['fastmcp'] = None; import lite_indexer.mcp_server"
    proc = subprocess.run([sys.executable, "-c", code], cwd=repo, capture_output=True, text=True)
    assert proc.returncode != 0
    assert "fastmcp" in proc.stderr.lower()
