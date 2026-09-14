"""Tests for the hot/cold retention policy and the drift checker.

Covers:

* the canonical policy mapping (30/90/180/365d + time columns);
* TTL-expression generation and live-DDL parsing (both INTERVAL and
  toIntervalDay/toIntervalMonth spellings, backticks, column-vs-table TTL);
* drift evaluation (ok / drift / no-ttl / absent);
* runtime enforcement via a fake ClickHouse client (existing-only, non-fatal);
* CLI exit codes — 0 clean, 1 drift, 0 when CH is unreachable.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from lite_indexer.config import RETENTION_POLICY
from lite_indexer.retention import (
    DriftRow,
    ensure_retention_policies,
    evaluate_drift,
    format_report,
    main,
    parse_live_ttl,
    ttl_expression,
)

EXPECTED_POLICY: dict[str, tuple[int, str]] = {
    "evm_transfers": (30, "block_timestamp"),
    "dex_events": (90, "block_timestamp"),
    "internal_transfers": (90, "block_timestamp"),
    "solana_transfers": (90, "block_timestamp"),
    "scam_addresses": (180, "first_seen_at"),
    "wallet_labels": (180, "loaded_at"),
    "deployer_history": (180, "deployed_at"),
    "evm_launches": (365, "block_timestamp"),
    "solana_launches": (365, "block_timestamp"),
    "tron_launches": (365, "block_timestamp"),
}


class FakeCH:
    """Minimal ClickHouse client double for system.tables + ALTER DDL."""

    def __init__(
        self,
        ddl_by_table: dict[str, str] | None = None,
        fail_alter_for: set[str] | None = None,
        probe_raises: bool = False,
    ) -> None:
        self.ddl = ddl_by_table or {}
        self.fail_alter_for = fail_alter_for or set()
        self.probe_raises = probe_raises
        self.alters: list[str] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        if "engine_full" in sql:
            names = (params or {})["tables"]
            return [(n, self.ddl[n], "") for n in names if n in self.ddl]
        if "system.tables" in sql:
            if self.probe_raises:
                raise RuntimeError("probe boom")
            names = (params or {})["tables"]
            return [(n,) for n in names if n in self.ddl]
        if sql.startswith("ALTER TABLE"):
            self.alters.append(sql)
            table = sql.split(".")[1].split()[0]
            if table in self.fail_alter_for:
                raise RuntimeError("alter denied")
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


# ── policy ────────────────────────────────────────────────────────────


def test_policy_mapping_matches_contract() -> None:
    actual = {name: (rule.days, rule.time_column) for name, rule in RETENTION_POLICY.items()}
    assert actual == EXPECTED_POLICY


def test_ttl_expression_is_canonical() -> None:
    assert ttl_expression(RETENTION_POLICY["evm_transfers"]) == "block_timestamp + INTERVAL 30 DAY"
    assert ttl_expression(RETENTION_POLICY["scam_addresses"]) == "first_seen_at + INTERVAL 180 DAY"


def test_migration_013_matches_policy() -> None:
    """migrations/013_retention_classes.sql must state exactly the policy TTLs."""
    sql = (Path(__file__).resolve().parents[1] / "migrations" / "013_retention_classes.sql").read_text(encoding="utf-8")
    found = {
        table: (column, int(days))
        for table, column, days in re.findall(
            r"ALTER TABLE wallet_memory\.(\w+)\s+MODIFY TTL (\w+) \+ INTERVAL (\d+) DAY",
            sql,
        )
    }
    expected = {name: (rule.time_column, rule.days) for name, rule in RETENTION_POLICY.items()}
    assert found == expected


# ── parsing ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        ("ENGINE = MergeTree() ORDER BY x TTL block_timestamp + toIntervalDay(30)", ("block_timestamp", 30)),
        ("... TTL block_timestamp + INTERVAL 90 DAY", ("block_timestamp", 90)),
        ("... TTL loaded_at + toIntervalMonth(6)", ("loaded_at", 180)),
        ("... TTL `loaded_at` + INTERVAL 6 MONTH", ("loaded_at", 180)),
        (
            "`token_symbol` String DEFAULT '' TTL block_timestamp + INTERVAL 30 DAY,\n"
            ") ENGINE = MergeTree() ORDER BY x TTL block_timestamp + toIntervalDay(30)",
            ("block_timestamp", 30),
        ),
        ("ENGINE = MergeTree() ORDER BY x", None),
    ],
)
def test_parse_live_ttl(ddl: str, expected: tuple[str, int] | None) -> None:
    assert parse_live_ttl(ddl) == expected


def test_parse_live_ttl_prefers_last_clause() -> None:
    # A column TTL (60d) must not shadow the table TTL (30d).
    ddl = "`c` String TTL block_timestamp + INTERVAL 60 DAY\nTTL block_timestamp + INTERVAL 30 DAY"
    assert parse_live_ttl(ddl) == ("block_timestamp", 30)


# ── drift evaluation ──────────────────────────────────────────────────


def test_evaluate_drift_all_ok() -> None:
    live = {name: f"TTL {rule.time_column} + toIntervalDay({rule.days})" for name, rule in RETENTION_POLICY.items()}
    rows = evaluate_drift(live)
    assert {r.table for r in rows} == set(RETENTION_POLICY)
    assert all(r.ok and r.status == "ok" for r in rows)


def test_evaluate_drift_reports_wrong_window() -> None:
    live = {"evm_transfers": "TTL ingested_at + toIntervalDay(90)"}
    row = next(r for r in evaluate_drift(live) if r.table == "evm_transfers")
    assert row.status == "drift" and not row.ok
    assert row.actual_column == "ingested_at" and row.actual_days == 90
    assert row.expected_column == "block_timestamp" and row.expected_days == 30


def test_evaluate_drift_reports_missing_ttl() -> None:
    live = {"dex_events": "ENGINE = MergeTree() ORDER BY x"}
    row = next(r for r in evaluate_drift(live) if r.table == "dex_events")
    assert row.status == "no-ttl" and not row.ok


def test_evaluate_drift_absent_is_not_drift() -> None:
    rows = evaluate_drift({})
    assert all(r.status == "absent" and r.ok for r in rows)
    assert format_report(rows).count("absent") == len(RETENTION_POLICY)


# ── runtime enforcement ───────────────────────────────────────────────


def test_ensure_retention_applies_only_existing() -> None:
    client = FakeCH(ddl_by_table={"evm_transfers": "x", "dex_events": "x"})
    applied = ensure_retention_policies(client)
    assert applied == ["dex_events", "evm_transfers"]
    assert "ALTER TABLE wallet_memory.evm_transfers MODIFY TTL block_timestamp + INTERVAL 30 DAY" in client.alters
    assert "ALTER TABLE wallet_memory.dex_events MODIFY TTL block_timestamp + INTERVAL 90 DAY" in client.alters
    assert len(client.alters) == 2


def test_ensure_retention_failure_is_non_fatal() -> None:
    client = FakeCH(ddl_by_table={"evm_transfers": "x", "dex_events": "x"}, fail_alter_for={"evm_transfers"})
    applied = ensure_retention_policies(client)
    assert applied == ["dex_events"]


def test_ensure_retention_probe_failure_returns_empty() -> None:
    assert ensure_retention_policies(FakeCH(probe_raises=True)) == []


# ── CLI ───────────────────────────────────────────────────────────────


def test_main_skips_when_ch_unavailable(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr("lite_indexer.retention._connect", lambda: None)
    assert main() == 0
    assert "CH unavailable" in capsys.readouterr().out


def test_main_fails_on_drift(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    live = {name: f"TTL {rule.time_column} + toIntervalDay({rule.days})" for name, rule in RETENTION_POLICY.items()}
    live["evm_transfers"] = "TTL ingested_at + toIntervalDay(90)"
    monkeypatch.setattr("lite_indexer.retention._connect", lambda: FakeCH(ddl_by_table=live))
    assert main() == 1
    assert "RETENTION DRIFT" in capsys.readouterr().out


def test_main_passes_when_matching(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    live = {name: f"TTL {rule.time_column} + toIntervalDay({rule.days})" for name, rule in RETENTION_POLICY.items()}
    monkeypatch.setattr("lite_indexer.retention._connect", lambda: FakeCH(ddl_by_table=live))
    assert main() == 0
    assert "RETENTION OK" in capsys.readouterr().out


def test_drift_row_defaults() -> None:
    row = DriftRow("t", "c", 1, ok=True, status="ok")
    assert row.actual_column == "" and row.actual_days is None
