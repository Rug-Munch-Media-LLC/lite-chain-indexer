"""The retention gate must not report success when it measured nothing.

TWO silent-success paths existed and both exited 0:

1. A ZERO-ROW read. ``evaluate_drift({})`` reported every policy table as
   ``absent`` with ``ok=True``, so the caller printed "RETENTION OK - 0/10
   policy tables present and matching" and returned 0. A fresh deployment
   legitimately has absent tables; a zero-row read (wrong database, filtered
   query, degraded connection) is indistinguishable from that and was being
   scored as healthy.
2. An unreachable ClickHouse. ``main()`` printed "skipped (exit 0)" and
   returned 0, so an outage looked identical to a passing gate.

The contract now has three outcomes: 0 verified, 1 real drift, 2 indeterminate.
These tests pin all three, and pin that a non-empty read with genuinely absent
tables is still NOT drift — the legitimate case the old code was right about.
"""

from __future__ import annotations

from lite_indexer.retention import RETENTION_POLICY, evaluate_drift

_TABLES = sorted(RETENTION_POLICY)


def _ddl(table: str) -> str:
    """A live DDL in the exact shape parse_live_ttl reads (toIntervalDay)."""
    rule = RETENTION_POLICY[table]
    return (
        f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree() ORDER BY a "
        f"TTL {rule.time_column} + toIntervalDay({rule.days})"
    )


class TestZeroRowReadIsIndeterminate:
    def test_empty_live_marks_every_table_indeterminate(self) -> None:
        rows = evaluate_drift({})
        assert len(rows) == len(_TABLES)
        assert {r.status for r in rows} == {"indeterminate"}

    def test_empty_live_marks_nothing_ok(self) -> None:
        """The bug: an unreadable store scored 10/10 healthy."""
        assert not any(r.ok for r in evaluate_drift({}))

    def test_empty_live_is_distinct_from_absent(self) -> None:
        assert "indeterminate" != "absent"
        empty = {r.status for r in evaluate_drift({})}
        partial = {r.status for r in evaluate_drift({_TABLES[0]: _ddl(_TABLES[0])})}
        assert "indeterminate" in empty
        assert "indeterminate" not in partial

    def test_empty_live_names_the_expected_policy(self) -> None:
        for row, table in zip(evaluate_drift({}), _TABLES, strict=True):
            assert row.table == table
            assert row.expected_days == RETENTION_POLICY[table].days


class TestGenuinelyAbsentIsStillNotDrift:
    """The legitimate case the old code got right must not regress."""

    def test_one_present_table_leaves_the_rest_absent_and_ok(self) -> None:
        rows = evaluate_drift({_TABLES[0]: _ddl(_TABLES[0])})
        present = [r for r in rows if r.status == "ok"]
        absent = [r for r in rows if r.status == "absent"]
        assert len(present) == 1
        assert len(absent) == len(_TABLES) - 1
        assert all(r.ok for r in absent), "a fresh deploy must not read as drift"

    def test_absent_rows_carry_no_actual_values(self) -> None:
        absent = [r for r in evaluate_drift({_TABLES[0]: _ddl(_TABLES[0])}) if r.status == "absent"]
        assert all(r.actual_days is None and r.actual_column == "" for r in absent)


class TestDriftStillDetected:
    def test_wrong_window_is_drift(self) -> None:
        live = {t: _ddl(t) for t in _TABLES}
        table = _TABLES[0]
        live[table] = live[table].replace(f"toIntervalDay({RETENTION_POLICY[table].days})", "toIntervalDay(7)")
        drift = [r for r in evaluate_drift(live) if r.status == "drift"]
        assert [r.table for r in drift] == [table]

    def test_missing_ttl_is_drift(self) -> None:
        live = {t: _ddl(t) for t in _TABLES}
        live[_TABLES[0]] = f"CREATE TABLE {_TABLES[0]} (a UInt64) ENGINE=MergeTree ORDER BY a"
        assert any(r.status == "no-ttl" for r in evaluate_drift(live))

    def test_all_present_and_matching_is_fully_ok(self) -> None:
        rows = evaluate_drift({t: _ddl(t) for t in _TABLES})
        assert all(r.ok and r.status == "ok" for r in rows)


class TestCliExitCodes:
    def test_no_client_is_indeterminate_not_ok(self, monkeypatch) -> None:
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: None)
        assert rc.main() == 2, "an unreachable ClickHouse must not exit 0"

    def test_read_failure_is_indeterminate_not_ok(self, monkeypatch) -> None:
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: object())

        def _boom(_client):
            raise RuntimeError("connection reset")

        monkeypatch.setattr(rc, "fetch_live_ttls", _boom)
        assert rc.main() == 2

    def test_zero_row_read_is_indeterminate(self, monkeypatch, capsys) -> None:
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: object())
        monkeypatch.setattr(rc, "fetch_live_ttls", lambda _c: {})
        assert rc.main() == 2
        out = capsys.readouterr().out
        assert "INDETERMINATE" in out
        assert "RETENTION OK" not in out, "must not claim OK on an empty read"

    def test_real_drift_exits_one(self, monkeypatch) -> None:
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: object())
        table = _TABLES[0]
        live = {t: _ddl(t) for t in _TABLES}
        live[table] = live[table].replace(f"toIntervalDay({RETENTION_POLICY[table].days})", "toIntervalDay(7)")
        monkeypatch.setattr(rc, "fetch_live_ttls", lambda _c: live)
        assert rc.main() == 1

    def test_fully_verified_exits_zero(self, monkeypatch) -> None:
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: object())
        monkeypatch.setattr(rc, "fetch_live_ttls", lambda _c: {t: _ddl(t) for t in _TABLES})
        assert rc.main() == 0

    def test_partial_deploy_verified_exits_zero(self, monkeypatch) -> None:
        """Absent-but-legitimately-so must still be a verified pass."""
        import lite_indexer.retention_cli as rc

        monkeypatch.setattr(rc, "_connect", lambda: object())
        monkeypatch.setattr(rc, "fetch_live_ttls", lambda _c: {_TABLES[0]: _ddl(_TABLES[0])})
        assert rc.main() == 0
