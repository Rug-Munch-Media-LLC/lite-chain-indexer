"""Tests for the numbered-migration set and the migration runner.

Covers the two invariants that were previously unenforced: a duplicate-free,
strictly monotonic version sequence, and a runner that refuses to apply a
migration below the highest applied version instead of writing it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer.migrate import CATALOG_TABLE, applied_versions, ensure_catalog, run
from lite_indexer.migration_set import (
    MIGRATIONS_DIR,
    Migration,
    MigrationSetError,
    parse_migration_dir,
)


class FakeClient:
    """Minimal ClickHouse stand-in that records executed statements."""

    def __init__(self, applied: set[int] | None = None) -> None:
        self.applied = applied or set()
        self.statements: list[str] = []

    def execute(self, query: str, params: object = None) -> list[tuple[object, ...]]:
        self.statements.append(query)
        if "SELECT version FROM" in query:
            return [(v,) for v in sorted(self.applied)]
        if query.strip().upper().startswith("INSERT INTO"):
            version = int(query.split("VALUES (")[1].split(",")[0])
            self.applied.add(version)
        return []


def _write(directory: Path, name: str, body: str = "SELECT 1;") -> None:
    (directory / name).write_text(body, encoding="utf-8")


def test_migrations_are_duplicate_free_and_monotonic() -> None:
    """The real set has unique versions in strictly ascending order.

    Before the renumber this failed on three duplicated prefixes: 001, 004
    and 013, each shared by two files. Two files per number meant one was
    silently skippable depending on glob order.
    """
    found = parse_migration_dir(MIGRATIONS_DIR)

    versions = [m.version for m in found]
    assert versions == sorted(versions), "migrations are not in ascending order"
    assert len(versions) == len(set(versions)), "duplicate migration version in the set"
    assert all(v > 0 for v in versions)



def test_duplicate_version_is_rejected_at_parse_time(tmp_path: Path) -> None:
    """Two files sharing a version abort before any client call."""
    _write(tmp_path, "001_a.sql")
    _write(tmp_path, "001_b.sql")
    with pytest.raises(MigrationSetError, match="duplicate migration version 001"):
        parse_migration_dir(tmp_path)


def test_migration_runner_refuses_out_of_order() -> None:
    """A pending migration below max(applied) is refused, not applied.

    This is the "013 applied without 012" case: the missing file is a real
    schema gap and its DDL predates what is already live, so applying it
    now would run old DDL over a new schema. The runner must abort having
    executed no migration statement.
    """
    client = FakeClient(applied={13, 14})

    exit_code = run(client, apply=True)

    assert exit_code == 2, "out-of-order must be a hard error"
    # The catalog bootstrap (CREATE IF NOT EXISTS + the version read) runs
    # first by design; what must NOT happen is a migration INSERT.
    inserts = [s for s in client.statements if s.strip().upper().startswith("INSERT INTO")]
    assert inserts == [], f"out-of-order run wrote migrations: {inserts}"
    assert client.applied == {13, 14}, "catalog must be unchanged"


def test_runner_applies_pending_above_max_applied(tmp_path: Path) -> None:
    """The happy path records (version, name, checksum) for each migration."""
    _write(tmp_path, "001_a.sql", "SELECT 1;")
    _write(tmp_path, "002_b.sql", "SELECT 2;\n")
    client = FakeClient(applied={1})

    exit_code = run(client, apply=True, directory=tmp_path)

    assert exit_code == 0
    assert client.applied == {1, 2}
    inserts = [s for s in client.statements if s.strip().upper().startswith("INSERT INTO")]
    assert len(inserts) == 1 and "VALUES (2, 'b'" in inserts[0]
    assert "duration_ms" in inserts[0] and "checksum" in inserts[0]


def test_status_mode_writes_nothing(tmp_path: Path) -> None:
    """--status reports the set and never records an application."""
    _write(tmp_path, "001_a.sql")
    client = FakeClient()

    assert run(client, apply=False, directory=tmp_path) == 0
    assert client.applied == set()


def test_catalog_schema_matches_migration(tmp_path: Path) -> None:
    """The runner's bootstrap DDL and the 022 migration agree on the shape.

    A deployment that applied 022 through clickhouse-client and one that was
    bootstrapped by the runner must end up with the same table, or the
    runner's INSERT fails on a database the migration already created.
    """
    migration = (MIGRATIONS_DIR / "022_migration_catalog.sql").read_text(encoding="utf-8")
    for column in ("version", "name", "checksum", "applied_at", "duration_ms"):
        assert column in migration, f"022 migration is missing {column}"
    assert CATALOG_TABLE in migration

    client = FakeClient()
    ensure_catalog(client)
    assert any("CREATE TABLE IF NOT EXISTS" in s for s in client.statements)
    assert applied_versions(client) == set()


def test_every_migration_name_is_parseable() -> None:
    """All on-disk files carry a three-digit zero-padded prefix."""
    found = parse_migration_dir(MIGRATIONS_DIR)
    assert all(m.path.name[:3].isdigit() for m in found)
    assert all(isinstance(m, Migration) for m in found)
