"""Apply and verify the numbered ClickHouse migrations.

Replaces the manual ``clickhouse-client < migrations/NNN.sql`` convention
that produced an unenforced set: three duplicate versions, no catalog, and
a silent ``MODIFY TTL`` overwrite when files were applied out of order.

    python3 -m lite_indexer.migrate            # apply everything pending
    python3 -m lite_indexer.migrate --status   # report only, write nothing
    python3 -m lite_indexer.migrate --verify   # re-read live TTLs vs policy

The catalog is ``wallet_memory.schema_migrations`` (migration 022). It is
created on demand so the runner works on a database that has never had a
migration applied.

FAIL-LOUD, NOT FAIL-SILENT: a pending migration below the highest applied
version is a hard error before any statement runs. A migration that fails
mid-file leaves its partial statements applied and is NOT recorded, so the
next run retries it — ClickHouse DDL here is metadata-only and idempotent by
convention, which the file headers state.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.migration_set import (
    MIGRATIONS_DIR,
    Migration,
    MigrationSetError,
    checksum,
    parse_migration_dir,
    pending_migrations,
)

logger = get_logger(__name__)

CATALOG_TABLE = "wallet_memory.schema_migrations"

_CATALOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_TABLE} (
    version     UInt32,
    name        String,
    checksum    String,
    applied_at  DateTime DEFAULT now(),
    duration_ms UInt32
) ENGINE = MergeTree()
ORDER BY version
"""


def ensure_catalog(client: Any) -> None:
    """Create ``wallet_memory.schema_migrations`` if it does not exist."""
    client.execute(_CATALOG_DDL)


def applied_versions(client: Any) -> set[int]:
    """Versions already recorded in the catalog."""
    rows = client.execute(f"SELECT version FROM {CATALOG_TABLE} ORDER BY version")
    return {int(r[0]) for r in rows}


def _split_statements(sql: str) -> list[str]:
    """Split a migration file into statements on top-level semicolons.

    Comment-only fragments are dropped. Deliberately simple: the migration
    set has no stored routines, no dollar-quoted bodies and no semicolons
    inside string literals, and a real SQL splitter is a much larger
    dependency than this set justifies.
    """
    out: list[str] = []
    for chunk in sql.split(";"):
        body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            out.append(body)
    return out


def apply_migration(client: Any, mig: Migration) -> int:
    """Run one migration's statements, then record it. Returns duration_ms.

    The record is written only after every statement succeeded, so a failed
    migration is retried rather than counted as done.
    """
    started = time.monotonic()
    for statement in _split_statements(mig.path.read_text(encoding="utf-8")):
        client.execute(statement)
    duration_ms = int((time.monotonic() - started) * 1000)
    client.execute(
        f"INSERT INTO {CATALOG_TABLE} (version, name, checksum, duration_ms) "
        f"VALUES ({mig.version}, '{mig.name}', '{checksum(mig.path)}', {duration_ms})"
    )
    return duration_ms


def _status_lines(applied: set[int], found: list[Migration]) -> list[str]:
    lines = [f"catalog: {len(applied)} applied, {len(found)} on disk"]
    for mig in found:
        mark = "applied" if mig.version in applied else "PENDING"
        lines.append(f"  {mig.label:<48} {mark}")
    return lines


def run(client: Any, *, apply: bool, directory: Any = MIGRATIONS_DIR) -> int:
    """Apply (or just report) pending migrations. Returns a process exit code.

    Args:
        client: A ClickHouse client with ``execute(query) -> rows``.
        apply: False for a dry report that writes nothing.
        directory: Migrations directory (injectable for tests).
    """
    try:
        found = parse_migration_dir(directory)
    except MigrationSetError as exc:
        logger.error("migration_set_invalid", error=str(exc))
        return 2

    ensure_catalog(client)
    applied = applied_versions(client)
    logger.info("migrations_status", **{"on_disk": len(found), "applied": len(applied)})

    try:
        todo = pending_migrations(applied, directory)
    except MigrationSetError as exc:
        logger.error("migrations_out_of_order", error=str(exc))
        return 2

    if not apply:
        for line in _status_lines(applied, found):
            print(line)  # noqa: T201
        return 0

    if not todo:
        logger.info("migrations_up_to_date", applied=len(applied))
        return 0
    for mig in todo:
        duration_ms = apply_migration(client, mig)
        logger.info(
            "migration_applied",
            version=f"{mig.version:03d}",
            name=mig.name,
            duration_ms=duration_ms,
        )
    logger.info("migrations_applied", count=len(todo))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. ``--verify`` delegates to the retention checker."""
    parser = argparse.ArgumentParser(prog="lite_indexer.migrate", description=__doc__)
    parser.add_argument("--status", action="store_true", help="report only, write nothing")
    parser.add_argument("--verify", action="store_true", help="check live TTLs against RETENTION_POLICY")
    args = parser.parse_args(argv)

    if args.verify:
        from lite_indexer.retention import main as retention_main

        return retention_main()

    from lite_indexer.retention import _connect

    client = _connect()
    if client is None:
        logger.error("migrate_clickhouse_unavailable")
        return 1
    return run(client, apply=not args.status)


if __name__ == "__main__":
    sys.exit(main())
