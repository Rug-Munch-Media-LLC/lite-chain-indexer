"""Discovery + ordering validation for the numbered migration set.

The set is ``migrations/NNN_name.sql``. The invariants that were previously
unenforced and are now a hard error at PARSE time (before any ClickHouse
call, so a bad set can never half-apply):

* every file matches ``^(\\d{3})_`` — zero padding to three digits;
* no two files share a version (the tree carried three: 001, 004, 013);
* versions are strictly monotonic in sorted order.

One function per concern. :mod:`lite_indexer.migrate` applies them.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"

#: Filename prefix: exactly three digits, then an underscore, then a name.
_VERSION_RE = re.compile(r"^(?P<version>\d{3})_(?P<name>[A-Za-z0-9][A-Za-z0-9_\-]*)\.sql$")


class MigrationSetError(RuntimeError):
    """The migration set violates an ordering or naming invariant."""


@dataclass(frozen=True, slots=True)
class Migration:
    """One numbered migration file on disk."""

    version: int
    name: str
    path: Path

    @property
    def label(self) -> str:
        """``NNN_name`` — the stem, as it appears in the file listing."""
        return self.path.stem


def checksum(path: Path) -> str:
    """SHA-256 of the file bytes, hex-truncated to 16 chars.

    Short enough to read in a log line, wide enough that an edit to a
    migration after it was applied is caught by ``migrate.py --verify``.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def parse_migration_dir(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Return every migration in ascending version order.

    Raises:
        MigrationSetError: On an unparseable filename, a duplicate version,
            or a version sequence that is not strictly monotonic.
    """
    files = sorted(p for p in directory.glob("*.sql") if p.is_file())
    if not files:
        raise MigrationSetError(f"no migrations found in {directory}")

    found: list[Migration] = []
    for path in files:
        match = _VERSION_RE.match(path.name)
        if match is None:
            raise MigrationSetError(
                f"migration filename must be NNN_name.sql with a zero-padded 3-digit version: {path.name!r}"
            )
        found.append(Migration(version=int(match.group("version")), name=match.group("name"), path=path))

    by_version: dict[int, Migration] = {}
    for mig in found:
        clash = by_version.get(mig.version)
        if clash is not None:
            raise MigrationSetError(
                f"duplicate migration version {mig.version:03d}: "
                f"{clash.path.name} and {mig.path.name} — both would be applied "
                f"and the second silently overwrites the first's effects"
            )
        by_version[mig.version] = mig

    ordered = sorted(found, key=lambda m: m.version)
    for prev, curr in zip(ordered, ordered[1:], strict=False):
        if curr.version <= prev.version:
            raise MigrationSetError(f"migration versions must be strictly monotonic: {prev.label} then {curr.label}")
    return ordered


def pending_migrations(applied: set[int], directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Migrations on disk that the catalog has no record of.

    An empty directory is accepted (returns []) so a caller with no
    migrations on disk is not an error.

    Raises:
        MigrationSetError: When a pending migration's version is LOWER than
            the highest applied version. That is the "013 applied without
            012" failure: the missing file is a real gap in the schema, and
            applying it now would run an old DDL over a newer one.
    """
    try:
        found = parse_migration_dir(directory)
    except MigrationSetError:
        if not any(directory.glob("*.sql")):
            return []
        raise
    if not applied:
        return found
    highest = max(applied)
    out_of_order = [m for m in found if m.version not in applied and m.version < highest]
    if out_of_order:
        labels = ", ".join(m.label for m in out_of_order)
        raise MigrationSetError(
            f"refusing to apply {len(out_of_order)} migration(s) below the "
            f"highest applied version {highest:03d}: {labels}. These were "
            f"never applied and their DDL predates what is already live; "
            f"re-number them above {highest:03d} or backfill them by hand"
        )
    return [m for m in found if m.version not in applied]
