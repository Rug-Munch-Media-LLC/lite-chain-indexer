-- Migration 022: migration catalog (wallet_memory.schema_migrations).
--
-- WHY: there was no alembic, no schema_migrations table and no ordering
-- enforcement anywhere in the repo. `rg 'alembic|schema_migrations|
-- schema_version'` returned zero hits across 615 files. Migrations were
-- applied by hand with `clickhouse-client < file`, and three of the 21
-- files shared numbers (001, 004, 013), so two of each were silently
-- skippable depending on glob order. This table records what was applied,
-- when, how long it took, and a checksum so an edited-after-the-fact
-- migration is detectable.
--
-- The catalog is created FIRST in the sequence (this file is 022 and is
-- applied before any pending migration by `python3 -m lite_indexer.migrate`),
-- so the runner always has somewhere to read max(version) from.
--
-- It is intentionally a plain MergeTree with no TTL: an applied-migration
-- record must outlive the data it describes, or a rolled-back schema can be
-- re-applied on top of existing data.

CREATE TABLE IF NOT EXISTS wallet_memory.schema_migrations (
    version     UInt32,
    name        String,
    checksum    String,
    applied_at  DateTime DEFAULT now(),
    duration_ms UInt32
) ENGINE = MergeTree()
ORDER BY version;

-- Applied-migration count, by version. Used by `migrate.py --verify` and by
-- the out-of-order guard: a pending file with version <= max(applied) is a
-- hard error, never a write.
