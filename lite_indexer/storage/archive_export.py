"""ClickHouse → B2 parquet archive engine (driven by ``scripts/archive-export.py``).

Split out of the script 2026-10-25 (one primary function per module + the size
canon). Layout is deterministic; a re-run overwrites the same keys, never
duplicates::

    <remote>/<chain>/<table>/<YYYY-MM-DD>/part-000.parquet
    <remote>/<chain>/<table>/<YYYY-MM-DD>/manifest.json

One unit = (chain, table, cursor-date). A unit is uploaded only after its row
count equals ClickHouse ``count()`` for the exact same predicate; any mismatch
aborts that unit and never produces a partial object. Existing objects are
never rewritten, so a second writer cannot clobber a verified unit.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from lite_indexer.storage.archive_specs import ARCHIVE_TABLES, CURSOR, SNAPSHOT, TableSpec

CH = os.environ.get("CH_BIN", "docker exec rmi-clickhouse clickhouse-client").split()
DB = os.environ.get("DB", "wallet_memory")
REMOTE = os.environ.get("ARCHIVE_REMOTE", "b2-crypt-v2:chain-data/parquet")
RCLONE = os.environ.get("RCLONE_BIN", "rclone")


@dataclass(frozen=True)
class Unit:
    chain: str
    table: str
    day: str
    rows: int


def ch(sql: str, fmt: str = "TSV") -> str:
    proc = subprocess.run([*CH, "--query", sql, "--format", fmt], capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode()[-400:])
    return proc.stdout.decode()


def ch_parquet(sql: str, dest: Path) -> None:
    with dest.open("wb") as fh:
        proc = subprocess.run(
            [
                *CH,
                "--max_execution_time",
                "0",
                "--output_format_parquet_compression_method",
                "zstd",
                "--query",
                sql,
                "--format",
                "Parquet",
            ],
            stdout=fh,
            stderr=subprocess.PIPE,
            check=False,
        )
    if proc.returncode != 0 or dest.stat().st_size == 0:
        raise RuntimeError(proc.stderr.decode()[-400:] or "empty parquet")


def spec_for(table: str) -> TableSpec:
    """Registry spec, or a snapshot spec derived from ClickHouse columns."""
    if table in ARCHIVE_TABLES:
        return ARCHIVE_TABLES[table]
    out = ch(f"SELECT name FROM system.columns WHERE database = '{DB}' AND table = '{table}' ORDER BY position")
    cols = tuple(c for c in out.split() if c)
    if not cols:
        raise SystemExit(f"no such table in {DB}: {table}")
    return TableSpec(table, table, "", SNAPSHOT, "chain" if "chain" in cols else None, "", 0, cols)


def unit_predicate(spec: TableSpec, unit: Unit) -> str:
    parts = []
    if spec.chain_column:
        parts.append(f"{spec.chain_column} = '{unit.chain}'")
    if spec.mode == CURSOR:
        parts.append(f"toDate({spec.cursor}) = toDate('{unit.day}')")
    return " AND ".join(parts) or "1=1"


def discover_units(spec: TableSpec, chains: list[str]) -> list[Unit]:
    """Every unit ClickHouse can serve right now, with its authoritative count."""
    chain_expr = spec.chain_column or "'solana'"
    where = ""
    if chains and spec.chain_column:
        where = f"WHERE {spec.chain_column} IN ({','.join(repr(c) for c in chains)})"
    date_expr = f"toDate({spec.cursor})" if spec.mode == CURSOR else "toDate(now())"
    sql = (
        f"SELECT {chain_expr} AS c, {date_expr} AS d, count() AS r FROM {DB}.{spec.table} "
        f"{where} GROUP BY c, d ORDER BY d, c"
    )
    units = []
    for line in ch(sql).splitlines():
        if line.strip():
            c, d, r = line.split("\t")
            units.append(Unit(c, spec.table, d, int(r)))
    return units


def parquet_count(path: Path) -> int:
    sql = f"SELECT count(*) FROM read_parquet('{path}')"
    out = subprocess.run(["duckdb", "-noheader", "-list", "-c", sql], capture_output=True, text=True, check=True).stdout
    return int(out.strip())


def upload(local: Path, key: str) -> None:
    subprocess.run([RCLONE, "copyto", str(local), f"{REMOTE}/{key}"], check=True, capture_output=True)


def object_exists(key: str) -> bool:
    proc = subprocess.run([RCLONE, "lsjson", f"{REMOTE}/{key}"], capture_output=True, text=True, check=False)
    return proc.returncode == 0 and bool(json.loads(proc.stdout or "[]"))


def export_unit(spec: TableSpec, unit: Unit) -> dict:
    key_dir = f"{unit.chain}/{spec.table}/{unit.day}"
    if object_exists(f"{key_dir}/part-000.parquet") or object_exists(f"{key_dir}/manifest.json"):
        return {"skipped_existing": True, "chain": unit.chain, "table": spec.table, "date": unit.day}
    cols = ", ".join(spec.columns)
    pred = unit_predicate(spec, unit)
    order = f" ORDER BY {spec.cursor}" if spec.mode == CURSOR else ""
    with tempfile.TemporaryDirectory(prefix="archive-") as tmp:
        part = Path(tmp) / "part-000.parquet"
        ch_parquet(f"SELECT {cols} FROM {DB}.{spec.table} WHERE {pred}{order}", part)
        got = parquet_count(part)
        if got != unit.rows:
            raise RuntimeError(f"row mismatch {unit.chain}/{spec.table}/{unit.day}: ch={unit.rows} parquet={got}")
        digest = hashlib.md5(part.read_bytes()).hexdigest()  # noqa: S324 — integrity, not auth
        size = part.stat().st_size
        upload(part, f"{key_dir}/part-000.parquet")
        manifest = {
            "v": 1,
            "table": spec.table,
            "chain": unit.chain,
            "date": unit.day,
            "mode": spec.mode,
            "cursor": spec.cursor or None,
            "rows": got,
            "bytes": size,
            "md5": digest,
            "part": "part-000.parquet",
            "predicate": pred,
            "exported_at": dt.datetime.now(dt.UTC).isoformat(),
            "clickhouse_rows": unit.rows,
        }
        mf = Path(tmp) / "manifest.json"
        mf.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        upload(mf, f"{key_dir}/manifest.json")
    return manifest


def gate(spec: TableSpec, chains: list[str]) -> list[str]:
    """Completeness gate: every unit ClickHouse still holds must have a manifest.

    One recursive listing per chain (not one request per unit), compared as a
    set — a missing unit is a FAILED run, never a silent omission.
    """
    units = discover_units(spec, chains)
    present: set[str] = set()
    for chain in sorted({u.chain for u in units}):
        proc = subprocess.run(
            [RCLONE, "lsf", "-R", "--files-only", f"{REMOTE}/{chain}/{spec.table}"],
            capture_output=True,
            text=True,
            check=False,
        )
        present |= {f"{chain}/{spec.table}/{line.strip()}" for line in proc.stdout.splitlines() if line.strip()}
    return [
        f"{u.chain}/{spec.table}/{u.day}/manifest.json"
        for u in units
        if f"{u.chain}/{spec.table}/{u.day}/manifest.json" not in present
    ]
