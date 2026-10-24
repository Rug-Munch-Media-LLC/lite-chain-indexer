#!/usr/bin/env python3
"""Export ClickHouse tables to the permanent B2 parquet archive.

Layout (deterministic; a re-run overwrites the same keys, never duplicates)::

    <remote>/<chain>/<table>/<YYYY-MM-DD>/part-000.parquet
    <remote>/<chain>/<table>/<YYYY-MM-DD>/manifest.json

One unit = (chain, table, cursor-date). A unit is uploaded only after its row
count equals ClickHouse ``count()`` for the exact same predicate; any mismatch
aborts that unit and never produces a partial object. Modes come from
``lite_indexer.storage.archive_specs`` (``cursor`` or ``snapshot``).

Existing objects are never rewritten (``skipped_existing``): a second writer
on the same layout must not clobber a verified unit.

Usage:
  archive-export.py --table solana_spl_balance_changes --oldest-first --limit-units 1
  archive-export.py --table internal_transfers --chain scroll --oldest-first
  archive-export.py --gate --table address_activity   # completeness gate
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lite_indexer.storage.archive_specs import (  # noqa: E402
    ARCHIVE_TABLES,
    CURSOR,
    SNAPSHOT,
    TableSpec,
)

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
            [*CH, "--max_execution_time", "0",
             "--output_format_parquet_compression_method", "zstd",
             "--query", sql, "--format", "Parquet"],
            stdout=fh, stderr=subprocess.PIPE, check=False,
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
    sql = (f"SELECT {chain_expr} AS c, {date_expr} AS d, count() AS r FROM {DB}.{spec.table} "
           f"{where} GROUP BY c, d ORDER BY d, c")
    units = []
    for line in ch(sql).splitlines():
        if line.strip():
            c, d, r = line.split("\t")
            units.append(Unit(c, spec.table, d, int(r)))
    return units


def parquet_count(path: Path) -> int:
    sql = f"SELECT count(*) FROM read_parquet('{path}')"
    out = subprocess.run(["duckdb", "-noheader", "-list", "-c", sql],
                         capture_output=True, text=True, check=True).stdout
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
            raise RuntimeError(
                f"row mismatch {unit.chain}/{spec.table}/{unit.day}: ch={unit.rows} parquet={got}")
        digest = hashlib.md5(part.read_bytes()).hexdigest()  # noqa: S324 — integrity, not auth
        size = part.stat().st_size
        upload(part, f"{key_dir}/part-000.parquet")
        manifest = {
            "v": 1, "table": spec.table, "chain": unit.chain, "date": unit.day,
            "mode": spec.mode, "cursor": spec.cursor or None, "rows": got,
            "bytes": size, "md5": digest, "part": "part-000.parquet",
            "predicate": pred, "exported_at": dt.datetime.now(dt.UTC).isoformat(),
            "clickhouse_rows": unit.rows,
        }
        mf = Path(tmp) / "manifest.json"
        mf.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        upload(mf, f"{key_dir}/manifest.json")
    return manifest


def gate(spec: TableSpec, chains: list[str]) -> list[str]:
    missing = []
    for unit in discover_units(spec, chains):
        key = f"{unit.chain}/{spec.table}/{unit.day}/manifest.json"
        if not object_exists(key):
            missing.append(key)
    return missing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", action="append")
    ap.add_argument("--chain", action="append", default=[])
    ap.add_argument("--oldest-first", action="store_true")
    ap.add_argument("--limit-units", type=int, default=0)
    ap.add_argument("--gate", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    tables = args.table or ["solana_spl_balance_changes", "internal_transfers", "address_activity"]
    rc = 0
    for name in tables:
        spec = spec_for(name)
        units = discover_units(spec, args.chain)
        units.sort(key=lambda u: (u.day, u.chain), reverse=not args.oldest_first)
        print(f"== {name}: {len(units)} units mode={spec.mode} cursor={spec.cursor or '-'}", flush=True)
        if args.dry_run:
            for u in units[:5]:
                print(f"   {u.chain} {u.day} {u.rows}")
            continue
        done = 0
        for u in units:
            if args.limit_units and done >= args.limit_units:
                break
            try:
                m = export_unit(spec, u)
                done += 1
                if m.get("skipped_existing"):
                    print(f"   skip {m['chain']}/{name}/{m['date']} (already archived)", flush=True)
                else:
                    print(f"   ok {m['chain']}/{name}/{m['date']} rows={m['rows']} bytes={m['bytes']}", flush=True)
            except Exception as exc:  # noqa: BLE001 — one unit must not stop a table
                rc = 1
                print(f"   FAIL {u.chain}/{name}/{u.day}: {str(exc)[:200]}", flush=True)
        if args.gate:
            missing = gate(spec, args.chain)
            print(f"   GATE {name}: {len(missing)} units without manifest", flush=True)
            for key in missing[:10]:
                print(f"     missing {key}")
            rc = rc or (1 if missing else 0)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
