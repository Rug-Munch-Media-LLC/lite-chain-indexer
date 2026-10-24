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

The engine (ClickHouse/B2/parquet IO + the row-count gate) lives in
:mod:`lite_indexer.storage.archive_export`; this file is the CLI.

Usage:
  archive-export.py --table solana_spl_balance_changes --oldest-first --limit-units 1
  archive-export.py --table internal_transfers --chain scroll --oldest-first
  archive-export.py --gate --table address_activity   # completeness gate
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lite_indexer.storage.archive_export import (  # noqa: E402
    Unit,
    discover_units,
    export_unit,
    gate,
    spec_for,
)
from lite_indexer.storage.archive_specs import TableSpec  # noqa: E402

DEFAULT_TABLES = ("solana_spl_balance_changes", "internal_transfers", "address_activity")


def parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI surface for one export run."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", action="append")
    ap.add_argument("--chain", action="append", default=[])
    ap.add_argument("--oldest-first", action="store_true")
    ap.add_argument("--limit-units", type=int, default=0)
    ap.add_argument("--gate", action="store_true")
    ap.add_argument("--gate-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    return ap.parse_args(argv)


def _report_unit(name: str, result: dict) -> None:
    """Print one unit result: skipped (already archived) or uploaded."""
    if result.get("skipped_existing"):
        print(f"   skip {result['chain']}/{name}/{result['date']} (already archived)", flush=True)
        return
    print(f"   ok {result['chain']}/{name}/{result['date']} rows={result['rows']} bytes={result['bytes']}", flush=True)


def export_units(name: str, spec: TableSpec, units: list[Unit], args: argparse.Namespace) -> int:
    """Upload ``units`` in order, honouring --limit-units; rc 1 on any failure."""
    rc = 0
    done = 0
    for unit in [] if args.gate_only else units:
        if args.limit_units and done >= args.limit_units:
            break
        try:
            result = export_unit(spec, unit)
            done += 0 if result.get("skipped_existing") else 1
            _report_unit(name, result)
        except Exception as exc:  # noqa: BLE001 — one unit must not stop a table
            rc = 1
            print(f"   FAIL {unit.chain}/{name}/{unit.day}: {str(exc)[:200]}", flush=True)
    return rc


def gate_rc(name: str, spec: TableSpec, chains: list[str]) -> int:
    """Print the completeness gate for one table; rc 1 when units are missing."""
    missing = gate(spec, chains)
    print(f"   GATE {name}: {len(missing)} units without manifest", flush=True)
    for key in missing[:10]:
        print(f"     missing {key}")
    return 1 if missing else 0


def run_table(name: str, args: argparse.Namespace) -> int:
    """Discover, optionally export and optionally gate one table."""
    spec = spec_for(name)
    units = discover_units(spec, args.chain)
    units.sort(key=lambda u: (u.day, u.chain), reverse=not args.oldest_first)
    print(f"== {name}: {len(units)} units mode={spec.mode} cursor={spec.cursor or '-'}", flush=True)
    if args.dry_run:
        for unit in units[:5]:
            print(f"   {unit.chain} {unit.day} {unit.rows}")
        return 0
    rc = export_units(name, spec, units, args)
    if args.gate:
        rc = gate_rc(name, spec, args.chain) or rc
    return rc


def main(argv: list[str] | None = None) -> int:
    """Export every requested table; rc 1 if any unit or completeness gate failed."""
    args = parse_cli(argv)
    rc = 0
    for name in args.table or list(DEFAULT_TABLES):
        rc = run_table(name, args) or rc
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
