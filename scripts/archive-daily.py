#!/usr/bin/env python3
"""Nightly archive driver: preflight, export what is missing, publish metrics.

Only tables whose unit discovery prunes to a partition are driven here
(``evm_transfers``/``internal_transfers``/``dex_events`` are partitioned by
month of their cursor). ``address_activity`` is unpartitioned and sorted by
address, so even one day costs a 262 GiB scan: it runs in its own weekly
one-pass unit (``archive-stream``) and this driver reports its last known
state instead of re-scanning it nightly.

Writes a Prometheus textfile so "units exported / units missing" is a metric,
not a paragraph.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lite_indexer.storage.archive_export import discover_units, export_unit, gate, spec_for  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
STATE = Path("/var/lib/lite-indexer/archive-state.json")
PROM = Path("/var/lib/lite-indexer/archive.prom")
DAILY = (
    "solana_spl_balance_changes",
    "internal_transfers",
    "evm_transfers",
    "dex_events",
    "scam_addresses",
    "rag_scan_labels",
    "sanctions_crypto",
    "wallet_entities",
    "deployer_history",
)


def preflight() -> list[str]:
    """Run the shell preflight; return its problem lines."""
    out = subprocess.run([str(REPO / "scripts" / "archive-preflight.sh")], capture_output=True, text=True, check=False)
    return [line for line in (out.stdout + out.stderr).splitlines() if "FAIL" in line]


def run_table(name: str, limit: int) -> dict:
    """Export up to ``limit`` missing units of one table; report coverage."""
    spec = spec_for(name)
    units = sorted(discover_units(spec, []), key=lambda u: u.day)
    uploaded, failed = 0, []
    for unit in units:
        if limit and uploaded >= limit:
            break
        try:
            if not export_unit(spec, unit).get("skipped_existing"):
                uploaded += 1
        except Exception as exc:  # noqa: BLE001 — one unit must not kill the night
            failed.append(f"{unit.chain}/{unit.day}: {str(exc)[:120]}")
    missing = len(gate(spec, []))
    return {"units": len(units), "uploaded": uploaded, "missing": missing, "failed": failed}


def write_metrics(rows: dict, problems: list[str], started: float) -> None:
    """Publish the run as a Prometheus textfile plus a JSON state."""
    lines = [f'lite_archive_units_missing{{table="{t}"}} {r["missing"]}' for t, r in rows.items()]
    lines += [f'lite_archive_units_exported{{table="{t}"}} {r["uploaded"]}' for t, r in rows.items()]
    lines += [f'lite_archive_units_total{{table="{t}"}} {r["units"]}' for t, r in rows.items()]
    lines.append(f"lite_archive_last_run_unixtime {int(time.time())}")
    lines.append(f"lite_archive_last_run_duration_seconds {int(time.time() - started)}")
    lines.append(f"lite_archive_preflight_failures {len(problems)}")
    PROM.write_text("\n".join(lines) + "\n")
    STATE.write_text(
        json.dumps({"at": dt.datetime.now(dt.UTC).isoformat(), "tables": rows, "preflight": problems}, indent=2)
    )


def main() -> int:
    started = time.time()
    problems = preflight()
    if problems:
        write_metrics({}, problems, started)
        print("preflight failed, refusing to export: " + "; ".join(problems), file=sys.stderr)
        return 2
    rows, rc = {}, 0
    write_metrics(rows, [], started)  # the metric exists from the first second, not only at the end
    for name in DAILY:
        result = run_table(name, limit=int(os.environ.get("ARCHIVE_MAX_UNITS_PER_TABLE", "0")))
        rows[name] = result
        rc = rc or (1 if result["failed"] else 0)
        print(
            f"== {name}: units={result['units']} uploaded={result['uploaded']} missing={result['missing']}", flush=True
        )
        write_metrics(rows, [], started)  # publish incrementally: a long night still yields a metric
    write_metrics(rows, [], started)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
