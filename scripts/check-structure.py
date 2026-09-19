#!/usr/bin/env python3
"""check-structure.py — firm per-file size policy (shrink-only ratchet).

Policy (docs/MODERNIZATION.md §2.2 / §4.3, tightened 2026-09-19):

  * a file > ADVISORY (150) lines is REPORTED (advisory),
  * a file > 200 lines NOT in the baseline is a HARD FAIL (firm ≤200 policy),
  * an allowlisted (baselined) file may never GROW — growth is a HARD FAIL,
  * a file > 500 lines is a HARD FAIL unless allowlisted (which must still shrink).

Baselined files that are now ≤ 200 lines (or gone) are printed as
``[remove]`` candidates — the allowlist should only ever get shorter.

``--update-baseline`` regenerates the allowlist (only files > 200) but REFUSES
to raise any existing entry or to grandfather a *new* >200-line file (bootstrap
aside). The ratchet is one-way: the codebase can only get smaller per file.

Usage:
  python3 scripts/check-structure.py --repo .                 # gate
  python3 scripts/check-structure.py --repo . --update-baseline
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ADVISORY_THRESHOLD = 150
FAIL_THRESHOLD = 200
GOD_THRESHOLD = 500
DEFAULT_BASELINE = "scripts/structure-baseline.json"
DEFAULT_SCAN_ROOTS = ("src/rugbrain",)

SKIP_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "dist",
    "build",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "site-packages",
    "mutants",
    ".tox",
    ".nox",
    "htmlcov",
}


def count_lines(path: Path) -> int:
    try:
        return len(path.read_text(errors="ignore").splitlines())
    except OSError:
        return 0


def scan(root: Path, scan_roots: tuple[str, ...]) -> dict[str, int]:
    """Map posix path -> line count for every scanned *.py > ADVISORY."""
    sizes: dict[str, int] = {}
    for rel in scan_roots:
        base = root / rel
        if not base.is_dir():
            continue
        for p in base.rglob("*.py"):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            n = count_lines(p)
            if n > ADVISORY_THRESHOLD:
                sizes[p.relative_to(root).as_posix()] = n
    return sizes


def load_baseline(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text()).get("files", {})
    except (OSError, json.JSONDecodeError):
        return {}


def write_baseline(path: Path, sizes: dict[str, int]) -> None:
    payload = {
        "thresholds": {
            "advisory": ADVISORY_THRESHOLD,
            "fail": FAIL_THRESHOLD,
            "hard": GOD_THRESHOLD,
        },
        "files": dict(sorted(sizes.items())),
    }
    path.write_text(json.dumps(payload, indent=1) + "\n")


def evaluate(
    sizes: dict[str, int], base: dict[str, int]
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Classify every scanned file: (failures, reports, shrinks, removals)."""
    failures: list[str] = []
    reports: list[str] = []
    shrinks: list[str] = []
    removals: list[str] = []
    for path, n in sorted(sizes.items()):
        if path in base:
            recorded = base[path]
            if n > recorded:
                failures.append(f"{path}: {recorded} -> {n} lines (allowlisted file grew)")
            elif n < recorded:
                shrinks.append(f"{path}: {recorded} -> {n} (run --update-baseline)")
            if n <= FAIL_THRESHOLD:
                removals.append(f"{path}: {n} lines (<= {FAIL_THRESHOLD} — remove from allowlist)")
        elif n > GOD_THRESHOLD:
            failures.append(f"{path}: {n} lines (NEW file >{GOD_THRESHOLD} — split it)")
        elif n > FAIL_THRESHOLD:
            failures.append(f"{path}: {n} lines (NEW file >{FAIL_THRESHOLD} — split it)")
        else:
            reports.append(f"{path}: {n} lines (advisory > {ADVISORY_THRESHOLD})")
    for path, recorded in sorted(base.items()):
        if path not in sizes:
            removals.append(
                f"{path}: recorded {recorded} — gone or <= {ADVISORY_THRESHOLD} "
                "(remove from allowlist)"
            )
    return failures, reports, shrinks, removals


def update_baseline(sizes: dict[str, int], old: dict[str, int], path: Path) -> int:
    bootstrap = not path.exists()
    raised = {k: (old[k], v) for k, v in sizes.items() if k in old and v > old[k]}
    new_over = [k for k, v in sizes.items() if k not in old and v > FAIL_THRESHOLD]
    if raised:
        print("REFUSING to raise the structure baseline (shrink-only):")
        for k, (o, n) in sorted(raised.items()):
            print(f"  {k}: {o} -> {n}")
        return 1
    if new_over and not bootstrap:
        print("REFUSING to grandfather NEW >200-line files (split them first):")
        for k in sorted(new_over):
            print(f"  {k}: {sizes[k]}")
        return 1
    kept = {k: min(old[k], sizes[k]) for k in old if k in sizes and sizes[k] > FAIL_THRESHOLD}
    if bootstrap:
        kept.update({k: v for k, v in sizes.items() if v > FAIL_THRESHOLD})
    write_baseline(path, kept)
    print(f"structure baseline updated: {path} ({len(kept)} files > {FAIL_THRESHOLD})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default=DEFAULT_BASELINE)
    ap.add_argument("--scan-root", action="append", dest="scan_roots")
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    scan_roots = tuple(args.scan_roots or DEFAULT_SCAN_ROOTS)
    sizes = scan(root, scan_roots)
    baseline_path = root / args.baseline
    old = load_baseline(baseline_path)

    if args.update_baseline:
        return update_baseline(sizes, old, baseline_path)

    failures, reports, shrinks, removals = evaluate(sizes, old)
    print(
        f"structure gate: {len(sizes)} files > {ADVISORY_THRESHOLD} lines (allowlisted: {len(old)})"
    )
    for line in reports:
        print(f"  [report] {line}")
    for line in shrinks:
        print(f"  [shrink] {line}")
    for line in removals:
        print(f"  [remove] {line}")
    if failures:
        print("\nFAIL (size ratchet):")
        for line in failures:
            print(f"  - {line}")
        return 1
    print("structure gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
