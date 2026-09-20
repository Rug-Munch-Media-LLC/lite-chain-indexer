#!/usr/bin/env python3
"""Dedicated monkeypatch ratchet gate (decrease-only).

docs/MODERNIZATION.md §2.3 / §4.3: the repo-wide monkeypatch count may never
increase. This gate is deliberately separate from the aggregate
``check-antipatterns.py`` baseline so that an unrelated change cannot inflate
the monkeypatch budget while other rules shrink to compensate.

Counts monkeypatch-attribute access, mock-patch calls, patch decorators,
patch-object calls and sys.modules deletions (Python sources only).
The matched patterns live in ``MONKEY_RE`` below.

Usage:
  python3 scripts/check-monkeypatch --repo .                 # gate
  python3 scripts/check-monkeypatch --repo . --update-baseline
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

DEFAULT_BASELINE = "scripts/monkeypatch-baseline.json"
MONKEY_RE = re.compile(r"monkeypatch\.|mock\.patch|@patch\(|patch\.object|del sys\.modules")
PROD_RE = re.compile(r"monkeypatch\.")
PROD_ROOTS = ("lite_indexer", "src")

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


def scan(root: Path) -> tuple[int, dict[str, int]]:
    """Return (total occurrences, per-file counts for files with matches)."""
    total = 0
    per_file: dict[str, int] = {}
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        try:
            lines = p.read_text(errors="ignore").splitlines()
        except OSError:
            continue
        n = sum(1 for ln in lines if MONKEY_RE.search(ln))
        if n:
            rel = p.relative_to(root).as_posix()
            per_file[rel] = n
            total += n
    return total, per_file


def scan_production(root: Path) -> dict[str, int]:
    """Count ``monkeypatch.`` occurrences in production code (hard-zero rule)."""
    per_file: dict[str, int] = {}
    for name in PROD_ROOTS:
        base = root / name
        if not base.is_dir():
            continue
        for p in base.rglob("*.py"):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            try:
                lines = p.read_text(errors="ignore").splitlines()
            except OSError:
                continue
            n = sum(1 for ln in lines if PROD_RE.search(ln))
            if n:
                per_file[p.relative_to(root).as_posix()] = n
    return per_file


def load_baseline(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(json.loads(path.read_text()).get("total", 0))
    except (OSError, ValueError, TypeError):
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default=DEFAULT_BASELINE)
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    total, per_file = scan(root)
    baseline_path = root / args.baseline
    base = load_baseline(baseline_path)
    prod = scan_production(root)

    if args.update_baseline:
        bootstrap = not baseline_path.exists()
        if total > base and not bootstrap:
            print(f"REFUSING to raise the monkeypatch baseline: {base} -> {total}")
            return 1
        baseline_path.write_text(json.dumps({"total": total, "files": dict(sorted(per_file.items()))}, indent=1) + "\n")
        print(f"monkeypatch baseline updated: {baseline_path} (total {total})")
        return 0

    print(f"monkeypatch gate: {total} occurrences (baseline {base})")
    for rel, n in sorted(per_file.items(), key=lambda x: -x[1])[:10]:
        print(f"  {n:>4}  {rel}")
    if prod:
        print("\nFAIL (zero monkeypatch in production):")
        for rel, n in sorted(prod.items()):
            print(f"  - {rel}: {n} (replace with a DI seam)")
        return 1
    print(f"monkeypatch gate: production clean (0); test debt: {total} occurrences in tests (shrink-only)")
    if total > base:
        print(f"\nFAIL (monkeypatch ratchet): {base} -> {total} — decrease-only, fix don't add")
        return 1
    print("monkeypatch gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
