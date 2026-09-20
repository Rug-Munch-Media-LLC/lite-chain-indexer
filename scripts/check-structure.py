#!/usr/bin/env python3
"""check-structure.py — firm per-file size policy (shrink-only ratchet).

Policy: >150 REPORTED · >200 HARD FAIL unless allowlisted · allowlisted files
may never GROW · >500 HARD FAIL unless allowlisted. ``--update-baseline``
only ever removes/shrinks; per-repo roots via [tool.fleet-gates].scan-roots.

Usage:
  python3 scripts/check-structure.py --repo .                 # gate
  python3 scripts/check-structure.py --repo . --update-baseline
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

ADVISORY_THRESHOLD = 150
FAIL_THRESHOLD = 200
GOD_THRESHOLD = 500
FN_DENSITY_TARGET = 4
DEFAULT_BASELINE = "scripts/structure-baseline.json"

_SKIP = ".git .hg .svn node_modules __pycache__ .venv venv dist build .mypy_cache .ruff_cache .pytest_cache site-packages mutants .tox .nox htmlcov .next coverage .openhands"
SKIP_DIRS = frozenset(_SKIP.split())
SKIP_TOP = frozenset(["scripts", "tests", "docs", "migrations", ".github", ".forgejo"])


def count_lines(path: Path) -> int:
    """Return the line count of one file (0 when unreadable)."""
    try:
        return len(path.read_text(errors="ignore").splitlines())
    except OSError:
        return 0


def count_top_level_defs(path: Path) -> int:
    """Count top-level function/class defs (0 when unparseable)."""
    try:
        tree = ast.parse(path.read_text(errors="ignore"))
    except (OSError, SyntaxError, ValueError):
        return 0
    return sum(1 for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)))


def scan(root: Path, scan_roots: tuple[str, ...]) -> tuple[dict[str, int], dict[str, int]]:
    """Map posix path -> line count (>ADVISORY) and -> top-level def count (all)."""
    sizes: dict[str, int] = {}
    defs: dict[str, int] = {}
    for rel in scan_roots:
        base = root / rel
        if not base.is_dir():
            continue
        for p in base.rglob("*.py"):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            # Gate toolkit (check-*.py / ci-gate-*) is tooling, not product code.
            if p.name.startswith(("check-", "ci-gate")):
                continue
            relpath = p.relative_to(root).as_posix()
            n = count_lines(p)
            if n > ADVISORY_THRESHOLD:
                sizes[relpath] = n
            d = count_top_level_defs(p)
            if d > FN_DENSITY_TARGET:
                defs[relpath] = d
    return sizes, defs


def resolve_scan_roots(root: Path, explicit: tuple[str, ...]) -> tuple[str, ...]:
    """Explicit > [tool.fleet-gates].scan-roots > auto-detect (no repo defaults)."""
    if explicit:
        return explicit
    cfg = root / "pyproject.toml"
    if cfg.exists():
        try:
            import tomllib

            tool = tomllib.loads(cfg.read_text(encoding="utf-8")).get("tool", {})
        except (ImportError, OSError, ValueError):
            tool = {}
        roots = (tool.get("fleet-gates") or {}).get("scan-roots")
        if roots:
            return tuple(str(r) for r in roots)
    src = root / "src"
    if src.is_dir():
        subs = sorted(
            d.name
            for d in src.iterdir()
            if d.is_dir() and not d.name.startswith(".") and not d.name.endswith(".egg-info")
        )
        return tuple(f"src/{s}" for s in subs) or ("src",)
    top = tuple(
        sorted(
            d.name
            for d in root.iterdir()
            if d.is_dir()
            and not d.name.startswith(".")
            and d.name not in SKIP_TOP
            and d.name not in SKIP_DIRS
            and any(d.rglob("*.py"))
        )
    )
    if top:
        return top
    return ("scripts",) if (root / "scripts").is_dir() else (".",)


def load_baseline(path: Path) -> dict[str, int]:
    """Read the shrink-only file-size allowlist from JSON."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text()).get("files", {})
    except (OSError, json.JSONDecodeError):
        return {}


def write_baseline(path: Path, sizes: dict[str, int]) -> None:
    """Write the file-size allowlist to JSON."""
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
    sizes: dict[str, int],
    base: dict[str, int],
    defs: dict[str, int] | None = None,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Classify every scanned file: (failures, reports, shrinks, removals)."""
    defs = defs or {}
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
    for path, d in sorted(defs.items()):
        if path not in sizes and path not in base and d > FN_DENSITY_TARGET:
            reports.append(
                f"{path}: {d} top-level defs (new file fn-density "
                f"> {FN_DENSITY_TARGET} — one primary function per module)"
            )
    for path, recorded in sorted(base.items()):
        if path not in sizes:
            removals.append(f"{path}: recorded {recorded} — gone or <= {ADVISORY_THRESHOLD} (remove)")
    return failures, reports, shrinks, removals


def debt_report(base: dict[str, int]) -> None:
    """Print the shrink-only ledger: every allowlisted file's deficit to 200."""
    print(f"\ndebt ledger ({len(base)} allowlisted files, deficit to {FAIL_THRESHOLD}):")
    for path, recorded in sorted(base.items(), key=lambda x: -x[1]):
        print(f"  {FAIL_THRESHOLD - recorded:>5}  {path}: {recorded}")


def update_baseline(sizes: dict[str, int], old: dict[str, int], path: Path) -> int:
    """Regenerate the allowlist; only removals/shrinks are permitted."""
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
    """CLI entry: enforce the firm <=200 policy and the shrink-only allowlist."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default=DEFAULT_BASELINE)
    ap.add_argument("--scan-root", action="append", dest="scan_roots")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--debt", action="store_true", help="print the shrink-only debt ledger")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    scan_roots = resolve_scan_roots(root, tuple(args.scan_roots or ()))
    print(f"structure gate: scan roots = {', '.join(scan_roots)}")
    sizes, all_defs = scan(root, scan_roots)
    baseline_path = root / args.baseline
    old = load_baseline(baseline_path)
    if args.update_baseline:
        return update_baseline(sizes, old, baseline_path)
    if args.debt:
        debt_report(old)
        return 0
    defs = {p: n for p, n in all_defs.items() if p not in old}
    failures, reports, shrinks, removals = evaluate(sizes, old, defs)
    print(f"structure gate: {len(sizes)} files > {ADVISORY_THRESHOLD} lines (allowlisted: {len(old)})")
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
