#!/usr/bin/env python3
"""check-monkeypatch.py — decrease-only monkeypatch ratchet (fleet canon).

SCOPE (2026-10-25)
  * PRODUCTION: ZERO. Any monkeypatch or patch call in non-test code fails.
    Production roots are DISCOVERED, not listed: the old `PROD_ROOTS =
    ("lite_indexer", "src")` missed `api/`, so a production monkeypatch there
    was invisible.
  * TEST DEBT: `mock.patch`, `@patch(`, `patch.object`, `del sys.modules`,
    counted repo-wide, shrink-only. pytest's `monkeypatch` FIXTURE is the
    sanctioned DI seam and is NOT debt — counting its attribute access is what
    inflated lite-chain-indexer 288 -> 290 while production was already clean.
  * STRINGS AND COMMENTS ARE NOT CODE. Tokens are stripped with `tokenize`, so
    a docstring that MENTIONS ``monkeypatch.setenv`` (api/write_guard.py:38) is
    not a finding. A file that cannot be tokenized is scanned as plain text —
    never silently skipped.

Usage:
  python3 scripts/check-monkeypatch.py --repo .                 # gate
  python3 scripts/check-monkeypatch.py --repo . --update-baseline
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import tokenize
from pathlib import Path

DEFAULT_BASELINE = "scripts/monkeypatch-baseline.json"

# Test debt: real mocking. `monkeypatch.` is deliberately absent — see header.
TEST_RE = re.compile(r"mock\.patch|@patch\(|patch\.object|del sys\.modules")
# Production: the whole family, including the pytest fixture's attribute use.
PROD_RE = re.compile(r"monkeypatch\.|mock\.patch|@patch\(|patch\.object|del sys\.modules")

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
    ".wt",
    "_wt",
}
TEST_DIRS = {"tests", "test", "testing"}
TEST_NAMES = {"conftest.py"}


def is_test(rel: Path) -> bool:
    """True for test code: test dirs, test_*.py, *_test.py, conftest.py."""
    if any(part in TEST_DIRS for part in rel.parts[:-1]):
        return True
    name = rel.name
    return name in TEST_NAMES or name.startswith("test_") or name.endswith("_test.py")


def code_lines(path: Path) -> list[str]:
    """Lines with strings and comments BLANKED, keeping line numbers aligned."""
    text = path.read_text(errors="ignore")
    lines = text.splitlines()
    masked = [list(line) for line in lines]
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for tok in tokens:
            if tok.type not in (tokenize.STRING, tokenize.COMMENT):
                continue
            (srow, scol), (erow, ecol) = tok.start, tok.end
            for row in range(srow, erow + 1):
                idx = row - 1
                if idx < 0 or idx >= len(masked):
                    continue
                start = scol if row == srow else 0
                end = ecol if row == erow else len(masked[idx])
                for col in range(start, min(end, len(masked[idx]))):
                    masked[idx][col] = " "
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return lines  # never skip a file because it would not tokenize
    return ["".join(chars) for chars in masked]


def scan(root: Path) -> tuple[int, dict[str, int]]:
    total, per_file = 0, {}
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        try:
            lines = code_lines(p)
        except OSError:
            continue
        n = sum(1 for ln in lines if TEST_RE.search(ln))
        if n:
            per_file[p.relative_to(root).as_posix()] = n
            total += n
    return total, per_file


def scan_production(root: Path) -> dict[str, int]:
    per_file = {}
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        rel = p.relative_to(root)
        if is_test(rel):
            continue
        try:
            lines = code_lines(p)
        except OSError:
            continue
        n = sum(1 for ln in lines if PROD_RE.search(ln))
        if n:
            per_file[rel.as_posix()] = n
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

    print(f"monkeypatch gate: {total} test-debt occurrences (baseline {base})")
    for rel, n in sorted(per_file.items(), key=lambda x: -x[1])[:10]:
        print(f"  {n:>4}  {rel}")
    if prod:
        print("\nFAIL (zero monkeypatch in PRODUCTION):")
        for rel, n in sorted(prod.items()):
            print(f"  - {rel}: {n} (replace with a DI seam)")
        return 1
    print(f"monkeypatch gate: production clean (0); test debt: {total} (shrink-only)")
    if total > base:
        print(f"\nFAIL (monkeypatch ratchet): {base} -> {total} — decrease-only, fix don't add")
        return 1
    print("monkeypatch gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
