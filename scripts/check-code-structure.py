#!/usr/bin/env python3
"""check-code-structure.py — anti-god-module + anti-monkeypatch gate.

Fails the build when:
  1. A Python file exceeds MAX_FILE_LINES and is not grandfathered in the baseline.
  2. A grandfathered file GROWS past its baselined size (ratchet: shrink freely, grow = fail).
  3. Monkeypatch usage grows — per-file counts vs baseline (fleet anti-pattern:
     "NEVER monkeypatch" — the ratchet only goes DOWN).

Warnings (non-blocking): functions longer than MAX_FUNC_LINES.

Baseline: scripts/structure-baseline.json — regenerate deliberately with
`--update-baseline` when a refactor shrinks offenders.

Usage: python3 scripts/check-code-structure.py [--update-baseline] [--quiet]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASELINE = Path(__file__).resolve().parent / "structure-baseline.json"

MAX_FILE_LINES = 1500
MAX_FUNC_LINES = 150

EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "crypto-skills",
    "reference",
    "deploy",
    ".forgejo",
    ".github",
    # generated / tool output — never source of truth
    "mutants",
    "dist",
    "build",
    ".eggs",
    ".tox",
    ".nox",
    "htmlcov",
    ".next",
    ".nuxt",
    ".output",
    # vendored/upstream agent skills — not our source of truth
    ".openhands",
}
EXCLUDE_FILES = {"structure-baseline.json"}

MONKEY_RE = re.compile(r"monkeypatch\.setattr|mocker\.patch|unittest\.mock\.patch|\bmock\.patch\(")
DEF_RE = re.compile(r"^(\s*)(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)")


def iter_py():
    for p in sorted(REPO.rglob("*.py")):
        rel = p.relative_to(REPO)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if p.name in EXCLUDE_FILES:
            continue
        yield p, rel


def scan():
    files: dict[str, int] = {}
    monkey: dict[str, int] = {}
    long_funcs: list[str] = []
    for p, rel in iter_py():
        try:
            text = p.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        lines = text.splitlines()
        files[str(rel)] = len(lines)
        n = len(MONKEY_RE.findall(text))
        if n:
            monkey[str(rel)] = n
        # function length scan (indent-stack)
        stack: list[tuple[int, str, int]] = []
        for i, line in enumerate(lines):
            m = DEF_RE.match(line)
            indent = len(m.group(1)) if m else None
            while stack and (m is None or stack[-1][0] >= indent):
                d_indent, d_name, d_start = stack.pop()
                length = i - d_start
                if length > MAX_FUNC_LINES:
                    long_funcs.append(f"{rel}:{d_start + 1} {d_name} ({length} lines)")
            if m and not line.lstrip().startswith("#"):
                stack.append((indent, m.group(2), i))
        for _d_indent, d_name, d_start in stack:
            length = len(lines) - d_start
            if length > MAX_FUNC_LINES:
                long_funcs.append(f"{rel}:{d_start + 1} {d_name} ({length} lines)")
    return files, monkey, long_funcs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    files, monkey, long_funcs = scan()

    if args.update_baseline:
        BASELINE.write_text(
            json.dumps(
                {
                    "comment": "Grandfathered offenders. Files may SHRINK freely; GROWTH or NEW offenders fail the gate. monkeypatch counts only go down.",
                    "max_file_lines": MAX_FILE_LINES,
                    "max_func_lines": MAX_FUNC_LINES,
                    "files": {k: v for k, v in sorted(files.items()) if v > MAX_FILE_LINES},
                    "monkeypatch": dict(sorted(monkey.items())),
                },
                indent=2,
            )
            + "\n"
        )
        print(f"baseline updated: {BASELINE}")
        return 0

    base = (
        json.loads(BASELINE.read_text()) if BASELINE.exists() else {"files": {}, "monkeypatch": {}}
    )
    base_files = base.get("files", {})
    base_monkey = base.get("monkeypatch", {})

    failures: list[str] = []

    for f, n in sorted(files.items()):
        if n > MAX_FILE_LINES:
            if f not in base_files:
                failures.append(f"NEW god file: {f} ({n} lines > {MAX_FILE_LINES})")
            elif n > base_files[f]:
                failures.append(
                    f"god file GREW: {f} ({base_files[f]} -> {n} lines; shrink it, never grow it)"
                )

    for f, n in sorted(monkey.items()):
        if f not in base_monkey:
            failures.append(
                f"NEW monkeypatch usage: {f} ({n} occurrences) — fleet anti-pattern, use dependency injection"
            )
        elif n > base_monkey[f]:
            failures.append(f"monkeypatch usage GREW: {f} ({base_monkey[f]} -> {n})")

    if not args.quiet:
        gods = {f: n for f, n in files.items() if n > MAX_FILE_LINES}
        print(
            f"structure gate: {len(files)} files scanned | god files (grandfathered): {len(gods)}"
        )
        for f, n in sorted(gods.items(), key=lambda kv: -kv[1]):
            budget = base_files.get(f)
            print(f"  {f}: {n} lines" + (f" (budget {budget})" if budget else ""))
        if long_funcs:
            print(f"  WARN: {len(long_funcs)} function(s) > {MAX_FUNC_LINES} lines (non-blocking):")
            for lf in long_funcs[:6]:
                print(f"    {lf}")
        print(
            f"  monkeypatch total: {sum(monkey.values())} across {len(monkey)} files (ratchet: only down)"
        )

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("structure gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
