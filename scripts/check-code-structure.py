#!/usr/bin/env python3
"""check-code-structure.py — anti-god-module + anti-monkeypatch gate.

Fails the build when:
  1. A Python file holds more than one module's worth of functions, or a function
     nobody can follow (cognitive above the ceiling). There is NO line threshold:
     line counts were removed on 2026-11-12 because a number deciding what is worth
     looking at lets a small multi-job file pass and nags a large single-job one.
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

from structure_report import (
    Report,
    _failures,
    _load_baseline,
    _report,
    _write_baseline,
)

REPO = Path(__file__).resolve().parent.parent
BASELINE = Path(__file__).resolve().parent / "structure-baseline.json"


def _canon_thresholds() -> tuple[int, int]:
    """(max_file_lines, max_func_lines) from the ONE canonical manifest.

    NO GATE HARDCODES A NUMBER (2026-11-12). This file used to say 1500/150 while
    `quality-thresholds.json` said 200/80 and five sibling repos had drifted to
    three different values between them. A 1500-line limit is not enforcement: it
    passes everything short of a genuinely unmanageable file, so the gate was
    decorative exactly where it mattered.

    The manifest's own policy is functional-first - "line counts are a smell
    detector that surfaces split candidates" - so these numbers are a REPORTING
    threshold, not a verdict. `_is_structurally_one_unit` decides, and a long file
    holding one job passes.

    A missing manifest is fatal rather than defaulted: silently falling back to a
    number would recreate the drift this reads canon to prevent.
    """
    manifest = REPO.parent / "standards" / "quality-thresholds.json"
    if not manifest.is_file():
        manifest = REPO / "quality-thresholds.json"
    data = json.loads(manifest.read_text(encoding="utf-8"))
    limits = data["thresholds"]
    return int(limits["max_file_lines_hard"]), int(limits["max_func_lines_hard"])


MAX_FILE_LINES, MAX_FUNC_LINES = _canon_thresholds()

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

MONKEY_RE = re.compile(
    r"monkeypatch\.setattr|mocker\.patch|unittest\.mock\.patch|\bmock\.patch\("
)
DEF_RE = re.compile(r"^(\s*)(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)")


def is_test_file(rel: Path) -> bool:
    """A test file, by path or by name.

    INLINED so the gate is self-contained and installable in one piece (2026-11-12).
    It used to delegate to `size_exemptions.is_test`, which made the gate a
    four-file installation - and a repo that received three of them crashed on
    import. One definition, here, is the whole answer: a test module is a test file
    or lives in a tests directory.
    """
    text = str(rel).replace("\\", "/")
    name = text.rsplit("/", 1)[-1]
    return name.startswith("test_") or "/tests/" in text or text.startswith("tests/")


def iter_py():
    for p in sorted(REPO.rglob("*.py")):
        rel = p.relative_to(REPO)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if p.name in EXCLUDE_FILES:
            continue
        yield p, rel


def count_monkeypatch(rel: Path, text: str) -> int:
    """Production monkeypatch occurrences in one file; always 0 for a test file.

    MONKEYPATCH IS A PRODUCTION RULE, NOT A TEST RULE (2026-11-12).
    The fleet anti-pattern is PATCHING A SYMBOL OUT OF LIVE CODE instead of fixing
    a root cause or introducing a DI seam. Doing that inside a TEST is pytest's own
    sanctioned fixture — the opposite of the sin. Counting it everywhere made this
    gate fail on its own test files with a baseline of zero, so every test that
    legitimately used the fixture read as "new monkeypatch usage".
    """
    if is_test_file(rel):
        return 0
    return len(MONKEY_RE.findall(text))


def _spans(source: str) -> list[tuple[str, int, int]]:
    """Every function in a source file as (name, start_line, end_line), 1-based.

    AN AST, NOT AN INDENT STACK. The previous implementation walked lines and popped
    a span whenever it met a non-`def` line:

        while stack and (m is None or stack[-1][0] >= indent):

    `m is None` is true for every ordinary statement, so the first body line closed
    the function it belonged to. A 200-line function measured as 1 line and this
    check has therefore NEVER reported anything — verified by running the old code
    against a synthetic 200-line function and getting []. A gate that cannot fire is
    worse than no gate: it is a green light with a rule printed on it.

    `ast.walk` also gets decorators, async defs, nested defs and one-liners right
    without special cases. Syntax-error files are the caller's problem (quality_metrics
    reports them); here they simply yield no spans.
    """
    import ast

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    out: list[tuple[str, int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            end = getattr(node, "end_lineno", node.lineno)
            out.append((node.name, node.lineno, end))
    return out


def long_functions(rel: Path, source: str) -> list[str]:
    """Functions in one file longer than MAX_FUNC_LINES, as report lines."""
    return [
        f"{rel}:{start} {name} ({end - start + 1} lines)"
        for name, start, end in _spans(source)
        if end - start + 1 > MAX_FUNC_LINES
    ]


def scan():
    """File sizes, monkeypatch counts and long functions across the repo."""
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
        n = count_monkeypatch(rel, text)
        if n:
            monkey[str(rel)] = n
        long_funcs.extend(long_functions(rel, text))
    return files, monkey, long_funcs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    files, monkey, long_funcs = scan()

    if args.update_baseline:
        return _write_baseline(BASELINE, MAX_FILE_LINES, MAX_FUNC_LINES, files, monkey)

    base = _load_baseline(BASELINE)
    failures = _failures(files, monkey, base)

    if not args.quiet:
        _report(
            Report(
                files=files,
                monkey=monkey,
                long_funcs=long_funcs,
                base_files=base.get("files", {}),
                max_file=MAX_FILE_LINES,
                max_func=MAX_FUNC_LINES,
            )
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
