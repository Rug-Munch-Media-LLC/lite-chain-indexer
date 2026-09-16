#!/usr/bin/env python3
"""check-antipatterns.py — per-repo anti-pattern ratchet gate (self-contained).

Scans THIS repo for the house anti-pattern list and fails CI when any rule's
count exceeds the committed baseline. Ratchet semantics: counts may only go
DOWN; lower them deliberately with --update-baseline.

Rules: monkeypatch, bare-except, fstring-logging, sync-io-async, eval-exec,
god-file (>500 — MODULARITY.md), long-function (>80), too-many-params (>5),
wildcard-import, mutable-default, slop-markers, ts-ignore.

Usage:
  python3 check-antipatterns.py --repo .                     # gate
  python3 check-antipatterns.py --repo . --update-baseline   # lower the bar (shrink only)
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

SKIP_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "dist",
    "build",
    ".next",
    ".nuxt",
    "coverage",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "site-packages",
    "mutants",
    ".tox",
    ".nox",
    "htmlcov",
    ".output",
    "crypto-skills",
}

MONKEY_RE = re.compile(r"monkeypatch\.|mock\.patch|@patch\(|patch\.object|del sys\.modules")
SLOP_RE = re.compile(r"#\s*(HACK|WORKAROUND|TEMP|XXX|!!!|FIX ME|DO NOT SHIP)\b", re.I)
FSTRING_LOG_RE = re.compile(r"log(?:ger)?\.(?:debug|info|warning|error|critical)\(\s*f[\"']")
WILDCARD_RE = re.compile(r"^\s*from\s+[\w.]+\s+import\s+\*", re.M)
EVAL_RE = re.compile(r"(?<![\w.])eval\(|(?<![\w.])exec\(")
TS_IGNORE_RE = re.compile(r"@ts-ignore|@ts-nocheck|:\s*any\b|as any\b")
SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|secret|password|private[_-]?key)\b\s*[:=]\s*[\"'][A-Za-z0-9_\-]{16,}[\"']"
)
SYNC_IO_RE = re.compile(r"\b(requests\.(get|post|put|delete)|urllib\.request|time\.sleep)\(")
LOGGING_RE = re.compile(
    r"log(?:ger)?\.(debug|info|warning|error|critical|exception)\(|raise\b|logging\."
)


def scan_python(path: Path, counts: dict[str, int]) -> None:
    try:
        src = path.read_text(errors="ignore")
        tree = ast.parse(src)
    except (SyntaxError, UnicodeDecodeError, OSError):
        return
    lines = src.splitlines()
    if len(lines) > 500:
        counts["god-file"] = counts.get("god-file", 0) + 1
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            n = (node.end_lineno or node.lineno) - node.lineno
            if n > 80:
                counts["long-function"] = counts.get("long-function", 0) + 1
            args = node.args
            total = len(args.args) + len(args.posonlyargs) + len(args.kwonlyargs)
            if total > 5:
                counts["too-many-params"] = counts.get("too-many-params", 0) + 1
            for d in args.defaults + args.kw_defaults:
                if isinstance(d, (ast.List, ast.Dict, ast.Set)):
                    counts["mutable-default"] = counts.get("mutable-default", 0) + 1
            if isinstance(node, ast.AsyncFunctionDef):
                seg = ast.get_source_segment(src, node) or ""
                if SYNC_IO_RE.search(seg):
                    counts["sync-io-async"] = counts.get("sync-io-async", 0) + 1
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                counts["bare-except"] = counts.get("bare-except", 0) + 1
            else:
                seg = ast.get_source_segment(src, node) or ""
                if not LOGGING_RE.search(seg) and "raise" not in seg:
                    counts["bare-except"] = counts.get("bare-except", 0) + 1
    for ln in lines:
        if MONKEY_RE.search(ln):
            counts["monkeypatch"] = counts.get("monkeypatch", 0) + 1
        if SLOP_RE.search(ln):
            counts["slop-markers"] = counts.get("slop-markers", 0) + 1
        if FSTRING_LOG_RE.search(ln):
            counts["fstring-logging"] = counts.get("fstring-logging", 0) + 1
        if WILDCARD_RE.search(ln):
            counts["wildcard-import"] = counts.get("wildcard-import", 0) + 1
        if EVAL_RE.search(ln) and "ast.literal_eval" not in ln:
            counts["eval-exec"] = counts.get("eval-exec", 0) + 1
        if SECRET_RE.search(ln) and "os.environ" not in ln and "getenv" not in ln:
            counts["hardcoded-secret"] = counts.get("hardcoded-secret", 0) + 1


def scan_ts(path: Path, counts: dict[str, int]) -> None:
    try:
        src = path.read_text(errors="ignore")
    except OSError:
        return
    if len(src.splitlines()) > 500:
        counts["god-file"] = counts.get("god-file", 0) + 1
    for ln in src.splitlines():
        if TS_IGNORE_RE.search(ln):
            counts["ts-ignore"] = counts.get("ts-ignore", 0) + 1


def scan(root: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        scan_python(p, counts)
    for ext in ("*.ts", "*.tsx"):
        for p in root.rglob(ext):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            scan_ts(p, counts)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default="scripts/antipattern-baseline.json")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    counts = scan(root)
    total = sum(counts.values())
    baseline_path = root / args.baseline

    if args.update_baseline:
        old = {}
        if baseline_path.exists():
            try:
                old = json.loads(baseline_path.read_text()).get("rules", {})
            except Exception:  # noqa: BLE001
                old = {}
        grown = {r: (old.get(r, 0), n) for r, n in counts.items() if n > old.get(r, 0)}
        if grown and baseline_path.exists():
            print("REFUSING to raise the baseline (ratchet is shrink-only):")
            for r, (o, n) in grown.items():
                print(f"  {r}: {o} -> {n}")
            return 1
        baseline_path.write_text(json.dumps({"rules": counts, "total": total}, indent=1) + "\n")
        print(f"baseline updated: {baseline_path} (total {total})")
        return 0

    baseline = {}
    if baseline_path.exists():
        try:
            baseline = json.loads(baseline_path.read_text()).get("rules", {})
        except Exception:  # noqa: BLE001
            baseline = {}

    failures = []
    for rule, n in sorted(counts.items()):
        base = baseline.get(rule, 0)
        if n > base:
            failures.append(f"{rule}: {base} -> {n} (ratchet: fix, don't add)")

    if not args.quiet:
        print(f"antipattern gate: {total} findings across {len(counts)} rules")
        for rule, n in sorted(counts.items(), key=lambda x: -x[1]):
            mark = "  " if n <= baseline.get(rule, 0) else "!!"
            print(f"  {mark} {rule:<18} {n:>5} (baseline {baseline.get(rule, 0)})")
    if failures:
        print("\nFAIL (antipattern ratchet):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("antipattern gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
