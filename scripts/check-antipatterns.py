#!/usr/bin/env python3
"""check-antipatterns.py — per-repo anti-pattern ratchet gate (self-contained).

Fails CI when any rule's count exceeds the committed baseline. Counts may only
go DOWN (--update-baseline); RAISING one requires --justify "<why>", which is
recorded in the baseline file, so drift is auditable in `git log`.

Every failing rule prints its offending file:line (top N via --top) — a count
you cannot act on gets ignored, so the gate always names the sites.

Rules: monkeypatch, bare-except (typed except that neither logs nor re-raises),
fstring-logging, sync-io-async, eval-exec, hardcoded-secret, god-file (>500),
long-function (>80), too-many-params (>5), wildcard-import, mutable-default,
slop-markers, ts-ignore.

Usage:
  python3 check-antipatterns.py --repo .                       # gate
  python3 check-antipatterns.py --repo . --update-baseline     # shrink only
  python3 check-antipatterns.py --repo . --update-baseline \
      --justify "measured reason the count grew"               # raise, recorded
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

SKIP_DIRS = frozenset(
    {
        ".git",
        ".openhands",
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
)

MONKEY_RE = re.compile(r"monkeypatch\.|mock\.patch|@patch\(|patch\.object|del sys\.modules")
SLOP_RE = re.compile(r"#\s*(HACK|WORKAROUND|TEMP|XXX|!!!|FIX ME|DO NOT SHIP)\b", re.I)
FSTRING_LOG_RE = re.compile(r"log(?:ger)?\.(?:debug|info|warning|error|critical)\(\s*f[\"']")
WILDCARD_RE = re.compile(r"^\s*from\s+[\w.]+\s+import\s+\*")
EVAL_RE = re.compile(r"(?<![\w.])eval\(|(?<![\w.])exec\(")
TS_IGNORE_RE = re.compile(r"@ts-ignore|@ts-nocheck|:\s*any\b|as any\b")
SECRET_RE = re.compile(r"(?i)\b(api[_-]?key|secret|password|private[_-]?key)\b\s*[:=]\s*[\"'][A-Za-z0-9_\-]{16,}[\"']")
SYNC_IO_RE = re.compile(r"\b(requests\.(get|post|put|delete)|urllib\.request|time\.sleep)\(")
LOGGING_RE = re.compile(r"log(?:ger)?\.(?:debug|info|warning|error|critical|exception)\(|raise\b|logging\.")
LINE_RULES = (
    ("monkeypatch", MONKEY_RE),
    ("slop-markers", SLOP_RE),
    ("fstring-logging", FSTRING_LOG_RE),
    ("wildcard-import", WILDCARD_RE),
    ("eval-exec", EVAL_RE),
    ("hardcoded-secret", SECRET_RE),
)

Findings = tuple[dict[str, int], dict[str, list[str]]]


def _note(counts: dict[str, int], hits: dict[str, list[str]], rule: str, site: str) -> None:
    """Record one finding for rule at site (repo-relative path:line)."""
    counts[rule] = counts.get(rule, 0) + 1
    hits.setdefault(rule, []).append(site)


def _scan_signature(node: ast.AST, rel: str, found: Findings) -> None:
    """Check one function definition for length, arity and mutable defaults."""
    counts, hits = found
    line = node.lineno
    if (node.end_lineno or line) - line > 80:
        _note(counts, hits, "long-function", f"{rel}:{line}")
    args = node.args
    if len(args.args) + len(args.posonlyargs) + len(args.kwonlyargs) > 5:
        _note(counts, hits, "too-many-params", f"{rel}:{line}")
    if any(isinstance(d, (ast.List, ast.Dict, ast.Set)) for d in args.defaults + args.kw_defaults):
        _note(counts, hits, "mutable-default", f"{rel}:{line}")


def _scan_async_body(node: ast.AST, src: str, rel: str, found: Findings) -> None:
    """Flag blocking IO calls inside an async function body."""
    if not isinstance(node, ast.AsyncFunctionDef):
        return
    if SYNC_IO_RE.search(ast.get_source_segment(src, node) or ""):
        _note(found[0], found[1], "sync-io-async", f"{rel}:{node.lineno}")


def _scan_except(node: ast.ExceptHandler, src: str, rel: str, found: Findings) -> None:
    """Check one except block: bare, or typed without logging/re-raise."""
    segment = ast.get_source_segment(src, node) or ""
    if node.type is None or (not LOGGING_RE.search(segment) and "raise" not in segment):
        _note(found[0], found[1], "bare-except", f"{rel}:{node.lineno}")


def _scan_line(line: str, rel: str, lineno: int, found: Findings) -> None:
    """Apply the per-line regex rules to one source line."""
    for rule, pattern in LINE_RULES:
        if not pattern.search(line):
            continue
        if rule == "eval-exec" and "ast.literal_eval" in line:
            continue
        if rule == "hardcoded-secret" and ("os.environ" in line or "getenv" in line):
            continue
        _note(found[0], found[1], rule, f"{rel}:{lineno}")


def scan_python(path: Path, rel: str, found: Findings) -> None:
    """Scan one Python file: AST rules first, then the per-line regex rules."""
    try:
        src = path.read_text(errors="ignore")
        tree = ast.parse(src)
    except (SyntaxError, UnicodeDecodeError, OSError):
        return
    lines = src.splitlines()
    if len(lines) > 500:
        _note(found[0], found[1], "god-file", f"{rel}:1")
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            _scan_signature(node, rel, found)
            _scan_async_body(node, src, rel, found)
        elif isinstance(node, ast.ExceptHandler):
            _scan_except(node, src, rel, found)
    for lineno, line in enumerate(lines, 1):
        _scan_line(line, rel, lineno, found)


def scan_ts(path: Path, rel: str, found: Findings) -> None:
    """Check one TS/JS file: god-file size and type-escape markers."""
    try:
        src = path.read_text(errors="ignore")
    except OSError:
        return
    if len(src.splitlines()) > 500:
        _note(found[0], found[1], "god-file", f"{rel}:1")
    for lineno, line in enumerate(src.splitlines(), 1):
        if TS_IGNORE_RE.search(line):
            _note(found[0], found[1], "ts-ignore", f"{rel}:{lineno}")


def scan(root: Path) -> Findings:
    """Scan the repo tree, returning per-rule counts and their file:line sites."""
    found: Findings = ({}, {})
    self_path = Path(__file__).resolve()
    for pattern in ("*.py", "*.ts", "*.tsx"):
        for path in root.rglob(pattern):
            if any(part in SKIP_DIRS for part in path.parts) or path.resolve() == self_path:
                continue
            rel = str(path.relative_to(root))
            if pattern == "*.py":
                scan_python(path, rel, found)
            else:
                scan_ts(path, rel, found)
    return found


def _load(path: Path) -> dict:
    """Read a baseline file, tolerating absence and malformed content."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _write_baseline(path: Path, counts: dict[str, int], grown: dict, justify: str) -> None:
    """Persist counts; record any raise with its justification for audit."""
    payload: dict = {"rules": counts, "total": sum(counts.values())}
    if grown:
        payload["raised"] = {rule: list(delta) for rule, delta in grown.items()}
        payload["justification"] = justify
    path.write_text(json.dumps(payload, indent=1) + "\n")


def _report(counts: dict[str, int], hits: dict[str, list[str]], baseline: dict, top: int) -> int:
    """Print the counts table, then the offenders behind every rule over baseline."""
    print(f"antipattern gate: {sum(counts.values())} findings across {len(counts)} rules")
    for rule, n in sorted(counts.items(), key=lambda x: -x[1]):
        mark = "  " if n <= baseline.get(rule, 0) else "!!"
        print(f"  {mark} {rule:<18} {n:>5} (baseline {baseline.get(rule, 0)})")
    grown = sorted((r for r, n in counts.items() if n > baseline.get(r, 0)), key=lambda r: -counts[r])
    if not grown:
        print("antipattern gate: PASS")
        return 0
    print("\nFAIL (antipattern ratchet):")
    for rule in grown:
        print(f"  - {rule}: {baseline.get(rule, 0)} -> {counts[rule]} (ratchet: fix, don't add)")
        sites = hits.get(rule, [])
        for site in sites[:top]:
            print(f"        {site}")
        if len(sites) > top:
            print(f"        ... {len(sites) - top} more (--top to widen)")
    return 1


def _update(path: Path, counts: dict[str, int], old: dict, justify: str) -> int:
    """Write the baseline; refuse to raise a rule unless --justify explains it."""
    grown = {r: (old.get(r, 0), n) for r, n in counts.items() if n > old.get(r, 0)}
    if grown and path.exists() and not justify:
        print("REFUSING to raise the baseline without --justify (ratchet is shrink-only):")
        for rule, (before, after) in grown.items():
            print(f"  {rule}: {before} -> {after}")
        return 1
    _write_baseline(path, counts, grown, justify)
    print(f"baseline updated: {path} (total {sum(counts.values())})")
    for rule, (before, after) in grown.items():
        print(f"  RAISED {rule}: {before} -> {after} - {justify}")
    return 0


def main() -> int:
    """CLI entry: scan, then gate against the ratchet baseline or update it."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default="scripts/antipattern-baseline.json")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--justify", default="", help="why a rule count legitimately grows")
    ap.add_argument("--top", type=int, default=10, help="offender lines printed per rule")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    root = Path(args.repo).resolve()
    counts, hits = scan(root)
    baseline_path = root / args.baseline
    old = _load(baseline_path).get("rules", {})
    if args.update_baseline:
        return _update(baseline_path, counts, old, args.justify)
    if args.quiet:
        return 0 if all(n <= old.get(r, 0) for r, n in counts.items()) else 1
    return _report(counts, hits, old, args.top)


if __name__ == "__main__":
    sys.exit(main())
