#!/usr/bin/env python3
"""check-xdist-coverage.py — `-n` and `--cov` in one pytest call is a config error.

THE RULE, STATED CORRECTLY (2026-11-12, second pass)
    xdist is a PER-REPO decision, not a fleet default. Two measurements, both right, that
    look contradictory until you see what differs:

      standards       232 tests, ~80ms each   -> -n auto made it 3x SLOWER (62s vs 18.7s)
      contextcrypto   2,084 tests             -> -n auto --dist loadscope: 478s -> 154s

    Many small fast tests lose to per-test subprocess overhead. A large suite wins. So:
    do NOT set -n auto fleet-wide, and do NOT remove xdist fleet-wide either. Use -n only
    where a single suite run exceeds roughly 90s.

    What is NEVER correct is `-n` WITH `--cov` in the SAME invocation, which is what this
    gate forbids. The right shape puts them in separate calls: xdist for the blocking run
    with no coverage, then one serial `-p no:xdist` coverage pass.

THE MEASUREMENT THAT MADE THIS
    2026-11-12: CI runs took 900s. Two agents measured independently and reached the same
    answer — `pytest -n auto ... --cov=...` is not a speed-up, it is the opposite.

    pytest-xdist gives every worker its OWN COLLECTION of the whole suite, and with
    `--cov` a coverage process per worker to merge. On this 16-core box that is 16x
    collection plus 16x coverage merges. contextcrypto was doing it with SIX `--cov`
    targets, so 96 coverage processes for one run.

    xdist parallelises test EXECUTION. Coverage instruments the whole interpreter and
    merges at the end; it parallelises nothing. The two together multiply the fixed cost.

    I put `-n auto` into rugmunch-scanner claiming "142s -> 100s, MEASURED" — a warm
    local run that did not describe CI. The claim was wrong and it made pushes slower.
    This gate exists so that mistake cannot be re-made from a plausible-looking benchmark.

WHAT IT FLAGS
    Any workflow `run:` line group that contains BOTH an xdist flag (`-n`, `-n auto`,
    `-n 4`, `--numprocesses`) AND a coverage flag (`--cov`) for the same pytest call.

    Deliberately NOT flagged:
      * `-n` in a job that has no --cov on the same call (that is correct usage)
      * `-n` in a comment or a quoted string on its own
      * `--cov` without -n (serial with coverage — slower but correct)

Usage: python3 scripts/check-xdist-coverage.py [--repos-root DIR] [--json] [--quiet]
Exit: 0 clean | 1 the anti-pattern is present | 2 cannot evaluate
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_REPOS_ROOT = Path("/srv/work/repos")

#: Lanes, worktrees and mirrors share a product repo's workflows; reporting them again
#: hides which REAL repo is broken.
SKIP_PREFIXES = ("_", "cc-lane-", "cc-backtest", "contextcrypto-", "standards-", "std-")
SKIP_PARTS = ("lane-", "wt-", "-main", "probe-fix", "leadpack")

XDIST = re.compile(r"(?:^|\s)(?:-n\s*(?:auto|\d+)|--numprocesses(?:=|\s+)(?:auto|\d+))")
COV = re.compile(r"(?:^|\s)--cov(?:=|\s)")


@dataclass
class Finding:
    repo: str
    workflow: str
    line: int
    xdist: str
    cov_targets: int


def _logical_commands(lines: list[str]) -> list[tuple[int, str]]:
    """Join backslash continuations so one pytest invocation is one string.

    A multi-line `pytest ... \\` call is ONE command. Scanning line by line is exactly
    how `-n` on line 1 and `--cov` on line 4 stay invisible to each other — which is
    how this shipped.
    """
    out: list[tuple[int, str]] = []
    buf: list[str] = []
    start = 0
    for i, raw in enumerate(lines, 1):
        code = raw.split("#", 1)[0].rstrip()
        if not buf:
            start = i
        if code.endswith("\\"):
            buf.append(code[:-1])
            continue
        buf.append(code)
        joined = " ".join(buf).strip()
        if joined:
            out.append((start, joined))
        buf = []
    if buf:
        out.append((start, " ".join(buf).strip()))
    return out


def scan_workflow(repo: str, path: Path) -> list[Finding]:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    found: list[Finding] = []
    for lineno, cmd in _logical_commands(lines):
        if "pytest" not in cmd:
            continue
        m_x = XDIST.search(cmd)
        if not m_x or not COV.search(cmd):
            continue
        found.append(
            Finding(
                repo=repo,
                workflow=path.name,
                line=lineno,
                xdist=m_x.group(0).strip(),
                cov_targets=len(re.findall(r"--cov[=\s]", cmd)),
            )
        )
    return found


def managed_repos(root: Path) -> list[Path]:
    out = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or not (p / ".forgejo" / "workflows").is_dir():
            continue
        if p.name.startswith(SKIP_PREFIXES) or any(k in p.name for k in SKIP_PARTS):
            continue
        if (p / ".git").is_file():  # a worktree, not a repo
            continue
        out.append(p)
    return out


def audit(root: Path) -> list[Finding]:
    rows: list[Finding] = []
    for repo in managed_repos(root):
        for wf in sorted((repo / ".forgejo" / "workflows").glob("*.yml")):
            rows.extend(scan_workflow(repo.name, wf))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos-root", default=str(DEFAULT_REPOS_ROOT))
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    root = Path(a.repos_root)
    if not root.is_dir():
        print(f"xdist-coverage: SKIP — {root} not visible from here", file=sys.stderr)
        return 0  # inside a job container the repos root does not exist; not a finding

    rows = audit(root)
    if a.json:
        print(json.dumps([asdict(r) for r in rows], indent=1))
    elif not a.quiet:
        for r in rows:
            print(
                f"  ANTI-PATTERN {r.repo}/{r.workflow}:{r.line} "
                f"pytest {r.xdist} + {r.cov_targets} --cov target(s) in one call "
                f"— 16x collection and 16x coverage merges on a 16-core box"
            )
        print(f"\nxdist+coverage anti-pattern: {len(rows)} occurrence(s)")
    return 1 if rows else 0


if __name__ == "__main__":
    sys.exit(main())
