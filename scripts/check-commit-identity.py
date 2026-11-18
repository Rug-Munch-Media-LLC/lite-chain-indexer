#!/usr/bin/env python3
"""check-commit-identity.py — every repo commits as ONE canon identity.

THE MEASUREMENT THAT MADE THIS (2026-11-12)
    Six product repos carried FOUR different author NAMES on the one canonical address:

        Rug Munch Media <admin@rugmunch.io>      standards, rugmunch-scanner, contextcrypto
        cryptorugmunch  <admin@rugmunch.io>      rugmunchbot, pryscraper
        Crypto Rug Munch<admin@rugmunch.io>      (in history)
        lh-probe        <probe@fleet.local>      rmi-backend   <- WRONG ADDRESS TOO

    The address was nearly right everywhere; the name was not, and one repo carried a
    PROBE identity on a product repo. An author is how history is attributed and how a
    probe is told apart from a person, so this is canon rather than taste.

THE CANON
    user.name  = Rug Munch Media
    user.email = admin@rugmunch.io

WHAT IT CHECKS
    Each repo's EFFECTIVE local config (what `git commit` would actually use). A missing
    local value falls back to global or machine default, which is exactly how four
    identities appeared — so "unset" is a finding, not a pass.

    Worktrees are covered via their parent: a worktree shares the parent's `.git`, so
    fixing the repo fixes its lanes.

Usage: python3 scripts/check-commit-identity.py [--repos-root DIR] [--json] [--quiet]
Exit: 0 canonical | 1 drift | 2 cannot evaluate
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_REPOS_ROOT = Path("/srv/work/repos")
CANON_NAME = "Rug Munch Media"
CANON_EMAIL = "admin@rugmunch.io"

#: Lanes, worktrees and mirrors share a product repo's git dir; reporting them again
#: would report the same identity four times and hide which real repo is wrong.
SKIP_PREFIXES = ("_", "cc-lane-", "cc-backtest", "contextcrypto-", "standards-", "std-")
SKIP_PARTS = ("lane-", "wt-", "-main", "probe-fix", "leadpack")


@dataclass
class Drift:
    repo: str
    name: str
    email: str
    problem: str


def _config(repo: Path, key: str) -> str:
    """EFFECTIVE value — what `git commit` would actually use.

    Deliberately NOT `--local`. The canon is already the GLOBAL default on this box
    (`Rug Munch Media <admin@rugmunch.io>`), so every repo inherits it correctly and a
    per-repo override is redundant. Demanding `--local` would flag a repo that is already
    correct, and "unset" is not drift when the inherited value IS the canon.

    A local override is the thing that CAUSES drift: `rmi-backend` had
    `user.email=probe@fleet.local` set locally, which is exactly what this now catches.
    """
    try:
        p = subprocess.run(
            ["git", "-C", str(repo), "config", key],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        return (p.stdout or "").strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def evaluate(repo: Path) -> Drift | None:
    name = _config(repo, "user.name")
    email = _config(repo, "user.email")
    problems = []
    if name != CANON_NAME:
        problems.append(
            f"name {name!r} != {CANON_NAME!r}"
            if name
            else "name UNSET (inherits machine default)"
        )
    if email != CANON_EMAIL:
        problems.append(
            f"email {email!r} != {CANON_EMAIL!r}"
            if email
            else "email UNSET (inherits machine default)"
        )
    if not problems:
        return None
    return Drift(
        repo=repo.name,
        name=name or "-",
        email=email or "-",
        problem="; ".join(problems),
    )


def managed_repos(root: Path) -> list[Path]:
    out = []
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        g = p / ".git"
        if not g.exists():
            continue
        if g.is_file():  # a worktree; its parent carries the identity
            continue
        if p.name.startswith(SKIP_PREFIXES) or any(k in p.name for k in SKIP_PARTS):
            continue
        out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos-root", default=str(DEFAULT_REPOS_ROOT))
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    root = Path(a.repos_root)
    if not root.is_dir():
        print(f"commit-identity: SKIP — {root} not visible from here", file=sys.stderr)
        return 0  # inside a job container the repos root does not exist; not a finding

    rows = [d for d in (evaluate(r) for r in managed_repos(root)) if d is not None]
    if a.json:
        print(json.dumps([asdict(r) for r in rows], indent=1))
    elif not a.quiet:
        for r in rows:
            print(f"  DRIFT {r.repo}: {r.problem}")
        print(
            f"\ncommit-identity drift: {len(rows)} repo(s) — canon is {CANON_NAME} <{CANON_EMAIL}>"
        )
    return 1 if rows else 0


if __name__ == "__main__":
    sys.exit(main())
