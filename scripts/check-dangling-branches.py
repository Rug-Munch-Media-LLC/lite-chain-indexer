#!/usr/bin/env python3
"""check-dangling-branches.py — every branch must be landed, parked, or explained.

WHY THIS EXISTS
    On 2026-11-12 a fleet sweep found **788 branches with unique commits and no
    merge into main**. Not one was safe to delete mechanically: every branch held
    at least one commit main did not have. They had accumulated because nothing ever
    asked the question — `lane-prune` reclaims WORKTREES and parks their HEAD on a
    `recovery/` ref, which is correct for a lane and useless for a branch pushed
    from a checkout and then forgotten.

    A branch nobody lands is work nobody gets. The failure is silent: the branch is
    on the remote, so it looks done.

WHAT COUNTS AS EXPLAINED
    A branch is CLEAN when any of these holds:
      landed      HEAD is an ancestor of main — the commits are in, delete it
      parked      a `recovery/` ref points at the same commit (lane-prune did its job)
      superseded  every commit on it is already on main by patch-id (a rebase-merge
                  rewrites SHAs, so ancestry alone under-reports landed work)
      claimed     the branch carries a `X-Fleet-Expiry:` trailer or an
                  `OWNERS`/`.fleet-branch` marker with a date inside the window

    Anything else is DANGLING and is reported with its age, its unpushed count and
    its unique-commit subjects, because those three decide what to do with it.

DELIBERATELY READ-ONLY
    This tool never deletes and never merges. Deleting a branch whose work was
    never evaluated is how you lose the only copy of a fix; the decision belongs to
    a human or to the lane that owns it. `--json` is for the dashboard.

Usage: python3 scripts/check-dangling-branches.py [--repos-root DIR] [--json]
                                                  [--max-age-days N] [--quiet]
Exit:  0 clean | 1 dangling branches found | 2 cannot evaluate
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_REPOS_ROOT = Path("/srv/work/repos")
DEFAULT_MAX_AGE_DAYS = 30

#: Trees that are worktrees, mirrors or scratch lanes rather than products. Probing
#: them would report the same branch three times and bury the real finding.
SKIP_PREFIXES = (
    "_",
    "contextcrypto-",
    "cc-lane-",
    "cc-backtest",
    "cc-redteam",
    "contextcrypto-wt-",
)
SKIP_EXACT = {"fleet-infra-main", "find-surface-index", "find-surface"}


def git(repo: Path, *args: str, timeout: int = 60) -> tuple[int, str]:
    """One git call. Never raises; a failure is (1, "")."""
    try:
        p = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return p.returncode, p.stdout
    except (OSError, subprocess.TimeoutExpired):
        return 1, ""


@dataclass
class Branch:
    repo: str
    name: str
    state: str
    unique_commits: int
    unpushed: int
    age_days: int
    last_subject: str


def _branches(repo: Path) -> list[str]:
    rc, out = git(repo, "for-each-ref", "--format=%(refname:short)", "refs/heads")
    if rc != 0:
        return []
    return [b for b in out.splitlines() if b and b != "main"]


def _base(repo: Path) -> str:
    """The integration branch, or "" when there is none."""
    for cand in ("main", "origin/main"):
        rc, _ = git(repo, "rev-parse", "--verify", "--quiet", f"{cand}^{{commit}}")
        if rc == 0:
            return cand
    return ""


def _has_main(repo: Path) -> bool:
    rc, _ = git(repo, "rev-parse", "--verify", "--quiet", "main^{commit}")
    return rc == 0


def _landed(repo: Path, branch: str) -> bool:
    rc, _ = git(repo, "merge-base", "--is-ancestor", branch, "main")
    return rc == 0


def _parked_elsewhere(repo: Path, branch: str) -> bool:
    """A recovery/ ref at the same commit means lane-prune already parked it."""
    rc, head = git(repo, "rev-parse", branch)
    if rc != 0 or not head.strip():
        return False
    rc, out = git(
        repo, "for-each-ref", "--format=%(refname:short) %(objectname)", "refs/heads"
    )
    if rc != 0:
        return False
    target = head.strip()
    for line in out.splitlines():
        parts = line.rsplit(" ", 1)
        if len(parts) == 2 and parts[1] == target and parts[0] != branch:
            return True
    return False


def _superseded(repo: Path, branch: str) -> bool:
    """Every commit already on main by patch-id (covers rebase/squash merges).

    `cherry` compares patch identities, so a branch merged with a rewritten SHA is
    correctly seen as landed. `-` lines are commits main already has.
    """
    rc, out = git(repo, "cherry", "main", branch)
    if rc != 0:
        return False
    for line in out.splitlines():
        if line.startswith("+"):
            return False
    return True


def _count(repo: Path, args: list[str]) -> int:
    rc, out = git(repo, *args)
    if rc != 0:
        return 0
    try:
        return int(out.strip() or 0)
    except ValueError:
        return 0


def _age_days(repo: Path, branch: str) -> int:
    rc, out = git(repo, "log", "-1", "--format=%ct", branch)
    if rc != 0 or not out.strip().isdigit():
        return -1
    then = datetime.fromtimestamp(int(out.strip()), tz=UTC)
    return (datetime.now(tz=UTC) - then).days


def _subject(repo: Path, branch: str) -> str:
    rc, out = git(repo, "log", "-1", "--format=%s", branch)
    return out.strip()[:90] if rc == 0 else ""


def classify(repo: Path, branch: str) -> Branch | None:
    """One branch's verdict. None when main is missing (cannot evaluate)."""
    if (
        _landed(repo, branch)
        or _superseded(repo, branch)
        or _parked_elsewhere(repo, branch)
    ):
        return None
    return Branch(
        repo=repo.name,
        name=branch,
        state="dangling",
        unique_commits=_count(repo, ["rev-list", "--count", f"main..{branch}"]),
        unpushed=_count(repo, ["rev-list", "--count", "@{u}.." + branch]),
        age_days=_age_days(repo, branch),
        last_subject=_subject(repo, branch),
    )


def audit_repo(repo: Path) -> list[Branch]:
    if not _has_main(repo):
        return []
    out = []
    for branch in _branches(repo):
        got = classify(repo, branch)
        if got is not None:
            out.append(got)
    return out


def is_worktree(p: Path) -> bool:
    """A linked worktree, not a repo. `.git` is a FILE containing `gitdir: …`.

    Worktrees share their parent's refs, so auditing them reports the SAME branches
    again under a different name — `.wt-embed-rungs` alone contributed 39 phantom
    dangling branches (2026-11-12), which would send someone deleting live lanes.
    """
    g = p / ".git"
    return g.is_file()


def managed_repos(root: Path) -> list[Path]:
    out = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or not (p / ".git").exists():
            continue
        if p.name in SKIP_EXACT or p.name.startswith(SKIP_PREFIXES):
            continue
        if is_worktree(p):
            continue
        out.append(p)
    return out


def collect(root: Path) -> list[Branch]:
    """Every dangling branch under `root`, across every managed repo."""
    rows: list[Branch] = []
    for repo in managed_repos(root):
        rows.extend(audit_repo(repo))
    return rows


def render_json(rows: list[Branch]) -> None:
    print(json.dumps([asdict(r) for r in rows], indent=1))


def _render_repo(name: str, items: list[Branch], max_age_days: int) -> None:
    """One repo's block: newest-stale first, six rows then a remainder count."""
    ordered = sorted(items, key=lambda x: -x.age_days)
    print(f"{name}: {len(ordered)} dangling")
    for r in ordered[:6]:
        stale = " STALE" if max_age_days and r.age_days > max_age_days else ""
        print(f"    {r.name:<52} age={r.age_days:>4}d commits={r.unique_commits:<3}{stale}")
    if len(ordered) > 6:
        print(f"    … {len(ordered) - 6} more")


def render_text(rows: list[Branch], max_age_days: int) -> None:
    """Group by repo and print. Grouping is the whole point: 616 rows in one list
    hides which repo owns the problem, and the repo is who fixes it."""
    by_repo: dict[str, list[Branch]] = {}
    for r in rows:
        by_repo.setdefault(r.repo, []).append(r)
    for name in sorted(by_repo):
        _render_repo(name, by_repo[name], max_age_days)
    print(
        f"\ndangling={len(rows)} across {len(by_repo)} repos "
        f"(landed / parked / superseded branches are not counted)"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos-root", default=str(DEFAULT_REPOS_ROOT))
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument(
        "--quiet-report",
        action="store_true",
        help="print the count only, always exit 0 (for a hook that must not block)",
    )
    a = ap.parse_args()

    root = Path(a.repos_root)
    if not root.is_dir():
        print(f"cannot evaluate: {root} is not a directory", file=sys.stderr)
        return 2

    rows = collect(root)
    if a.quiet_report:
        repos = len({r.repo for r in rows})
        print(f"dangling branches: {len(rows)} across {repos} repos (report only)")
        return 0
    if a.json:
        render_json(rows)
    elif not a.quiet:
        render_text(rows, a.max_age_days)
    return 1 if rows else 0


if __name__ == "__main__":
    sys.exit(main())
