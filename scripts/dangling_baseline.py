#!/usr/bin/env python3
"""dangling_baseline.py — the shrink-only budget for the fleet's unlanded branches.

WHY THIS IS ITS OWN MODULE
    `check-dangling-branches.py` answers three questions: what is dangling, how do we park
    it, and are we allowed to push. Those are separate jobs, and the third one arrived last
    — a baseline that turns "566 branches are dangling" from a wall into a number that may
    only go down. Mixing it into the auditor took that file to 26 functions against a limit
    of 25, and the structure gate was right to refuse it: a file whose function list reads
    as four unrelated jobs is a file nobody can hold in their head.

    Nothing here knows what a Branch is. It takes a count per repo, a path, and returns a
    verdict — which is exactly the surface a budget needs, and why the split costs nothing.

THE RULE, in one paragraph
    A baseline records what already exists. A repo absent from it must be clean; a repo
    present may not exceed its recorded count. New debt fails the push, existing debt stays
    visible and can only shrink. Without this, wiring the raw count into pre-push blocks
    every push in every repo on debt that predates the person pushing — an outage dressed
    as a gate, and the fastest way to teach a fleet to bypass hooks.

A MISSING BASELINE IS A REFUSAL
    An unreadable or absent registry returns 1, never 0. The alternative — treat it as
    empty — would grandfather every branch in the fleet on a typo'd path, which is a gate
    that silently stops gating. Measured on this fleet: `--baseline /tmp/nope.json` must
    exit 1 and say why.

VERIFIED 2026-11-12 (see check-dangling-branches.py --help for the CLI that calls these):
    baseline as recorded ..... rc=0   (561 branches grandfathered, not blocked)
    one repo over count ...... rc=1   "standards: 116 dangling (baseline 115)"
    missing baseline file .... rc=1   "no readable baseline; refusing to guess"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load_baseline(path: Path) -> dict[str, int] | None:
    """The recorded counts, or None when the file is absent or unreadable.

    None is a distinct answer from `{}`: an empty dict means "every repo must be clean",
    which is a legitimate baseline, while None means we do not know and must refuse.
    """
    try:
        loaded = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return loaded if isinstance(loaded, dict) else None


def write_baseline(counts: dict[str, int], path: Path) -> int:
    """Record the current counts as the new high-water mark."""
    path.write_text(json.dumps(dict(sorted(counts.items())), indent=1) + "\n")
    print(f"dangling baseline: {len(counts)} repo(s) -> {path}")
    return 0


def gate_against_baseline(counts: dict[str, int], path: Path, quiet: bool) -> int:
    """Fail when a repo is new or has grown past its recorded count."""
    known = load_baseline(path)
    if known is None:
        print(
            f"dangling: no readable baseline at {path}; refusing to guess",
            file=sys.stderr,
        )
        return 1
    over = [
        f"{repo}: {count} dangling (baseline {known[repo]})"
        for repo, count in counts.items()
        if repo not in known or count > known[repo]
    ]
    if over:
        print(f"dangling: {len(over)} repo(s) over baseline:", file=sys.stderr)
        for line in over[:5]:
            print(f"  {line}", file=sys.stderr)
        return 1
    if not quiet:
        print(
            f"dangling: {sum(counts.values())} branch(es) within baseline "
            f"({len(counts)} repo(s))"
        )
    return 0
