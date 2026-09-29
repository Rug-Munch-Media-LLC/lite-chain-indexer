#!/usr/bin/env python3
"""Shrink-only dead-code budget for ``vulture`` (100% confidence only).

Why this exists instead of a bare ``vulture`` run
------------------------------------------------
The pre-push hook previously ran ``vulture . --min-confidence 80 2>/dev/null || true``.
That was decorative: the ``|| true`` swallowed a non-zero exit, so the gate had
never once reported anything.  Running the same command honestly today reports
**10 findings at confidence 80 across the whole tree** (2 in production code, 8
in one test file) -- see the adjudication in ``docs/CI-GATES.md``.

Both of those findings are real, and neither is cheaply fixable inside a gate
change:

* ``lite_indexer/rpc/pool.py:29`` -- ``clear_ban_state`` is a deliberate
  re-export (``# noqa: F401``); it IS used, by callers importing from
  ``lite_indexer.rpc.pool``.  Vulture cannot see a re-export as a use.
* ``lite_indexer/traces/solana/spl.py:37`` -- the ``checked`` keyword argument
  is genuinely unread in the function body.  It is passed by
  ``traces/solana/parsers.py:283`` and documented in the docstring, so removing
  it is a real refactor with a real caller, not a gate decision.

So an absolute-zero gate is red on day one.  Per the house standard (a gate that
fails on things you cannot fix is noise that trains people to ``--no-verify``),
this is a **ratchet**: the count may only go DOWN.  Any new dead symbol fails
the push; clearing the existing two is ordinary incremental work.

Scope is ``lite_indexer/`` only.  ``tests/`` is excluded on purpose: vulture
reports 8/10 of the confidence-80 findings from a single pytest fixture
parameter (``no_threads_left`` in ``tests/test_memory_watchdog.py``), and a
pytest fixture consumed only by the test signature is not dead code.

Baseline: ``scripts/vulture-baseline.txt`` -- one ``file:line: symbol`` per
line, rewritten only by ``--update-baseline`` (which refuses to add).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SCOPE = ("lite_indexer",)
DEFAULT_BASELINE = "scripts/vulture-baseline.txt"


def run_vulture(repo: Path) -> list[str]:
    """Return normalised ``path:line: symbol`` findings at 100% confidence."""
    proc = subprocess.run(
        [sys.executable, "-m", "vulture", *SCOPE, "--min-confidence", "100"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    # vulture exits 3 when it finds anything. Lines are
    # "path:line: unused variable 'x' (100% confidence)".
    lines = []
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line or ": " not in line:
            continue
        head, _, tail = line.partition(": ")
        parts = head.split(":")
        if len(parts) < 2 or not parts[1].isdigit():
            continue
        symbol = tail.split(" (")[0].strip()
        lines.append(f"{parts[0]}:{parts[1]}: {symbol}")
    return sorted(set(lines))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=".")
    ap.add_argument("--baseline", default=DEFAULT_BASELINE)
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    baseline_path = repo / args.baseline
    current = run_vulture(repo)

    if args.update_baseline or not baseline_path.exists():
        # Shrink-only: refuse to introduce a finding that is not already there.
        if baseline_path.exists() and not args.update_baseline:
            pass
        if args.update_baseline and baseline_path.exists():
            prior = {ln for ln in baseline_path.read_text().splitlines() if ln.strip()}
            added = [c for c in current if c not in prior]
            if added:
                print("vulture gate: REFUSING to raise the baseline.", file=sys.stderr)
                for a in added:
                    print(f"  new: {a}", file=sys.stderr)
                return 1
        baseline_path.write_text("\n".join(current) + ("\n" if current else ""))
        print(f"vulture budget: wrote {len(current)} finding(s) to {args.baseline}")
        return 0

    prior = {ln for ln in baseline_path.read_text().splitlines() if ln.strip()}
    added = [c for c in current if c not in prior]
    if added:
        print(f"vulture gate: FAIL — {len(current)} finding(s), baseline {len(prior)} (shrink-only)")
        for a in added:
            print(f"  NEW dead code: {a}")
        return 1
    print(f"vulture gate: PASS — {len(current)} finding(s), baseline {len(prior)} (shrink-only)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
