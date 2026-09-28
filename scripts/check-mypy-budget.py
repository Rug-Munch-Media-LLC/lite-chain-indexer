#!/usr/bin/env python3
"""Shrink-only mypy budget for ``lite_indexer/``.

``mypy`` is a pre-push and CI gate, but it has been red since before
2026-09-28: ``lite_indexer/`` alone reports hundreds of errors. Most are
``attr-defined`` on two deliberate patterns that mypy cannot resolve:

* the ``_facade`` re-export idiom — 13 modules do
  ``import lite_indexer.<parent> as _facade`` to reach the composing class,
* Protocol and mixin attributes assigned outside ``__init__``.

Those are an architectural choice, not a backlog of 200 separate defects, so
an absolute-zero gate is unreachable without a rewrite. This script applies
the same shrink-only ratchet the repo already uses for size, antipatterns and
monkeypatch: it records today's per-file error count, and fails if any file's
count RISES or if the total rises. Clearing the debt is then ordinary
incremental work — fixing errors lowers the budget, and
``--update-baseline`` records the drop.

Usage:
  python3 scripts/check-mypy-budget.py --repo .                  # gate
  python3 scripts/check-mypy-budget.py --repo . --update-baseline
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_BASELINE = "scripts/mypy-baseline.json"
#: The scope both CI workflows type check. Keep in step with the `mypy` hook
#: in .lefthook.yml and the `Type check with mypy` step in both CI workflows.
SCOPE = "lite_indexer/"
ERR_RE = re.compile(r"^(?P<file>[^:]+):(?P<line>\d+):\s+error:")


def run_mypy(repo: Path) -> tuple[dict[str, int], int]:
    """Return ``(per-file error counts, total)`` for :data:`SCOPE`."""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", SCOPE, "--ignore-missing-imports"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    counts: dict[str, int] = {}
    total = 0
    for line in proc.stdout.splitlines():
        match = ERR_RE.match(line)
        if not match:
            continue
        total += 1
        path = match.group("file")
        try:
            rel = str(Path(path).resolve().relative_to(repo.resolve()))
        except ValueError:
            log.debug("mypy path outside repo, counted verbatim", path=path)
            rel = path
        counts[rel] = counts.get(rel, 0) + 1
    return counts, total


def _write(repo: Path, path: Path, counts: dict[str, int], total: int) -> None:
    """Write the budget file."""
    path.write_text(
        json.dumps({"scope": SCOPE, "total": total, "files": dict(sorted(counts.items()))}, indent=1) + "\n"
    )


def main() -> int:
    """Compare current mypy output against the recorded budget."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--update-baseline", action="store_true")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    baseline_path = repo / DEFAULT_BASELINE

    counts, total = run_mypy(repo)

    if args.update_baseline and not baseline_path.exists():
        # Bootstrap: there is nothing to compare against, so record today's
        # counts. Every later run is shrink-only.
        _write(repo, baseline_path, counts, total)
        print(f"mypy budget bootstrapped: {total} errors across {len(counts)} files")
        return 0

    if args.update_baseline:
        previous = {}
        if baseline_path.exists():
            previous = json.loads(baseline_path.read_text()).get("files", {})
        grew = {f: n for f, n in counts.items() if n > previous.get(f, 0) and f in previous}
        # A file with no prior entry is NEW debt, not a shrink. Allowing it
        # would let any new module start with an arbitrary error count and the
        # ratchet would never bite, so treat a missing entry as growth. The
        # only legitimate way to admit a new file is to add it to the baseline
        # by hand with its true count.
        added = {f: n for f, n in counts.items() if f not in previous}
        if grew or added:
            if grew:
                print("refusing to raise the mypy budget for:", file=sys.stderr)
                for f, n in sorted(grew.items()):
                    print(f"  {f}: {previous[f]} -> {n}", file=sys.stderr)
            if added:
                print("refusing to add NEW mypy debt for:", file=sys.stderr)
                for f, n in sorted(added.items()):
                    print(f"  {f}: (new) -> {n}", file=sys.stderr)
            print(
                "  Fix the errors, or record the new file deliberately by adding it to "
                f"{DEFAULT_BASELINE} by hand with its real count.",
                file=sys.stderr,
            )
            return 1
        _write(repo, baseline_path, counts, total)
        print(f"mypy budget recorded: {total} errors across {len(counts)} files")
        return 0

    if not baseline_path.exists():
        print(
            f"no baseline at {DEFAULT_BASELINE}; run with --update-baseline to record it",
            file=sys.stderr,
        )
        return 1

    data = json.loads(baseline_path.read_text())
    allowed: dict[str, int] = data.get("files", {})
    allowed_total: int = int(data.get("total", 0))

    offenders = {f: n for f, n in counts.items() if n > allowed.get(f, 0)}
    budget_grew = total > allowed_total
    if not offenders and not budget_grew:
        print(f"mypy budget: PASS — {total} errors, budget {allowed_total} (shrink-only)")
        return 0

    print(f"FAIL (mypy ratchet): {allowed_total} -> {total}", file=sys.stderr)
    for f, n in sorted(offenders.items(), key=lambda kv: -kv[1]):
        print(f"  - {f}: {allowed.get(f, 0)} -> {n} (ratchet: fix, don't add)", file=sys.stderr)
    print(
        f"  {len(offenders)} file(s) grew. The budget is decrease-only: fix the errors, "
        f"then run `python3 scripts/check-mypy-budget.py --repo . --update-baseline`.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
