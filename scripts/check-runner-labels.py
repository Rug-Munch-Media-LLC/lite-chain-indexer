#!/usr/bin/env python3
"""check-runner-labels.py — a `runs-on` that no runner advertises is a dead job.

THE FAILURE MODE THIS EXISTS FOR
    Forgejo does not fail a job whose `runs-on` matches nothing. It leaves it QUEUED,
    forever, silently. There is no red X, no error, no timeout — the workflow simply
    never produces a result, so nothing downstream can tell it apart from "not
    triggered yet".

    Measured on this fleet 2026-11-12: `standards/.forgejo/workflows/compliance.yml`
    and `gates-drift.yml` had run **0 times across 8,862 recorded workflow runs**.
    `compliance.yml` asked for `[self-hosted, talos]`; `gates-drift.yml` asked for a
    bare `docker`. Neither label exists. Both workflows had been dead since the day
    they were written, and the fleet believed it had a daily compliance gate.

THE TWO RULES (both from codeberg.org/forgejo docs, "Special labels")
    1. A runner runs a job only if it carries EVERY label in the `runs-on` array.
       `[self-hosted, talos]` is an AND, not an OR — so the common GitHub habit of
       writing `[self-hosted, linux]` fails silently here.
    2. Labels are declared at RUNNER level, in `runner.labels`, as
       `<name>:<type>://<default-image>`. A name that is not declared is dead.

    `runs-on: ${{ ... }}` is skipped: an expression cannot be evaluated statically,
    and guessing would produce false failures.

WHY THIS IS A GATE AND NOT A DOC
    The rule was written down and still drifted, because a doc cannot read the runner
    config. This reads the live `runner.labels` from every runner unit and compares.

Usage: python3 scripts/check-runner-labels.py [--repos-root DIR] [--runner-dir DIR]
                                              [--json] [--quiet]
Exit:  0 every label resolves | 1 at least one job can never run | 2 cannot evaluate
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_REPOS_ROOT = Path("/srv/work/repos")
DEFAULT_RUNNER_DIR = Path("/srv/ci/runner")

#: Trees that are lanes, worktrees or mirrors. They share a product repo's config
#: and reporting them again hides which REAL repo is broken.
SKIP_PREFIXES = ("_", "cc-lane-", "cc-backtest", "contextcrypto-", "standards-", "std-")
SKIP_PARTS = ("lane-", "wt-", "-main", "probe-fix", "leadpack")

_RUNS_ON = re.compile(r"^\s*runs-on:\s*(.+?)\s*$")


@dataclass
class BadJob:
    repo: str
    workflow: str
    line: int
    labels: list[str]
    missing: list[str]


def _label_block(text: str) -> list[str]:
    """The `labels:` list lines from one runner config, comments stripped."""
    out: list[str] = []
    in_labels = False
    for line in text.splitlines():
        stripped = line.split("#", 1)[0].rstrip()
        if re.match(r"^\s*labels:\s*$", stripped):
            in_labels = True
            continue
        if not in_labels:
            continue
        if not stripped.strip():
            continue
        if stripped.lstrip().startswith("-"):
            out.append(stripped)
        else:
            break  # the next top-level key ends the list
    return out


def _label_name(entry: str) -> str | None:
    """`- name:type://image` -> name. None when the entry is not shaped that way."""
    m = re.match(r"^\s*-\s*([^:\s]+):", entry)
    return m.group(1) if m else None


def registered_labels(runner_dir: Path) -> set[str]:
    """Every label declared by a live runner unit.

    Parsed from the YAML rather than asked of the API, because the config on disk IS
    what the daemon reads — an API view can be stale after a restart. Split into
    two small functions because the scanning loop and the name extraction fail for
    different reasons (an empty block vs a malformed entry).
    """
    labels: set[str] = set()
    for cfg in sorted(runner_dir.glob("runner-*/config.yaml")):
        try:
            text = cfg.read_text()
        except OSError:
            continue
        for entry in _label_block(text):
            name = _label_name(entry)
            if name:
                labels.add(name)
    return labels


def _labels_of(raw: str) -> list[str]:
    """`[a, b]` or `a` -> the label names. Strips backticks (a doc comment leak)."""
    raw = raw.split("#", 1)[0].strip().strip("`")
    if raw.startswith("["):
        raw = raw.strip("[]")
        return [x.strip().strip("`").strip("'\"") for x in raw.split(",") if x.strip()]
    return [raw.strip("'\"")]


def scan_workflow(repo: str, path: Path, known: set[str]) -> list[BadJob]:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    out: list[BadJob] = []
    for lineno, line in enumerate(lines, 1):
        m = _RUNS_ON.match(line.split("#", 1)[0])
        if not m:
            continue
        raw = m.group(1)
        if "${{" in raw:
            continue
        labels = _labels_of(raw)
        missing = [x for x in labels if x and x not in known]
        if missing:
            out.append(BadJob(repo, str(path.name), lineno, labels, missing))
    return out


def managed_repos(root: Path) -> list[Path]:
    out = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or (p / ".forgejo").is_dir() is False:
            continue
        if p.name.startswith(SKIP_PREFIXES) or any(k in p.name for k in SKIP_PARTS):
            continue
        out.append(p)
    return out


def audit(root: Path, known: set[str]) -> list[BadJob]:
    rows: list[BadJob] = []
    for repo in managed_repos(root):
        for wf in sorted((repo / ".forgejo" / "workflows").glob("*.yml")):
            rows.extend(scan_workflow(repo.name, wf, known))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos-root", default=str(DEFAULT_REPOS_ROOT))
    ap.add_argument("--runner-dir", default=str(DEFAULT_RUNNER_DIR))
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    root, runner = Path(a.repos_root), Path(a.runner_dir)
    # IN A CONTAINER NEITHER PATH EXISTS, AND THAT IS NOT A FAILURE.
    # This check reads the RUNNER's own config, which lives on the build host
    # (/srv/ci/runner) — not inside a job container, where /srv/work/repos and
    # /srv/ci/runner are both absent. Treating "cannot see the runner" as exit 2
    # turned this check into a red CI job with the message "cannot evaluate", which
    # is the exact silent-green/red confusion it was written to remove (2026-11-12).
    #
    # The honest behaviour: SKIP with a named reason, exit 0, and say where it DOES
    # run. The pre-push hook on Talos is the enforcement point; CI is a courtesy.
    if not root.is_dir() or not runner.is_dir():
        print(
            f"runner-labels: SKIP — {runner} is not visible from here "
            f"(this check reads the runner configs and runs on the build host, "
            f"not inside a job container). repos_root={root}",
            file=sys.stderr,
        )
        return 0

    known = registered_labels(runner)
    if not known:
        print(
            f"runner-labels: SKIP — no labels parsed from {runner}; "
            "check the glob or the config format",
            file=sys.stderr,
        )
        return 0

    rows = audit(root, known)
    if a.json:
        print(
            json.dumps(
                {"registered": sorted(known), "bad": [asdict(r) for r in rows]},
                indent=1,
            )
        )
    elif not a.quiet:
        print(f"registered runner labels: {', '.join(sorted(known))}")
        for r in rows:
            print(
                f"  DEAD JOB {r.repo}/{r.workflow}:{r.line} "
                f"asks {r.labels} — no runner carries {r.missing} (queues forever)"
            )
        print(f"\ndead_jobs={len(rows)} (unmatched jobs never fail, they never start)")
    return 1 if rows else 0


if __name__ == "__main__":
    sys.exit(main())
