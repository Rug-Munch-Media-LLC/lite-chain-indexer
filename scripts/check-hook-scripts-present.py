#!/usr/bin/env python3
"""check-hook-scripts-present.py — a hook that calls a missing script blocks EVERY push.

THE FAILURE, MEASURED THREE TIMES ON THIS FLEET (2026-11-12)
    1. `check-runner-labels.py` — the pre-push hook ran it; the file existed in
       `standards` and in no repo. Every push died at `can't open file`, exit status 2,
       for every commit, by everyone.
    2. `check-dangling-branches.py` — identical shape, identical outcome.
    3. `check-xdist-coverage.py` — a NEW gate added to the shared hook file, propagated
       to the gate tree, never to the repos. Same crash.

    Each time the diagnosis cost a push cycle and the fix was copying one file. Nothing
    in the system could see it coming, because the hook references a PATH and nothing
    compares that path against the repo it runs in.

WHY THIS IS A GATE AND NOT A CHECKLIST
    The shared `lefthook.yml` lives in `standards`/the gate tree and runs in EVERY repo.
    Adding a gate there is one edit; the script it calls has to travel to 40 checkouts,
    and nothing enforced that. A doc cannot close it: the class recurred three times
    while the rule was obvious to everyone.

WHAT IT ASSERTS
    Every `scripts/<name>.py` / `scripts/<name>.sh` a hook invokes exists in the repo
    the hook is running in. One finding per missing script, naming it.

    Deliberately narrow: it does not check that the script is CORRECT, only that the push
    will not die before the gate can say anything. A missing script is worse than a
    failing one — a failing gate reports a verdict; a missing file reports nothing and
    takes the whole push with it.

EXIT
    0 = every referenced script is present. 1 = at least one is missing.

Usage:
    check-hook-scripts-present.py                  # this repo, against the shared hook
    check-hook-scripts-present.py --repo DIR
    check-hook-scripts-present.py --hook PATH      # a specific lefthook file
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

#: Scripts a hook may invoke. Matched inside any `run:` value, including the multi-line
#: `|` form, because a gate is just as dead when the path is on the second line.
SCRIPT_REF = re.compile(r"(?:python3?|bash|sh)\s+(scripts/[A-Za-z0-9_./-]+\.(?:py|sh))")

#: Where the fleet's shared hook file lives, in preference order. The gate tree copy is
#: what actually runs on a push; the standards copy is the source it is deployed from.
DEFAULT_HOOKS = (
    Path("/srv/work/fleet-gate-tree/lefthook.yml"),
    Path("/srv/work/repos/standards/lefthook.yml"),
)


def hook_path(explicit: str | None) -> Path | None:
    """The hook file to read, or None when the fleet's hook is not reachable."""
    if explicit:
        path = Path(explicit)
        return path if path.is_file() else None
    for candidate in DEFAULT_HOOKS:
        if candidate.is_file():
            return candidate
    return None


def _strip_comments(text: str) -> str:
    r"""Drop YAML comment lines before scanning.

    MEASURED 2026-11-12: without this the gate reported 42 of 44 repos as broken, and the
    largest single cause was PROSE. `lefthook.yml` explains each gate in a comment above
    it, and those comments quote the very command they describe — `# bare
    \`bash scripts/guard-destructive-git.sh\` died with exit 127` is documentation of a
    bug that was fixed, not an invocation. Scanning it made the guard flag its own
    changelog.

    Only whole-line comments are removed. A trailing `# ...` after a `run:` value is left
    alone, because a `#` inside a shell string is not a comment and guessing would drop
    real commands.
    """
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


def _guarded(text: str, ref: str) -> bool:
    """True when the hook cannot be taken down by this script being absent.

    A gate that cries wolf is one people learn to bypass, and MEASURED 2026-11-12 it cried
    wolf 42 times out of 44 repos. Every one of those was a real guard that this function
    failed to recognise, not a missing script. Four shapes, all of which leave the push
    alive:

        if [ -f scripts/x.py ]; then ... fi      plain existence check
        test ! -f scripts/x.py || python3 ...    inverted check, runs only if present
        if [ -f "$_c/scripts/x.py" ]; then       a PREFIXED path (another root's copy)
        ... 2>/dev/null || true                  failure swallowed, exits 0

    The `-f` patterns deliberately allow anything before the ref, because a guard checks a
    path (`$_c/scripts/x.py`, `$_std/scripts/x.py`) at least as often as a bare relative
    one. Erring towards "guarded" is correct here: this gate exists to catch a push that
    dies before any verdict, and a guarded reference cannot do that.
    """
    name = ref.replace(".", r"\.")
    patterns = (
        # `-f <anything>/scripts/x.py` — plain or prefixed, inside [ ] or after `test`.
        rf"-f\s+[^\s\]]*{name}",
        # a failure SWALLOWED on the same line: `... 2>/dev/null || true` or `|| true`.
        rf"{name}[^\n]*\|\|[^\n]*true",
        # the whole line already ends in `|| true`
        rf"{name}[^\n]*\|\|\s*true\s*$",
    )
    return any(re.search(pat, text, re.MULTILINE) for pat in patterns)


def referenced_scripts(hook: Path) -> tuple[list[str], list[str]]:
    """(unguarded, guarded) `scripts/...` paths the hook invokes.

    Both are returned, sorted and de-duplicated, because the caller needs to report the
    guarded ones as context without failing on them.
    """
    try:
        text = hook.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return [], []
    every = sorted(set(SCRIPT_REF.findall(_strip_comments(text))))
    guarded = [r for r in every if _guarded(text, r)]
    return [r for r in every if r not in guarded], guarded


def missing_in(repo: Path, scripts: list[str]) -> list[str]:
    """The referenced scripts that do not exist in this repo."""
    return [s for s in scripts if not (repo / s).is_file()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--hook", default=None)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    hook = hook_path(args.hook)
    if hook is None:
        # NOT a failure: a repo checked out alone has no fleet hook to compare against,
        # and failing there would block pushes in exactly the situation this gate exists
        # to protect. Say so, loudly enough to notice, and pass.
        if not args.quiet:
            print("hook-scripts: no shared lefthook.yml reachable; nothing to compare")
        return 0

    repo = Path(args.repo).resolve()
    scripts, guarded = referenced_scripts(hook)
    missing = missing_in(repo, scripts)
    if not missing:
        if not args.quiet:
            tail = (
                f" ({len(guarded)} guarded by an existence check)"
                if guarded
                else ""
            )
            print(f"hook-scripts: {len(scripts)} unguarded reference(s), all present{tail}")
        return 0

    print(f"hook-scripts: {len(missing)} referenced script(s) MISSING from {repo}")
    for name in missing:
        print(f"  - {name}")
    print(
        "\nA hook calling a missing script fails with `can't open file` and exit 2, "
        "which blocks EVERY push before any gate can report a verdict. Copy the script "
        "from standards/scripts (it is byte-identical there) and commit it, or remove "
        "the hook line."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
