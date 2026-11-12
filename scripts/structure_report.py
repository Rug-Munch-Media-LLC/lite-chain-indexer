# SPDX-License-Identifier: MIT
# Baseline write/load, the failure diff, and the console report for the
# structure gate. Split out so the gate entrypoint stays a coordinator.
# No scanning logic lives here — check-code-structure.py owns that.
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


def _write_baseline(
    baseline: Path,
    max_file: int,
    max_func: int,
    files: dict[str, int],
    monkey: dict[str, int],
) -> int:
    """Rewrite the grandfathered-offender baseline. Only ever called with
    --update-baseline, which is a deliberate human action."""
    baseline.write_text(
        json.dumps(
            {
                "comment": (
                    "Grandfathered offenders, recorded by FUNCTION COUNT (the cohesion "
                    "signal), never by line count. A file may shrink freely; growing "
                    "its function count fails the gate. monkeypatch counts only go down."
                ),
                "files": {
                    f: (facts or {}).get("functions", 0)
                    for f in sorted(files)
                    if not _is_structurally_one_unit(f, (facts := _structure(f)))
                },
                "monkeypatch": dict(sorted(monkey.items())),
            },
            indent=2,
        )
        + "\n"
    )
    print(f"baseline updated: {baseline}")
    return 0


def _load_baseline(baseline: Path) -> dict[str, dict[str, int]]:
    if not baseline.exists():
        return {"files": {}, "monkeypatch": {}}
    return json.loads(baseline.read_text())


def _is_test_file(rel: str) -> bool:
    """A test module: function count there means test cases, not responsibilities."""
    name = rel.rsplit("/", 1)[-1]
    return name.startswith("test_") or "/tests/" in rel or rel.startswith("tests/")


def _is_structurally_one_unit(rel: str, facts: dict) -> bool:
    """True when a long file is still ONE coherent job.

    A LINE COUNT IS NOT THE MEASURE (2026-11-11, learned the hard way: length alone
    flagged 235 files, almost all of them flat test tables). A file is a god file
    when it does several jobs, and that is structural - how many functions it
    exposes and how hard the worst one is to follow.

    Test modules are exempt from the function count because there a "function" is a
    test case, not a responsibility. Their only structural signal is cognitive
    complexity: a test body that cannot be followed is a test that will rot.
    """
    import structure_metrics as sm

    if not facts:
        return False  # unreadable: do not silently excuse it
    if facts.get("worst_cognitive", 0) > sm.COGNITIVE_CEILING:
        return False
    if _is_test_file(rel):
        return True
    return facts.get("functions", 0) <= sm.MAX_FUNCTIONS_IN_FILE


def _structure(rel: str) -> dict:
    """Structural facts for one repo-relative path, or {} when unreadable."""
    from pathlib import Path as _P

    import structure_metrics as sm

    here = _P(__file__).resolve().parent
    for base in (here.parent, here.parent.parent):
        candidate = base / rel
        if candidate.is_file():
            return sm.inspect_file(candidate) or {}
    return {}


def _why_structural(facts: dict) -> str:
    """The structural reason a file failed, in the terms the rule is written in.

    Never "too many lines". If the file exposes more functions than one module
    should, say that; if its hardest function is unfollowable, say that with the
    score. A reader should be able to act without knowing any threshold.
    """
    import structure_metrics as sm

    if not facts:
        return "unreadable; could not measure structure"
    reasons: list[str] = []
    funcs = facts.get("functions", 0)
    if funcs > sm.MAX_FUNCTIONS_IN_FILE:
        reasons.append(f"{funcs} functions (one module should expose one job's worth)")
    worst = facts.get("worst_cognitive", 0)
    if worst > sm.COGNITIVE_CEILING:
        reasons.append(f"hardest function cognitive {worst} (ceiling {sm.COGNITIVE_CEILING})")
    return "; ".join(reasons) or "holds more than one responsibility — split by job"


def _failures(
    files: dict[str, int], monkey: dict[str, int], base: dict, max_file: int = 0
) -> list[str]:
    """Every file is judged on STRUCTURE. No line count appears anywhere.

    THE LINE GATE IS GONE (2026-11-12). It selected which files were judged, and that
    was the whole problem: a number decided what was worth looking at, so a 194-line
    module with three responsibilities passed silently while a 300-line module with
    one job was told to split. MODULE-BOUNDARIES R0/R4 forbids exactly this - an edit
    whose only purpose is to move a file under a threshold is rejected, and the way to
    enforce R4 is to stop measuring.

    `files` is still accepted because the caller has the sizes and the report shows
    them as context. It is never a threshold and never a verdict.

    The verdict is two structural facts per file:
      * functions exposed — more than one module's worth means more than one job
      * worst cognitive complexity — a function nobody can follow will rot
    Test modules are exempt from the function count because there a "function" is a
    test case, not a responsibility.
    """
    base_files, base_monkey = base.get("files", {}), base.get("monkeypatch", {})
    out: list[str] = []
    for f in sorted(files):
        facts = _structure(f)
        if _is_structurally_one_unit(f, facts):
            continue
        detail = _why_structural(facts)
        if f not in base_files:
            out.append(f"NEW god file: {f} — {detail}")
        elif facts and facts.get("functions", 0) > base_files[f]:
            out.append(f"god file GREW: {f} ({base_files[f]} -> {facts['functions']} functions) — {detail}")
    for f, n in sorted(monkey.items()):
        if f not in base_monkey:
            out.append(
                f"NEW monkeypatch usage: {f} ({n} occurrences) — fleet anti-pattern, use dependency injection"
            )
        elif n > base_monkey[f]:
            out.append(f"monkeypatch usage GREW: {f} ({base_monkey[f]} -> {n})")
    return out


@dataclass(frozen=True)
class Report:
    """Everything the console report needs. Bundled so the signature stays
    under the 5-parameter cap and adding a field is a one-line change."""

    files: dict[str, int]
    monkey: dict[str, int]
    long_funcs: list[str]
    base_files: dict[str, int]
    max_file: int
    max_func: int


def _report(r: Report) -> None:
    files, monkey = r.files, r.monkey
    long_funcs, base_files = r.long_funcs, r.base_files
    max_file, max_func = r.max_file, r.max_func
    gods = {f: n for f, n in files.items() if n > max_file}
    print(
        f"structure gate: {len(files)} files scanned | god files (grandfathered): {len(gods)}"
    )
    for f, n in sorted(gods.items(), key=lambda kv: -kv[1]):
        budget = base_files.get(f)
        print(f"  {f}: {n} lines (context only)" + (f" | functions budget {budget}" if budget else ""))
    if long_funcs:
        print(
            f"  WARN: {len(long_funcs)} function(s) > {max_func} lines (non-blocking):"
        )
        for lf in long_funcs[:6]:
            print(f"    {lf}")
    print(
        f"  monkeypatch total: {sum(monkey.values())} across {len(monkey)} files (ratchet: only down)"
    )
