"""structure_metrics — the measurements check-structure.py gates on.

Split out of check-structure.py so the gate file stays readable and its own
policy (functions-per-file, cognitive ceiling, fat signatures) lives beside the
code that computes it instead of inside the CLI that reports it.

Every function here answers ONE question about ONE file:

    cognitive(funcs)      how hard is the hardest function to hold in your head
    inspect_file(path)    the structural facts, or None for a non-Python file

Nothing here decides pass/fail. The thresholds are check-structure.py's, because
a threshold is a policy and a measurement is a fact, and the two age differently.
"""

from __future__ import annotations

import ast
from pathlib import Path

#: A file exposing more than this many functions is doing more than one job.
MAX_FUNCTIONS_IN_FILE = 25
#: One function above this is hard to follow (SonarSource's maintainability view).
COGNITIVE_CEILING = 15
#: Parameters beyond this indicate weak cohesion, not a style preference.
MAX_PARAMS = 5

SKIP_SUFFIX = (
    ".json",
    ".jsonc",
    ".jsonl",
    ".csv",
    ".tsv",
    ".lock",
    ".parquet",
    ".svg",
    ".png",
    ".jpg",
    ".woff",
    ".woff2",
)
SKIP_DIRS = {
    "node_modules",
    ".venv",
    "venv",
    "dist",
    "build",
    "__pycache__",
    ".git",
    "site-packages",
    "reference",
    "fixtures",
    "testdata",
}


def _cognitive(func) -> int:
    """SonarSource cognitive complexity, iterative so a big file stays cheap."""
    score = 0
    stack = [(func, 0)]
    while stack:
        node, depth = stack.pop()
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child,
                (
                    ast.If,
                    ast.For,
                    ast.AsyncFor,
                    ast.While,
                    ast.Try,
                    ast.With,
                    ast.AsyncWith,
                ),
            ):
                score += 1 + depth
                stack.append((child, depth + 1))
            elif isinstance(child, ast.BoolOp):
                score += len(child.values) - 1 + depth
                stack.append((child, depth))
            elif isinstance(child, ast.IfExp):
                score += 1 + depth
                stack.append((child, depth + 1))
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                stack.append((child, 0))
            else:
                stack.append((child, depth))
    return score


def inspect_file(path: Path) -> dict | None:
    """Structural facts about one python file, or None when not judgeable."""
    if path.suffix != ".py":
        return None
    try:
        src = path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(src)
    except (SyntaxError, OSError, ValueError):
        return None
    funcs = [
        n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    worst = max((_cognitive(f) for f in funcs), default=0)
    fat = sum(
        1
        for f in funcs
        if len(f.args.posonlyargs) + len(f.args.args) + len(f.args.kwonlyargs)
        > MAX_PARAMS
    )
    return {
        "functions": len(funcs),
        "worst_cognitive": worst,
        "fat_params": fat,
        "lines": src.count("\n") + 1,  # CONTEXT ONLY — never gates
    }


def skip(path: Path) -> bool:
    """True when this path is outside the module rules (generated, vendored, data)."""
    if any(part in SKIP_DIRS for part in path.parts):
        return True
    return path.suffix in SKIP_SUFFIX
