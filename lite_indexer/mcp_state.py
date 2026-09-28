"""Small readers shared by the MCP server tools.

``lci_status`` / ``lci_rpc_health`` / ``lci_chain_head`` all need the same
two primitives — read a JSON object off disk, and enumerate the chains that
have a state file — and both are pure enough to live outside the tool module
so the tools stay a thin, testable surface.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from lite_indexer.logging import redact_secrets

#: EVM 0x + 32-44 char Solana base58 address, for the tool-level filters.
ADDRESS_RE = re.compile(r"(?:0x[a-fA-F0-9]{40})|(?:[1-9A-HJ-NP-Za-km-z]{32,44})")


def scrub(value: str) -> str:
    """Strip secrets from a value before it leaves the MCP surface."""
    return str(redact_secrets(None, "mcp", {"value": value})["value"])


#: State-file globs that mark a chain as configured for this deployment.
CHAIN_GLOBS: tuple[str, ...] = ("lite_*.json", "highwater_*.json")


def read_json_object(path: Path | str) -> dict[str, Any]:
    """Read a JSON object from ``path``; an unreadable or non-object file yields ``{}``.

    Args:
        path: File to read.

    Returns:
        The parsed object, or an empty dict when the file is missing,
        malformed, or holds a non-object (a state file, a quota ledger
        entry, a throttle snapshot — all of which may be corrupt on disk
        after a crash and must never take the server down).
    """
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def chain_names(state_dir: Path | str) -> list[str]:
    """Sorted chains that have a state or high-water file in ``state_dir``.

    Args:
        state_dir: Directory holding ``lite_<chain>.json`` /
            ``highwater_<chain>.json``.

    Returns:
        Sorted, de-duplicated chain names; empty when the directory does not
        exist yet.
    """
    names: set[str] = set()
    for pattern in CHAIN_GLOBS:
        prefix = pattern.split("*")[0]
        names.update(p.stem.removeprefix(prefix) for p in Path(state_dir).glob(pattern))
    return sorted(names)


__all__ = ["ADDRESS_RE", "CHAIN_GLOBS", "chain_names", "read_json_object", "scrub"]
