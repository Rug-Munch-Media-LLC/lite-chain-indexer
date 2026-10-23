"""Parked-chain declarations — silence is only allowed with a reason.

THE DAILY CADENCE CONTRACT
--------------------------
Every chain the daemon runs must land at least one row per 24 h
(:data:`DAILY_CADENCE_SECONDS`). The freshness alert pages at 900 s of
silence and raises a daily-cadence breach at 24 h. A chain that genuinely
cannot meet it is declared PARKED in ``config/chain-states.yml`` with a
reason and a review date — never dropped from the daemon just to quiet the
page (2026-10-24: 11 of 16 chains looked "parked" only because they had no
freshness series at all; none of them was actually declared).

A declaration that cannot be read is a LOUD startup failure, not a quiet
park: a malformed reason must never be able to hide a dead chain.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Cadence every non-parked chain must meet.
DAILY_CADENCE_SECONDS: int = 86_400

#: Reasons shorter than this, or equal to a shrug, are rejected.
MIN_REASON_CHARS: int = 20
_SHRUGS = frozenset({"n/a", "na", "tbd", "todo", "unknown", "none", "later"})

#: Default declaration file: ``<repo>/config/chain-states.yml``.
DEFAULT_PATH: str = os.environ.get("CHAIN_STATES_PATH") or str(
    Path(__file__).resolve().parent.parent / "config" / "chain-states.yml"
)


class ChainStatesError(RuntimeError):
    """A parked-chain declaration is unreadable, unknown or reason-free."""


@dataclass(frozen=True)
class ParkedChain:
    """One declared-parked chain: why, since when, and when to re-check."""

    chain: str
    reason: str
    since: str
    review_by: str


def _parse_entry(chain: str, entry: Any) -> ParkedChain:
    """Validate one ``parked`` mapping, or raise :class:`ChainStatesError`."""
    if not isinstance(entry, dict):
        raise ChainStatesError(f"parked.{chain}: expected a mapping with reason/since/review_by")
    reason = str(entry.get("reason") or "").strip()
    if len(reason) < MIN_REASON_CHARS or reason.lower() in _SHRUGS:
        raise ChainStatesError(
            f"parked.{chain}: a reason of at least {MIN_REASON_CHARS} chars is required "
            f"(got {reason!r}) — a parked chain without a reason is a disabled chain"
        )
    unknown = set(entry) - {"reason", "since", "review_by"}
    if unknown:
        raise ChainStatesError(f"parked.{chain}: unknown key(s) {sorted(unknown)}")
    return ParkedChain(
        chain=chain,
        reason=reason,
        since=str(entry.get("since") or "unknown"),
        review_by=str(entry.get("review_by") or "unscheduled"),
    )


def load_parked(path: str | None = None, known: set[str] | None = None) -> dict[str, ParkedChain]:
    """Load and validate the parked-chain declarations.

    Args:
        path: Declaration file; defaults to :data:`DEFAULT_PATH`. A missing
            file means "nothing parked" (every chain must meet the cadence).
        known: Optional set of chains the daemon runs. A declaration for a
            chain outside it is an error — a typo must not silence a chain.

    Returns:
        Mapping of chain name to its :class:`ParkedChain` declaration.

    Raises:
        ChainStatesError: unreadable YAML, unknown chain, unknown key, or a
            missing/shrugged reason.
    """
    import yaml

    target = Path(path or DEFAULT_PATH)
    if not target.exists():
        return {}
    try:
        doc = yaml.safe_load(target.read_text()) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ChainStatesError(f"cannot read chain states {target}: {exc}") from exc
    if not isinstance(doc, dict):
        raise ChainStatesError(f"{target}: top level must be a mapping")
    unknown_keys = set(doc) - {"parked"}
    if unknown_keys:
        raise ChainStatesError(f"{target}: unknown top-level key(s) {sorted(unknown_keys)}")
    parked_raw = doc.get("parked") or {}
    if not isinstance(parked_raw, dict):
        raise ChainStatesError(f"{target}: 'parked' must be a mapping of chain -> declaration")
    if known is not None:
        stray = sorted(set(parked_raw) - set(known))
        if stray:
            raise ChainStatesError(f"{target}: parked chain(s) not run by this daemon: {stray}")
    return {chain: _parse_entry(chain, entry) for chain, entry in parked_raw.items()}
