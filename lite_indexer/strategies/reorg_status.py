"""Shared reorg-detection result types.

Reorg detection is **fail-CLOSED**: a ClickHouse/RPC failure while comparing
a checkpoint must never be reported as "no reorg" — that would let orphaned
chain data be persisted as canonical. :class:`ReorgStatus.UNKNOWN` is the
explicit "cannot tell" state.

:class:`ReorgCheck` carries the status plus:

* ``fork_block`` — the common-ancestor block/slot when a reorg is known;
* ``error`` — the failure detail when UNKNOWN was caused by an error
  (checkpoint lookup / RPC fetch / rollback failure). It is ``None`` for the
  benign "no checkpoint yet" case, so callers can tell "cannot verify yet"
  apart from "verification failed" and fail closed only on the latter.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReorgStatus(StrEnum):
    """Outcome of a checkpoint comparison."""

    NO_REORG = "no_reorg"
    REORG = "reorg"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ReorgCheck:
    """Result of a reorg check / staging pass (see module docstring)."""

    status: ReorgStatus
    fork_block: int | None = None
    error: str | None = None


__all__ = ["ReorgCheck", "ReorgStatus"]
