"""Temporal completeness: is there a DAY a chain should have produced rows and did not?

WHY THIS EXISTS (2026-11-16)
    The daily ``integrity-check`` reported ``status: gaps`` on EVERY chain for
    months and nobody could act on it, because it measured the wrong thing:

        contiguous_pct = uniqExact(block_number) / (max_block - min_block + 1)

    This indexer does not index every block -- it indexes the blocks that carry
    transfers, and it samples. So ``contiguous_pct`` can never reach 100% and
    the ``status: ok`` branch (>=99%) is unreachable by construction. Ten chains
    sat stale for 20-24 days -- arbitrum, bsc, mantle, zksync, avalanche,
    fantom, tron, base, metis, robinhood -- while the audit printed the same
    meaningless "gaps" verdict it had printed for months.

    A metric that can never pass is not a strict gate; it is a mute one.

THE REAL QUESTION
    Not "did we index every block" but "is there a calendar day where this chain
    produced no rows at all". A chain that answers for yesterday and today is
    healthy regardless of how many individual blocks it touched. A chain whose
    newest row is three weeks old is dead, and that is unambiguous.

    Freshness is also the only signal that maps to what a user experiences: ask
    the scanner about a token on a stale chain and it answers from three-week-old
    data with no indication that it did.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: A chain whose newest row is older than this is STALE. One day of slack
#: absorbs a slow exporter run without crying wolf; two would hide a full day
#: of lost data, which is the failure this check exists to catch.
STALE_AFTER_DAYS = 1

#: Statuses. ``stale`` is a real data-loss signal; ``lagging`` is a warning
#: that the writer is behind but still moving.
STATUS_STALE = "stale"
STATUS_LAGGING = "lagging"
STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_UNAVAILABLE = "unavailable"


@dataclass
class ChainFreshness:
    """One chain's temporal completeness verdict."""

    chain: str
    newest_day: str | None = None
    days_stale: int | None = None
    rows_recent: int = 0
    status: str = STATUS_UNAVAILABLE
    detail: str = ""


def classify(
    chain: str,
    newest_day: str | None,
    days_stale: int | None,
    rows_recent: int,
    *,
    stale_after_days: int = STALE_AFTER_DAYS,
) -> ChainFreshness:
    """Derive a freshness verdict from measured values.

    Pure: takes measurements, returns a verdict, reads nothing. That makes the
    policy testable without a database.

    Args:
        chain: Chain name.
        newest_day: ISO date of the newest row, or ``None`` when the chain has
            no rows at all.
        days_stale: Days between ``newest_day`` and today, or ``None``.
        rows_recent: Rows written in the last 24 hours.
        stale_after_days: Threshold for ``stale``.

    Returns:
        A populated :class:`ChainFreshness`.
    """
    if newest_day is None or days_stale is None:
        return ChainFreshness(
            chain=chain,
            status=STATUS_EMPTY,
            detail="no rows at all - the writer has never produced for this chain",
        )
    if days_stale > stale_after_days:
        return ChainFreshness(
            chain=chain,
            newest_day=newest_day,
            days_stale=days_stale,
            rows_recent=rows_recent,
            status=STATUS_STALE,
            detail=f"newest row is {days_stale}d old - nothing written since {newest_day}",
        )
    if rows_recent == 0:
        return ChainFreshness(
            chain=chain,
            newest_day=newest_day,
            days_stale=days_stale,
            rows_recent=0,
            status=STATUS_LAGGING,
            detail=f"newest row is {newest_day} but nothing in the last 24h",
        )
    return ChainFreshness(
        chain=chain,
        newest_day=newest_day,
        days_stale=days_stale,
        rows_recent=rows_recent,
        status=STATUS_OK,
    )


@dataclass
class FreshnessReport:
    """Fleet-wide freshness verdict."""

    chains: dict[str, ChainFreshness] = field(default_factory=dict)
    as_of: str = ""

    def by_status(self, status: str) -> list[ChainFreshness]:
        """Return the chains currently in ``status``."""
        return [c for c in self.chains.values() if c.status == status]

    def summary(self) -> dict[str, Any]:
        """Counts per status plus the stale chain names, for JSON output."""
        counts: dict[str, int] = {}
        for c in self.chains.values():
            counts[c.status] = counts.get(c.status, 0) + 1
        return {
            "as_of": self.as_of,
            "counts": counts,
            "stale": [c.chain for c in self.by_status(STATUS_STALE)],
            "lagging": [c.chain for c in self.by_status(STATUS_LAGGING)],
            "ok": [c.chain for c in self.by_status(STATUS_OK)],
        }

    def worst(self) -> list[ChainFreshness]:
        """Stale chains ordered by how long they have been silent."""
        return sorted(
            self.by_status(STATUS_STALE),
            key=lambda c: c.days_stale or 0,
            reverse=True,
        )


def ok_for_scanner(report: FreshnessReport) -> bool:
    """Whether the scanner may advertise itself as serving fresh data.

    A scanner that answers from a three-week-old chain has not failed, but it
    must not claim freshness. This is the predicate the scan output uses.
    """
    return not report.by_status(STATUS_STALE)


__all__ = [
    "ChainFreshness",
    "FreshnessReport",
    "STALE_AFTER_DAYS",
    "STATUS_EMPTY",
    "STATUS_LAGGING",
    "STATUS_OK",
    "STATUS_STALE",
    "STATUS_UNAVAILABLE",
    "classify",
    "ok_for_scanner",
]
