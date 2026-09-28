"""Row-limit policy for the external lake reads (BigLake / BigQuery).

Both providers truncate a caller-supplied ``limit`` to a hard page ceiling
and hand the caller the *newest* page as if it were the answer. A caller
asking for 1,000,000 rows silently received 1000 with no signal, which is
the worst failure mode a data reader has: a short result that looks
complete. This module is the single place that decides what a lake read is
allowed to do with a ``limit``.

Policy:

* ``limit > LAKE_MAX_ROWS`` → :func:`reject_oversize_limit` raises
  ``ValueError``. Truncation is never silent.
* ``limit <= LAKE_MAX_ROWS`` → :func:`clamp_limit` returns it unchanged.
"""

from __future__ import annotations

# Hard ceiling on rows returned by one external-lake read. DuckDB (BigLake)
# and BigQuery both page; a caller must page explicitly rather than be handed
# a silently-clamped single page.
LAKE_MAX_ROWS = 1000

# Solana's BigQuery fill page size. LIMIT does not change scan bytes, so this
# is a page ceiling rather than a result ceiling, and it is reported in
# truncation reporting as a distinct number from LAKE_MAX_ROWS.
SOLANA_MAX_PAGE = 50_000

_LIMIT_SCOPE = "lake read"


def reject_oversize_limit(limit: int, ceiling: int = LAKE_MAX_ROWS) -> int:
    """Return ``limit`` or raise if it exceeds the lake page ceiling.

    Raises:
        ValueError: When ``limit`` is greater than ``ceiling``. The message
            names the ceiling so an operator can page explicitly.
    """
    want = int(limit)
    if want > ceiling:
        raise ValueError(f"{_LIMIT_SCOPE} limit {want} exceeds ceiling {ceiling}; page explicitly")
    return want


def clamp_limit(limit: int, ceiling: int = LAKE_MAX_ROWS) -> int:
    """Return ``limit`` bounded to ``[1, ceiling]``.

    Only for limits the caller has already been cleared for. Lake reads use
    :func:`reject_oversize_limit` first so a clamped value is never
    reported as a complete result.
    """
    return max(1, min(int(limit), ceiling))
