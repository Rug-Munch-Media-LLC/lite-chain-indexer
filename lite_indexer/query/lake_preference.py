"""Whether the free external lake should be consulted for a query.

The tier ordering (ClickHouse → cold/B2 → BigLake/S3 → Envio → indexed →
SQD → RPC) is the design; this predicate is the wiring that was missing.

``_needs_rpc_fallback`` answers "does the paid RPC leg need to run", and it
returns False for a bounded *wide* range even when the range is completely
unindexed. Gating the free lake on that predicate meant the one tier that
holds wide history and costs nothing per call was skipped precisely when it
had something to offer.
"""

from __future__ import annotations


def should_try_lake(
    ch_rows: list[dict[str, object]],
    cold_rows: list[dict[str, object]],
    from_block: int | None,
    to_block: int | None,
    requested_rows: int | None = None,
) -> bool:
    """Return True when a free lake read is worth making.

    Two conditions, both required:

    * The indexed (hot) and cold tiers returned nothing. When they have
      data for the range, the lake adds nothing and costs a round trip.
    * The range is bounded. The lake is date-partitioned and has no
      block→date index, so an unbounded read would derive no partitions and
      scan far more than the caller asked for.

    A partially-filled narrow range still reaches the lake: the gap is real
    and the lake is free, so filling it costs no money.
    """
    if from_block is None or to_block is None:
        # No bounded range means no date partitions can be derived, so there is
        # nothing for a date-partitioned lake to read.
        return False

    have = len(ch_rows) + len(cold_rows)
    if have == 0:
        return True

    # 2026-11-16: A PARTIAL GAP MUST REACH THE LAKE.
    # This used to return False as soon as ANY row was present, which is the
    # commonest backfill shape: ClickHouse holds a few days of an 84-day range,
    # so the range is neither empty nor complete. Treating "not empty" as "done"
    # sent the entire remainder of the range to the PAID RPC leg — the free tier
    # that holds exactly that history was skipped precisely when it had the most
    # to offer. The caller passes how many rows it wanted; anything less than
    # that is a gap the lake may fill.
    requested = requested_rows if requested_rows is not None else have
    return have < requested
