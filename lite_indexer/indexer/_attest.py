"""Merkle-leaf attestation of a committed indexer range.

Own module so the sync body keeps a one-line call to it and stays under its
shrink-only size ratchet. Deliberately best-effort: attestation is an
integrity receipt, not a correctness dependency, so nothing here may abort a
sync cycle.

The caller must pass block hashes through :class:`AttestOptions`. Passing
them positionally binds the list to ``options`` instead, and the range is then
recorded with no block hashes at all — silently, and invisibly to the test
suite, which calls the function directly rather than through the sync body.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from lite_indexer import metrics
from lite_indexer.attestation_record import AttestOptions, record_range_attestation
from lite_indexer.config import ATTESTATION_ENABLED
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


async def attest_range(
    chain: str,
    from_block: int,
    to_block: int,
    events: Sequence[Any],
    dex_events: Sequence[Any],
) -> str | None:
    """Append the committed range to its chain's attestation accumulator.

    Args:
        chain: Chain name the range belongs to.
        from_block: Inclusive start of the committed range.
        to_block: Inclusive end of the committed range.
        events: Parsed transfer events written this cycle.
        dex_events: Parsed DEX events written this cycle.

    Returns:
        The new Merkle root hex, or ``None`` when attestation is disabled,
        there is nothing to attest, or the attempt failed.
    """
    if not (ATTESTATION_ENABLED and (events or dex_events)):
        return None
    try:
        from lite_indexer.attestation import collect_block_hashes

        return await asyncio.to_thread(
            record_range_attestation,
            chain,
            from_block,
            to_block,
            list(events) + list(dex_events),
            AttestOptions(block_hashes=collect_block_hashes(events, dex_events)),
        )
    except Exception as exc:  # noqa: BLE001 — attestation must not abort a sync
        # A path/permission bug here used to be logged as a warning and lost;
        # three chains silently stopped attesting (2026-10-24 Errno 13). ERROR
        # plus a counter makes it alertable instead of decorative.
        logger.error("attestation_failed", chain=chain, error=str(exc)[:120])
        metrics.incr_labeled("lite_indexer_attestation_failures", chain=chain)
        return None


__all__ = ["attest_range"]
