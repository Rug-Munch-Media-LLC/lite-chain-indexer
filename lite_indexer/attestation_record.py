"""Append a committed range to its chain's attestation accumulator.

Split out of :mod:`lite_indexer.attestation` so the primitive module stays
under its shrink-only size ratchet, and so the write path can be tested with
``enabled=`` / ``state_dir=`` injected rather than by patching module globals.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lite_indexer.attestation import RangeAttestor, hash_rows
from lite_indexer.config import ATTESTATION_ENABLED
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class AttestOptions:
    """Injected switches for :func:`record_range_attestation`.

    Grouped so the write path takes one options object instead of four
    keyword parameters, which keeps it under the too-many-params ratchet.
    """

    state_dir: Path | str | None = None
    write_manifest_file: bool = True
    enabled: bool | None = None
    block_hashes: Sequence[str] | None = None


def record_range_attestation(
    chain: str,
    from_block: int,
    to_block: int,
    rows: Iterable[Mapping[str, Any]],
    options: AttestOptions | None = None,
) -> str | None:
    """Append a committed range to the chain's attestation accumulator.

    Write-only/observe: a no-op when ``ATTESTATION_ENABLED`` is off, and any
    failure is logged and swallowed (the sync path must never be blocked or
    aborted by attestation). Returns the new root hex, or ``None``.

    Args:
    """
    opts = options or AttestOptions()
    if not (ATTESTATION_ENABLED if opts.enabled is None else opts.enabled):
        return None
    try:
        digest = hash_rows(rows)
        attestor = RangeAttestor(chain, state_dir=opts.state_dir)
        root = attestor.append_range(from_block, to_block, list(opts.block_hashes or []), digest)
        if opts.write_manifest_file:
            attestor.write_manifest(from_block, to_block)
        metrics_incr("attestations_appended")
        logger.info(
            "attestation_appended",
            chain=chain,
            from_block=from_block,
            to_block=to_block,
            root=root[:16],
            leaf_count=attestor.leaf_count,
        )
        return root
    except Exception as exc:  # noqa: BLE001 — attestation is best-effort
        logger.warning("attestation_failed", chain=chain, error=str(exc)[:120])
        return None


__all__ = ["AttestOptions", "record_range_attestation"]
