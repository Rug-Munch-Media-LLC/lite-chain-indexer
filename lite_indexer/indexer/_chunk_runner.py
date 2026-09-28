"""Run-scoped state for one backfill dispatch wave.

Own module rather than a class inside ``backfill.py`` so the orchestrator
stays under its shrink-only size ratchet. Every field is shared by every
chunk in a wave, which is why bundling it removes six positional parameters
from ``_run_chunk`` without hiding a per-chunk value.

The queue/chunk/executor types live in ``backfill.py``; importing them at
runtime would be circular, and they are only needed for annotations, so they
come in under ``TYPE_CHECKING``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lite_indexer.indexer.backfill import BackfillExecutor, BackfillQueue


@dataclass(frozen=True, slots=True)
class ChunkRunner:
    """State shared by every chunk in one dispatch wave."""

    queue: BackfillQueue
    executor: BackfillExecutor
    semaphore: asyncio.Semaphore
    stats: dict[str, int]


__all__ = ["ChunkRunner"]
