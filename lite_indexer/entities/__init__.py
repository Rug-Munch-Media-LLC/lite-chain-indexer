"""Wallet-entity clustering — heuristics that group wallets into cohorts.

Each module owns one heuristic and writes ``wallet_memory.wallet_entities``.
A cluster is **evidence, not an accusation**: every row carries a bounded float
``confidence_score`` and none is ever upgraded into a label.

* :mod:`lite_indexer.entities.cex_deposit` — wallets whose first inbound funding
  came from a labelled exchange address.
"""

from lite_indexer.entities.cex_deposit import (
    MAX_CONFIDENCE,
    EntityRow,
    Evidence,
    SourceLabel,
    build_rows,
    score_confidence,
    source_entity_id,
)

__all__ = [
    "MAX_CONFIDENCE",
    "EntityRow",
    "Evidence",
    "SourceLabel",
    "build_rows",
    "score_confidence",
    "source_entity_id",
]
