"""CEX deposit clustering — wallets whose *first inbound funding* came from an exchange.

READ THIS BEFORE USING THE OUTPUT
---------------------------------
This module produces **evidence, never an accusation**. A shared exchange hot
wallet funds millions of unrelated customers: one Binance deposit wallet alone
funds the entire retail population of a chain. Two wallets that share a funder
are therefore almost never the same owner.

What ``entity_id`` identifies is a **cohort**: "the set of wallets this exchange
address funded first". ``entity_category`` is ``exchange_cluster`` for exactly
that reason. It is the correct key for *excluding* exchange-customer noise from a
same-owner analysis, and the wrong key for asserting two wallets share an owner.

``confidence_score`` is recorded and never upgraded into a label. A wrong
attribution in a scanner is a false accusation with a customer behind it, so the
score is a float, it stays strictly below 1.0 (``MAX_CONFIDENCE``), and it is
never allowed to speak for itself.

DATA PROVENANCE — what we have, and what we do not
-------------------------------------------------
``internal_transfers`` is the only table holding native-value (ETH/BNB) movement,
which is what "funded a wallet" means. ``evm_transfers`` holds **ERC20 rows only**
(``token_address`` is non-empty on every row), so it cannot see a native deposit;
its earliest token inbound is a strictly weaker second tier and is scored as such.
Both tables are TTL-bounded (90d / 30d), so "first inbound" means "first inbound
*we still hold*". Window-edge cases are penalised, never trusted.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

#: Label categories that mark an address as exchange infrastructure.
EXCHANGE_CATEGORIES: tuple[str, ...] = ("exchange", "cex_hot_wallet")

#: Evidence tiers. Native funding is the real signal; a token inbound is not.
TIER_NATIVE = "native"
TIER_TOKEN = "token"

_BASE_SCORE: dict[str, float] = {TIER_NATIVE: 0.55, TIER_TOKEN: 0.28}
_SUBTYPE_BONUS: dict[str, float] = {"cex_deposit_wallet": 0.12, "cex_hot_wallet": 0.10}
_CATEGORY_BONUS: dict[str, float] = {"exchange": 0.04, "cex_hot_wallet": 0.10}
_TIER_A_BONUS = 0.03
_SUPPORT_CAP = 0.20
_EDGE_PENALTY_MAX = 0.15

#: Ceiling for any score. Deliberately below 1.0: shared funding is never proof.
MAX_CONFIDENCE = 0.90

#: An inbound inside this many seconds of the earliest timestamp we hold may not
#: be the true first inbound — the table simply starts there.
WINDOW_EDGE_SECONDS = 3600


@dataclass(frozen=True)
class SourceLabel:
    """The exchange label attached to one funding source address."""

    label_name: str
    label_category: str
    tier: str = "C"
    label_subtype: str = ""

    @property
    def is_exchange(self) -> bool:
        """True only for a labelled exchange address — the gate for emission."""
        return self.label_category in EXCHANGE_CATEGORIES

    @property
    def bonus(self) -> float:
        """Label strength: deposit wallet > hot wallet > generic."""
        if self.label_subtype in _SUBTYPE_BONUS:
            return _SUBTYPE_BONUS[self.label_subtype]
        base = _CATEGORY_BONUS.get(self.label_category, 0.02)
        return base + (_TIER_A_BONUS if self.tier == "A" else 0.0)


@dataclass(frozen=True)
class Evidence:
    """One wallet, the address that funded it first, and how we saw the funding."""

    chain_id: str
    wallet_address: str
    source_address: str
    first_seen_epoch: int
    tier: str = TIER_NATIVE


@dataclass(frozen=True)
class EntityRow:
    """A fully scored ``wallet_memory.wallet_entities`` row."""

    entity_id: str
    wallet_address: str
    chain_id: str
    heuristic_type: str
    confidence_score: float
    entity_label: str
    entity_category: str
    first_linked_at: int
    last_updated_at: int

    def as_tuple(self) -> tuple[object, ...]:
        """Values in ``INSERT_COLUMNS`` order."""
        return (
            self.entity_id,
            self.wallet_address,
            self.chain_id,
            self.heuristic_type,
            self.confidence_score,
            self.entity_label,
            self.entity_category,
            self.first_linked_at,
            self.last_updated_at,
        )


def source_entity_id(chain_id: str, source_address: str) -> str:
    """Stable id for a (chain, exchange source) cohort.

    Deterministic, so a re-run over the same evidence writes the same id and
    ``ReplacingMergeTree`` collapses it instead of minting a new one. Addresses
    are lower-cased: EVM addressing is case-insensitive, the label set is not.
    """
    key = f"{chain_id.lower()}:{source_address.lower()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def support_bonus(cluster_size: int) -> float:
    """Score contribution from cluster size — log-scaled and capped.

    1 wallet -> 0.06, 8 -> 0.19, 100+ -> 0.20. A larger cluster is *weaker*
    evidence of shared ownership (a hot wallet funds everyone), so it caps early
    and can never dominate the tier.
    """
    if cluster_size < 1:
        return 0.0
    return min(_SUPPORT_CAP, 0.06 * math.log2(cluster_size + 1))


def edge_penalty(first_seen_epoch: int, window_start_epoch: int) -> float:
    """Penalty when the funding sits at the start of the data we hold."""
    if window_start_epoch <= 0:
        return 0.0
    lag = max(0, first_seen_epoch - window_start_epoch)
    if lag >= WINDOW_EDGE_SECONDS:
        return 0.0
    return _EDGE_PENALTY_MAX * (1.0 - lag / WINDOW_EDGE_SECONDS)


def score_confidence(
    evidence: Evidence,
    label: SourceLabel,
    cluster_size: int,
    window_start_epoch: int,
) -> float:
    """Bounded 0..1 float from evidence strength. Never a boolean.

    Components: tier base + log-scaled cluster support + label quality - window
    edge penalty. Clamped to ``[0.0, MAX_CONFIDENCE]``.
    """
    score = _BASE_SCORE.get(evidence.tier, 0.0)
    score += support_bonus(cluster_size)
    score += label.bonus
    score -= edge_penalty(evidence.first_seen_epoch, window_start_epoch)
    return round(min(MAX_CONFIDENCE, max(0.0, score)), 4)


def build_rows(
    evidence: list[Evidence],
    labels: dict[str, SourceLabel],
    window_starts: dict[str, int] | None = None,
) -> list[EntityRow]:
    """Turn raw evidence into scored rows; emits nothing for unlabelled sources.

    ``labels`` is keyed by lower-cased source address. ``last_updated_at`` comes
    from the evidence, not ``now()``, so a re-run writes an identical row and
    stays idempotent under ``ReplacingMergeTree``.
    """
    starts = window_starts or {}
    sizes: dict[tuple[str, str], int] = {}
    newest: dict[tuple[str, str], int] = {}
    for item in evidence:
        key = (item.chain_id, item.source_address.lower())
        sizes[key] = sizes.get(key, 0) + 1
        newest[key] = max(newest.get(key, 0), item.first_seen_epoch)

    rows: list[EntityRow] = []
    for item in evidence:
        key = (item.chain_id, item.source_address.lower())
        label = labels.get(key[1])
        if label is None or not label.is_exchange:
            continue
        rows.append(
            EntityRow(
                entity_id=source_entity_id(item.chain_id, item.source_address),
                wallet_address=item.wallet_address,
                chain_id=item.chain_id,
                heuristic_type="cex_deposit",
                confidence_score=score_confidence(item, label, sizes[key], starts.get(item.chain_id, 0)),
                entity_label=label.label_name,
                entity_category="exchange_cluster",
                first_linked_at=item.first_seen_epoch,
                last_updated_at=newest[key],
            )
        )
    return rows
