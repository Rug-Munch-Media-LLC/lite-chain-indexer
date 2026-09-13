"""Cross-provider consensus verification for critical RPC reads.

A single free/buggy RPC cannot be trusted: one malicious or misconfigured
provider in the pool can return wrong (empty, truncated, or fabricated)
data and silently corrupt the index.  For a sampled fraction of *critical*
reads we therefore dispatch the SAME request to N endpoints owned by
DISTINCT provider accounts, hash-canonicalize each ``result``, and accept
the answer only when a quorum agrees.

This is the client-side equivalent of the multi-upstream quorum that
managed RPC tiers (Goldsky/eRPC-class) provide, built on the existing
in-process pool:

* **Selection reuses the routing selector** (:func:`select_endpoint_for_method`
  / :func:`get_best_endpoint`) so archive-capability / health / tier
  filtering is identical to the normal read path.
* **Deduplication is by account** (:func:`account_key_for_endpoint`):
  two URLs of the same provider account count as ONE provider, never two.
* **Transport is not duplicated** — every leg goes through the caller's
  existing ``RpcClient.call`` (which already applies per-endpoint throttle,
  per-account concurrency/RPS caps, the circuit breaker and the ledger).
* Consensus is **sampled** (``RPC_CONSENSUS_SAMPLE_RATE``, default 5%) so
  normal throughput is unaffected; every verified/mismatched sample emits a
  counter so the cost is observable.

Fail-safe semantics:

* Quorum reached  → majority result returned, ``verified=True``.
* Successful results disagree and no quorum forms → the read FAILS
  (``mismatch=True``, ``result=None``); disagreeing endpoints are penalized
  through the pool health/score path.  Callers retry/escalate rather than
  committing one provider's answer.
* Fewer distinct providers than needed, or too few successful answers to
  judge → ``verified=False`` and no failure (the caller keeps its original
  result) so consensus availability never becomes a data-availability
  outage.

Env:
    RPC_CONSENSUS_ENABLED        1 to enable (default on)
    RPC_CONSENSUS_MIN_AGREEMENT  agreeing providers required (default 2)
    RPC_CONSENSUS_SAMPLE_RATE    fraction of reads verified (default 0.05)
    RPC_CONSENSUS_MAX_ENDPOINTS  max distinct providers queried (default 3)

Logs never contain raw endpoint URLs (which may embed API keys): endpoints
are identified by their non-secret account key (``provider_id:key_hash``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from lite_indexer.config import (
    RPC_CONSENSUS_ENABLED,
    RPC_CONSENSUS_MAX_ENDPOINTS,
    RPC_CONSENSUS_MIN_AGREEMENT,
    RPC_CONSENSUS_SAMPLE_RATE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.pool import record_endpoint_failure
from lite_indexer.rpc.provider_caps import account_key_for_endpoint

logger = get_logger(__name__)

#: Sentinel distinguishing a JSON-RPC ``result`` of ``null`` (a valid answer)
#: from a response we could not use (transport failure, JSON-RPC error).
_MISSING: object = object()


class RpcCaller(Protocol):
    """Minimal transport surface consensus needs (structurally RpcClient)."""

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = ...,
    ) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class ConsensusResult:
    """Outcome of a cross-provider verification.

    Attributes:
        result: The majority ``result`` value when ``verified``; else None.
        agreement: Size of the winning agreement group.
        total: Number of endpoints queried (distinct provider accounts).
        verified: True when ``agreement >= min_agreement``.
        mismatch: True when enough providers answered but none reached
            quorum — the read must fail.
        reason: Machine-readable outcome: ``ok``, ``mismatch``,
            ``insufficient_providers``, ``insufficient_evidence``,
            ``no_result`` or ``disabled``.
        hashes: Non-secret account label -> content hash for each endpoint
            that produced a usable answer.
    """

    result: object | None = None
    agreement: int = 0
    total: int = 0
    verified: bool = False
    mismatch: bool = False
    reason: str = "ok"
    hashes: Mapping[str, str] = field(default_factory=dict)


def should_sample() -> bool:
    """Whether this read should be consensus-verified.

    Returns False when consensus is disabled or the sample rate is <= 0;
    otherwise draws uniformly against :data:`RPC_CONSENSUS_SAMPLE_RATE`.
    """
    if not RPC_CONSENSUS_ENABLED:
        return False
    rate = RPC_CONSENSUS_SAMPLE_RATE
    if rate <= 0.0:
        return False
    if rate >= 1.0:
        return True
    return random.random() < rate


def canonical_hash(result: object) -> str:
    """Stable content hash of a JSON-RPC ``result`` value.

    Lists are sorted by their canonical JSON form so two providers that
    returned the same log set in a different order (or the same object with
    different key order) still hash identically. Non-JSON values fall back
    to ``str`` (``default=str``) rather than raising.
    """
    blob = json.dumps(
        _normalize(result),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


async def verify_across_providers(
    client: RpcCaller,
    chain: str,
    method: str,
    params: list[Any] | None,
    min_agreement: int = RPC_CONSENSUS_MIN_AGREEMENT,
    *,
    endpoints: list[str] | None = None,
    max_endpoints: int | None = None,
) -> ConsensusResult:
    """Query N distinct provider accounts and return the majority answer.

    Args:
        client: Transport exposing ``call(endpoint, method, params)``.
        chain: Chain name used to resolve the endpoint pool when
            ``endpoints`` is not supplied.
        method: JSON-RPC method.
        params: Method parameters (identical for every leg).
        min_agreement: Providers that must return the same ``result`` hash
            before the answer is accepted.
        endpoints: Explicit candidate pool (tests / callers that already
            hold a filtered pool). Defaults to the chain's pool.
        max_endpoints: Cap on distinct providers queried (defaults to
            ``RPC_CONSENSUS_MAX_ENDPOINTS``).

    Returns:
        :class:`ConsensusResult`; callers must treat ``mismatch`` as failure.
    """
    if not RPC_CONSENSUS_ENABLED:
        return ConsensusResult(reason="disabled")

    limit = max(1, int(max_endpoints if max_endpoints is not None else RPC_CONSENSUS_MAX_ENDPOINTS))
    pool = list(endpoints) if endpoints is not None else _chain_endpoints(chain)
    selected = _select_distinct_endpoints(pool, method=method, chain=chain, max_endpoints=limit)
    needed = max(2, int(min_agreement))

    if len(selected) < needed:
        logger.debug(
            "rpc_consensus_insufficient_providers",
            chain=chain,
            method=method,
            providers=len(selected),
            needed=needed,
        )
        return ConsensusResult(total=len(selected), reason="insufficient_providers")

    responses = await asyncio.gather(
        *(client.call(endpoint, method, params) for endpoint in selected),
        return_exceptions=True,
    )

    groups: dict[str, list[str]] = {}
    payloads: dict[str, object] = {}
    labels: dict[str, str] = {}
    for endpoint, response in zip(selected, responses, strict=False):
        result = _extract_result(response)
        if result is _MISSING:
            continue
        content_hash = canonical_hash(result)
        groups.setdefault(content_hash, []).append(endpoint)
        payloads.setdefault(content_hash, result)
        labels[_endpoint_label(endpoint)] = content_hash

    successful = sum(len(group) for group in groups.values())
    if successful < needed:
        # Not enough usable answers to judge — fail-open, never fail the read.
        return ConsensusResult(total=len(selected), reason="insufficient_evidence", hashes=labels)

    winning_hash, winners = max(groups.items(), key=lambda item: len(item[1]))

    if len(winners) >= needed:
        metrics_incr("rpc_consensus_verified")
        logger.info(
            "rpc_consensus_verified",
            chain=chain,
            method=method,
            agreement=len(winners),
            total=len(selected),
        )
        return ConsensusResult(
            result=payloads[winning_hash],
            agreement=len(winners),
            total=len(selected),
            verified=True,
            reason="ok",
            hashes=labels,
        )

    metrics_incr("rpc_consensus_mismatch")
    _penalize_disagreeing(groups, winning_hash)
    logger.warning(
        "rpc_consensus_mismatch",
        chain=chain,
        method=method,
        providers=len(selected),
        agreement=len(winners),
        needed=needed,
        endpoints=sorted(labels),
        hashes=dict(labels),
    )
    return ConsensusResult(
        agreement=len(winners),
        total=len(selected),
        mismatch=True,
        reason="mismatch",
        hashes=labels,
    )


# ── Internals ──────────────────────────────────────────────────────────


def _chain_endpoints(chain: str) -> list[str]:
    """Resolve a chain's configured endpoint pool (never raises)."""
    if not chain:
        return []
    try:
        from lite_indexer.chains import GET_LOGS_ENDPOINTS

        return list(GET_LOGS_ENDPOINTS.get(chain, []))
    except Exception as exc:  # noqa: BLE001 — pool lookup must not break verification
        logger.debug("rpc_consensus_chain_lookup_failed", chain=chain, error=str(exc)[:80])
        return []


def _select_distinct_endpoints(
    endpoints: list[str],
    *,
    method: str,
    chain: str | None,
    max_endpoints: int,
) -> list[str]:
    """Pick up to ``max_endpoints`` endpoints, one per provider account.

    Reuses the routing selector for health/tier/archive ranking, but walks
    the pool account-by-account so two URLs behind the same provider account
    are never counted as independent sources (they share a budget and a
    failure domain — a compromised account controls both).
    """
    from lite_indexer.rpc.routing import select_endpoint_for_method

    selected: list[str] = []
    used_accounts: set[str] = set()
    remaining = [ep for ep in endpoints if ep and "${" not in ep]

    while remaining and len(selected) < max_endpoints:
        candidates = [ep for ep in remaining if account_key_for_endpoint(ep) not in used_accounts]
        if not candidates:
            break
        try:
            pick = select_endpoint_for_method(method, candidates, chain=chain)
        except Exception as exc:  # noqa: BLE001 — selection fallback is first candidate
            logger.debug("rpc_consensus_select_fallback", chain=chain, method=method, error=str(exc)[:80])
            pick = candidates[0]
        if pick not in candidates:
            pick = candidates[0]
        selected.append(pick)
        used_accounts.add(account_key_for_endpoint(pick))
        remaining = [ep for ep in remaining if ep != pick]

    return selected


def _extract_result(response: object) -> object:
    """Return a response's ``result`` or :data:`_MISSING` when unusable."""
    if not isinstance(response, dict):
        return _MISSING
    if response.get("error") is not None:
        return _MISSING
    if "result" not in response:
        return _MISSING
    return response["result"]


def _penalize_disagreeing(groups: dict[str, list[str]], winning_hash: str) -> None:
    """Feed every endpoint outside the modal group through the pool pointer."""
    for content_hash, endpoints in groups.items():
        if content_hash == winning_hash:
            continue
        for endpoint in endpoints:
            try:
                record_endpoint_failure(endpoint)
            except Exception as exc:  # noqa: BLE001 — penalty is best-effort
                logger.debug("rpc_consensus_penalty_skipped", error=str(exc)[:80])


def _endpoint_label(endpoint: str) -> str:
    """Non-secret endpoint identity for logs (domain + hashed key id)."""
    try:
        return account_key_for_endpoint(endpoint)
    except Exception:  # noqa: BLE001 — label must never break verification
        return "unknown"


def _normalize(value: object) -> object:
    """Canonicalize a JSON value for stable hashing (order-insensitive lists)."""
    if isinstance(value, list):
        items = [_normalize(item) for item in value]
        return sorted(items, key=_stable_key)
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    return value


def _stable_key(value: object) -> str:
    """Sort key for canonical list ordering."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


__all__ = [
    "ConsensusResult",
    "RpcCaller",
    "canonical_hash",
    "should_sample",
    "verify_across_providers",
]
