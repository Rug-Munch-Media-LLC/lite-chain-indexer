"""Classify why a chain cannot advance, so the stall is named, not guessed.

Two failure shapes used to produce the SAME log line
(``reorg_status_unknown`` / "tip block hash unavailable") and the SAME
alert, even though they need opposite operator responses:

* the chain is merely BEHIND, and the tip-hash read raced past the window
  its endpoints keep — retry on the next cycle and it resolves;
* the WHOLE POOL refuses us, and no amount of retrying will help until the
  operator changes an endpoint, a key, or the client identity.

:func:`classify_pool_refusal` takes the per-endpoint verdicts a chain's
tip-hash read already collected and returns the verdict plus the reason
string that goes into the log and the alert. It is pure: no network, no
clock, no client state — which is the point, because the case we most need
to be sure about is the one we can least afford to diagnose live.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

#: Why the tip-hash read found nothing. ``refused`` is the case that is a
#: dead end until a human intervenes; ``unserved`` is transient.
RefusalKind = Literal["none", "refused", "unserved"]

#: Status codes that mean "this provider will not serve us", as opposed to
#: "this provider is having a bad moment". 401/403 are identity or policy
#: refusals; 402/451 are the same class (billing / legal takedown).
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger("indexer.rpc.pool_refusal")

REFUSAL_STATUSES: frozenset[int] = frozenset({401, 402, 403, 451})


@dataclass(frozen=True, slots=True)
class PoolRefusal:
    """Verdict for one chain's failed tip-hash read.

    Attributes:
        kind: ``"refused"`` when every endpoint that answered did so with a
            refusal status, ``"unserved"`` when endpoints answered 200 with
            a null/garbage body (pruned or archive-gated window), and
            ``"none"`` when at least one endpoint produced a usable hash.
        reason: Short, stable reason string for logs and alert labels.
        tried: How many endpoints were dialled.
        refused: How many came back with a refusal status.
        unserved: How many answered 200 without a block hash.
    """

    kind: RefusalKind
    reason: str
    tried: int
    refused: int
    unserved: int

    @property
    def all_refused(self) -> bool:
        """Whether every endpoint that answered refused us outright."""
        return self.kind == "refused" and self.tried > 0 and self.refused == self.tried


def classify_pool_refusal(statuses: list[int | None]) -> PoolRefusal:
    """Classify a set of per-endpoint HTTP statuses from one tip-hash sweep.

    ``None`` marks an endpoint that failed at the transport level (timeout,
    connection error) — it is neither a refusal nor a successful answer, so
    it is counted in ``tried`` only. A 200 means the endpoint answered but
    returned no usable block (a pruned or archive-gated window), which is a
    different problem from a refusal and must not be reported as one.

    Args:
        statuses: HTTP status per endpoint, in the order tried. ``None`` for
            an endpoint that never produced a response.

    Returns:
        A :class:`PoolRefusal`. ``kind == "none"`` when nothing was tried or
        at least one endpoint answered 200 — the caller then has a usable
        hash and this verdict is informational.
    """
    tried = len(statuses)
    refused = sum(1 for s in statuses if s is not None and s in REFUSAL_STATUSES)
    unserved = sum(1 for s in statuses if s == 200)
    if tried == 0:
        return PoolRefusal(kind="none", reason="no_endpoints", tried=0, refused=0, unserved=0)
    # `tried > 1` is load-bearing: with one endpoint there are no peers, so a
    # lone 403 is a mixed outage, not the pool refusing us. Without it a
    # single unlucky endpoint pages a condition the other four would serve.
    if refused == tried and tried > 1:
        return PoolRefusal(
            kind="refused",
            reason="pool_refused_by_provider",
            tried=tried,
            refused=refused,
            unserved=unserved,
        )
    if unserved:
        return PoolRefusal(
            kind="unserved",
            reason="no_endpoint_served_block",
            tried=tried,
            refused=refused,
            unserved=unserved,
        )
    # Everything either refused or died on the wire, with no 200 in sight:
    # a mixed outage rather than a policy refusal. Do not call it "refused".
    return PoolRefusal(
        kind="none",
        reason="pool_unreachable",
        tried=tried,
        refused=refused,
        unserved=unserved,
    )




def report_exhausted(chain: str, block: int, tried: Sequence[str], statuses: Sequence[int | None]) -> None:
    """Name WHY the pool could not serve ``block``, in the log and metrics.

    The distinction the stall hinged on: ``200`` with no block is *not* a
    refusal — the retained window does not reach this block. 401/402/403/451
    is a refusal, and no retry fixes that without an operator.
    """
    verdict = classify_pool_refusal(list(statuses))
    logger.error(
        "block_hash_unserved",
        chain=chain,
        block=block,
        endpoints=len(tried),
        kind=verdict.kind,
        reason=verdict.reason,
    )
    metrics_incr("rpc_block_hash_unserved")
    if verdict.all_refused:
        logger.error("block_hash_pool_refused", chain=chain, endpoints=len(tried))


__all__ = [
    "REFUSAL_STATUSES",
    "PoolRefusal",
    "RefusalKind",
    "classify_pool_refusal",
    "report_exhausted",
]
