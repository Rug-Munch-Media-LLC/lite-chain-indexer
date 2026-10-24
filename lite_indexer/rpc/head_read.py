"""Hedged head reads, with an explicitly labelled stale fallback.

Why this module exists: the tip used to be read from one endpoint at a time
(``get_best_endpoint`` then ``eth_blockNumber``), so a single slow-but-alive
provider stalled the whole sync loop, and a pool that was fully down returned
``None`` — indistinguishable from "no new blocks". Both are fail-closed
silences.

Rules encoded here:

* **Hedging is for the tip only.** ``HEAD_METHODS`` is an allowlist; every
  other method is refused before a request is built. A hedged log-range read
  or a hedged block fetcher would race two providers over the same cursor
  step, which is how duplicate/rewound indexing happens. Cursor writes are
  never hedged, by contract, not by configuration.
* **First valid answer wins.** "Valid" means a positive integer; a malformed
  or empty answer is a failure for that leg and is recorded as one.
* **Never an empty success.** When every leg fails, the last-known-good tip is
  served with ``stale=True`` and its exact age, or ``RpcUnavailable`` is
  raised. There is no third outcome.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr
from lite_indexer.rpc import last_good
from lite_indexer.rpc.hedging import hedged_rpc
from lite_indexer.rpc.pool import record_endpoint_failure, record_endpoint_success
from lite_indexer.rpc.rpc_unavailable import RpcUnavailable

logger = get_logger(__name__)

#: Methods a hedge may be spent on. Anything else is single-shot by contract.
HEAD_METHODS = frozenset({"eth_blockNumber", "eth_getBlockByNumber", "blockNumber"})

#: How old a last-known-good tip may be before it is refused instead of served.
MAX_STALE_AGE_S = float(os.getenv("RPC_HEAD_MAX_STALE_AGE", "120"))


@dataclass(frozen=True, slots=True)
class HeadReadSpec:
    """Knobs for one head read (kept in one object to keep the call small).

    Attributes:
        method: Must be in :data:`HEAD_METHODS`.
        hedge_delay_s: Override the hedge delay (default from ``hedging``).
        max_stale_age_s: Override the staleness ceiling.
    """

    method: str = "eth_blockNumber"
    hedge_delay_s: float | None = None
    max_stale_age_s: float | None = None


@dataclass(frozen=True, slots=True)
class HeadReading:
    """The tip the caller gets, and whether it is actually live.

    Attributes:
        value: Block number (live, or last-known-good when ``stale``).
        stale: True when a provider could not be reached for this read.
        age_s: Seconds since the value was confirmed; ``0.0`` when live.
        endpoint: Endpoint that answered, or ``None`` for a stale value.
    """

    value: int
    stale: bool
    age_s: float
    endpoint: str | None = None


def _valid(value: Any) -> int | None:
    """Return ``value`` as a positive int, or ``None`` when it is not usable."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


async def read_head(
    client: Any,
    chain: str,
    endpoints: list[str],
    spec: HeadReadSpec | None = None,
) -> HeadReading:
    """Read the chain tip, hedging across two providers.

    Args:
        client: Object exposing ``eth_block_number(endpoint) -> int | None``.
        chain: Chain name (keys the last-known-good store).
        endpoints: Candidate pool; empty is a valid input (stale or raise).
        spec: Optional overrides (see :class:`HeadReadSpec`).

    Returns:
        A :class:`HeadReading`; ``stale`` is True when no provider answered.

    Raises:
        ValueError: The spec's method is not a head method (writes are never
            hedged).
        RpcUnavailable: every provider failed and no acceptable stale value
            exists.
    """
    opts = spec or HeadReadSpec()
    if opts.method not in HEAD_METHODS:
        raise ValueError(f"read_head refuses non-head method {opts.method!r}: hedging is for the tip only")

    async def _leg(endpoint: str) -> int | None:
        try:
            value = _valid(await client.eth_block_number(endpoint))
        except Exception as exc:  # noqa: BLE001 — a failed leg is a decision, not a crash
            logger.debug("head_leg_failed", chain=chain, endpoint=endpoint[:50], error=str(exc)[:60])
            value = None
        if value is None:
            record_endpoint_failure(endpoint)
            return None
        record_endpoint_success(endpoint)
        return value

    value = await hedged_rpc(_leg, endpoints, delay_s=opts.hedge_delay_s) if endpoints else None
    if value is not None:
        last_good.remember(chain, value)
        incr("rpc_head_reads")
        return HeadReading(value=value, stale=False, age_s=0.0, endpoint=None)

    ceiling = MAX_STALE_AGE_S if opts.max_stale_age_s is None else opts.max_stale_age_s
    entry = last_good.recall(chain, max_age_s=ceiling)
    if entry is not None:
        age = entry.age_s()
        incr("rpc_stale_served")
        logger.warning("rpc_head_serving_stale", chain=chain, age_s=round(age, 1), tip=entry.value)
        return HeadReading(value=entry.value, stale=True, age_s=age)

    incr("rpc_head_unavailable")
    logger.error("rpc_head_unavailable", chain=chain, endpoints=len(endpoints))
    raise RpcUnavailable(chain, age_s=None, detail="no provider answered and no last-known-good tip")


__all__ = ["HEAD_METHODS", "MAX_STALE_AGE_S", "HeadReadSpec", "HeadReading", "read_head"]
