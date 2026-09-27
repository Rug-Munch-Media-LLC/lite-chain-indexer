"""Read a block hash from a pool that only PART of the endpoints can serve.

THE 2026-09-26 HYPERLIQUID FORWARD STALL. ``reorg_stage`` asks for the hash
of the *stored head*, not the tip. With the head 45,388,234 blocks behind the
tip, that request lands outside the ~200k-block window the public gateways
retain, and they answer ``{"result": null}`` on a 200 — a perfectly healthy
endpoint that simply does not have that block.

The earlier selector asked ONE endpoint chosen by ``get_best_endpoint``,
which ends in a random power-of-two-choices draw, so over many cycles roughly
two thirds of the reads hit an endpoint that cannot answer. The chain stalled
on average every third cycle, and reordering the pool could never fix it:
order does not decide a random draw.

THE HONEST MODEL IS PER-BLOCK CAPABILITY, NOT TIP CAPABILITY. Whether an
endpoint can serve a given block is a property of the block, not of the
provider. ``tip_capable`` encodes the opposite question ("can it hash the
tip?") and cannot express "serves block 1576295, does not serve 46964529".
:func:`resolve_block_hash` therefore tries candidates in order and returns
the first endpoint that produced a real hash, recording each attempt so the
call site can name the failure instead of guessing at it.

Candidate ORDER is deterministic too (see :func:`_ordered_candidates`), so a
chain whose pool retains history converges on the endpoints that work rather
than re-rolling a coin every cycle.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import block_serving
from lite_indexer.rpc.pool_refusal import report_exhausted

if TYPE_CHECKING:
    from collections.abc import Sequence


logger = get_logger(__name__)

#: How many endpoints one block-hash read may dial before giving up. The
#: pool is small (single digits) and a null answer is cheap, so the bound is
#: generous enough to cover a pool where most endpoints lack the block.
MAX_BLOCK_READ_ATTEMPTS = 6


class BlockHashReader(Protocol):
    """The one capability this module needs from an RPC client.

    Named and structural (not the concrete ``RpcClient``) so a caller can
    inject a stand-in and so this module never grows an import cycle on the
    client facade.

    ``eth_get_block_by_number_with_status`` returns ``(block, status)``. The
    status is not decoration: it is what separates a 200-with-a-pruned-body
    (a window boundary, transient) from a 401/402/403/451 (a provider
    refusal that only a human fixes). See :mod:`lite_indexer.rpc.pool_refusal`.
    """

    async def eth_get_block_by_number_with_status(
        self,
        endpoint: str,
        block_number: int,
        full_tx: bool = False,
        **kwargs: Any,
    ) -> tuple[dict[str, Any] | None, int | None]: ...


@dataclass(frozen=True, slots=True)
class BlockHashResult:
    """Outcome of one pool-wide block-hash read.

    Attributes:
        block_hash: The hash, or ``None`` when no endpoint served the block.
        endpoint: Which endpoint answered (recorded even on failure — it is
            the next candidate to try, not a random redraw).
        tried: Endpoints dialled, in attempt order.
        served: How many returned a real hash (1 on success).
        last_status: HTTP status of the final attempt, or ``None`` when the
            attempt died on the wire. Drives the refusal classifier.
    """

    block_hash: str | None
    endpoint: str | None
    tried: tuple[str, ...]
    served: int
    last_status: int | None

    @property
    def ok(self) -> bool:
        """Whether some endpoint produced a usable hash."""
        return self.block_hash is not None


def _ordered_candidates(chain: str, endpoints: Sequence[str], block: int) -> list[str]:
    """Return the pool ordered by demonstrated ability to serve ``block``.

    Endpoints that have answered for a block inside this chain's current
    served window lead; everything else keeps its configured order behind
    them. The order is a pure function of recorded evidence and the pool —
    no draw — so consecutive cycles converge instead of re-rolling.
    """
    return block_serving.order_by_served_range(chain, endpoints, block)


async def resolve_block_hash(
    rpc: BlockHashReader,
    chain: str,
    block: int,
    endpoints: Sequence[str],
    *,
    max_attempts: int = MAX_BLOCK_READ_ATTEMPTS,
) -> BlockHashResult:
    """Return the hash of ``block`` from whichever pool endpoint has it.

    Tries the pool in order and stops at the first endpoint that returns a
    block carrying a hash. Every endpoint that answers ``200`` with no block
    is recorded as *served nothing*, not as a failure: that is a window
    boundary, and the next candidate is the correct response to it.

    Args:
        rpc: Client used for the read (injected; never constructed here).
        chain: Chain name — selects the recorded serving evidence.
        block: Block number to read.
        endpoints: The chain's pool, in configured order.
        max_attempts: Cap on endpoints dialled for one read.

    Returns:
        A :class:`BlockHashResult`. ``last_status`` is set to ``403``-class
        values only when an endpoint actually answered with one, so the
        caller's refusal classification stays honest.
    """
    if not endpoints:
        return BlockHashResult(None, None, (), 0, None)

    tried: list[str] = []
    statuses: list[int | None] = []
    for endpoint in _ordered_candidates(chain, endpoints, block)[:max_attempts]:
        tried.append(endpoint)
        body, status = await _read(rpc, endpoint, block)
        # The REAL status, never a placeholder: a 200 here is the pruned
        # window (transient), a 403 is a refusal (only a human fixes it).
        statuses.append(status)
        if body is None:
            logger.debug("block_hash_not_served", chain=chain, block=block, endpoint_label=endpoint[:40])
            continue
        block_serving.record_served(chain, endpoint, block)
        logger.info("block_hash_resolved", chain=chain, block=block, attempts=len(tried))
        metrics_incr("rpc_block_hash_fallback")
        return BlockHashResult(str(body["hash"]), endpoint, tuple(tried), 1, status)

    block_serving.record_unserved(chain, tried, block)
    report_exhausted(chain, block, tried, statuses)
    return BlockHashResult(None, None, tuple(tried), 0, statuses[-1] if statuses else None)


async def _read(
    rpc: BlockHashReader,
    endpoint: str,
    block: int,
) -> tuple[dict[str, Any] | None, int | None]:
    """Return ``(block_body, http_status)`` for one endpoint.

    ``http_status`` is the status that actually answered, or ``None`` when
    the attempt never got a response. A body is only returned when it is a
    mapping carrying a non-empty ``hash``; anything else is ``None`` with
    the status preserved, so a 200-with-null stays a 200 and cannot be
    mistaken for a refusal.

    Any transport or decode failure is treated as "this endpoint cannot
    answer right now" (``(None, None)``) — the next candidate is tried.
    That is deliberate: a read that raises must not abort a stage that a
    working peer can complete.
    """
    try:
        body, status = await rpc.eth_get_block_by_number_with_status(endpoint, block)
    except (OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
        logger.debug("block_read_failed", endpoint_label=endpoint[:40], error=str(exc)[:80])
        return None, None
    if not isinstance(body, dict):
        return None, status
    with contextlib.suppress(TypeError, ValueError):
        if str(body.get("hash") or "").strip():
            return body, status
    return None, status


__all__ = ["MAX_BLOCK_READ_ATTEMPTS", "BlockHashReader", "BlockHashResult", "resolve_block_hash"]
