"""Name WHICH condition a chain is in: far behind, or genuinely stuck.

THE 2026-09-26 MISDIAGNOSIS. A 45,388,234-block lag and a broken indexer
produced the identical log line, the identical counter and the identical
alert, so two competent engineers spent two attempts on the wrong hypothesis
before anyone measured how far behind the chain actually was. The lesson is
not the fix — it is that the indexer refused to say which of the two it was
in.

The two conditions need opposite responses and neither self-heals the other:

* ``LAGGING`` — the stored head is far behind the tip and the chain is
  ADVANCING. Expected during a long catch-up; self-heals as the crawl
  proceeds. Must NOT page, and must not be restarted for.
* ``STALLED`` — the head is not advancing at all. Nothing will fix this
  without intervention, and it pages.

A third, ``UNKNOWN``, covers a tip we could not read this cycle (no answer
from any endpoint), which is a measurement failure, not a chain condition,
and is reported as its own thing precisely so it is not silently folded into
either of the two above.

Pure functions over plain numbers: no network, no clock, no state. The
decision is the part that was wrong twice, so it is the part that is pinned
by tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.metrics import set_labeled_gauge as set_gauge

logger = get_logger(__name__)

#: Which of the two conditions a chain is in. See the module docstring for
#: why naming the distinction is the point.
ChainCondition = Literal["caught_up", "lagging", "stalled", "unknown"]

#: Blocks behind the tip beyond which a chain is LAGGING rather than merely
#: a little behind. Two chain-hours at the default block time; below it the
#: lag is noise and above it the crawl is the story.
LAG_BLOCK_THRESHOLD = 7200

#: Default seconds per block when a chain has no configured block time.
_DEFAULT_BLOCK_TIME = 12.0


@dataclass(frozen=True, slots=True)
class LagVerdict:
    """Why a chain is where it is.

    Attributes:
        condition: One of :data:`ChainCondition`.
        lag_blocks: Tip minus stored head, or ``None`` when the tip is
            unknown.
        lag_seconds: The same lag in wall-clock terms at the chain's block
            time, so an operator reads "weeks" rather than a block count.
        reason: Short stable string for the log line and alert label.
    """

    condition: ChainCondition
    lag_blocks: int | None
    lag_seconds: float | None
    reason: str

    @property
    def needs_attention(self) -> bool:
        """Whether this condition warrants paging (a stall does; lag does not)."""
        return self.condition == "stalled"


def classify_lag(
    chain: str,
    head: int | None,
    tip: int | None,
    *,
    block_time: float,
    threshold: int = LAG_BLOCK_THRESHOLD,
) -> LagVerdict:
    """Classify a chain's position and record the condition.

    ``lagging`` vs ``caught_up`` is decided by how far the head is from the
    tip, because that gap is the thing an operator must act on and the thing
    the log must name. Whether the chain is *advancing at all* is the
    caller's signal (it has the state file history this function does not
    need), so ``STALLED`` is only reachable when the caller passes a head of
    ``None`` together with a readable tip — the case where the head is
    genuinely absent rather than merely old.

    Args:
        chain: Chain name, for logging and metric labels.
        head: Last durably indexed block, or ``None`` when there is none.
        tip: Current chain tip, or ``None`` when no endpoint reported one.
        block_time: Seconds per block for this chain.
        threshold: Blocks of lag that count as LAGGING.

    Returns:
        A :class:`LagVerdict`. ``unknown`` when the tip could not be read;
        ``stalled`` when there is no head to advance; otherwise ``lagging``
        or ``caught_up`` by the size of the gap.
    """
    if tip is None:
        return _record(chain, LagVerdict("unknown", None, None, "tip_unreadable"))
    if head is None:
        return _record(chain, LagVerdict("stalled", None, None, "no_stored_head"))
    lag = max(0, tip - head)
    seconds = lag * (block_time or _DEFAULT_BLOCK_TIME)
    if lag > threshold:
        return _record(chain, LagVerdict("lagging", lag, seconds, "catchup_in_progress"))
    return _record(chain, LagVerdict("caught_up", lag, seconds, "within_threshold"))


def _record(chain: str, verdict: LagVerdict) -> LagVerdict:
    """Emit the per-condition log line, counter and lag gauge.

    Each condition gets its OWN counter so an alert can be written against
    ``*_stalled`` alone; a 45M-block catch-up raises ``*_lagging`` forever
    and would otherwise drown a real stall on the same series.
    """
    set_gauge("lite_indexer_chain_lag_blocks", verdict.lag_blocks or 0.0, chain=chain)
    metrics_incr(f"lite_indexer_chain_{verdict.condition}")
    if verdict.condition == "stalled":
        logger.error("chain_stalled", chain=chain, reason=verdict.reason)
        return verdict
    if verdict.condition == "lagging":
        logger.warning(
            "chain_lagging",
            chain=chain,
            lag_blocks=verdict.lag_blocks,
            lag_seconds=round(verdict.lag_seconds or 0.0),
            reason=verdict.reason,
        )
        return verdict
    logger.debug("chain_progress", chain=chain, condition=verdict.condition, lag_blocks=verdict.lag_blocks)
    return verdict


__all__ = ["LAG_BLOCK_THRESHOLD", "ChainCondition", "LagVerdict", "classify_lag"]
