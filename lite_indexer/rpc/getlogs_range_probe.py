"""Bounded eth_getLogs range ladder probe.

Split out of capabilities.py (2026-10-24): the rung walk plus its per-rung
timeout budget. Behaviour is unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lite_indexer.logging import get_logger
from lite_indexer.rpc.capability_classify import _GetLogsProbeStatus
from lite_indexer.rpc.capability_constants import (
    GETLOGS_PROBE_CANDIDATES,
    GETLOGS_PROBE_MAX_CALLS,
    GETLOGS_PROBE_RUNG_TIMEOUT_CAP,
    GETLOGS_PROBE_TIMEOUT,
)
from lite_indexer.rpc.capability_ladder import (
    conservative_bound,
    ladder_rungs,
    probe_timeout_for,
)
from lite_indexer.rpc.getlogs_probe import _probe_getlogs_once

if TYPE_CHECKING:  # pragma: no cover — annotation-only, avoids import cycle
    from lite_indexer.rpc.client import RpcClient

logger = get_logger(__name__)


async def probe_getlogs_range(
    client: RpcClient,
    endpoint: str,
    topic0: str,
    latest_block: int | None = None,
    timeout: float = GETLOGS_PROBE_TIMEOUT,
) -> int:
    """Probe a conservative eth_getLogs max range within a fixed call budget.

    Ascends :data:`GETLOGS_PROBE_CANDIDATES` from small to large and stops at
    the first rung that fails (or once :data:`GETLOGS_PROBE_MAX_CALLS` calls
    are spent). The returned value is a **conservative lower bound**, not the
    exact maximum: routing only needs a safe chunk size, and binary-searching
    the exact max added up to 8 extra round-trips — each up to the probe
    timeout — which was the dominant startup cost (measured ~40 s/chain on
    bsc, 2026-09-14).

    A client-side timeout is treated as a range error, so a slow endpoint gets
    a conservative range. A ``0`` return means even the smallest candidate
    failed (the endpoint cannot serve logs at any probed size).

    Args:
        client: RpcClient instance (used only for tip discovery).
        endpoint: RPC endpoint URL to probe.
        topic0: Event topic used for the probe filter.
        latest_block: Chain tip to anchor probe ranges against. When
            omitted, discovered via ``eth_blockNumber``; when that fails
            the probe anchors at block 1 (best-effort).
        timeout: Per-request probe timeout in seconds.

    Returns:
        A conservative largest accepted range size (0 when the first
        candidate failed).
    """
    anchor = latest_block
    if anchor is None:
        anchor = await client.eth_block_number(endpoint)
    if anchor is None:
        anchor = 1

    calls = 0
    accepted: list[int] = []
    for size in ladder_rungs(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS):
        calls += 1
        from_block = max(1, anchor - size + 1)
        # Per-rung budget: a 100-block answer cannot be measured with the
        # 10-block timeout (measured 23-35 s on bsc against 2.5 s).
        rung_timeout = probe_timeout_for(
            size,
            base_timeout=timeout,
            base_span=GETLOGS_PROBE_CANDIDATES[0],
            cap=GETLOGS_PROBE_RUNG_TIMEOUT_CAP,
        )
        status = await _probe_getlogs_once(endpoint, from_block, anchor, topic0, timeout=rung_timeout)
        if status == _GetLogsProbeStatus.OK:
            accepted.append(size)
            continue
        # First failure bounds the range — keep accepted rungs, stop spending.
        bound = conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, accepted)
        logger.debug(
            "capability_getlogs_range_bounded",
            endpoint=endpoint[:60],
            max_range=bound,
            failed_at=size,
            calls=calls,
        )
        return bound

    # No failure observed within the budget — every probed candidate was
    # accepted; the smallest accepted rung is the conservative ceiling.
    bound = conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, accepted)
    logger.debug(
        "capability_getlogs_range_ceiling",
        endpoint=endpoint[:60],
        max_range=bound,
        calls=calls,
    )
    return bound
