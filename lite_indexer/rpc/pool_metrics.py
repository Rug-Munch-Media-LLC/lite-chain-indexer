"""Honest RPC-pool capacity gauges: count what can actually serve logs.

The gauge this module produces used to answer "does the endpoint answer
``eth_blockNumber``" — and later "did it accept one synthetic tip-range
shape". Both lied. On 2026-10-23 bsc read 8/13 healthy while ZERO endpoints
could serve the span the sync loop needed, then read 0 while one endpoint
worked at a 10-block cap. A pruned or range-capped endpoint is alive but
useless; a working 10-block endpoint is NOT zero capacity.

Usable capacity is therefore measured per endpoint as the span it can
really serve, not as a boolean of liveness:

  - banned / health-failed / getLogs-probe-failed -> 0
  - record.logs_capable is False                  -> 0 (capability refusal)
  - record.logs_capable is True                   -> its measured span
  - record.getlogs_max_range > 0                  -> that span (working,
    even when the broad tip-shape probe was transient/unknown)
  - live probe passed, span unmeasured            -> PROBE_SPAN_BLOCKS

The per-chain gauges are the count of usable endpoints, the largest usable
span, and the summed usable span (total per-call capacity — the catch-up
signal that was missing while "healthy" hid a 10-block crawl).
"""

from __future__ import annotations

from collections.abc import Mapping

from lite_indexer.logging import get_logger
from lite_indexer.metrics import set_labeled_gauge

logger = get_logger(__name__)


def usable_span(endpoint: str) -> int:
    """Blocks per ``eth_getLogs`` call ``endpoint`` can serve (0 = unusable).

    Args:
        endpoint: RPC endpoint URL as configured (keyed URLs included).

    Returns:
        A positive span in blocks when the endpoint is usable right now,
        otherwise 0. The span is the measured/conservative lower bound, so a
        caller can trust ``span > 0`` as "can serve logs".
    """
    from lite_indexer.rpc.ban_state import is_endpoint_banned
    from lite_indexer.rpc.capabilities import get_capability, peek_capability
    from lite_indexer.rpc.health import is_endpoint_healthy
    from lite_indexer.rpc.health_logs import PROBE_SPAN_BLOCKS, logs_probe_verdict

    if is_endpoint_banned(endpoint) or not is_endpoint_healthy(endpoint):
        return 0
    verdict = logs_probe_verdict(endpoint)
    if verdict is False:
        return 0
    record = peek_capability(endpoint) or get_capability(endpoint)
    if record is not None:
        if record.logs_capable is False:
            return 0
        if record.getlogs_max_range > 0:
            return int(record.getlogs_max_range)
        if record.logs_capable is True:
            return int(PROBE_SPAN_BLOCKS)
    if verdict is True:
        return int(PROBE_SPAN_BLOCKS)
    return 0


def publish_pool_metrics(chain_endpoints: Mapping[str, list[str]] | None = None) -> dict[str, int]:
    """Publish the per-chain usable / max-span / usable-span gauges.

    Args:
        chain_endpoints: Optional chain -> endpoints map (tests); defaults to
            the configured ``GET_LOGS_ENDPOINTS``.

    Returns:
        Usable-endpoint count per chain (tests and callers that log it).
    """
    if chain_endpoints is None:
        from lite_indexer.chains import GET_LOGS_ENDPOINTS

        chain_endpoints = GET_LOGS_ENDPOINTS

    counts: dict[str, int] = {}
    for chain, endpoints in chain_endpoints.items():
        count = max_span = total_span = 0
        for endpoint in endpoints:
            span = usable_span(endpoint)
            if span <= 0:
                continue
            count += 1
            max_span = max(max_span, span)
            total_span += span
        set_labeled_gauge("lite_indexer_rpc_pool_healthy_endpoints", count, chain=chain)
        # Catch-up signal: one working 10-block endpoint means ~10^6 calls for
        # 9.2M blocks. Without the span gauges the pool "looks healthy" while
        # the chain advances 10 blocks at a time (the silent half of the
        # 48-day bsc stall).
        set_labeled_gauge("lite_indexer_rpc_pool_max_span", max_span, chain=chain)
        set_labeled_gauge("lite_indexer_rpc_pool_usable_span", total_span, chain=chain)
        counts[chain] = count
        logger.debug(
            "rpc_pool_metrics",
            chain=chain,
            usable=count,
            configured=len(endpoints),
            max_span=max_span,
            usable_span=total_span,
        )
    return counts
