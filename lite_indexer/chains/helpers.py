"""
Helper functions for endpoint selection based on rate limits and ranges.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .limits import DEFAULT_MAX_RANGE, DEFAULT_RATE_LIMIT, MAX_BLOCK_RANGE, PROVIDER_RATE_LIMITS


def get_provider_rate_limit(endpoint: str) -> dict[str, int]:
    """Get rate limits for a given endpoint URL based on provider pattern.

    Args:
        endpoint: Full RPC URL.

    Returns:
        Dict with 'req_per_sec' and 'burst' keys.
    """
    domain = urlparse(endpoint).hostname or ""
    for pattern, limits in PROVIDER_RATE_LIMITS.items():
        if pattern in domain:
            return limits
    return dict(DEFAULT_RATE_LIMIT)


def _measured_max_block_range(endpoint: str) -> int | None:
    """Measured eth_getLogs max range for ``endpoint`` (None when unknown).

    Reads the capability registry populated by the startup probe. Imported
    lazily to avoid a ``chains -> rpc`` import cycle at module load; a broken
    registry degrades to the static provider limit.
    """
    try:
        from lite_indexer.rpc.capabilities import peek_capability
    except Exception:  # noqa: BLE001 — cycle/import guard, best-effort
        return None
    try:
        record = peek_capability(endpoint)
    except Exception:  # noqa: BLE001 — registry failure must not break routing
        return None
    if record is None:
        return None
    if record.getlogs_max_range <= 0:
        # A PROBED zero is a verdict (cannot serve logs at the smallest
        # probed span), not an unknown: the ladder first rung (10 blocks,
        # production shape) failed. Returning None here made a probe-and-fail
        # endpoint look UNLIMITED, so it stayed in every getLogs rotation,
        # failed, opened its circuit, and dragged the chain to pool-fully-down
        # (bsc dataseeds, 2026-10-23). Only an unprobed record stays unknown.
        return 0 if record.probed_at > 0 else None
    return record.getlogs_max_range


def get_max_block_range(endpoint: str) -> int | None:
    """Get the maximum eth_getLogs block range for this endpoint.

    The static per-provider limit is further constrained by the endpoint's
    MEASURED ``getlogs_max_range`` from capability probing (a conservative
    lower bound) so the first getLogs range never starts larger than the
    endpoint has proven it can serve — preventing a 413 / "response too
    large" on range-capped free RPCs. Unknown endpoints keep the static cap.

    Args:
        endpoint: Full RPC URL.

    Returns:
        Max block range, or None if unlimited.
    """
    domain = urlparse(endpoint).hostname or ""
    configured: int | None = DEFAULT_MAX_RANGE
    for pattern, max_range in MAX_BLOCK_RANGE.items():
        if pattern in domain:
            configured = max_range
            break
    measured = _measured_max_block_range(endpoint)
    if measured is not None:
        return measured if configured is None else min(configured, measured)
    return configured


def get_endpoints_for_range(
    endpoints: list[str],
    num_blocks: int,
) -> list[str]:
    """Filter endpoints that support the requested block range.

    Removes endpoints whose max range is smaller than num_blocks.
    Falls through to the original list if all would be filtered.

    Args:
        endpoints: List of RPC URLs.
        num_blocks: Number of blocks to query.

    Returns:
        Filtered endpoint list (maintains original order).
    """
    filtered = [ep for ep in endpoints if (max_r := get_max_block_range(ep)) is None or max_r >= num_blocks]
    return filtered if filtered else endpoints
