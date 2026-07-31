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


def get_max_block_range(endpoint: str) -> int | None:
    """Get the maximum eth_getLogs block range for this endpoint.

    Args:
        endpoint: Full RPC URL.

    Returns:
        Max block range, or None if unlimited.
    """
    domain = urlparse(endpoint).hostname or ""
    for pattern, max_range in MAX_BLOCK_RANGE.items():
        if pattern in domain:
            return max_range
    return DEFAULT_MAX_RANGE


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
