"""
Helper functions for endpoint selection based on rate limits and ranges.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .limits import DEFAULT_RATE_LIMIT, PROVIDER_RATE_LIMITS


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
