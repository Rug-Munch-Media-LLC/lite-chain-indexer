"""Rate-limit header parsing for the RPC throttle.

Normalizes the header families seen in the wild (KeyedProvider, Blast,
Dedicated, generic Cloudflare/proxy layers) into one dict of
``limit`` / ``remaining`` / ``reset_at`` / ``retry_after`` values.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from datetime import UTC, datetime

# Sentinel returned by wait computations when an endpoint is inside an
# explicit Retry-After window.
_RETRY_AFTER = "retry_after"


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup.

    ``httpx`` normalizes header names to lowercase, but callers may pass
    any Mapping (tests, proxies, other HTTP stacks) — so we scan keys
    case-insensitively instead of relying on a fixed casing.
    """
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target:
            return value
    return None


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return None


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        return None


def extract_rate_limit(headers: Mapping[str, str]) -> dict[str, int | float | None]:
    """Parse rate-limit headers into a normalized dict.

    Handles both header families:
      - ``X-RateLimit-Limit`` + ``X-RateLimit-Remaining`` + ``X-RateLimit-Reset``
        (epoch seconds when the window resets)
      - ``X-RateLimit-Reset`` in delta-seconds form (some proxies)
      - ``Retry-After`` (HTTP-date or delta-seconds) for 429 responses

    Args:
        headers: HTTP response headers (any Mapping, case-insensitive).

    Returns:
        ``{limit, remaining, reset_at, retry_after}`` with ``None`` for
        headers that were absent or unparseable.
    """
    limit = _parse_int(_header_value(headers, "x-ratelimit-limit"))
    if limit is None:
        limit = _parse_int(_header_value(headers, "ratelimit-limit"))
    remaining = _parse_int(_header_value(headers, "x-ratelimit-remaining"))
    if remaining is None:
        remaining = _parse_int(_header_value(headers, "ratelimit-remaining"))

    reset_at: float | None = None
    reset = _header_value(headers, "x-ratelimit-reset")
    if reset is None:
        reset = _header_value(headers, "x-ratelimit-reset-epoch")
    if reset is not None:
        parsed = _parse_float(reset)
        if parsed is not None:
            # If the reset value is in the near future it is a delta
            # (proxy style); otherwise treat it as an epoch timestamp.
            reset_at = time.time() + parsed if parsed < 1_000_000_000 else parsed

    retry_after: float | None = None
    ra = _header_value(headers, "retry-after")
    if ra is not None:
        parsed = _parse_float(ra)
        if parsed is not None:
            retry_after = parsed
        else:
            # HTTP-date form (e.g. "Wed, 21 Oct 2026 07:28:00 GMT").
            from email.utils import parsedate_to_datetime

            try:
                retry_after = max(0.0, (parsedate_to_datetime(ra) - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError):  # pragma: no cover — defensive
                retry_after = None

    return {
        "limit": limit,
        "remaining": remaining,
        "reset_at": reset_at,
        "retry_after": retry_after,
    }


__all__ = ["extract_rate_limit"]
