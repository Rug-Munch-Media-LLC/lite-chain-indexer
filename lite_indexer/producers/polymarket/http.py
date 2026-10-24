"""Bounded, fail-soft HTTP access to the Polymarket public read APIs.

A scheduler cannot call an MCP, so the producer dials the same keyless public
endpoints the ``polymarket`` MCP wraps (gamma-api for markets, data-api for
trades and holders). The API is free, not infinite: one pooled client, a hard
4 req/s floor, and a retry budget that ends in :class:`ProviderError` so the
caller can fail one dataset instead of the whole cycle.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

TIMEOUT = httpx.Timeout(20.0, connect=5.0)
MIN_INTERVAL = 0.25  # 4 req/s ceiling
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class ProviderError(RuntimeError):
    """Upstream failed (or refused) after the retry budget was spent."""


def client() -> httpx.Client:
    """One pooled sync client per cycle — the indexer engine is synchronous."""
    return httpx.Client(
        timeout=TIMEOUT,
        headers={"accept": "application/json", "user-agent": "rmi-lite-indexer/pm-producer"},
        limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
        follow_redirects=True,
    )


def fetch_json(http: httpx.Client, url: str, params: dict[str, Any], attempts: int = 3) -> Any:
    """GET one endpoint; retry 429/5xx/transport, raise :class:`ProviderError` when spent."""
    delay = 1.0
    last = "no attempt"
    for attempt in range(1, attempts + 1):
        time.sleep(MIN_INTERVAL if attempt == 1 else delay)
        try:
            resp = http.get(url, params=params)
        except httpx.HTTPError as exc:
            last = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code < 400:
                return resp.json()
            last = f"HTTP {resp.status_code}"
            if resp.status_code not in RETRY_STATUS:
                raise ProviderError(f"{url} -> {last}")
        logger.warning("polymarket_retry", url=url, attempt=attempt, error=last)
        delay *= 2
    raise ProviderError(f"{url} failed after {attempts} attempts: {last}")
