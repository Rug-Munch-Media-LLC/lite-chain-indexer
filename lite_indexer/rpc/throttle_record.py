"""Response recording for the RPC throttle.

Feeds the advertised budget and the 429 window into the endpoint state; a
clean 200 closes the 429 streak (half-open probe style).
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import cast

from lite_indexer.logging import get_logger
from lite_indexer.rpc.throttle_base import ThrottleBase
from lite_indexer.rpc.throttle_headers import extract_rate_limit

logger = get_logger(__name__)


class ThrottleRecordMixin(ThrottleBase):
    """Record the outcome of an RPC call and its rate-limit headers."""

    def record_response(
        self,
        endpoint: str,
        method: str,
        status: int,
        headers: Mapping[str, str] | None = None,
        now: float | None = None,
    ) -> None:
        """Record the outcome of an RPC call and its rate-limit headers.

        Called after every HTTP response (success or 429). Updates the
        advertised budget and — for 429 responses — arms an exact
        Retry-After window (falling back to an exponential window when
        no Retry-After header is present).

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method name.
            status: HTTP status code.
            headers: Response headers (rate-limit headers are parsed).
            now: Epoch time override (tests).
        """
        state = self._entry(endpoint)
        now = time.time() if now is None else now
        state.updated_at = now
        state.last_status = status

        info = extract_rate_limit(headers or {})
        if info["limit"] is not None:
            # limit/remaining are parsed via _parse_int → always int at runtime.
            state.limit = cast(int, info["limit"])
        if info["remaining"] is not None:
            state.remaining = cast(int, info["remaining"])
        if info["reset_at"] is not None:
            state.reset_at = info["reset_at"]

        if status == 429:
            state.rate_limit_hits += 1
            wait = info["retry_after"]
            if wait is not None and wait > 0:
                state.backoff_until = now + wait
                logger.warning(
                    "rpc_throttle_retry_after",
                    endpoint=endpoint[:60],
                    method=method,
                    wait_s=round(wait, 1),
                )
            else:
                # No Retry-After header: exponential window scaled by the
                # number of consecutive 429s (cap at 5 min).
                state.backoff_until = now + min(300, 2 ** min(state.rate_limit_hits, 8))
                logger.warning(
                    "rpc_throttle_429_backoff",
                    endpoint=endpoint[:60],
                    method=method,
                    hits=state.rate_limit_hits,
                )
        elif status == 200 and state.rate_limit_hits > 0:
            # A clean success closes the 429 streak (half-open probe style).
            state.rate_limit_hits = 0

        # Rate-limited disk writes: at most once per persist_interval.
        self.persist_if_stale(now)


__all__ = ["ThrottleRecordMixin"]
