"""Shared helpers — RPC usage / cost tracking ledger.

An in-memory ledger records which endpoints are actually serving data
vs. returning errors/rate-limits, keyed by method, so operators can see
where free-tier budget is being spent. :func:`log_rpc_usage` emits a
periodic summary.

Split out of the former monolithic ``rpc/routing.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config import RPC_USAGE_MAX_ENDPOINTS
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass
class EndpointUsage:
    """Per-endpoint usage ledger entry."""

    endpoint: str
    calls: int = 0
    errors: int = 0
    rate_limits: int = 0
    bytes_received: int = 0
    last_error: str = ""
    last_ok_at: float = 0.0
    last_error_at: float = 0.0
    by_method: dict[str, int] = field(default_factory=dict)


class RpcUsageTracker:
    """Bounded in-memory ledger of per-endpoint RPC usage/cost.

    Records every call (success or failure) so operators can see which
    endpoints are actually serving data vs. burning budget on errors.
    The ledger is bounded to :data:`RPC_USAGE_MAX_ENDPOINTS` entries.
    """

    def __init__(self, max_endpoints: int = RPC_USAGE_MAX_ENDPOINTS) -> None:
        self._max_endpoints = max_endpoints
        self._ledger: dict[str, EndpointUsage] = {}

    def record(
        self,
        endpoint: str,
        method: str,
        *,
        ok: bool,
        bytes_received: int = 0,
        error: str = "",
        rate_limited: bool = False,
    ) -> None:
        """Record the outcome of one RPC call.

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method name.
            ok: Whether the call succeeded (returned usable data).
            bytes_received: Approximate response bytes (cost proxy).
            error: Error message on failure.
            rate_limited: Whether the call hit a 429.
        """
        now = time.time()
        usage = self._ledger.get(endpoint)
        if usage is None:
            if len(self._ledger) >= self._max_endpoints:
                # Evict the least-recently-used entry (oldest last_ok/error).
                oldest = min(
                    self._ledger,
                    key=lambda ep: min(
                        self._ledger[ep].last_ok_at or self._ledger[ep].last_error_at,
                        self._ledger[ep].last_error_at or self._ledger[ep].last_ok_at,
                    ),
                )
                del self._ledger[oldest]
            usage = EndpointUsage(endpoint=endpoint)
            self._ledger[endpoint] = usage

        usage.calls += 1
        usage.by_method[method] = usage.by_method.get(method, 0) + 1
        usage.bytes_received += max(0, bytes_received)
        if ok:
            usage.last_ok_at = now
        else:
            usage.errors += 1
            usage.last_error = (error or "unknown")[:200]
            usage.last_error_at = now
        if rate_limited:
            usage.rate_limits += 1

    def usage_for(self, endpoint: str) -> EndpointUsage | None:
        """Return the ledger entry for an endpoint, or None."""
        return self._ledger.get(endpoint)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return the full ledger as plain dicts (for logging/metrics)."""
        return {
            ep: {
                "calls": u.calls,
                "errors": u.errors,
                "rate_limits": u.rate_limits,
                "bytes": u.bytes_received,
                "last_error": u.last_error,
                "methods": dict(u.by_method),
            }
            for ep, u in self._ledger.items()
        }

    def worst_endpoints(self, limit: int = 10) -> list[tuple[str, float]]:
        """Return the endpoints with the highest error ratio.

        Args:
            limit: Max entries to return.

        Returns:
            List of ``(endpoint, error_ratio)`` sorted worst-first.
        """
        ranked: list[tuple[str, float]] = []
        for ep, u in self._ledger.items():
            ratio = (u.errors / u.calls) if u.calls else 0.0
            if u.calls:
                ranked.append((ep, ratio))
        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked[:limit]

    def reset(self) -> None:
        """Clear the ledger."""
        self._ledger.clear()


# Module-level tracker shared across the codebase.
USAGE_TRACKER = RpcUsageTracker()


def record_rpc_usage(
    endpoint: str,
    method: str,
    *,
    ok: bool,
    bytes_received: int = 0,
    error: str = "",
    rate_limited: bool = False,
) -> None:
    """Record usage in the shared tracker (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
        ok: Whether the call succeeded.
        bytes_received: Approximate response bytes.
        error: Error message on failure.
        rate_limited: Whether the call hit a 429.
    """
    USAGE_TRACKER.record(
        endpoint,
        method,
        ok=ok,
        bytes_received=bytes_received,
        error=error,
        rate_limited=rate_limited,
    )


def log_rpc_usage(limit: int = 10) -> dict[str, dict[str, Any]]:
    """Log a summary of RPC usage (worst error ratios first) and return it.

    Args:
        limit: Max endpoints to include in the summary log.

    Returns:
        The full usage snapshot dict.
    """
    snapshot = USAGE_TRACKER.snapshot()
    if not snapshot:
        logger.info("rpc_usage_empty")
        return snapshot

    total_calls = sum(u["calls"] for u in snapshot.values())
    total_errors = sum(u["errors"] for u in snapshot.values())
    total_bytes = sum(u["bytes"] for u in snapshot.values())
    worst = USAGE_TRACKER.worst_endpoints(limit)
    logger.info(
        "rpc_usage_summary",
        endpoints=len(snapshot),
        total_calls=total_calls,
        total_errors=total_errors,
        total_bytes_mb=round(total_bytes / 1e6, 2),
        worst=[{"endpoint": ep[:60], "error_ratio": round(r, 3)} for ep, r in worst],
    )
    return snapshot
