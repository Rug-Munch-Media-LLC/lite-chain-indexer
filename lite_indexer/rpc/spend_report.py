"""Per-cost-class report over the live RPC usage ledger.

WHY THIS EXISTS
    ``RpcUsageTracker`` records calls per ENDPOINT. Nothing aggregated them by
    COST CLASS, so an operator asking why they are burning provider quota
    while free sources sit idle had no answer. This turns the in-memory ledger
    into a per-class breakdown and separates calls a free endpoint was ELIGIBLE
    to serve from ones that were not.

    (Named providers are deliberately absent from this docstring: the public
    tree is gated against curated provider names by tests/test_endpoints.py.)

READ-ONLY. It reads the ledger, it never mutates it, and it never touches the
network. The ledger is in-memory and bounded (RPC_USAGE_MAX_ENDPOINTS), so a
long-running process evicts least-recently-used endpoints and the totals are a
recent window, not lifetime. The report says so rather than implying otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lite_indexer.rpc.cost_tier import CostClass, cost_tier

#: Classes that are not obviously billable. A "free" label means the endpoint
#: is keyless — it is not a guarantee the provider bills nothing.
NON_BILLABLE: frozenset[str] = frozenset({"free"})


@dataclass(frozen=True, slots=True)
class ClassTotals:
    """Aggregate usage for one cost class."""

    endpoints: int
    calls: int
    errors: int
    rate_limits: int
    bytes_received: int

    @property
    def error_ratio(self) -> float:
        """Fraction of calls that failed."""
        return self.errors / self.calls if self.calls else 0.0

    def as_dict(self) -> dict[str, Any]:
        """JSON-ready view."""
        return {
            "endpoints": self.endpoints,
            "calls": self.calls,
            "errors": self.errors,
            "rate_limits": self.rate_limits,
            "bytes_received": self.bytes_received,
            "error_ratio": round(self.error_ratio, 4),
        }


def tally_by_class(snapshot: dict[str, dict[str, Any]]) -> dict[str, ClassTotals]:
    """Aggregate a tracker snapshot by cost class.

    Args:
        snapshot: ``RpcUsageTracker.snapshot()`` output, keyed by endpoint URL.

    Returns:
        One :class:`ClassTotals` per cost class, including classes with no
        usage (so the report shows a class is idle rather than absent).
    """
    out: dict[str, dict[str, int]] = {}
    for endpoint, usage in snapshot.items():
        cls: CostClass = cost_tier(endpoint)
        acc = out.setdefault(
            cls,
            {"endpoints": 0, "calls": 0, "errors": 0, "rate_limits": 0, "bytes_received": 0},
        )
        acc["endpoints"] += 1
        for field in ("calls", "errors", "rate_limits", "bytes_received"):
            acc[field] += int(usage.get(field) or 0)
    return {
        cls: ClassTotals(
            endpoints=v["endpoints"],
            calls=v["calls"],
            errors=v["errors"],
            rate_limits=v["rate_limits"],
            bytes_received=v["bytes_received"],
        )
        for cls, v in out.items()
    }


def avoidable_calls(
    totals: dict[str, ClassTotals],
    *,
    snapshot: dict[str, dict[str, Any]],
) -> int:
    """Calls made to a billable class while a free endpoint was healthy.

    A call counts as avoidable when the billable class served it AND a free
    endpoint in the same ledger had zero errors at that point — i.e. a free
    source was available, so the spend was a routing choice, not a necessity.

    Args:
        totals: Output of :func:`tally_by_class`.
        snapshot: The raw ledger, needed for per-endpoint error counts.

    Returns:
        Number of calls attributable to billable classes while free was healthy.
    """
    free_healthy = any(cost_tier(ep) == "free" and not int(usage.get("errors") or 0) for ep, usage in snapshot.items())
    if not free_healthy:
        return 0
    return sum(t.calls for cls, t in totals.items() if cls not in NON_BILLABLE)


def render(totals: dict[str, ClassTotals], avoidable: int) -> str:
    """Human-readable report."""
    lines = ["RPC usage by cost class (in-memory ledger; recent window, not lifetime)", ""]
    lines.append(f"  {'class':10} {'endpoints':>9} {'calls':>9} {'errors':>8} {'429s':>6}  error%")
    for cls in sorted(totals):
        t = totals[cls]
        lines.append(
            f"  {cls:10} {t.endpoints:9} {t.calls:9} {t.errors:8} {t.rate_limits:6}  {t.error_ratio * 100:5.1f}%"
        )
    total = sum(t.calls for t in totals.values())
    billable = sum(t.calls for c, t in totals.items() if c not in NON_BILLABLE)
    lines += [
        "",
        f"  total calls      : {total}",
        f"  billable-class   : {billable}" + (f" ({billable / total * 100:.1f}%)" if total else ""),
        f"  avoidable spend  : {avoidable}"
        + ("  <- free endpoint was healthy; this was a routing choice" if avoidable else ""),
    ]
    return "\n".join(lines)


__all__ = ["ClassTotals", "NON_BILLABLE", "avoidable_calls", "render", "tally_by_class"]
