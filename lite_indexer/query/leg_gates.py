"""Per-leg gating for the QueryRouter tier cascade.

The cascade in ``router._route_query`` is six legs, each guarded by the
same shape of predicate:

    if use_rpc and self.<gate> and self._needs_rpc_fallback(...):
        rows = await self._<leg>_route(req)

Extracted so the gate a leg uses is stated once. That is the whole point of
this module: the free legs (BigLake, Envio, indexed.xyz, SQD) were gated on
``on_demand_enabled`` — the PAID-RPC switch — so an operator stopping RPC
spend silently lost every free historical read.

Tier order is unchanged: CH -> cold/B2 -> biglake/S3 -> envio -> indexed ->
sqd -> RPC. Only the gate each leg consults changed.
"""

from __future__ import annotations

from typing import Any

#: Legs that read a free external source, gated by LAKE_READ_ENABLED.
FREE_LEGS = ("biglake", "envio", "indexed", "sqd")


def leg_enabled(router: Any, leg: str, use_rpc: bool) -> bool:
    """Whether ``leg`` may run for this query.

    The RPC leg consults ``on_demand_enabled`` (paid). Every leg in
    :data:`FREE_LEGS` consults ``lake_read_enabled``, which is a separate
    switch with a separate default.
    """
    if not use_rpc:
        return False
    if leg == "rpc":
        return bool(router.on_demand_enabled)
    return bool(router.lake_read_enabled)
