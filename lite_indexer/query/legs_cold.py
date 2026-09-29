"""B2 parquet cold-tier leg for :class:`QueryRouter`.

Reads historical parquet via the cold store and renders router filter
semantics as a DuckDB-compatible WHERE fragment. Bound as ``QueryRouter``
methods by the facade so the public surface is unchanged.
"""

from __future__ import annotations

from typing import Any, cast

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.legs_common import cold_lit
from lite_indexer.storage.cold_store import ColdStore

logger = get_logger(__name__)


def query_cold(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Query the B2 parquet cold store. Returns [] when unavailable."""
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if router.cold is None:
        return []
    where = router._build_cold_where(filters)
    cold = router.cold
    try:
        # router is Any, so router.cold is Any. The router's cold backend is
        # duck-typed (tests inject a FakeCold that is not a ColdStore), so an
        # isinstance narrowing here would be a behaviour change, not a typing
        # fix. Cast to the real ColdStore.query contract instead; the keyword
        # arguments below are exactly the ones it declares.
        return cast("ColdStore", cold).query(
            table,
            chain,
            from_block=from_block,
            to_block=to_block,
            where=where,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001 — cold reads are best-effort
        metrics_incr("ondemand_cold_errors")
        logger.debug("ondemand_cold_query_failed", table=table, error=str(exc)[:100])
        return []


def build_cold_where(filters: dict[str, Any] | list[dict[str, Any]] | None) -> str:
    """Render filters as a DuckDB-compatible WHERE fragment."""
    if not filters:
        return ""
    if isinstance(filters, dict):
        return " AND ".join(f"{col} = {cold_lit(value)}" for col, value in filters.items())
    return "(" + " OR ".join(f"{item['col']} = {cold_lit(item['value'])}" for item in filters) + ")"
