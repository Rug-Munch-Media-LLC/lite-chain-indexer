"""External portal legs for :class:`QueryRouter`.

Envio HyperRPC, Indexed.xyz R2 lake and SQD portal traces — three
independent paths for bounded wallet queries the indexed tiers missed.
Bound as ``QueryRouter`` methods by the facade so the public surface is
unchanged.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, cast

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.envio_source import EnvioSource
from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.sqd_source import SQDSource

logger = get_logger(__name__)

#: ``router._filter_rows`` is a read off ``router: Any``. It is bound from
#: ``legs_lake.filter_rows`` (a staticmethod on the router), whose contract is
#: ``(table, rows, filters) -> list[dict[str, Any]]``. Naming it once here is
#: what lets every leg below return a real ``list[dict[str, Any]]`` instead of
#: passing Any straight through to the caller.
FilterRows = Callable[[str, list[dict[str, Any]], "dict[str, Any] | list[dict[str, Any]] | None"], list[dict[str, Any]]]


async def envio_route(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Try Envio HyperRPC for bounded wallet queries the indexed tiers missed.

    Only serves ``evm_transfers`` address queries with an explicit
    block window; anything else falls through to the RPC leg.
    """
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if table != "evm_transfers" or from_block is None or to_block is None:
        return []
    if not router.envio.available() or chain not in EnvioSource.CHAIN_IDS:
        return []
    address = router._address_from_filters(filters) or router._single_address(filters)
    if not address:
        return []
    try:
        rows = await asyncio.to_thread(
            router.envio.address_transfers, chain, address, from_block, to_block, limit=limit
        )
    except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
        logger.warning("envio_route_failed", error=str(exc)[:120])
        return []
    filtered = cast("FilterRows", router._filter_rows)(table, rows, filters)
    if filtered:
        metrics_incr("envio_routes")
    return filtered


async def indexed_route(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Try the Indexed.xyz R2 lake for bounded wallet queries.

    Independent third path for EVM chains (ethereum/base/gnosis/linea/
    optimism/zksync/zora). Date partitions are derived from the block
    range via :func:`lake_dates`. Best-effort: failures return an
    empty list so routing falls through to the RPC leg.
    """
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if table != "evm_transfers" or from_block is None or to_block is None:
        return []
    if not router.indexed.available() or chain not in router.indexed.CHAINS:
        return []
    address = router._address_from_filters(filters) or router._single_address(filters)
    if not address:
        return []
    dates = await router._lake_dates(chain, from_block, to_block)
    try:
        rows = await asyncio.to_thread(
            router.indexed.query,
            table,
            chain,
            from_block=from_block,
            to_block=to_block,
            address=address,
            dates=dates,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
        logger.warning("indexed_route_failed", error=str(exc)[:120])
        return []
    filtered = cast("FilterRows", router._filter_rows)(table, rows, filters)
    if filtered:
        metrics_incr("indexed_routes")
    return filtered


async def sqd_traces_route(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Try the SQD portal traces dataset for full-history EVM internals.

    Serves ``internal_transfers`` for ~13 EVM chains with ``callFrom`` /
    ``callTo`` server-side filtering. Best-effort: failures return [].
    """
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if table != "internal_transfers" or from_block is None or to_block is None:
        return []
    if not router.sqd.available() or chain not in SQDSource.EVM_DATASETS:
        return []
    address = router._address_from_filters(filters) or router._single_address(filters)
    try:
        rows = await asyncio.to_thread(router.sqd.evm_traces, chain, from_block, to_block, address=address, limit=limit)
    except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
        logger.warning("sqd_traces_route_failed", error=str(exc)[:120])
        return []
    filtered = cast("FilterRows", router._filter_rows)(table, rows, filters)
    if filtered:
        metrics_incr("sqd_traces_routes")
    return filtered
