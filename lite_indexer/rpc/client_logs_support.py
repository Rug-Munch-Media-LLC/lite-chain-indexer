"""eth_getLogs support — query object, gates, cache and consensus verify.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import lite_indexer.rpc.client as _facade
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)


@dataclass(slots=True)
class _LogsQuery:
    """One eth_getLogs request (keeps helper signatures ≤5 params)."""

    endpoint: str
    from_block: int
    to_block: int
    topic: str
    address: str | None
    chain: str | None
    tip: int | None

    def filter(self) -> dict[str, Any]:
        """Build one ``eth_getLogs`` filter object for the block range."""
        return _facade.RpcClient._logs_filter(
            self.from_block, self.to_block, self.topic, self.address
        )

    def cache_params(self) -> list[Any]:
        """Filter shaped for the persistent response cache."""
        return [self.filter()]


def _getlogs_gates(endpoint: str, from_block: int, to_block: int) -> bool:
    """Quota-degrade + archive fail-closed gates for eth_getLogs.

    Level 1 defers wide backfill scans, level 2 defers all, level 3
    (parked) defers everything. RC-1 (2026-09-19): None is the failure
    shape — [] was read as "no logs" and callers advanced state over
    unfetched blocks. Returns True when blocked (already logged).
    """
    reason = _facade._defer_reason_for(endpoint, "eth_getLogs", range_blocks=to_block - from_block)
    if reason:
        _facade._warn_deferred(endpoint, "eth_getLogs", reason)
        return True
    from lite_indexer.rpc.capabilities import archive_guard_reason

    # Archive-class fail-closed gate: eth_getLogs is ARCHIVE-class — a
    # known non-archive endpoint silently returns wrong/empty logs, so
    # fail closed (None = failure shape).
    archive_reason = archive_guard_reason(endpoint, "eth_getLogs")
    if archive_reason:
        metrics_incr("rpc_archive_fail_closed")
        logger.warning(
            "rpc_archive_fail_closed",
            endpoint=endpoint[:60],
            method="eth_getLogs",
            reason=archive_reason,
        )
        return True
    return False


def _logs_cached(query: _LogsQuery) -> list[dict[str, Any]] | None:
    """Serve immutable historical ranges from the persistent cache.

    Confirmed past ``RPC_CACHE_CONFIRMATIONS`` ranges are served from
    L1/L2 without a dial; returns None on a cache miss.
    """
    from lite_indexer.rpc import respcache

    cached = respcache.get(
        "eth_getLogs",
        query.cache_params(),
        chain=query.chain,
        from_block=query.from_block,
        to_block=query.to_block,
        tip=query.tip,
    )
    if cached is not None and isinstance(cached.get("result"), list):
        return cast(list[dict[str, Any]], cached["result"])
    return None


def _store_logs(query: _LogsQuery, result: list[dict[str, Any]] | None) -> None:
    """Cache a resolved getLogs result (best-effort, immutable ranges)."""
    if result is None:
        return
    from lite_indexer.rpc import respcache

    respcache.put(
        "eth_getLogs",
        query.cache_params(),
        {"jsonrpc": "2.0", "id": 1, "result": result},
        chain=query.chain,
        from_block=query.from_block,
        to_block=query.to_block,
        tip=query.tip,
    )


async def _verify_and_store(
    writer: Any,
    query: _LogsQuery,
    logs: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Sampled cross-provider consensus + cache write for a getLogs range.

    A critical getLogs range result is trusted only once a quorum of
    DISTINCT provider accounts returns the same content hash; a mismatch
    FAILS the read (None). Verification must never break the read.
    """
    from lite_indexer.rpc.consensus import should_sample, verify_across_providers

    if logs is None or not query.chain or not should_sample():
        _store_logs(query, logs)
        return logs
    try:
        consensus = await verify_across_providers(
            writer,
            query.chain,
            "eth_getLogs",
            [query.filter()],
            expect_non_empty=bool(logs),
        )
    except Exception as exc:  # noqa: BLE001 — verification must never break the read
        logger.warning("rpc_consensus_error", chain=query.chain, error=str(exc)[:120])
        _store_logs(query, logs)
        return logs
    if consensus.mismatch:
        logger.warning(
            "getlogs_consensus_mismatch",
            chain=query.chain,
            from_block=query.from_block,
            to_block=query.to_block,
            agreement=consensus.agreement,
            total=consensus.total,
        )
        return None
    if consensus.verified and isinstance(consensus.result, list):
        verified_logs = cast(list[dict[str, Any]], consensus.result)
        _store_logs(query, verified_logs)
        return verified_logs
    _store_logs(query, logs)
    return logs
