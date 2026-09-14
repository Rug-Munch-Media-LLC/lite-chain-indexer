"""
Endpoint verification orchestration for the indexer.

Owns the EVM endpoint-verification flow so ``LiteIndexer`` stays focused
on orchestration: verify configured endpoints, temporarily swap the
healthy set into the chain config for the sync cycle, and restore the
originals afterwards.

Only EVM chains participate — verification uses an ``eth_getLogs`` probe
via :func:`lite_indexer.rpc.health.verify_endpoints_for_chain`. Every
endpoint that passes liveness is then gated on a broad tip-range
``eth_getLogs`` capability probe: endpoints that structurally refuse the
request shape (JSON-RPC ``-32701``, HTTP 405, auth/tenant/key refusals,
malformed-range refusals) are marked ``CANNOT_SERVE_LOGS``, excluded from
the log path, and retained only as tip/head-only endpoints. The archive +
getLogs-range capability probes run best-effort and are persisted on the
endpoint's :class:`~lite_indexer.rpc.capabilities.CapabilityRecord`.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from lite_indexer.chains import (
    ENDPOINT_VERIFY_CONCURRENCY,
    ENDPOINT_VERIFY_INTERVAL,
    GET_LOGS_ENDPOINTS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.metrics import set_labeled_gauge
from lite_indexer.rpc.capabilities import (
    CAPABILITY_PROBE_TIMEOUT,
    TRANSFER_TOPIC,
    CapabilityRecord,
    GetLogsCapability,
    get_capability,
    probe_archive,
    probe_getlogs_range,
    probe_logs_capability,
    set_capability,
)
from lite_indexer.rpc.client import RpcClient
from lite_indexer.rpc.health import verify_endpoints_for_chain
from lite_indexer.rpc.pool import get_best_endpoint

logger = get_logger(__name__)

# One-shot startup verification results consumed by each chain's first sync
# cycle. Keyed by chain. Populated by :func:`verify_all_chains`; popped by
# :func:`consume_startup_result` so the expensive first pass happens once at
# daemon start instead of serially inside the per-chain loops.
_STARTUP_RESULTS: dict[str, EndpointVerifyResult] = {}
# Per-chain futures resolved AS SOON AS that chain's startup verification
# finishes, so a chain's sync loop starts the moment its own verify is done
# instead of waiting for the whole pass. Awaited by :func:`wait_startup_result`.
_STARTUP_FUTURES: dict[str, asyncio.Future[EndpointVerifyResult | None]] = {}


@dataclass
class EndpointVerifyResult:
    """Outcome of verifying a chain's endpoints.

    Attributes:
        chain: Chain name.
        original_endpoints: Endpoints as configured before verification.
        verified_endpoints: Endpoints that passed the broad tip-range
            ``eth_getLogs`` capability probe (the only endpoints allowed on
            the log path).
        tip_only_endpoints: Endpoints that passed liveness but structurally
            refuse broad ``eth_getLogs`` (``CANNOT_SERVE_LOGS``). Retained
            as tip/head-only endpoints (``eth_blockNumber``) but never
            routed an ``eth_getLogs`` call.
    """

    chain: str
    original_endpoints: list[str]
    verified_endpoints: list[str]
    tip_only_endpoints: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Number of configured endpoints."""
        return len(self.original_endpoints)

    @property
    def verified(self) -> int:
        """Number of endpoints that passed verification."""
        return len(self.verified_endpoints)


async def _probe_and_record_capabilities(
    client: RpcClient,
    endpoint: str,
    chain: str,
    tip: int | None,
) -> GetLogsCapability:
    """Probe + persist archive/getLogs-range/broad-getLogs capability.

    Cached: when the endpoint already carries a fresh, conclusive
    ``logs_capable`` verdict the stored result is reused instead of
    re-probing (``ENDPOINT_VERIFY_INTERVAL``). Best-effort: a probe failure
    must never fail liveness verification or stall the sync cycle.

    Args:
        client: RpcClient instance used for the archive probes.
        endpoint: Endpoint URL that passed liveness verification.
        chain: Chain name.
        tip: Chain tip used to anchor the broad-getLogs probe.

    Returns:
        The broad-getLogs capability classification (TRANSIENT on probe error).
    """
    existing = get_capability(endpoint)
    if (
        existing is not None
        and existing.logs_capable is not None
        and (time.time() - existing.probed_at) < ENDPOINT_VERIFY_INTERVAL
    ):
        return GetLogsCapability.CAPABLE if existing.logs_capable else GetLogsCapability.CANNOT_SERVE_LOGS

    try:
        archive = await probe_archive(client, endpoint, chain)
        max_range = await probe_getlogs_range(
            client,
            endpoint,
            TRANSFER_TOPIC,
            timeout=CAPABILITY_PROBE_TIMEOUT,
        )
        status = await probe_logs_capability(endpoint, tip)
        logs_capable = None if status is GetLogsCapability.TRANSIENT else status is GetLogsCapability.CAPABLE
        set_capability(
            CapabilityRecord(
                endpoint=endpoint,
                chain=chain,
                archive=archive,
                getlogs_max_range=max_range,
                logs_capable=logs_capable,
                probed_at=time.time(),
            )
        )
        logger.info(
            "endpoint_capability_probed",
            endpoint=endpoint[:60],
            chain=chain,
            archive=archive,
            getlogs_max_range=max_range,
            logs_capable=logs_capable,
        )
        return status
    except Exception:  # noqa: BLE001 — capability probing is best-effort.
        logger.warning(
            "endpoint_capability_probe_failed",
            endpoint=endpoint[:60],
            chain=chain,
            exc_info=True,
        )
        return GetLogsCapability.TRANSIENT


async def verify_chain_endpoints(
    chain: str,
    rpc_client: RpcClient,
    *,
    probe_semaphore: asyncio.Semaphore | None = None,
) -> EndpointVerifyResult | None:
    """Verify all configured endpoints for an EVM chain.

    Returns ``None`` when the chain has no endpoints configured — the
    caller should abort the sync cycle. Otherwise returns a result that
    carries the original list, the log-capable set (passed the broad
    tip-range ``eth_getLogs`` probe), and the tip-only set (capability-
    excluded: retained for ``eth_blockNumber`` but never routed logs) so
    the caller can swap the log set in temporarily.

    Args:
        chain: Chain name.
        rpc_client: RPC client used to fetch the tip for range checks.
        probe_semaphore: Optional SHARED bound applied to every probe
            (liveness + capability). The startup pass passes one global
            semaphore so in-flight probes stay memory-bounded across all
            chains; a standalone call creates its own bound.

    Returns:
        :class:`EndpointVerifyResult`, or ``None`` if no endpoints are configured.
    """
    chain_endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not chain_endpoints:
        logger.error("no_endpoints_for_chain", chain=chain)
        return None

    probe_sem = probe_semaphore or asyncio.Semaphore(max(1, ENDPOINT_VERIFY_CONCURRENCY))

    # Get latest block for verification range
    tip_for_verify = await rpc_client.eth_block_number(get_best_endpoint(chain_endpoints))
    verified_endpoints = await verify_endpoints_for_chain(
        chain_endpoints,
        chain,
        latest_block=tip_for_verify,
        semaphore=probe_sem,
    )

    # Gate every liveness-verified endpoint on the broad tip-range getLogs
    # probe. Capability refusals are excluded from the log path and kept as
    # tip-only; transient failures stay in the log set (never excluded on a
    # timeout). Archive + range capabilities are persisted alongside.
    #
    # PROBED IN PARALLEL: the per-endpoint archive + range-ladder probes
    # dominate a chain's verification time; serializing them made a single
    # chain's first verify take minutes, which (multiplied by 21 chains) was
    # the real reason the log pool stayed empty. All probes share the one
    # global ``probe_sem`` so the bound is memory-safe across chains;
    # provider-account concurrency remains bounded inside RpcClient's budget
    # slots.
    async def _gate(ep: str) -> tuple[str, GetLogsCapability]:
        async with probe_sem:
            return ep, await _probe_and_record_capabilities(rpc_client, ep, chain, tip_for_verify)

    gated = await asyncio.gather(*(_gate(ep) for ep in verified_endpoints))
    log_capable: list[str] = []
    tip_only: list[str] = []
    for ep, status in gated:
        if status is GetLogsCapability.CANNOT_SERVE_LOGS:
            tip_only.append(ep)
            metrics_incr("endpoint_logs_capability_excluded")
            logger.warning(
                "endpoint_logs_capability_excluded",
                endpoint=ep[:60],
                chain=chain,
                usable_log_endpoints=len(log_capable),
            )
        else:
            log_capable.append(ep)

    # Visible usable-log-pool size per chain + a counter for exclusions so a
    # shrinking log pool is dashable/alertable.
    set_labeled_gauge("lite_indexer_rpc_pool_logs_endpoints", len(log_capable), chain=chain)
    logger.info(
        "endpoint_verification_complete",
        chain=chain,
        total=len(chain_endpoints),
        verified=len(verified_endpoints),
        log_capable=len(log_capable),
        tip_only=len(tip_only),
    )
    return EndpointVerifyResult(
        chain=chain,
        original_endpoints=list(chain_endpoints),
        verified_endpoints=log_capable,
        tip_only_endpoints=tip_only,
    )


def consume_startup_result(chain: str) -> EndpointVerifyResult | None:
    """Pop the pre-verified startup result for ``chain`` (one-shot).

    The per-chain sync loop calls this before falling back to a fresh
    verification, so the expensive cold pass happens once at daemon start
    and the first sync cycle applies the already-verified log pool.
    """
    return _STARTUP_RESULTS.pop(chain, None)


async def wait_startup_result(chain: str, timeout: float | None = None) -> EndpointVerifyResult | None:
    """Wait for ``chain``'s startup verification (returns None when absent).

    Unlike :func:`consume_startup_result`, this blocks only on THIS chain's
    future — a chain starts syncing the moment its own verification lands,
    without waiting for the rest of the pass. Falls back to any buffered
    result (or None) when the chain was not part of the startup pass, the
    wait times out, or the pass was cancelled.
    """
    future = _STARTUP_FUTURES.get(chain)
    if future is None:
        return consume_startup_result(chain)
    try:
        result = await (asyncio.wait_for(asyncio.shield(future), timeout) if timeout else asyncio.shield(future))
    except (TimeoutError, asyncio.CancelledError):
        return consume_startup_result(chain)
    _STARTUP_FUTURES.pop(chain, None)
    _STARTUP_RESULTS.pop(chain, None)
    return result


def clear_startup_results() -> None:
    """Clear buffered startup results + unresolved futures (tests / restart)."""
    _STARTUP_RESULTS.clear()
    _STARTUP_FUTURES.clear()


async def verify_all_chains(
    chains: list[str],
    *,
    rpc_client: RpcClient | None = None,
    concurrency: int | None = None,
    store_results: bool = True,
) -> dict[str, EndpointVerifyResult | None]:
    """Verify every EVM chain's endpoints CONCURRENTLY (startup pass).

    Verification previously ran inside each chain's sync loop and was
    serialized by ``MAX_CONCURRENT_CHAINS``, so all but the first few
    chains sat with an empty log pool for hours. This pass runs before the
    sync loops settle and covers every chain under a small dedicated
    concurrency bound.

    Each chain's verification populates the existing endpoint-health cache
    and capability registry, so the subsequent per-chain calls (and a
    restart within ``ENDPOINT_VERIFY_INTERVAL``) skip re-probing. Results
    are buffered in :data:`_STARTUP_RESULTS` for one-shot consumption by
    the chain loops.

    Fail-soft: a chain whose probe raises is logged and recorded as
    ``None``; a chain with zero log-capable endpoints logs ``no_log_endpoints``
    and is recorded with an empty set. Neither aborts the pass.

    Args:
        chains: Chain names to verify (non-EVM / unconfigured skipped).
        rpc_client: Optional shared client (created + closed here when None).
        concurrency: Max in-flight verification probes GLOBALLY (default
            :data:`ENDPOINT_VERIFY_CONCURRENCY`). One shared semaphore is
            handed to every chain so the bound is memory-safe.
        store_results: Buffer results for :func:`consume_startup_result`.

    Returns:
        Mapping ``chain -> EndpointVerifyResult | None`` for the chains
        that have configured endpoints.
    """
    targets = [chain for chain in chains if GET_LOGS_ENDPOINTS.get(chain)]
    results: dict[str, EndpointVerifyResult | None] = {}
    if not targets:
        logger.info("endpoint_verify_startup_skipped", reason="no_configured_endpoints")
        return results

    limit = concurrency if concurrency is not None else ENDPOINT_VERIFY_CONCURRENCY
    # ONE global semaphore across every chain's probes (not one per chain):
    # every chain task runs, but at most `limit` probes are ever in flight.
    # This bounds peak memory (each probe can buffer a large getLogs
    # response) while letting all chains progress in parallel.
    probe_semaphore = asyncio.Semaphore(max(1, int(limit)))
    client = rpc_client or RpcClient()
    owns_client = rpc_client is None
    started = time.monotonic()

    loop = asyncio.get_running_loop()
    if store_results:
        for chain in targets:
            _STARTUP_FUTURES.setdefault(chain, loop.create_future())

    def _resolve(chain: str, result: EndpointVerifyResult | None) -> None:
        """Publish a chain's result to the buffered map + its waiter future."""
        if not store_results:
            return
        if result is not None:
            _STARTUP_RESULTS[chain] = result
        future = _STARTUP_FUTURES.get(chain)
        if future is not None and not future.done():
            future.set_result(result)

    async def _verify_one(chain: str) -> tuple[str, EndpointVerifyResult | None]:
        try:
            result = await verify_chain_endpoints(chain, client, probe_semaphore=probe_semaphore)
        except Exception:  # noqa: BLE001 — one chain must never abort the pass
            logger.warning("endpoint_verify_chain_failed", chain=chain, exc_info=True)
            result = None
        # Resolve as soon as THIS chain is done so its sync loop starts
        # without waiting for the rest of the pass.
        _resolve(chain, result)
        if result is None or not result.verified_endpoints:
            logger.warning("no_log_endpoints", chain=chain, total=result.total if result else 0)
        return chain, result

    try:
        outcomes = await asyncio.gather(*(_verify_one(chain) for chain in targets))
    finally:
        # Never leave a chain waiting forever, even if the pass was aborted.
        for chain in targets:
            future = _STARTUP_FUTURES.get(chain)
            if future is not None and not future.done():
                future.set_result(None)
        if owns_client:
            await client.aclose()

    for chain, result in outcomes:
        results[chain] = result

    with_logs = sum(1 for r in results.values() if r is not None and r.verified_endpoints)
    logger.info(
        "endpoint_verify_startup_complete",
        chains=len(targets),
        chains_with_logs=with_logs,
        concurrency=max(1, int(limit)),
        wall_s=round(time.monotonic() - started, 1),
    )
    return results


def apply_verified_endpoints(result: EndpointVerifyResult) -> None:
    """Temporarily replace the chain's configured endpoints with the log set.

    Only endpoints that passed the broad-getLogs probe are applied. When
    every verified endpoint was capability-excluded the log set is applied
    EMPTY (never fall through to the original list, which would resurrect
    the excluded endpoints on the getLogs path). A pure verification outage
    (nothing passed liveness, no capability exclusions) is a no-op and
    leaves the original configuration in place, matching legacy behavior.

    Args:
        result: Verification result for the chain.
    """
    import lite_indexer.chains as chains_mod

    if result.verified_endpoints:
        chains_mod.GET_LOGS_ENDPOINTS[result.chain] = list(result.verified_endpoints)
    elif result.tip_only_endpoints:
        chains_mod.GET_LOGS_ENDPOINTS[result.chain] = []


def restore_original_endpoints(result: EndpointVerifyResult) -> None:
    """Restore the chain's original endpoints after a sync cycle.

    Args:
        result: Verification result for the chain.
    """
    import lite_indexer.chains as chains_mod

    chains_mod.GET_LOGS_ENDPOINTS[result.chain] = result.original_endpoints


__all__ = [
    "EndpointVerifyResult",
    "apply_verified_endpoints",
    "clear_startup_results",
    "consume_startup_result",
    "restore_original_endpoints",
    "verify_all_chains",
    "verify_chain_endpoints",
]
