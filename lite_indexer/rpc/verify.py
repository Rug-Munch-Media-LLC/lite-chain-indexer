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

import time
from dataclasses import dataclass, field

from lite_indexer.chains import ENDPOINT_VERIFY_INTERVAL, GET_LOGS_ENDPOINTS
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


async def verify_chain_endpoints(chain: str, rpc_client: RpcClient) -> EndpointVerifyResult | None:
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

    Returns:
        :class:`EndpointVerifyResult`, or ``None`` if no endpoints are configured.
    """
    chain_endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not chain_endpoints:
        logger.error("no_endpoints_for_chain", chain=chain)
        return None

    # Get latest block for verification range
    tip_for_verify = await rpc_client.eth_block_number(get_best_endpoint(chain_endpoints))
    verified_endpoints = await verify_endpoints_for_chain(
        chain_endpoints,
        chain,
        latest_block=tip_for_verify,
    )
    # Gate every liveness-verified endpoint on the broad tip-range getLogs
    # probe. Capability refusals are excluded from the log path and kept as
    # tip-only; transient failures stay in the log set (never excluded on a
    # timeout). Archive + range capabilities are persisted alongside.
    log_capable: list[str] = []
    tip_only: list[str] = []
    for ep in verified_endpoints:
        status = await _probe_and_record_capabilities(rpc_client, ep, chain, tip_for_verify)
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
    "verify_chain_endpoints",
    "apply_verified_endpoints",
    "restore_original_endpoints",
]
