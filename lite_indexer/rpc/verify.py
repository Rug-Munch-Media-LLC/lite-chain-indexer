"""
Endpoint verification orchestration for the indexer.

Owns the EVM endpoint-verification flow so ``LiteIndexer`` stays focused
on orchestration: verify configured endpoints, temporarily swap the
healthy set into the chain config for the sync cycle, and restore the
originals afterwards.

Only EVM chains participate — verification uses an ``eth_getLogs`` probe
via :func:`lite_indexer.rpc.health.verify_endpoints_for_chain`. For each
endpoint that passes liveness the archive + getLogs-range capability
probes are run best-effort and persisted on the endpoint's
:class:`~lite_indexer.rpc.capabilities.CapabilityRecord` (never blocking
or failing verification).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.logging import get_logger
from lite_indexer.rpc.capabilities import (
    CAPABILITY_PROBE_TIMEOUT,
    TRANSFER_TOPIC,
    CapabilityRecord,
    probe_archive,
    probe_getlogs_range,
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
        verified_endpoints: Endpoints that passed verification.
    """

    chain: str
    original_endpoints: list[str]
    verified_endpoints: list[str]

    @property
    def total(self) -> int:
        """Number of configured endpoints."""
        return len(self.original_endpoints)

    @property
    def verified(self) -> int:
        """Number of endpoints that passed verification."""
        return len(self.verified_endpoints)


async def _probe_and_record_capabilities(client: RpcClient, endpoint: str, chain: str) -> None:
    """Best-effort archive + getLogs-range probe for a verified endpoint.

    Persists the result on the endpoint's :class:`CapabilityRecord` via
    the capabilities registry (consulted later by pool admission /
    routing). Exception-guarded: a probe failure must never fail
    liveness verification or stall the sync cycle.

    Args:
        client: RpcClient instance used for the probes.
        endpoint: Endpoint URL that passed liveness verification.
        chain: Chain name.
    """
    try:
        archive = await probe_archive(client, endpoint, chain)
        max_range = await probe_getlogs_range(
            client,
            endpoint,
            TRANSFER_TOPIC,
            timeout=CAPABILITY_PROBE_TIMEOUT,
        )
        set_capability(
            CapabilityRecord(
                endpoint=endpoint,
                chain=chain,
                archive=archive,
                getlogs_max_range=max_range,
                probed_at=time.time(),
            )
        )
        logger.info(
            "endpoint_capability_probed",
            endpoint=endpoint[:60],
            chain=chain,
            archive=archive,
            getlogs_max_range=max_range,
        )
    except Exception:  # noqa: BLE001 — capability probing is best-effort.
        logger.warning(
            "endpoint_capability_probe_failed",
            endpoint=endpoint[:60],
            chain=chain,
            exc_info=True,
        )


async def verify_chain_endpoints(chain: str, rpc_client: RpcClient) -> EndpointVerifyResult | None:
    """Verify all configured endpoints for an EVM chain.

    Returns ``None`` when the chain has no endpoints configured — the
    caller should abort the sync cycle. Otherwise returns a result that
    carries both the original and the verified endpoint lists so the
    caller can swap them in temporarily.

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
    # Capability probes (archive + getLogs max range) run after liveness
    # for every endpoint that passed — best-effort, stored on the
    # endpoint's CapabilityRecord for pool admission / chunk sizing.
    for ep in verified_endpoints:
        await _probe_and_record_capabilities(rpc_client, ep, chain)
    logger.info(
        "endpoint_verification_complete",
        chain=chain,
        total=len(chain_endpoints),
        verified=len(verified_endpoints),
    )
    return EndpointVerifyResult(
        chain=chain,
        original_endpoints=list(chain_endpoints),
        verified_endpoints=verified_endpoints,
    )


def apply_verified_endpoints(result: EndpointVerifyResult) -> None:
    """Temporarily replace the chain's configured endpoints with the verified set.

    No-op when no endpoints passed verification (falls through to the
    original configuration).

    Args:
        result: Verification result for the chain.
    """
    if not result.verified_endpoints:
        return
    import lite_indexer.chains as chains_mod

    chains_mod.GET_LOGS_ENDPOINTS[result.chain] = result.verified_endpoints


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
