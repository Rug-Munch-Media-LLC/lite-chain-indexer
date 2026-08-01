"""
Endpoint verification orchestration for the indexer.

Owns the EVM endpoint-verification flow so ``LiteIndexer`` stays focused
on orchestration: verify configured endpoints, temporarily swap the
healthy set into the chain config for the sync cycle, and restore the
originals afterwards.

Only EVM chains participate — verification uses an ``eth_getLogs`` probe
via :func:`lite_indexer.rpc.health.verify_endpoints_for_chain`.
"""

from __future__ import annotations

from dataclasses import dataclass

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.logging import get_logger
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
