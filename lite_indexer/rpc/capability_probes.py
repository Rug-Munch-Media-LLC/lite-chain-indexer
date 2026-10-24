"""Liveness-adjacent capability probes: archive, receipts, CU tariff, suite.

Split out of capabilities.py (2026-10-24). Behaviour is unchanged.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from lite_indexer.logging import get_logger
from lite_indexer.rpc.capability_constants import (
    CAPABILITY_PROBE_TIMEOUT,
    KNOWN_FUNDED_ADDRESS,
    SEEDED_CU_TARIFF,
    TRANSFER_TOPIC,
)
from lite_indexer.rpc.capability_record import CapabilityRecord
from lite_indexer.rpc.capability_registry import set_capability
from lite_indexer.rpc.getlogs_range_probe import probe_getlogs_range

if TYPE_CHECKING:  # pragma: no cover — annotation-only, avoids import cycle
    from lite_indexer.rpc.client import RpcClient

logger = get_logger(__name__)


async def probe_archive(client: RpcClient, endpoint: str, chain: str) -> bool:
    """Probe whether ``endpoint`` can serve state at genesis-era blocks.

    Calls ``eth_getBalance`` at block 1 (falling back to block 256 when
    the node reports the block is unknown) for a known funded address.
    Any hex balance result — including ``0x0`` — proves the node can
    answer historical state queries; non-archive nodes return a
    missing-trie-node / header-not-found error instead.

    Args:
        client: RpcClient instance (call transport + pool accounting).
        endpoint: RPC endpoint URL to probe.
        chain: Chain name (logging only).

    Returns:
        True when the endpoint answered a genesis-era balance query.
    """
    for block in (1, 256):
        result = await client.call(
            endpoint,
            "eth_getBalance",
            [KNOWN_FUNDED_ADDRESS, hex(block)],
            timeout=CAPABILITY_PROBE_TIMEOUT,
        )
        if result is None:
            # Transport failure / timeout — retry at the next block, then
            # give up (not-archive) rather than raising into verification.
            continue
        if "result" in result and result["result"] is not None:
            logger.debug(
                "capability_archive_ok",
                endpoint=endpoint[:60],
                chain=chain,
                block=block,
            )
            return True
        err = result.get("error") or {}
        logger.debug(
            "capability_archive_rejected",
            endpoint=endpoint[:60],
            chain=chain,
            block=block,
            error=str(err.get("message", err))[:80] if isinstance(err, dict) else str(err)[:80],
        )
    return False


async def probe_block_receipts(
    client: RpcClient,
    endpoint: str,
    block_number: int | None = None,
) -> bool:
    """Probe whether ``endpoint`` supports ``eth_getBlockReceipts``.

    Args:
        client: RpcClient instance.
        endpoint: RPC endpoint URL to probe.
        block_number: Block to probe (defaults to the chain tip).

    Returns:
        True when the endpoint returned a receipt list.
    """
    target = block_number
    if target is None:
        target = await client.eth_block_number(endpoint)
        if target is None:
            return False
    result = await client.call(
        endpoint,
        "eth_getBlockReceipts",
        [hex(target)],
        timeout=CAPABILITY_PROBE_TIMEOUT,
    )
    if result is None:
        return False
    # A recent block always has receipts, so a list result proves support;
    # null / error results mean unsupported or errored.
    return isinstance(result.get("result"), list)


async def probe_cu_tariff(client: RpcClient, endpoint: str) -> dict[str, float]:
    """Return seeded per-method compute-unit weights for ``endpoint``.

    The weights come from provider documentation (KeyedProvider-style CU
    table) rather than live measurement; they are intentionally static
    so cost-aware ranking is stable across verification cycles. The
    ``client``/``endpoint`` parameters keep the interface uniform with
    the other probes and reserve room for a live-measurement upgrade.

    Args:
        client: RpcClient instance (unused — interface uniformity).
        endpoint: RPC endpoint URL (unused — logging only).

    Returns:
        Copy of the seeded CU tariff keyed by JSON-RPC method name.
    """
    logger.debug("capability_cu_tariff_seeded", endpoint=endpoint[:60])
    return dict(SEEDED_CU_TARIFF)


async def probe_endpoint_capabilities(
    client: RpcClient,
    endpoint: str,
    chain: str,
    topic0: str = TRANSFER_TOPIC,
) -> CapabilityRecord:
    """Run the full capability probe suite for ``endpoint`` and persist it.

    Best-effort: individual probe failures leave their fields at the
    dataclass defaults instead of raising, so callers (verification,
    pool admission) can always get a record back.

    Args:
        client: RpcClient instance.
        endpoint: RPC endpoint URL to probe.
        chain: Chain name.
        topic0: Event topic used for the getLogs-range probe.

    Returns:
        The persisted :class:`CapabilityRecord`.
    """
    record = CapabilityRecord(
        endpoint=endpoint,
        chain=chain,
        archive=await probe_archive(client, endpoint, chain),
        getlogs_max_range=await probe_getlogs_range(client, endpoint, topic0),
        block_receipts=await probe_block_receipts(client, endpoint),
        cu_tariff=await probe_cu_tariff(client, endpoint),
        probed_at=time.time(),
    )
    set_capability(record)
    return record
