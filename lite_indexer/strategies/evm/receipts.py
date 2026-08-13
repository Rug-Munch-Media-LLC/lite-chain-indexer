"""
EVM block-receipt fetching — one-call ``eth_getBlockReceipts`` with fallback.

Providers that support ``eth_getBlockReceipts`` return every receipt in
a block with a single RPC call; providers that do not (most public free
tiers) must fall back to one ``eth_getTransactionReceipt`` call per
transaction.

Cost: a typical EVM block carries ~25 transactions, so the one-call path
is ~25x cheaper than the per-tx fallback loop. :func:`get_block_receipts`
hides that choice behind one signature: callers use it whenever they
need a full block's receipts (backfill materialization, reorg repair,
deployer verification) and get the fast path automatically on capable
endpoints.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import RPC_CALL_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.rpc.client import RpcClient

logger = get_logger(__name__)


async def get_block_receipts(
    rpc_client: RpcClient,
    endpoint: str,
    block_number: int,
) -> list[dict[str, Any]] | None:
    """Fetch all transaction receipts for ``block_number``.

    Tries one ``eth_getBlockReceipts`` call first. When the endpoint
    does not support the method (or the call errors/returns null), falls
    back to fetching the block header with ``full_tx=False`` and issuing
    one ``eth_getTransactionReceipt`` per transaction hash — the N-call
    path is ~25x more expensive on a typical block, which is exactly why
    capable endpoints are preferred for receipt-heavy work.

    Args:
        rpc_client: RpcClient instance (pool accounting + transport).
        endpoint: RPC endpoint URL (the endpoint is probed per-call;
            the client itself is endpoint-agnostic).
        block_number: Block whose receipts are requested.

    Returns:
        List of raw receipt dicts, ``[]`` for an empty block, or ``None``
        when the block could not be fetched at all or every per-tx
        receipt call failed (caller should treat ``None`` as a retryable
        failure, not an empty result).
    """
    # Fast path: one call returns the whole block's receipts.
    batch_result = await rpc_client.call(
        endpoint,
        "eth_getBlockReceipts",
        [hex(block_number)],
        timeout=RPC_CALL_TIMEOUT,
    )
    if batch_result is not None and isinstance(batch_result.get("result"), list):
        return list(batch_result["result"])

    logger.debug(
        "get_block_receipts_fallback_per_tx",
        block=block_number,
        endpoint=endpoint[:60],
    )

    # Fallback: block header with full_tx=False gives tx hash strings —
    # cheaper than full_tx=True which embeds whole transaction objects.
    block = await rpc_client.eth_get_block_by_number(endpoint, block_number, full_tx=False)
    if block is None:
        logger.warning("get_block_receipts_block_missing", block=block_number)
        return None

    transactions = block.get("transactions") or []
    if not transactions:
        return []

    receipts: list[dict[str, Any]] = []
    for tx in transactions:
        tx_hash = tx if isinstance(tx, str) else (tx.get("hash") if isinstance(tx, dict) else None)
        if not tx_hash:
            continue
        receipt = await rpc_client.eth_get_transaction_receipt(endpoint, tx_hash)
        if receipt is not None:
            receipts.append(receipt)

    if not receipts:
        logger.warning(
            "get_block_receipts_all_failed",
            block=block_number,
            tx_count=len(transactions),
        )
        return None
    return receipts


__all__ = ["get_block_receipts"]
