"""RPC convenience reads — tip, block headers, receipts, eth_call.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

from typing import Any, cast

import lite_indexer.rpc.client as _facade
from lite_indexer.config import RPC_CALL_TIMEOUT, TIMESTAMP_FETCH_TIMEOUT


class BlocksMixin:
    """Small typed wrappers around the single-call path."""

    async def eth_block_number(self, endpoint: str) -> int | None:
        """Get latest block number. Handles empty/malformed responses,
        connection errors and non-hex results (all return None)."""
        result = await self.call(endpoint, "eth_blockNumber")
        if not result or "result" not in result:
            _facade._safe_record(_facade.record_endpoint_tip_failure, endpoint)
            return None
        try:
            height = int(result["result"], 16)
        except (ValueError, TypeError) as exc:
            _facade.logger.debug(
                "eth_block_number_non_hex", endpoint=endpoint[:50], error=str(exc)[:40]
            )
            _facade._safe_record(_facade.record_endpoint_tip_failure, endpoint)
            return None
        _facade._safe_record(_facade.record_endpoint_block_height, endpoint, height)
        return height

    async def eth_get_block_by_number(
        self,
        endpoint: str,
        block_number: int,
        full_tx: bool = False,
        *,
        chain: str | None = None,
        tip: int | None = None,
    ) -> dict[str, Any] | None:
        """Get a block by its number.

        Historical blocks below ``RPC_CACHE_CONFIRMATIONS`` are served from
        the persistent response cache; tip/near-tip reads are never cached.
        """
        from lite_indexer.rpc import respcache

        params = [hex(block_number), full_tx]
        cached = respcache.get(
            "eth_getBlockByNumber", params, chain=chain, block_number=block_number, tip=tip
        )
        if cached is not None and isinstance(cached.get("result"), dict):
            return cast(dict[str, Any], cached["result"])

        result = await self.call(
            endpoint,
            "eth_getBlockByNumber",
            params,
            timeout=TIMESTAMP_FETCH_TIMEOUT,
        )
        if result and "result" in result:
            block = cast(dict[str, Any], result["result"])
            if block:
                # Feed the staleness gate (skip null blocks — a missing
                # block would skew the median toward 0).
                _facade._safe_record(_facade.record_endpoint_block_height, endpoint, block_number)
                block_hash = block.get("hash")
                respcache.put(
                    "eth_getBlockByNumber",
                    params,
                    result,
                    chain=chain,
                    block_number=block_number,
                    block_hash=str(block_hash) if block_hash else None,
                    tip=tip,
                )
            return block
        return None

    async def eth_get_transaction_receipt(
        self, endpoint: str, tx_hash: str
    ) -> dict[str, Any] | None:
        """Get a transaction receipt.

        Routed to dedicated (API-key) nodes by default — receipts are
        small, latency-sensitive calls better served by a dedicated node
        than by a heavily-loaded public tier. See
        :func:`lite_indexer.rpc.routing.select_endpoint_for_method`.

        Args:
            endpoint: RPC endpoint URL.
            tx_hash: 0x-prefixed transaction hash.

        Returns:
            Receipt dict, or None on failure.
        """
        result = await self.call(
            endpoint,
            "eth_getTransactionReceipt",
            [tx_hash],
            timeout=RPC_CALL_TIMEOUT,
        )
        if result and "result" in result:
            return cast(dict[str, Any], result["result"])
        return None

    async def eth_call(self, endpoint: str, to: str, data: str) -> str | None:
        """Execute an eth_call against a contract.

        Args:
            endpoint: RPC endpoint URL.
            to: Contract address.
            data: Hex-encoded call data (selector + args).

        Returns:
            Hex result string, or None on failure.
        """
        result = await self.call(
            endpoint,
            "eth_call",
            [{"to": to, "data": data}, "latest"],
        )
        if result and "result" in result:
            return str(result["result"])
        return None
