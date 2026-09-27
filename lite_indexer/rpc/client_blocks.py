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
from lite_indexer.rpc import status_recorder


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
            _facade.logger.debug("eth_block_number_non_hex", endpoint=endpoint[:50], error=str(exc)[:40])
            _facade._safe_record(_facade.record_endpoint_tip_failure, endpoint)
            return None
        _facade._safe_record(_facade.record_endpoint_block_height, endpoint, height)
        return height

    async def eth_get_block_by_number_with_status(
        self,
        endpoint: str,
        block_number: int,
        full_tx: bool = False,
        *,
        chain: str | None = None,
        tip: int | None = None,
    ) -> tuple[dict[str, Any] | None, int | None]:
        """Get a block by number, and the HTTP status that answered it.

        :meth:`eth_get_block_by_number` cannot tell a caller *why* it
        returned ``None``: a 200 with a pruned result, a pool-wide 403, and
        a transport failure all collapse to the same shape. The 2026-09-26
        pool walk needs that difference — it is the whole basis for
        separating a window boundary from a refusal — so this variant
        reports it.

        The status comes from the *last* completed HTTP attempt, read out of
        the call path's response-status record (see
        :mod:`lite_indexer.rpc.status_recorder`). It is ``None`` when no
        attempt completed at all.

        Args:
            endpoint: RPC endpoint URL.
            block_number: Block to fetch.
            full_tx: Whether to include transaction objects.
            chain: Chain name, for the response cache key.
            tip: Chain tip, for cache freshness decisions.

        Returns:
            ``(block, status)``. ``block`` is ``None`` when the endpoint had
            no such block; ``status`` is the HTTP status of the final
            attempt, or ``None`` when the attempt never got a response.
        """
        from lite_indexer.rpc import respcache

        params = [hex(block_number), full_tx]
        cached = respcache.get("eth_getBlockByNumber", params, chain=chain, block_number=block_number, tip=tip)
        if cached is not None and isinstance(cached.get("result"), dict):
            return cast(dict[str, Any], cached["result"]), 200

        status_recorder.begin(endpoint)
        result = await self.call(
            endpoint,
            "eth_getBlockByNumber",
            params,
            timeout=TIMESTAMP_FETCH_TIMEOUT,
        )
        status = status_recorder.last_response_status(endpoint)
        if result and "result" in result:
            block = cast(dict[str, Any], result["result"])
            if block:
                # Feed the staleness gate (skip null blocks — a missing
                # block would skew the median toward 0).
                _facade._safe_record(_facade.record_endpoint_block_height, endpoint, block_number)
                respcache.put(
                    "eth_getBlockByNumber",
                    params,
                    result,
                    chain=chain,
                    block_number=block_number,
                    block_hash=str(block.get("hash")) if block.get("hash") else None,
                    tip=tip,
                )
            return block, status
        return None, status

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
        block, _ = await self.eth_get_block_by_number_with_status(endpoint, block_number, full_tx, chain=chain, tip=tip)
        return block

    async def eth_get_transaction_receipt(self, endpoint: str, tx_hash: str) -> dict[str, Any] | None:
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
