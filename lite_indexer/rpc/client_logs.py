"""eth_getLogs path — range fetch with split, batching and consensus.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.rpc.client_logs_support import (
    _getlogs_gates,
    _logs_cached,
    _LogsQuery,
    _verify_and_store,
)
from lite_indexer.rpc.client_mixin_api import MixinApi

logger = get_logger(__name__)


class LogsMixin:
    """eth_getLogs orchestration on top of the single/batch call paths."""

    async def get_logs(
        self: MixinApi,
        endpoint: str,
        from_block: int,
        to_block: int,
        topic: str,
        address: str | None = None,
        max_retries: int = 3,
        *,
        chain: str | None = None,
        tip: int | None = None,
    ) -> list[dict[str, Any]] | None:
        """Fetch event logs via eth_getLogs with retry.

        Applies per-provider token bucket rate limiting before each request.
        Automatically falls back to smaller ranges if the endpoint complains
        about range size.

        Args:
            endpoint: RPC endpoint URL.
            from_block: Start block.
            to_block: End block.
            topic: Event topic hash.
            address: Optional contract address filter.
            max_retries: Max retry attempts on rate limits.
            chain: Chain name — required for sampled cross-provider
                consensus verification (a chain's pool is resolved from
                it). When omitted the read is returned unverified.

        Returns:
            List of raw log dicts, or None on failure.
        """
        if _getlogs_gates(endpoint, from_block, to_block):
            return None
        query = _LogsQuery(
            endpoint=endpoint,
            from_block=from_block,
            to_block=to_block,
            topic=topic,
            address=address,
            chain=chain,
            tip=tip,
        )
        cached = _logs_cached(query)
        if cached is not None:
            return cached

        from lite_indexer.rpc.range_split import fetch_logs_split

        async def _once(child_from: int, child_to: int) -> tuple[str, list[dict[str, Any]] | None]:
            return await self._fetch_logs_range(endpoint, child_from, child_to, topic, address, max_retries)

        async def _many(
            child_ranges: list[tuple[int, int]],
        ) -> list[tuple[str, list[dict[str, Any]] | None]]:
            # Sibling sub-ranges produced by the splitter go in ONE batched
            # POST (1 request for the request caps), halving the request cost
            # of an in-call split.
            return await self._fetch_logs_many(endpoint, child_ranges, topic, address)

        logs = await fetch_logs_split(_once, from_block, to_block, fetch_many=_many)
        return await _verify_and_store(self, query, logs)

    @staticmethod
    def _logs_filter(from_block: int, to_block: int, topic: str, address: str | None) -> dict[str, Any]:
        """Build one ``eth_getLogs`` filter object for a block range."""
        filter_params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [topic],
        }
        if address:
            filter_params["address"] = address
        return filter_params

    async def _fetch_logs_many(
        self: MixinApi,
        endpoint: str,
        ranges: list[tuple[int, int]],
        topic: str,
        address: str | None,
    ) -> list[tuple[str, list[dict[str, Any]] | None]]:
        """Fetch several getLogs ranges in one batch; classify each element."""
        from lite_indexer.rpc.range_split import classify_logs_error

        calls: list[tuple[str, list[Any] | None]] = [
            ("eth_getLogs", [self._logs_filter(f, t, topic, address)]) for f, t in ranges
        ]
        responses = await self.call_batch(endpoint, calls)
        out: list[tuple[str, list[dict[str, Any]] | None]] = []
        for resp in responses:
            if not isinstance(resp, dict):
                out.append(("error", None))
                continue
            if "result" in resp:
                result = resp["result"]
                if isinstance(result, list):
                    out.append(("ok", result))
                elif result is None:
                    out.append(("ok", []))
                else:
                    out.append(("error", None))
            else:
                out.append((classify_logs_error(resp), None))
        return out
