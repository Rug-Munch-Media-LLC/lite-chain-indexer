"""SolanaTraceCollector — fetch full transaction traces from free RPCs.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import httpx

from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS
from lite_indexer.logging import get_logger
from lite_indexer.util.http import http_limits

from .dex import build_transfer_graph_from_rows
from .parsers import extract_solana_transfers

logger = get_logger(__name__)


class SolanaTraceCollector:
    """Collect detailed Solana transaction traces from free RPCs.

    Wraps ``getTransaction`` (``jsonParsed`` +
    ``maxSupportedTransactionVersion=0``) with endpoint rotation and 429
    backoff, mirroring the Solana strategy's RPC client so the same pool
    of verified free endpoints is shared. The extraction itself is pure —
    the strategy feeds the transaction it already fetched into
    :meth:`extract_transfers` with no extra RPC call.
    """

    def __init__(
        self,
        endpoints: list[str] | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
        concurrency: int = 10,
    ) -> None:
        """Initialize the trace collector.

        Args:
            endpoints: RPC endpoints to rotate through. Defaults to the
                verified pool from :mod:`lite_indexer.chains.non_evm`
                (api.mainnet-beta.solana.com + solana-rpc.publicnode.com).
            client: Optional shared httpx AsyncClient (created lazily).
            timeout: Per-call timeout.
            concurrency: Max parallel getTransaction calls for
                ``build_transfer_graph`` (not used by single-call methods).
        """
        self.endpoints = list(endpoints) if endpoints else list(SOLANA_ENDPOINTS)
        self._client = client
        self.timeout = timeout
        self.concurrency = max(1, concurrency)

    # ── Public API ───────────────────────────────────────────────────

    async def collect_transaction_trace(self, signature: str) -> dict[str, Any] | None:
        """Fetch a full transaction trace via getTransaction.

        Uses ``encoding=jsonParsed`` + ``maxSupportedTransactionVersion=0``
        so the response includes parsed outer instructions AND
        ``meta.innerInstructions`` (the CPI tree) for version-0 txs.

        Args:
            signature: Transaction signature (base58).

        Returns:
            The parsed transaction dict (``result`` member of the RPC
            response), or None when every endpoint failed.
        """
        result = await self._rpc_call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "finalized",
                },
            ],
        )
        if not isinstance(result, dict) or not result:
            return None
        return result

    async def extract_transfers(self, parsed_tx: dict[str, Any], slot: int | None = None) -> list[dict[str, Any]]:
        """Extract ALL value transfers from an already-fetched transaction.

        Pure parsing — no RPC calls. The strategy calls this with the
        ``getTransaction`` result it already fetched for SPL balance
        changes, so full trace visibility costs zero extra requests.

        Args:
            parsed_tx: Parsed ``getTransaction`` result (jsonParsed).
            slot: Slot number (defaults to ``parsed_tx["slot"]``).

        Returns:
            List of standardized transfer-trace row dicts.
        """
        return extract_solana_transfers(parsed_tx, slot)

    async def build_transfer_graph(self, signature: str) -> dict[str, Any]:
        """Build a directed graph of all value movement in a transaction.

        Aggregates transfer rows by (from, to, mint, transfer_type),
        summing amounts so parallel legs of a swap collapse into one edge.

        Args:
            signature: Transaction signature (base58).

        Returns:
            Graph dict: ``{signature, chain, slot, block_timestamp,
            transfers, nodes, edges}``, or ``{signature, error}`` when the
            transaction could not be fetched.
        """
        tx_data = await self.collect_transaction_trace(signature)
        if tx_data is None:
            return {"signature": signature, "error": "transaction_fetch_failed"}

        rows = extract_solana_transfers(tx_data)
        return build_transfer_graph_from_rows(signature, tx_data, rows)

    # ── Internals ───────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the shared httpx client (created lazily)."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(self.timeout), limits=http_limits())
        return self._client

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent; called on shutdown)."""
        client, self._client = self._client, None
        if client is not None and not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()

    async def _rpc_call(self, method: str, params: list[Any] | None = None) -> Any | None:
        """Call one JSON-RPC method, rotating endpoints with 429 backoff.

        Mirrors the Solana strategy's ``rpc_call``: tries each endpoint in
        the pool, backs off briefly on 429, and returns the first
        successful ``result`` (a dict for getTransaction, a list for
        getSignaturesForAddress, etc.). Returns None when all endpoints
        failed.

        Args:
            method: Solana RPC method name.
            params: Method parameters.

        Returns:
            The response ``result`` member (Any), or None on failure.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        client = await self._get_client()
        last_error: str | None = None
        for url in self.endpoints:
            try:
                resp = await client.post(url, json=payload, timeout=self.timeout)
                if resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    result = data.get("result") if isinstance(data, dict) else None
                    if result is not None:
                        return result
                    if isinstance(data, dict) and "error" in data:
                        last_error = str(data["error"])
                        continue
                last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPError, ValueError) as exc:
                last_error = str(exc)[:60]
                continue
        logger.debug("solana_trace_rpc_failed", method=method, error=last_error)
        return None


__all__ = ["SolanaTraceCollector"]
