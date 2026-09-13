"""
Solana transfer adapter — SPL token transfers via a data-first RPC pool.

A focused, single-provider adapter that answers one question: "what SPL
token transfers happened for these mints in this slot range?" It uses the
SolanaProvider RPC (``getSignaturesForAddress`` + ``getTransaction``) and writes
the parsed rows to the ``wallet_memory.solana_transfers`` ClickHouse table.

Why a dedicated adapter (vs. the existing ``SolanaStrategy``)?

  * The strategy owns the full index/sync lifecycle (highwater, dynamic
    token discovery, launchpad watcher, deployer capture, DEX swaps,
    Jito bundles) and writes SPL balance changes to
    ``solana_spl_balance_changes``.
  * This adapter is a thin, reusable accessor for callers that just want
    SPL transfers from SolanaProvider written to ``solana_transfers`` — e.g. a
    backfill job, an on-demand scanner, or a one-off enrichment pass. It
    has no state, no highwater, and no side effects beyond the table write.

The endpoint pool is resolved from
``lite_indexer.config.data_sources.solana_rpc_endpoints()`` — our own /
dedicated RPCs and verified public endpoints FIRST, SolanaProvider LAST (fallback
only, resolved from ``SOLANA_PROVIDER_KEY``). When no endpoints resolve the
adapter degrades gracefully and logs, never hard-failing on a missing
key.

Rows follow the ``wallet_memory.solana_transfers`` schema (see
``migrations/004_solana_transfers.sql`` and
:func:`lite_indexer.traces.solana.extract_solana_transfers`): one row per
value movement with ``transfer_type`` in (``spl_transfer``,
``spl_transfer_checked``, ``native_sol``, ``cpi_sol``, ...).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.traces.solana import (
    ensure_solana_transfers_table,
    extract_solana_transfers,
    write_solana_transfers,
)

logger = get_logger(__name__)

#: Optional keyed provider RPC URL template (private overlay/env supplied).
#: The ``${...}`` placeholder is resolved from the environment; empty by
#: default, so the public build uses only the shared keyless pool.
SOLANA_PROVIDER_RPC_TEMPLATE: str = os.environ.get("SOLANA_PROVIDER_RPC_TEMPLATE", "")

#: Max signatures fetched per mint per call (bounds free-tier RPC spend).
MAX_SIGNATURES_PER_MINT: int = 100

#: Max concurrent ``getTransaction`` calls.
TX_CONCURRENCY: int = 10


def solana_rpc_url() -> str | None:
    """Resolve the optional keyed provider RPC URL, or None when unset.

    Returns:
        The fully-resolved provider RPC URL, or None when no template/key is
        configured (callers fall back to the shared keyless pool).
    """
    if not SOLANA_PROVIDER_RPC_TEMPLATE:
        return None
    url = os.path.expandvars(SOLANA_PROVIDER_RPC_TEMPLATE)
    if "${" in url or not url.startswith("http"):
        return None
    return url


class SolanaTransfersAdapter:
    """Fetch SPL token transfers from the Solana RPC pool and store them.

    Stateless and reusable: construct once, call :meth:`fetch_transfers`
    (or :meth:`fetch_and_store`) as needed. Each call opens its own
    ``httpx.AsyncClient`` and closes it when done, so the adapter is safe
    to use from short-lived jobs and long-running daemons alike.

    Args:
        tracked_tokens: List of ``{"mint", "symbol", "decimals", "name"}``
            dicts to index. Defaults to the strategy's tracked set.
        endpoint: Optional explicit RPC URL. Defaults to the data-first
            pool (own/dedicated + public RPCs first, SolanaProvider last).
    """

    def __init__(
        self,
        tracked_tokens: list[dict[str, Any]] | None = None,
        endpoint: str | None = None,
    ) -> None:
        from lite_indexer.strategies.solana.models import SOLANA_TRACKED_TOKENS  # noqa: PLC0415

        self.tracked_tokens = tracked_tokens or SOLANA_TRACKED_TOKENS
        self._mint_set: set[str] = {t["mint"] for t in self.tracked_tokens}
        # DATA-FIRST: own/dedicated + public RPC pool first, SolanaProvider LAST.
        # An explicit ``endpoint`` override still wins (e.g. tests, ops).
        if endpoint:
            self._endpoints: list[str] = [endpoint]
        else:
            from lite_indexer.data_sources import solana_rpc_endpoints  # noqa: PLC0415

            self._endpoints = solana_rpc_endpoints()
        self._endpoint = self._endpoints[0] if self._endpoints else None

    # ── RPC ──────────────────────────────────────────────────────────

    async def _rpc_call(
        self,
        client: httpx.AsyncClient,
        method: str,
        params: list[Any] | None = None,
    ) -> dict[str, Any] | None:
        """Make a JSON-RPC call, rotating through the adapter's endpoints.

        Args:
            client: The shared ``httpx.AsyncClient``.
            method: Solana RPC method name.
            params: Method parameters.

        Returns:
            The parsed JSON-RPC result dict, or None on failure.
        """
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
        last_error: str | None = None
        for url in self._endpoints:
            try:
                resp = await client.post(url, json=payload, timeout=30.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and "result" in data:
                        return data
                    if isinstance(data, dict) and "error" in data:
                        last_error = str(data["error"])
                        continue
                last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = str(exc)[:60]
                continue
        logger.warning("solana_adapter_rpc_failed", method=method, error=last_error)
        return None

    # ── Fetch ────────────────────────────────────────────────────────

    async def fetch_transfers(self, from_slot: int, to_slot: int) -> list[dict[str, Any]]:
        """Fetch SPL token transfers for the tracked mints in a slot range.

        For each tracked mint:
          1. ``getSignaturesForAddress`` — recent signatures (newest first).
          2. Filter signatures whose slot falls in ``[from_slot, to_slot]``.
          3. ``getTransaction`` (jsonParsed) for each relevant signature.
          4. Parse pre/post token balances into ``solana_transfers`` rows.

        Args:
            from_slot: Start slot (inclusive).
            to_slot: End slot (inclusive).

        Returns:
            List of parsed transfer rows (``solana_transfers`` schema).
        """
        if from_slot > to_slot:
            return []

        all_rows: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(TX_CONCURRENCY)

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        ) as client:
            for token in self.tracked_tokens:
                mint = token["mint"]
                try:
                    rows = await self._fetch_mint_transfers(client, mint, from_slot, to_slot, semaphore)
                    all_rows.extend(rows)
                except Exception as exc:  # noqa: BLE001 — per-mint isolation
                    logger.warning(
                        "solana_adapter_mint_fetch_failed",
                        mint=mint[:20],
                        error=str(exc)[:80],
                    )

        metrics_incr("solana_adapter_transfers_fetched", len(all_rows))
        return all_rows

    async def _fetch_mint_transfers(
        self,
        client: httpx.AsyncClient,
        mint: str,
        from_slot: int,
        to_slot: int,
        semaphore: asyncio.Semaphore,
    ) -> list[dict[str, Any]]:
        """Fetch and parse transfers for a single mint in a slot range.

        Args:
            client: The shared ``httpx.AsyncClient``.
            mint: SPL mint address.
            from_slot: Start slot (inclusive).
            to_slot: End slot (inclusive).
            semaphore: Shared concurrency limiter.

        Returns:
            List of parsed transfer rows for this mint.
        """
        result = await self._rpc_call(
            client,
            "getSignaturesForAddress",
            [mint, {"limit": MAX_SIGNATURES_PER_MINT}],
        )
        if not result or "result" not in result or not isinstance(result["result"], list):
            return []
        sig_infos = result["result"]

        relevant = [
            s
            for s in sig_infos
            if isinstance(s, dict)
            and s.get("slot") is not None
            and from_slot <= int(s["slot"]) <= to_slot
            and s.get("err") is None
        ]
        if not relevant:
            return []

        rows: list[dict[str, Any]] = []

        async def _fetch_and_parse(sig_info: dict[str, Any]) -> list[dict[str, Any]]:
            sig = sig_info["signature"]
            slot = int(sig_info["slot"])
            async with semaphore:
                tx_result = await self._rpc_call(
                    client,
                    "getTransaction",
                    [
                        sig,
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0,
                            "commitment": "finalized",
                        },
                    ],
                )
            if not tx_result or "result" not in tx_result or tx_result["result"] is None:
                return []
            return extract_solana_transfers(tx_result["result"], slot)

        outcomes = await asyncio.gather(
            *(_fetch_and_parse(s) for s in relevant),
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, list):
                rows.extend(outcome)
        return rows

    # ── Store ────────────────────────────────────────────────────────

    def store_transfers(self, ch_client: Any, rows: list[dict[str, Any]]) -> int:
        """Write transfer rows to ``wallet_memory.solana_transfers``.

        Ensures the table exists (idempotent) before inserting. Best-effort:
        returns the number of rows written (0 on failure).

        Args:
            ch_client: ClickHouse client (anything with ``execute``).
            rows: Transfer rows in the ``solana_transfers`` schema.

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
        ensure_solana_transfers_table(ch_client)
        return write_solana_transfers(ch_client, rows)

    async def fetch_and_store(self, ch_client: Any, from_slot: int, to_slot: int) -> int:
        """Fetch SPL transfers and write them to ClickHouse in one call.

        Convenience wrapper around :meth:`fetch_transfers` +
        :meth:`store_transfers`.

        Args:
            ch_client: ClickHouse client (anything with ``execute``).
            from_slot: Start slot (inclusive).
            to_slot: End slot (inclusive).

        Returns:
            Number of rows written to ClickHouse.
        """
        rows = await self.fetch_transfers(from_slot, to_slot)
        return self.store_transfers(ch_client, rows)
