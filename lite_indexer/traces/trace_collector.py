"""
TraceCollector — fetches internal transfers from free trace RPCs.

Subscribes to the ``debug_traceBlockByNumber`` (geth callTracer) and
``trace_block`` (parity/erigon) methods that free archive-capable RPCs
expose, then extracts value-transferring internal calls — the ETH flows
that ``eth_getLogs`` cannot see (WETH unwraps, bridge deposits, fee
flows, SELFDESTRUCT payouts).

Design notes:

  - Method preference: ``debug_traceBlockByNumber`` with the
    ``callTracer`` is tried first — it returns a nested call tree (one
    object per transaction, only frames the tracer visited) which is
    cheaper than parity's flat list. Endpoints that reject it (method not
    whitelisted / not supported) are remembered per-endpoint and skipped
    on subsequent blocks, falling back to ``trace_block``.
  - Endpoint rotation: the shared RPC pool's ``get_best_endpoint`` /
    ``record_endpoint_failure`` feedback is used so a dead or
    rate-limited trace endpoint is deprioritized just like the getLogs
    pool.
  - Rate limiting: per-provider token buckets are applied before every
    call (same as ``eth_getLogs``), because free-tier trace endpoints are
    aggressively rate-limited.
  - Best-effort: a trace failure never blocks the main indexer loop — the
    collector returns an empty list and logs a warning; callers treat
    traces as optional enrichment.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import TRACE_CALL_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import RpcClient, get_best_endpoint, record_endpoint_failure
from lite_indexer.rpc.ratelimit import apply_provider_rate_limit
from lite_indexer.traces.endpoints import TRACE_ENDPOINTS
from lite_indexer.traces.parsers import extract_internal_transfers as _parse_rows

logger = get_logger(__name__)

# JSON-RPC error text that means "this endpoint does not serve this
# trace method on the free tier" — the endpoint itself may be fine.
_UNSUPPORTED_MARKERS = (
    "whitelist",
    "not supported",
    "unsupported",
    "method not found",
    "does not exist",
    "not available",
    "paid plan",
    "archive",
    "personal token",
    "only core evm",
)


def _is_unsupported_method(error: Any) -> bool:
    """Return True when an RPC error indicates the method is unsupported.

    Args:
        error: The ``error`` member of a JSON-RPC response (dict or None).

    Returns:
        True when the error is a "method not supported / not whitelisted"
        style error rather than a transient network or data failure.
    """
    if not isinstance(error, dict):
        return False
    code = error.get("code")
    if code == -32601:  # Method not found
        return True
    message = str(error.get("message") or "")
    lowered = message.lower()
    return any(marker in lowered for marker in _UNSUPPORTED_MARKERS)


class TraceCollector:
    """Collect internal transfers from trace_* RPC methods.

    One collector per chain. Lazily creates its own RpcClient and
    reuses the shared endpoint pool's health/rotation feedback.
    """

    def __init__(
        self,
        chain: str,
        endpoints: list[str] | None = None,
        rpc: RpcClient | None = None,
        timeout: float = TRACE_CALL_TIMEOUT,
    ) -> None:
        """Initialize the collector for a chain.

        Args:
            chain: Chain name (e.g. 'ethereum').
            endpoints: Trace-capable RPC endpoints. Defaults to the
                verified list from :mod:`lite_indexer.traces.endpoints`.
            rpc: Optional shared RpcClient (created lazily otherwise).
            timeout: Per-call timeout (trace calls can be slow).
        """
        self.chain = chain
        self.endpoints = list(endpoints) if endpoints else list(TRACE_ENDPOINTS.get(chain, []))
        self.rpc = rpc or RpcClient()
        self.timeout = timeout
        # endpoint -> method that worked ("debug_traceBlockByNumber" or
        # "trace_block"). Learned on first use; avoids re-probing every
        # block after an endpoint tells us it rejects a method.
        self._method_cache: dict[str, str] = {}
        # block_number -> datetime, cached so a full-range sweep issues
        # one timestamp call per block (reused across rows of the block).
        self._ts_cache: dict[int, datetime] = {}

    # ── Public API ─────────────────────────────────────────────────

    async def collect_block_traces(self, chain: str, block_number: int) -> list[dict[str, Any]]:
        """Fetch + parse internal transfers for a single block.

        Args:
            chain: Chain name (must match the collector's endpoints).
            block_number: Block to trace.

        Returns:
            List of standardized internal-transfer rows (possibly empty
            when the block has no value-moving internal calls, or when
            all trace endpoints failed).
        """
        if not self.endpoints:
            logger.warning("trace_no_endpoints", chain=chain)
            return []

        raw = await self._fetch_block(block_number)
        if not raw:
            return []

        block_timestamp = await self._block_timestamp(block_number)
        return self.extract_internal_transfers(raw, chain, block_number, block_timestamp)

    async def collect_range(
        self,
        chain: str,
        from_block: int,
        to_block: int,
        concurrency: int = 2,
        max_blocks: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch + parse internal transfers for a block range.

        Blocks are fetched with bounded concurrency to respect free-tier
        rate limits. Best-effort: individual block failures are logged and
        skipped, the successful blocks' rows are returned.

        Args:
            chain: Chain name.
            from_block: First block (inclusive).
            to_block: Last block (inclusive).
            concurrency: Max parallel trace calls (default 2 — trace
                endpoints are rate-limited).
            max_blocks: Optional cap on how many blocks to sweep in one
                call (most recent first); None sweeps the full range.

        Returns:
            List of standardized internal-transfer rows.
        """
        blocks = list(range(from_block, to_block + 1))
        if max_blocks is not None and max_blocks > 0:
            blocks = blocks[-max_blocks:]

        if not self.endpoints:
            logger.warning("trace_no_endpoints", chain=chain)
            return []

        semaphore = asyncio.Semaphore(max(1, concurrency))

        async def _one(block_number: int) -> list[dict[str, Any]]:
            async with semaphore:
                try:
                    return await self.collect_block_traces(chain, block_number)
                except Exception as exc:  # noqa: BLE001 — best-effort tracing
                    logger.warning(
                        "trace_block_failed",
                        chain=chain,
                        block=block_number,
                        error=str(exc)[:100],
                    )
                    return []

        results = await asyncio.gather(*(_one(b) for b in blocks), return_exceptions=True)
        rows: list[dict[str, Any]] = []
        for res in results:
            if isinstance(res, list):
                rows.extend(res)
        metrics_incr("traces_collected", len(rows))
        return rows

    def extract_internal_transfers(
        self,
        trace_results: list[Any],
        chain: str | None = None,
        block_number: int = 0,
        block_timestamp: datetime | str | None = None,
    ) -> list[dict[str, Any]]:
        """Extract only value-transferring internal calls.

        Args:
            trace_results: Raw ``result`` array from trace_block or
                debug_traceBlockByNumber (callTracer).
            chain: Chain name (defaults to the collector's chain).
            block_number: Block number being traced.
            block_timestamp: Block timestamp (datetime or ISO string).

        Returns:
            Standardized internal-transfer rows: ``chain, block_number,
            block_timestamp, tx_hash, trace_address, from_address,
            to_address, value_wei, call_type, gas_used, error,
            input_method_id``.
        """
        return _parse_rows(
            trace_results,
            chain or self.chain,
            block_number,
            block_timestamp,
        )

    # ── Internals ─────────────────────────────────────────────────

    async def _fetch_block(self, block_number: int) -> list[Any]:
        """Fetch raw trace results for a block, rotating endpoints.

        Tries ``debug_traceBlockByNumber`` (callTracer) first, falling
        back to ``trace_block`` when the endpoint rejects the method.
        Returns the parsed ``result`` array, or [] when all endpoints
        failed.

        Args:
            block_number: Block to trace.

        Returns:
            Raw trace result list (may be empty).
        """
        block_hex = hex(block_number)
        endpoints = self.endpoints
        if not endpoints:
            return []
        start = endpoints.index(get_best_endpoint(endpoints)) if endpoints else 0
        rotated = endpoints[start:] + endpoints[:start]

        for endpoint in rotated:
            method = self._method_cache.get(endpoint, "debug_traceBlockByNumber")
            result = await self._call_method(endpoint, method, block_hex)
            if result is not None:
                return result

            # Method rejected → remember and try the parity fallback.
            if method == "debug_traceBlockByNumber":
                self._method_cache[endpoint] = "trace_block"
                result = await self._call_method(endpoint, "trace_block", block_hex)
                if result is not None:
                    return result

            record_endpoint_failure(endpoint)

        metrics_incr("trace_errors")
        logger.warning("trace_fetch_failed", chain=self.chain, block=block_number)
        return []

    async def _call_method(self, endpoint: str, method: str, block_hex: str) -> list[Any] | None:
        """Call one trace method on one endpoint.

        Applies provider rate limiting before the call. Returns the
        ``result`` list, or None when the call failed / the method is
        unsupported (caller decides whether to fall back).

        Args:
            endpoint: RPC endpoint URL.
            method: 'debug_traceBlockByNumber' or 'trace_block'.
            block_hex: Block number as 0x-prefixed hex.

        Returns:
            Result list, or None on failure.
        """
        await apply_provider_rate_limit(endpoint)
        if method == "debug_traceBlockByNumber":
            params: list[Any] = [block_hex, {"tracer": "callTracer"}]
        else:
            params = [block_hex]

        response = await self.rpc.call(endpoint, method, params, timeout=self.timeout)
        if response is None:
            return None

        error = response.get("error")
        if error is not None:
            if _is_unsupported_method(error):
                logger.debug(
                    "trace_method_unsupported",
                    endpoint=endpoint[:40],
                    method=method,
                    error=str(error)[:80],
                )
                return None
            logger.debug("trace_rpc_error", endpoint=endpoint[:40], method=method, error=str(error)[:80])
            return None

        result = response.get("result")
        if result is None:
            return []
        if not isinstance(result, list):
            return []
        return result

    async def _block_timestamp(self, block_number: int) -> datetime | None:
        """Fetch a block's timestamp (cached per block number).

        Uses ``eth_getBlockByNumber`` — the same call the indexer makes
        for transfer enrichment, so no new endpoint capability is needed.

        Args:
            block_number: Block number.

        Returns:
            Timezone-aware UTC datetime, or None when the fetch failed.
        """
        if block_number in self._ts_cache:
            return self._ts_cache[block_number]
        ts: datetime | None = None
        for endpoint in self.endpoints:
            block = await self.rpc.eth_get_block_by_number(endpoint, block_number)
            if block and block.get("timestamp"):
                try:
                    ts = datetime.fromtimestamp(int(block["timestamp"], 16), tz=UTC)
                except (ValueError, TypeError):
                    ts = None
                if ts is not None:
                    break
        if ts is not None:
            self._ts_cache[block_number] = ts
        return ts
