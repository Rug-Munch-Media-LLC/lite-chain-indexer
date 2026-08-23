"""
On-demand RPC fetcher — targeted calls for specific data, not full-chain scans.

The QueryRouter's last-resort data source. When the requested data is
neither in ClickHouse (hot) nor in the B2 parquet cold tier, this fetcher
dials the chain's RPC pool for exactly the block / tx / address in
question:

* ``fetch_transfer`` — ``eth_getTransactionReceipt`` + Transfer-log parse
  (one tx, one call).
* ``fetch_address_transfers`` — targeted ``eth_getLogs`` with the Transfer
  topic and an address filter over a bounded block window.
* ``fetch_deployer`` — creation-tx hint via ``eth_getTransactionByHash``,
  or a bounded ``trace_filter`` creation scan when supported.
* ``fetch_block_info`` — ``eth_getBlockByNumber`` with full transactions.

Every call is routed through the existing per-method endpoint selection
(``select_endpoint_for_method``), shares the pool's health scoring /
circuit breakers / throttling, and is protected by a consecutive-call
budget (``ON_DEMAND_RPC_MAX_CONSECUTIVE``) that pauses before free-tier
rate limits bite.

Fetch-through write-through
---------------------------
An RPC miss that returns a real row is persisted to the B2 Parquet lake
(staging prefix) so the NEXT identical scan is a lake hit after staging
is compacted (see ``lite_indexer.storage.compaction``). The persisted row
carries provenance: ``source="live_rpc"``, ``confidence=finalized|
confirmed`` and ``fetched_at``. Chain data is immutable, so positive rows
are written permanently (no TTL) and cached in-memory with no expiry;
only negative results (not found / pending) get a TTL (default 300s).

Rows still inside the chain's unfinalized reorg window (EVM ~128 blocks,
Solana ~32 slots, Tron ~19) are served live-only and NOT written to the
lake — they can still be reorganized, so persisting them would poison the
immutable archive.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from lite_indexer.chains.endpoints import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    HISTORICAL_PARQUET_REMOTE,
    ON_DEMAND_RPC_MAX_BLOCK_RANGE,
    ON_DEMAND_RPC_MAX_CONSECUTIVE,
    ON_DEMAND_RPC_PAUSE_SECONDS,
    TRANSFER_EVENT_TOPIC,
)
from lite_indexer.dex.transfers import parse_transfer_logs
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.cache import QueryCache
from lite_indexer.rpc import RpcClient, get_best_endpoint, select_endpoint_for_method
from lite_indexer.rpc.client import fetch_block_timestamps
from lite_indexer.rpc.hedging import hedged_rpc
from lite_indexer.storage.parquet import ParquetExporter

logger = get_logger(__name__)

# Trace-filter window (blocks) for the bounded contract-creation scan used
# when no creation-tx hint is available. Small on purpose: trace_filter on
# free tiers is expensive and rate-limited; anything outside this window
# must come from the indexed deployer_events table instead.
_DEPLOYER_TRACE_SCAN_BLOCKS = 1000

# B2 staging prefix for fetch-through writes. Every RPC miss that returns
# a real row is written here as a micro-batch so a later identical scan
# becomes a lake hit once the compactor folds staging into the canonical
# tree. Layout (kept deliberately separate from the hourly daily-export
# tree so the two writers can never collide):
#   {HISTORICAL_PARQUET_REMOTE}/staging/{table}/{chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_live_rpc_{ts}.parquet
_STAGING_PREFIX = "staging"

# Reorg window per chain — rows at or inside this unfinalized horizon are
# served live-only and never written to the lake (they can still be
# reorganized). EVM ~128 blocks, Solana ~32 slots, Tron ~19.
_REORG_HORIZON: dict[str, int] = {
    "ethereum": 128,
    "polygon": 128,
    "bsc": 128,
    "arbitrum": 128,
    "optimism": 128,
    "base": 128,
    "avalanche": 128,
    "gnosis": 128,
    "solana": 32,
    "tron": 19,
}

# Fallback horizon for chains not listed above (assume EVM-like block
# times until proven otherwise).
_DEFAULT_REORG_HORIZON = 128

# How long a resolved chain tip is reused before re-probing
# ``eth_blockNumber`` — keeps the finality gate from adding one cheap call
# per fetch (eth_blockNumber is nearly free on free tiers).
_TIP_CACHE_TTL = 60.0

# Sentinel stored as the payload of cached negative results. QueryCache
# payloads are never None, so an object sentinel is unambiguous.
_NEGATIVE: Any = object()


def _iso_ts(timestamp_hex: str) -> str:
    """Convert a hex block timestamp to an ISO-8601 UTC string ("" on bad input)."""
    try:
        return datetime.fromtimestamp(int(timestamp_hex, 16), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return ""


class RPCFetcher:
    """Makes targeted RPC calls for specific historical data on demand.

    Args:
        client: Optional :class:`lite_indexer.rpc.RpcClient` (a fresh one
            is created by default). Injectable for tests.
        endpoints: Optional chain → endpoint-list map (defaults to
            :data:`lite_indexer.chains.endpoints.GET_LOGS_ENDPOINTS`).
        max_block_range: Cap on the block window of one address query.
        max_consecutive: Calls allowed before the pause kicks in (0 =
            never pause).
        pause_seconds: Sleep duration after ``max_consecutive`` calls.
        cache: Optional :class:`lite_indexer.query.cache.QueryCache` for
            fetch-through caching (a default is built when omitted).
            Positive rows are cached with no expiry, negatives with the
            negative TTL (300s).
    """

    def __init__(
        self,
        client: RpcClient | None = None,
        endpoints: dict[str, list[str]] | None = None,
        *,
        max_block_range: int = ON_DEMAND_RPC_MAX_BLOCK_RANGE,
        max_consecutive: int = ON_DEMAND_RPC_MAX_CONSECUTIVE,
        pause_seconds: float = ON_DEMAND_RPC_PAUSE_SECONDS,
        cache: QueryCache | None = None,
    ) -> None:
        self._client = client or RpcClient()
        self._endpoints = dict(endpoints) if endpoints is not None else dict(GET_LOGS_ENDPOINTS)
        self.max_block_range = int(max_block_range)
        self.max_consecutive = int(max_consecutive)
        self.pause_seconds = float(pause_seconds)
        self._consecutive = 0
        self._cache = cache if cache is not None else QueryCache()
        # chain → (monotonic timestamp, tip) — avoids one eth_blockNumber
        # per fetch when gating the reorg window (see _chain_tip).
        self._tip_cache: dict[str, tuple[float, int]] = {}

    # ── Rate-limit protection ────────────────────────────────────────

    @property
    def consecutive_calls(self) -> int:
        """Number of calls since the last pause (diagnostics/tests)."""
        return self._consecutive

    async def _throttle(self) -> None:
        """Enforce the consecutive-call budget before the next RPC call.

        When ``_consecutive`` reaches ``max_consecutive``, sleep
        ``pause_seconds`` (giving the free tier room to breathe), reset the
        counter, then let the call proceed.
        """
        if self.max_consecutive > 0 and self._consecutive >= self.max_consecutive:
            metrics_incr("ondemand_rpc_pauses")
            logger.info(
                "ondemand_rpc_pause",
                pause_s=self.pause_seconds,
                consecutive=self._consecutive,
            )
            await asyncio.sleep(self.pause_seconds)
            self._consecutive = 0
        self._consecutive += 1

    # ── Endpoint selection ───────────────────────────────────────────

    def endpoints_for(self, chain: str) -> list[str]:
        """Return the active RPC endpoints for ``chain`` ([] when unknown)."""
        return self._endpoints.get(chain, [])

    async def _select_endpoint(self, chain: str, method: str) -> str | None:
        """Pick the best endpoint for ``method`` on ``chain`` (None when none)."""
        endpoints = self.endpoints_for(chain)
        if not endpoints:
            logger.debug("ondemand_no_endpoints", chain=chain, method=method)
            return None
        try:
            return select_endpoint_for_method(method, endpoints, chain=chain)
        except Exception:  # noqa: BLE001 — selection is best-effort
            return get_best_endpoint(endpoints)

    # ── Fetch-through cache (positives immutable, negatives TTL) ─────

    @staticmethod
    def _cache_key_transfer(chain: str, tx_hash: str) -> tuple[Any, ...]:
        return ("rpc", "transfer", chain, str(tx_hash).lower())

    @staticmethod
    def _cache_key_transfers(
        chain: str,
        address: str,
        from_block: int | None,
        to_block: int | None,
        limit: int,
    ) -> tuple[Any, ...]:
        return ("rpc", "transfers", chain, str(address).lower(), from_block, to_block, int(limit))

    @staticmethod
    def _cache_key_deployer(chain: str, contract: str, creation_tx: str | None) -> tuple[Any, ...]:
        return ("rpc", "deployer", chain, str(contract).lower(), creation_tx)

    def _cache_hit(self, key: tuple[Any, ...]) -> Any:
        """Return the cached payload, ``_NEGATIVE`` for a cached negative,
        or None when not cached."""
        return self._cache.get(key)

    def _cache_positive(self, key: tuple[Any, ...], value: Any) -> None:
        """Cache an immutable positive row with no expiry."""
        self._cache.set_positive(key, value)

    def _cache_negative(self, key: tuple[Any, ...]) -> None:
        """Cache a negative (not found / pending) result for the negative TTL."""
        self._cache.set_negative(key, _NEGATIVE)

    # ── Write-through to the B2 staging lake ─────────────────────────

    async def _chain_tip(self, chain: str) -> int:
        """Resolve the chain tip via ``eth_blockNumber`` (cached briefly).

        The tip gates the reorg-window check before a lake write. The
        result is reused for ``_TIP_CACHE_TTL`` seconds so the finality
        gate does not add a call per fetch. Returns 0 when unresolvable
        (caller must then skip the lake write — cannot prove finality).
        """
        now = time.monotonic()
        cached = self._tip_cache.get(chain)
        if cached is not None and now - cached[0] < _TIP_CACHE_TTL:
            return cached[1]
        await self._throttle()
        endpoint = await self._select_endpoint(chain, "eth_blockNumber")
        tip = 0
        if endpoint is not None:
            try:
                tip = int(await self._client.eth_block_number(endpoint) or 0)
            except Exception:  # noqa: BLE001 — best-effort
                tip = 0
        self._tip_cache[chain] = (now, tip)
        return tip

    @staticmethod
    def _reorg_horizon(chain: str) -> int:
        """Return the unfinalized reorg window for ``chain`` (blocks/slots)."""
        return _REORG_HORIZON.get(chain, _DEFAULT_REORG_HORIZON)

    @staticmethod
    def _row_block(row: dict[str, Any]) -> int:
        """Return the row's block number (or slot for Solana), 0 when absent."""
        raw = row.get("block_number", row.get("slot", 0))
        try:
            return int(raw or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _confidence(block: int, tip: int, horizon: int) -> str:
        """Provenance confidence for a persistable row.

        Rows well past the reorg window (double the horizon) are
        ``finalized``; rows just outside the window are ``confirmed``.
        """
        if block <= tip - 2 * horizon:
            return "finalized"
        return "confirmed"

    async def _persist_rows(self, chain: str, table: str, rows: list[dict[str, Any]]) -> None:
        """Best-effort write-through of finalized RPC rows to the staging lake.

        Rows inside the chain's unfinalized reorg window are served
        live-only and skipped (they can still be reorganized). Persisted
        rows get provenance: ``source="live_rpc"``, ``confidence`` and
        ``fetched_at``. Never raises — write-through must not break the
        read path.
        """
        if not rows:
            return
        try:
            tip = await self._chain_tip(chain)
            if tip <= 0:
                logger.debug("live_rpc_persist_skip_tip", chain=chain, table=table)
                return
            horizon = self._reorg_horizon(chain)
            persistable: list[dict[str, Any]] = []
            for row in rows:
                block = self._row_block(row)
                if block <= 0:
                    # No block/slot — cannot judge finality; serve live-only.
                    continue
                if block > tip - horizon:
                    metrics_incr("live_rpc_reorg_skipped")
                    logger.debug(
                        "live_rpc_reorg_skip",
                        chain=chain,
                        table=table,
                        block=block,
                        tip=tip,
                        horizon=horizon,
                    )
                    continue
                persistable.append(
                    {
                        **row,
                        "source": "live_rpc",
                        "confidence": self._confidence(block, tip, horizon),
                        "fetched_at": datetime.now(UTC).isoformat(),
                    }
                )
            if not persistable:
                return
            # rclone copy+check and pyarrow are blocking — run off the loop.
            await asyncio.to_thread(self._write_staging_batch, chain, table, persistable)
        except Exception as exc:  # noqa: BLE001 — write-through is best-effort
            logger.warning("live_rpc_persist_error", chain=chain, table=table, error=str(exc)[:200])

    def _write_staging_batch(self, chain: str, table: str, rows: list[dict[str, Any]]) -> bool:
        """Write a staging parquet micro-batch on B2 and verify the upload.

        Reuses the existing export pipeline (``ParquetExporter``) for the
        parquet serialization (pyarrow, JSONL fallback) and the rclone
        copy+check upload — the B2 serializer is not re-implemented here.

        Args:
            chain: Chain name.
            table: Logical event table (``evm_transfers``,
                ``deployer_events``) — becomes a directory component so
                compaction can address one table at a time.
            rows: Normalized rows with provenance attached.

        Returns:
            True when the file was written and the upload verified.
        """
        try:
            exporter = ParquetExporter(remote=HISTORICAL_PARQUET_REMOTE)
            now = datetime.now(UTC)
            local_dir = Path(tempfile.mkdtemp(prefix="lci-live-rpc-"))
            fname = f"{chain}_live_rpc_{now:%Y%m%d_%H%M%S}.parquet"
            path = local_dir / fname
            written = exporter._write_parquet(rows, path)  # noqa: SLF001 — reuse existing serializer
            if written <= 0:
                logger.warning("live_rpc_write_failed", chain=chain, table=table)
                return False
            remote = (
                f"{HISTORICAL_PARQUET_REMOTE.rstrip('/')}/{_STAGING_PREFIX}/{table}/{chain}/"
                f"{now:%Y}/{now:%m}/{now:%d}/{now:%H}"
            )
            ok = exporter._upload_dir(local_dir, remote)  # noqa: SLF001 — reuse existing upload+verify
            if ok:
                metrics_incr("live_rpc_lake_writes")
                logger.info(
                    "live_rpc_persisted",
                    chain=chain,
                    table=table,
                    rows=len(rows),
                    remote=remote,
                    fname=fname,
                )
            else:
                metrics_incr("live_rpc_persist_failed")
            return ok
        except Exception as exc:  # noqa: BLE001 — write-through is best-effort
            logger.warning("live_rpc_write_error", chain=chain, table=table, error=str(exc)[:200])
            return False

    # ── Transfer by tx hash ──────────────────────────────────────────

    async def fetch_transfer(self, chain: str, tx_hash: str) -> dict[str, Any] | None:
        """Single transaction lookup: receipt + first Transfer log parsed.

        Args:
            chain: Chain name.
            tx_hash: 0x-prefixed transaction hash.

        Returns:
            A transfer row shaped like the ``evm_transfers`` canonical
            columns (plus ``source="rpc"``), or None when the receipt is
            missing / has no ERC-20 Transfer log / RPC is unreachable.

        Fetch-through: a positive row is cached with no expiry and
        written to the B2 staging lake (``source="live_rpc"`` +
        ``confidence`` + ``fetched_at``) unless it still sits inside the
        chain's reorg window. A negative (not found / pending) is cached
        for the negative TTL (300s).
        """
        key = self._cache_key_transfer(chain, tx_hash)
        cached = self._cache_hit(key)
        if cached is _NEGATIVE:
            return None
        if cached is not None:
            return cast(dict[str, Any], cached)

        await self._throttle()
        endpoint = await self._select_endpoint(chain, "eth_getTransactionReceipt")
        if endpoint is None:
            return None
        receipt = await self._client.eth_get_transaction_receipt(endpoint, tx_hash)
        if not receipt:
            self._cache_negative(key)
            return None
        row = self._parse_transfer_from_receipt(chain, receipt)
        if row is None:
            self._cache_negative(key)
            return None
        self._cache_positive(key, row)
        await self._persist_rows(chain, "evm_transfers", [row])
        return row

    @staticmethod
    def _parse_transfer_from_receipt(chain: str, receipt: dict[str, Any]) -> dict[str, Any] | None:
        """Extract the first ERC-20 Transfer log from a receipt."""
        logs = receipt.get("logs") or []
        for log in logs:
            if log.get("removed", False):
                continue
            topics = log.get("topics") or []
            if len(topics) < 3 or str(topics[0]).lower() != TRANSFER_EVENT_TOPIC:
                continue
            from lite_indexer.dex.common import unpad_address

            value_hex = log.get("data", "0x0")
            try:
                value = (
                    str(int(value_hex, 16))
                    if isinstance(value_hex, str) and value_hex.startswith("0x")
                    else str(value_hex)
                )
            except (ValueError, TypeError):
                value = "0"
            if value == "0" and len(topics) >= 4:
                continue  # NFT transfer — skip, matching parse_transfer_logs
            block_hex = receipt.get("blockNumber", "0x0")
            try:
                block = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
            except (ValueError, TypeError):
                block = 0
            log_index_hex = log.get("logIndex", "0x0")
            try:
                log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
            except (ValueError, TypeError):
                log_index = 0
            return {
                "chain": chain,
                "block_number": block,
                "tx_hash": str(receipt.get("transactionHash", "")).lower(),
                "from_address": unpad_address(topics[1]),
                "to_address": unpad_address(topics[2]),
                "token_address": str(log.get("address", "")).lower(),
                "value": value,
                "block_timestamp": _iso_ts(str(receipt.get("blockTimestamp", "0x0"))),
                "log_index": log_index,
                "ingested_at": "",
                "source": "rpc",
            }
        return None

    # ── Transfers by address ─────────────────────────────────────────

    async def _default_range(self, chain: str) -> tuple[int, int]:
        """Return a recent [from, to] window for unbounded address queries.

        Uses ``eth_blockNumber`` (nearly free on free tiers) clamped to
        ``max_block_range`` blocks back from the tip. Returns (0, 0) when
        the tip cannot be resolved (caller should not dial getLogs).
        """
        await self._throttle()
        endpoint = await self._select_endpoint(chain, "eth_blockNumber")
        if endpoint is None:
            return 0, 0
        tip = await self._client.eth_block_number(endpoint)
        if tip is None or tip <= 0:
            return 0, 0
        return max(0, tip - self.max_block_range), tip

    async def fetch_address_transfers(
        self,
        chain: str,
        address: str,
        from_block: int | None = None,
        to_block: int | None = None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Targeted ``eth_getLogs`` for a specific address in a block range.

        Bounds: when the caller omits either bound, the range defaults to
        the newest ``max_block_range`` blocks (chain tip). A requested
        range wider than ``max_block_range`` is clamped to the newest
        window so one query can never exceed a free tier's range cap.

        Args:
            chain: Chain name.
            address: Wallet / contract address to filter logs by.
            from_block: Inclusive start block (None = recent window).
            to_block: Inclusive end block (None = recent window).
            limit: Max parsed rows to return (newest first).

        Returns:
            Parsed transfer rows (``evm_transfers`` canonical shape plus
            ``source="rpc"``), newest first. [] on failure or no logs.

        Fetch-through: positive rows are cached with no expiry and
        written to the B2 staging lake (``source="live_rpc"`` +
        ``confidence`` + ``fetched_at``) unless they still sit inside the
        chain's reorg window. An empty result is cached as a negative for
        the negative TTL (300s).
        """
        await self._throttle()
        if from_block is None or to_block is None:
            from_block, to_block = await self._default_range(chain)
            if from_block == 0 and to_block == 0:
                return []
        if to_block < from_block:
            from_block, to_block = to_block, from_block
        if to_block - from_block > self.max_block_range:
            from_block = max(0, to_block - self.max_block_range)

        key = self._cache_key_transfers(chain, address, from_block, to_block, limit)
        cached = self._cache_hit(key)
        if cached is _NEGATIVE:
            return []
        if cached is not None:
            return list(cached)

        endpoints = self.endpoints_for(chain)
        if not endpoints:
            return []

        async def _get_logs_at(endpoint: str):
            return await self._client.get_logs(
                endpoint, from_block, to_block, TRANSFER_EVENT_TOPIC, address=address
            )

        # Hedged dispatch (NodeCore-style): a slow primary no longer stalls
        # the fetch — a second leg fires after RPC_HEDGE_DELAY and the first
        # success wins. Fail-open to plain behaviour on any issue.
        try:
            raw = await hedged_rpc(_get_logs_at, endpoints)
        except Exception:  # noqa: BLE001 — hedge is an optimization
            endpoint = await self._select_endpoint(chain, "eth_getLogs")
            raw = None if endpoint is None else await _get_logs_at(endpoint)
        if raw is None:
            self._cache_negative(key)
            return []
        events = parse_transfer_logs(raw, chain, from_block, to_block)
        await self._attach_timestamps(chain, events)
        events.sort(key=lambda e: (int(e.get("blockNumber", 0) or 0), int(e.get("logIndex", 0) or 0)), reverse=True)
        events = events[: max(1, min(int(limit), 1000))]
        rows = [
            {
                "chain": e["chain"],
                "block_number": int(e["blockNumber"]),
                "tx_hash": e["txHash"],
                "from_address": e["from"],
                "to_address": e["to"],
                "token_address": e["tokenAddress"],
                "value": e["value"],
                "block_timestamp": e.get("blockTimestamp", "") or "",
                "log_index": int(e["logIndex"]),
                "ingested_at": "",
                "source": "rpc",
            }
            for e in events
        ]
        if rows:
            self._cache_positive(key, list(rows))
            await self._persist_rows(chain, "evm_transfers", rows)
        else:
            self._cache_negative(key)
        return rows

    async def _attach_timestamps(self, chain: str, events: list[dict[str, Any]]) -> None:
        """Best-effort block_timestamp resolution for parsed events."""
        block_numbers = {int(e.get("blockNumber", 0) or 0) for e in events if e.get("blockNumber")}
        if not block_numbers:
            return
        endpoints = self.endpoints_for(chain)
        if not endpoints:
            return
        try:
            timestamps = await fetch_block_timestamps(self._client, endpoints, block_numbers, chain=chain)
        except Exception:  # noqa: BLE001 — timestamps are best-effort
            timestamps = {}
        for e in events:
            ts = timestamps.get(int(e.get("blockNumber", 0) or 0), "")
            if ts:
                e["blockTimestamp"] = ts

    # ── Deployer info ────────────────────────────────────────────────

    async def fetch_deployer(
        self,
        chain: str,
        contract_address: str,
        creation_tx: str | None = None,
    ) -> dict[str, Any] | None:
        """Resolve the deployer (creation-tx ``from``) of a contract.

        Priority:
        1. ``eth_getTransactionByHash`` on an explicit ``creation_tx``
           hint (the router passes the deployment tx from indexed
           deployer_events when known) → the tx ``from`` is the EOA.
        2. A bounded ``trace_filter`` creation scan over the most recent
           ``_DEPLOYER_TRACE_SCAN_BLOCKS`` when the chain's endpoints
           support trace_filter (Erigon-style nodes).
        3. None — the caller keeps indexed data only.

        Args:
            chain: Chain name.
            contract_address: The pair/pool/contract address.
            creation_tx: Optional known deployment transaction hash.

        Returns:
            A deployer row shaped like the ``deployer_events`` canonical
            columns (plus ``source="rpc"``), or None when unresolvable.

        Fetch-through: a resolved row is cached with no expiry and
        written to the B2 staging lake (``source="live_rpc"`` +
        ``confidence`` + ``fetched_at``) unless it still sits inside the
        chain's reorg window. An unresolved lookup is cached as a negative
        for the negative TTL (300s).
        """
        key = self._cache_key_deployer(chain, contract_address, creation_tx)
        cached = self._cache_hit(key)
        if cached is _NEGATIVE:
            return None
        if cached is not None:
            return cast(dict[str, Any], cached)

        row: dict[str, Any] | None = None

        if creation_tx:
            await self._throttle()
            endpoint = await self._select_endpoint(chain, "eth_getTransactionByHash")
            if endpoint is not None:
                result = await self._client.call(endpoint, "eth_getTransactionByHash", [creation_tx])
                tx = result.get("result") if isinstance(result, dict) else None
                if isinstance(tx, dict) and tx.get("from"):
                    from_addr = str(tx["from"]).lower()
                    block_hex = tx.get("blockNumber", "0x0")
                    try:
                        block = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
                    except (ValueError, TypeError):
                        block = 0
                    row = {
                        "deployer": from_addr,
                        "deployer_eoa": from_addr,
                        "token_address": contract_address.lower(),
                        "token0": "",
                        "token1": "",
                        "pair_address": contract_address.lower(),
                        "chain": chain,
                        "block_number": block,
                        "block_timestamp": _iso_ts(str(tx.get("timestamp", "0x0"))),
                        "tx_hash": str(creation_tx).lower(),
                        "type": "rpc_lookup",
                        "factory": "",
                        "ingested_at": "",
                        "source": "rpc",
                    }

        # Bounded trace_filter creation scan (best-effort).
        if row is None:
            await self._throttle()
            endpoint = await self._select_endpoint(chain, "trace_filter")
            if endpoint is not None:
                to_block = await self._tip_or(endpoint, chain)
                if to_block > 0:
                    from_block = max(0, to_block - _DEPLOYER_TRACE_SCAN_BLOCKS)
                    result = await self._client.call(
                        endpoint,
                        "trace_filter",
                        [{"fromBlock": hex(from_block), "toBlock": hex(to_block), "toAddress": [], "fromAddress": []}],
                    )
                    traces = result.get("result") if isinstance(result, dict) else None
                    if isinstance(traces, list):
                        for trace in traces:
                            if not isinstance(trace, dict):
                                continue
                            action = trace.get("action") or {}
                            # Contract creation traces have no `to` (None) — the created
                            # contract lives in the trace result's `address`.
                            created = str(action.get("to") or "").lower()
                            creator = str(action.get("from", "")).lower()
                            if not created:
                                result_addr = str((trace.get("result") or {}).get("address", "")).lower()
                                if result_addr != contract_address.lower():
                                    continue
                            elif created != contract_address.lower():
                                continue
                            if creator:
                                row = {
                                    "deployer": creator,
                                    "deployer_eoa": creator,
                                    "token_address": contract_address.lower(),
                                    "token0": "",
                                    "token1": "",
                                    "pair_address": contract_address.lower(),
                                    "chain": chain,
                                    "block_number": int(trace.get("blockNumber", "0x0"), 16)
                                    if isinstance(trace.get("blockNumber", "0x0"), str)
                                    else int(trace.get("blockNumber", 0) or 0),
                                    "block_timestamp": "",
                                    "tx_hash": str(trace.get("transactionHash", "")).lower(),
                                    "type": "rpc_lookup",
                                    "factory": "",
                                    "ingested_at": "",
                                    "source": "rpc",
                                }
                                break

        if row is not None:
            self._cache_positive(key, row)
            await self._persist_rows(chain, "deployer_events", [row])
        else:
            self._cache_negative(key)
        return row

    async def _tip_or(self, endpoint: str, chain: str) -> int:
        """Return the chain tip via ``eth_blockNumber`` (0 on failure)."""
        try:
            tip = await self._client.eth_block_number(endpoint)
            return int(tip or 0)
        except Exception:  # noqa: BLE001 — best-effort
            return 0

    # ── Block info ───────────────────────────────────────────────────

    async def fetch_block_info(self, chain: str, block: int) -> dict[str, Any] | None:
        """Fetch a block via ``eth_getBlockByNumber`` with full transactions.

        Args:
            chain: Chain name.
            block: Block number.

        Returns:
            The raw RPC block dict (full transactions), or None on failure.
        """
        await self._throttle()
        endpoint = await self._select_endpoint(chain, "eth_getBlockByNumber")
        if endpoint is None:
            return None
        result = await self._client.call(endpoint, "eth_getBlockByNumber", [hex(block), True])
        if result and isinstance(result.get("result"), dict):
            return cast(dict[str, Any], result["result"])
        return None


__all__ = ["RPCFetcher", "_iso_ts"]
