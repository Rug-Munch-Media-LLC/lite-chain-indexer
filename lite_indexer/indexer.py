"""
LiteIndexer — Core indexing engine.

Orchestrates the complete index cycle:
  1. Determine block range (from state or override)
  2. Fetch Transfer events via eth_getLogs
  3. Fetch DEX Swap events (if enabled)
  4. Fetch PairCreated/PoolCreated events (if enabled)
  5. Enrich with block timestamps
  6. Write NDJSON + ClickHouse (if configured)
  7. Save state

Each chain gets its own LiteIndexer instance with independent state tracking.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    BATCH_SIZE,
    DATA_DIR,
    DISK_CAP_BYTES,
    FACTORY_ADDRESSES,
    HEAD_BUFFER,
    MAX_CONCURRENT_POOL_RESOLVE,
    PAIR_CREATED_TOPIC,
    POOL_CREATED_TOPIC,
    RPC_TIMEOUT,
    SWAP_V2_TOPIC,
    SWAP_V3_TOPIC,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    TRANSFER_EVENT_TOPIC,
    V3_FACTORY_ADDRESSES,
)
from lite_indexer.dex import (
    parse_pair_created_logs,
    parse_swap_logs,
    parse_transfer_logs,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    apply_endpoint_backoff,
    fetch_block_timestamps,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
    verify_endpoints_for_chain,
)
from lite_indexer.state import PairCache, StateManager
from lite_indexer.storage import (
    ClickHouseWriter,
    NDJSONWriter,
    check_disk_cap,
    check_disk_space,
)

logger = get_logger(__name__)


class LiteIndexer:
    """Minimal blockchain indexer using free RPCs.

    Fetches ERC-20 Transfer events, DEX Swap events (V2+V3), and
    PairCreated/PoolCreated events via eth_getLogs on curated free
    RPC endpoints. Persists to NDJSON and ClickHouse.

    Thread-safe for cron usage because every run creates its own state file
    and appends to the NDJSON (file is opened and closed each time).
    """

    def __init__(
        self,
        chain: str,
        from_block: int | None = None,
        to_block: int | None = None,
        batch_size: int = BATCH_SIZE,
        disk_cap: int = DISK_CAP_BYTES,
        start_from_head: bool = False,
        data_dir: Path | None = None,
    ) -> None:
        self.chain = chain
        self.batch_size = batch_size
        self.disk_cap = disk_cap
        self.dex_enabled = False
        self.deployers_enabled = False
        self.start_from_head = start_from_head
        self.data_dir = data_dir or DATA_DIR

        # State and pair cache
        self.state = StateManager(chain)
        self.pair_cache = PairCache(chain)

        # Latest batch of pair creation events for deployer tracking
        self._last_pair_events: list[dict[str, Any]] = []

        # Optional override blocks (for testing / backfill)
        self._override_from = from_block
        self._override_to = to_block

        # RPC client (direct HTTP, no backend dependency)
        self.rpc = RpcClient()

        # HTTP client for eth_getLogs batch calls
        self._client: httpx.AsyncClient | None = None

        # Storage writers
        self._ndjson_writer = NDJSONWriter(data_dir=self.data_dir)
        self._ch_writer: ClickHouseWriter | None = None

    @property
    def ch_writer(self) -> ClickHouseWriter:
        """Lazy-initialized ClickHouse writer."""
        if self._ch_writer is None:
            self._ch_writer = ClickHouseWriter()
        return self._ch_writer

    # ── HTTP Client ─────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client for eth_getLogs calls."""
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=limits,
            )
        return self._client

    # ── RPC: Latest Block ───────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get the latest block number."""
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            return None

        endpoint = get_best_endpoint(endpoints)
        return await self.rpc.eth_block_number(endpoint)

    # ── RPC: Block Timestamps ───────────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch timestamps for a set of block numbers.

        Delegates to the shared RPC timestamp fetcher.
        """
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return {}

        return await fetch_block_timestamps(self.rpc, endpoints, block_numbers)

    # ── RPC: Get Logs (Transfer Events) ─────────────────────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch ERC-20 Transfer events across ALL token contracts.

        Calls eth_getLogs with only the Transfer event topic filter
        (no address filter) so we catch every transfer on the chain.

        Uses a curated list of endpoints known to support eth_getLogs.
        Falls through errors to try the next endpoint. Returns the
        result with the most parsed events.
        """
        if from_block > to_block:
            return []

        if to_block - from_block > self.batch_size:
            to_block = from_block + self.batch_size

        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            return []

        # Rotate starting endpoint for load distribution
        start_idx = endpoints.index(get_best_endpoint(endpoints))
        rotated = endpoints[start_idx:] + endpoints[:start_idx]

        client = await self._get_client()
        filter_params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [TRANSFER_EVENT_TOPIC],
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter_params],
        }

        best_result: list[dict[str, Any]] | None = None
        last_error: str | None = None

        for url in rotated:
            await apply_endpoint_backoff(url)

            try:
                resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)

                if resp.status_code == 200:
                    data = resp.json()
                    if not isinstance(data, dict):
                        continue

                    if "result" in data:
                        logs = data["result"]
                        if isinstance(logs, list):
                            parsed = parse_transfer_logs(logs, self.chain, from_block, to_block)
                            if best_result is None or len(parsed) > len(best_result):
                                best_result = parsed
                                if len(parsed) == len(logs):
                                    break  # Got all events
                        elif isinstance(logs, dict) and "error" in logs:
                            last_error = logs["error"].get("message", str(logs))
                            record_endpoint_failure(url)
                            continue
                    elif "error" in data:
                        last_error = data["error"].get("message", str(data))
                        record_endpoint_failure(url)
                        continue
                elif resp.status_code == 429:
                    last_error = "rate_limited"
                    await handle_rate_limit(url)
                    continue
                else:
                    last_error = f"http_{resp.status_code}"
                    record_endpoint_failure(url)
                    continue

            except httpx.TimeoutException:
                last_error = "timeout"
                record_endpoint_failure(url)
                continue
            except (httpx.ConnectError, httpx.ReadError, httpx.WriteError) as exc:
                last_error = str(exc)[:60]
                record_endpoint_failure(url)
                continue
            except Exception as exc:
                last_error = str(exc)[:80]
                record_endpoint_failure(url)
                continue

        if best_result is not None:
            return best_result

        logger.error(
            "all_endpoints_failed",
            chain=self.chain,
            from_block=from_block,
            to_block=to_block,
            last_error=last_error,
        )
        return []

    # ── RPC: Get Logs (Swap Events) ────────────────────────────────────

    async def fetch_swap_events(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch DEX Swap events (V2 + V3) via eth_getLogs.

        First fetches PairCreated events from known factories to build a
        pair→(token0, token1) mapping, then fetches swap events. The mapping
        is used to populate token0/token1 fields.

        Runs two parallel requests — one for the V2 swap topic and one for
        the V3 swap topic. Returns merged, deduplicated swap events with
        eth_call fallback for unknown pools.
        """
        if from_block > to_block:
            return []

        # Build pair→tokens mapping from PairCreated events first
        try:
            await self._fetch_pair_created_events(from_block, to_block)
        except Exception as exc:
            logger.warning("pair_created_fetch_failed", chain=self.chain, error=str(exc)[:100])
            return []

        if to_block - from_block > self.batch_size:
            to_block = from_block + self.batch_size

        client = await self._get_client()

        async def _fetch_topic(topic: str, label: str) -> list[dict[str, Any]]:
            filter_params = {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [topic],
            }
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [filter_params],
            }
            endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
            if not endpoints:
                return []

            start_idx = endpoints.index(get_best_endpoint(endpoints))
            rotated = endpoints[start_idx:] + endpoints[:start_idx]

            best_result: list[dict[str, Any]] | None = None
            for url in rotated:
                await apply_endpoint_backoff(url)
                try:
                    resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, dict) and "result" in data:
                            logs = data["result"]
                            if isinstance(logs, list):
                                parsed = parse_swap_logs(
                                    logs,
                                    self.chain,
                                    from_block,
                                    to_block,
                                    topic,
                                    self.pair_cache.all,
                                )
                                if best_result is None or len(parsed) > len(best_result):
                                    best_result = parsed
                                    if len(parsed) == len(logs):
                                        break
                    elif resp.status_code == 429:
                        await handle_rate_limit(url)
                        continue
                    else:
                        record_endpoint_failure(url)
                        continue
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
                    record_endpoint_failure(url)
                    continue
                except Exception:
                    record_endpoint_failure(url)
                    continue

            return best_result or []

        # Fetch V2 and V3 swaps in parallel
        v2_task = _fetch_topic(SWAP_V2_TOPIC, "v2")
        v3_task = _fetch_topic(SWAP_V3_TOPIC, "v3")
        v2_results, v3_results = await asyncio.gather(v2_task, v3_task)

        # Merge and deduplicate
        seen_keys: set[str] = set()
        merged: list[dict[str, Any]] = []
        for evt in v2_results + v3_results:
            key = f"{evt['transactionHash']}:{evt['logIndex']}"
            if key not in seen_keys:
                seen_keys.add(key)
                merged.append(evt)

        # Batch-resolve unknown pools via eth_call fallback
        unknown_pairs: set[str] = set()
        for evt in merged:
            pair = evt.get("pairAddress", "").lower()
            if (not evt.get("token0") or not evt.get("token1")) and pair and pair not in self.pair_cache:
                unknown_pairs.add(pair)

        if unknown_pairs:
            resolved = await self._batch_resolve_pool_tokens(list(unknown_pairs))
            if resolved:
                for evt in merged:
                    pair = evt.get("pairAddress", "").lower()
                    if pair in resolved:
                        evt["token0"] = resolved[pair][0]
                        evt["token1"] = resolved[pair][1]

        return merged

    # ── PairCreated Fetch (internal) ─────────────────────────────────

    async def _fetch_pair_created_events(
        self,
        from_block: int,
        to_block: int,
    ) -> None:
        """Fetch PairCreated (V2) + PoolCreated (V3) events from known DEX factories.

        Builds a pair→(token0, token1) mapping in self.pair_cache.
        Also collects deployer events for downstream tracking.
        """
        queries: list[tuple[str, str, bool]] = []
        for addr in FACTORY_ADDRESSES.get(self.chain, []):
            queries.append((addr, PAIR_CREATED_TOPIC, False))
        for addr in V3_FACTORY_ADDRESSES.get(self.chain, []):
            queries.append((addr, POOL_CREATED_TOPIC, True))

        if not queries:
            logger.debug("no_factories_for_chain", chain=self.chain)
            return

        self._last_pair_events = []
        client = await self._get_client()
        pairs_before = len(self.pair_cache)

        for factory_addr, event_topic, _is_v3 in queries:
            filter_params = {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": factory_addr,
                "topics": [event_topic],
            }
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [filter_params],
            }

            endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
            if not endpoints:
                continue

            start_idx = endpoints.index(get_best_endpoint(endpoints))
            rotated = endpoints[start_idx:] + endpoints[:start_idx]

            for url in rotated:
                try:
                    resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, dict) and "result" in data:
                            logs = data["result"]
                            if isinstance(logs, list):
                                pair_tokens, deployer_events = parse_pair_created_logs(logs, self.chain)
                                self.pair_cache.update(pair_tokens)
                                self._last_pair_events.extend(deployer_events)
                                break
                    elif resp.status_code == 429:
                        await asyncio.sleep(2)
                        continue
                    else:
                        record_endpoint_failure(url)
                        continue
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
                    continue
                except Exception as exc:
                    logger.debug("factory_events_error", factory=factory_addr[:20], error=str(exc)[:60])
                    continue

        if len(self.pair_cache) > pairs_before:
            self.pair_cache.save()

    # ── Pool Token Resolution (eth_call fallback) ──────────────────

    async def _resolve_pool_tokens(self, pair_address: str) -> tuple[str, str]:
        """Resolve token0/token1 via eth_call on the pool contract.

        Calls token0() and token1() on the pair contract address.
        Also checks ClickHouse rugdex.pairs table first (much cheaper).
        """
        # 1. Try ClickHouse rugdex.pairs lookup
        try:
            ch_client = self.ch_writer._connect()
            if ch_client:
                rows = ch_client.execute(
                    "SELECT token0, token1 FROM rugdex.pairs WHERE pair_address = %(pair)s",
                    {"pair": pair_address},
                )
                if rows and len(rows[0]) == 2:
                    token0, token1 = rows[0]
                    if token0 and token1:
                        return (token0, token1)
        except Exception as exc:
            logger.debug("clickhouse_rugdex_lookup_failed", error=str(exc)[:60])

        # 2. Fall back to eth_call
        try:
            endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
            if not endpoints:
                return ("", "")
            endpoint = get_best_endpoint(endpoints)

            resp0 = await self.rpc.eth_call(endpoint, pair_address, TOKEN0_SELECTOR)
            resp1 = await self.rpc.eth_call(endpoint, pair_address, TOKEN1_SELECTOR)

            token0 = ""
            token1 = ""
            if resp0 and isinstance(resp0, str) and len(resp0) >= 42:
                token0 = "0x" + resp0[-40:].lower()
            if resp1 and isinstance(resp1, str) and len(resp1) >= 42:
                token1 = "0x" + resp1[-40:].lower()

            if token0 and token1:
                return (token0, token1)
        except Exception as exc:
            logger.debug("pool_resolve_failed", pair=pair_address[:20], error=str(exc)[:60])

        return ("", "")

    async def _batch_resolve_pool_tokens(
        self,
        pair_addresses: list[str],
    ) -> dict[str, tuple[str, str]]:
        """Batch-resolve token0/token1 for unknown pool addresses.

        Uses semaphore-limited concurrent eth_call with ClickHouse
        rugdex.pairs lookup as a faster first-pass filter.
        """
        if not pair_addresses:
            return {}

        # 1. First pass: check rugdex.pairs in ClickHouse
        bulk_resolved: dict[str, tuple[str, str]] = {}
        remaining: list[str] = []
        try:
            ch_client = self.ch_writer._connect()
            if ch_client:
                rows = ch_client.execute(
                    "SELECT pair_address, token0, token1 FROM rugdex.pairs WHERE pair_address IN %(pairs)s",
                    {"pairs": [p.lower() for p in pair_addresses]},
                )
                ch_hits = {}
                for row in rows:
                    addr, t0, t1 = row[0], row[1], row[2]
                    if t0 and t1:
                        ch_hits[addr.lower()] = (t0, t1)
                for p in pair_addresses:
                    pl = p.lower()
                    if pl in ch_hits:
                        bulk_resolved[pl] = ch_hits[pl]
                        self.pair_cache.set(pl, ch_hits[pl])
                    else:
                        remaining.append(pl)
            else:
                remaining = [p.lower() for p in pair_addresses]
        except Exception:
            remaining = [p.lower() for p in pair_addresses]

        if not remaining:
            if bulk_resolved:
                self.pair_cache.save()
            return bulk_resolved

        # 2. Second pass: eth_call fallback
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_POOL_RESOLVE)
        eth_resolved: dict[str, tuple[str, str]] = {}
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])

        async def _resolve_one(pair: str) -> tuple[str, str, str]:
            async with semaphore:
                try:
                    if not endpoints:
                        return (pair, "", "")
                    endpoint = get_best_endpoint(endpoints)
                    resp0 = await self.rpc.eth_call(endpoint, pair, TOKEN0_SELECTOR)
                    resp1 = await self.rpc.eth_call(endpoint, pair, TOKEN1_SELECTOR)
                    t0 = ""
                    t1 = ""
                    if resp0 and isinstance(resp0, str) and len(resp0) >= 42:
                        t0 = "0x" + resp0[-40:].lower()
                    if resp1 and isinstance(resp1, str) and len(resp1) >= 42:
                        t1 = "0x" + resp1[-40:].lower()
                    return (pair, t0, t1)
                except Exception:
                    return (pair, "", "")

        tasks = [_resolve_one(p) for p in remaining]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, tuple) and len(outcome) == 3:
                pair, t0, t1 = outcome
                if t0 and t1:
                    eth_resolved[pair] = (t0, t1)
                    self.pair_cache.set(pair, (t0, t1))

        all_resolved = {**bulk_resolved, **eth_resolved}
        if all_resolved:
            self.pair_cache.save()

        return all_resolved

    # ── Timestamp Enrichment ────────────────────────────────────────

    async def _enrich_timestamps(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Fetch block timestamps and attach to events."""
        if not events:
            return events

        block_numbers = {e["blockNumber"] for e in events}
        timestamps = await self.fetch_block_timestamps(block_numbers)

        for event in events:
            ts = timestamps.get(event["blockNumber"])
            if ts:
                event["blockTimestamp"] = ts
            else:
                from datetime import UTC, datetime

                event["blockTimestamp"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        return events

    # ── Block Range Determination ──────────────────────────────────

    async def _determine_block_range(self, summary: dict[str, Any]) -> tuple[int, int] | None:
        """Determine the block range for this sync cycle.

        When --start-from-head is active, ignore state file and start
        from (chain_tip - HEAD_BUFFER). No backfill from genesis.

        Returns (from_block, to_block) or None if we should abort.
        """
        if self.start_from_head:
            tip = await self.fetch_block_number()
            if tip is None:
                summary["status"] = "no_rpc_available"
                summary["errors"].append("fetch_block_number failed")
                logger.error("sync_aborted_no_rpc", chain=self.chain)
                return None
            from_block = max(0, tip - HEAD_BUFFER)
            to_block = tip
            return from_block, to_block

        # Normal path: resume from state file or override
        state = self.state.load()
        from_block = self._override_from if self._override_from is not None else state.get("last_block", 0) + 1

        if from_block <= 1:
            from_block = 0

        tip = await self.fetch_block_number()
        if tip is None:
            summary["status"] = "no_rpc_available"
            summary["errors"].append("fetch_block_number failed")
            logger.error("sync_aborted_no_rpc", chain=self.chain)
            return None

        to_block = self._override_to if self._override_to is not None else tip

        if from_block >= to_block:
            logger.info("caught_up", chain=self.chain, block=tip)
            summary["status"] = "caught_up"
            return None

        if to_block - from_block > self.batch_size:
            to_block = from_block + self.batch_size

        return from_block, to_block

    # ── Main Sync Loop ─────────────────────────────────────────────

    async def sync_loop(self) -> dict[str, Any]:
        """Main sync loop: load state → fetch → persist → save.

        Returns summary dict with stats.
        """
        start_time = time.monotonic()
        summary: dict[str, Any] = {
            "chain": self.chain,
            "status": "ok",
            "events_fetched": 0,
            "events_written_ndjson": 0,
            "events_written_ch": 0,
            "bytes_written": 0,
            "duration_seconds": 0,
            "errors": [],
        }

        # 1. Disk cap + disk space checks
        if not check_disk_cap(self.data_dir, self.disk_cap):
            summary["status"] = "disk_cap_exceeded"
            summary["errors"].append("disk cap exceeded")
            return summary

        if not check_disk_space():
            summary["status"] = "disk_full"
            summary["errors"].append("disk space critically low")
            return summary

        # 1b. Verify endpoints are healthy before proceeding
        chain_endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not chain_endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            summary["status"] = "no_endpoints"
            summary["errors"].append(f"no endpoints configured for {self.chain}")
            return summary

        # Get latest block for verification range
        tip_for_verify = await self.rpc.eth_block_number(get_best_endpoint(chain_endpoints))
        verified_endpoints = await verify_endpoints_for_chain(
            chain_endpoints,
            self.chain,
            latest_block=tip_for_verify,
        )
        logger.info(
            "endpoint_verification_complete",
            chain=self.chain,
            total=len(chain_endpoints),
            verified=len(verified_endpoints),
        )
        summary["endpoints_total"] = len(chain_endpoints)
        summary["endpoints_verified"] = len(verified_endpoints)

        # Temporarily use verified endpoints for this sync cycle
        # If no endpoints passed verification, the fallback in verify_endpoints_for_chain
        # returns the original list, so this is safe.
        import lite_indexer.chains as chains_mod

        original_endpoints = chains_mod.GET_LOGS_ENDPOINTS.get(self.chain, [])
        if verified_endpoints:
            chains_mod.GET_LOGS_ENDPOINTS[self.chain] = verified_endpoints

        try:
            result = await self._sync_loop_inner(summary, start_time)
            return result
        finally:
            # Restore original endpoints regardless of outcome
            chains_mod.GET_LOGS_ENDPOINTS[self.chain] = original_endpoints

    async def _sync_loop_inner(
        self,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any]:
        """Inner sync loop after endpoint verification.

        Separated from sync_loop so endpoint verification can be done
        before state management.
        """
        # 2. Determine block range
        block_range = await self._determine_block_range(summary)
        if block_range is None:
            return summary
        from_block, to_block = block_range

        # 3. Fetch transfer events
        logger.info("sync_start", chain=self.chain, from_block=from_block, to_block=to_block)

        events = await self.fetch_transfers(from_block, to_block)
        if events:
            summary["events_fetched"] = len(events)

        # 4. Fetch DEX swap events (if enabled)
        dex_events: list[dict[str, Any]] = []
        if self.dex_enabled:
            dex_events = await self.fetch_swap_events(from_block, to_block)
            summary["dex_events_fetched"] = len(dex_events)

        # 4a. Fetch pair created events for deployer tracking (if dex is disabled)
        if self.deployers_enabled and not self.dex_enabled:
            try:
                await self._fetch_pair_created_events(from_block, to_block)
            except Exception as exc:
                logger.warning("deployer_pair_fetch_failed", chain=self.chain, error=str(exc)[:100])

        if not events and not dex_events and not self.deployers_enabled:
            self.state.save(to_block, 0)
            summary["status"] = "no_events"
            summary["duration_seconds"] = time.monotonic() - start_time
            return summary

        # 5. Enrich with timestamps
        if events:
            events = await self._enrich_timestamps(events)
        if dex_events:
            dex_events = await self._enrich_timestamps(dex_events)

        # 5b. Write deployer events
        deployer_written = 0
        if self.deployers_enabled and self._last_pair_events:
            self._last_pair_events = await self._enrich_timestamps(self._last_pair_events)
            deployer_written = self.ch_writer.write_deployer_events(self.chain, self._last_pair_events)
            if deployer_written:
                summary["deployer_events_written"] = deployer_written

        # 6. Write NDJSON
        bytes_written = 0
        if events:
            bytes_written = self._ndjson_writer.write_events(self.chain, events)
            if bytes_written:
                summary["events_written_ndjson"] = len(events)
                summary["bytes_written"] = bytes_written

        if dex_events:
            dex_bytes = self._ndjson_writer.write_dex_events(self.chain, dex_events)
            if dex_bytes:
                summary["dex_events_written_ndjson"] = len(dex_events)
                summary["dex_bytes_written"] = dex_bytes

        # 7. Write ClickHouse
        if events:
            ch_inserted = self.ch_writer.write_transfers(self.chain, events)
            summary["events_written_ch"] = ch_inserted

        if dex_events:
            dex_ch_inserted = self.ch_writer.write_dex_events(self.chain, dex_events)
            summary["dex_events_written_ch"] = dex_ch_inserted

        # 8. Save state
        total_events = len(events) + len(dex_events) + deployer_written
        self.state.save(to_block, total_events)

        summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
        logger.info(
            "sync_complete",
            chain=self.chain,
            transfer_events=len(events),
            dex_events=len(dex_events),
            deployer_events=deployer_written,
            duration_s=summary["duration_seconds"],
        )

        return summary
