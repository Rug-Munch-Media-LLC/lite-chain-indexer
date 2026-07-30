"""
EVM chain strategy — ERC-20 Transfer, DEX Swap, and PairCreated indexing.

Uses eth_getLogs on curated free RPC endpoints. Supports:
  - ERC-20 Transfer events (no address filter — catches all tokens)
  - DEX Swap events (V2 + V3 Uniswap-style)
  - PairCreated/PoolCreated events (factory-based pair discovery)
  - Block timestamp enrichment via eth_getBlockByNumber
  - eth_call fallback for unknown pool token resolution
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.chains import (
    GET_LOGS_ENDPOINTS,
)
from lite_indexer.config import (
    BATCH_SIZE,
    FACTORY_ADDRESSES,
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
from lite_indexer.state import PairCache
from lite_indexer.strategies.base import AbstractChainStrategy

logger = get_logger(__name__)


class EVMChainStrategy(AbstractChainStrategy):
    """Strategy for EVM-compatible chains using eth_getLogs.

    Indexes ERC-20 Transfer events, DEX Swap events (V2+V3), and
    PairCreated/PoolCreated events. This is the original LiteIndexer
    logic extracted into the strategy pattern.
    """

    def __init__(
        self,
        chain: str,
        rpc: RpcClient | None = None,
        batch_size: int = BATCH_SIZE,
        pair_cache: PairCache | None = None,
    ) -> None:
        super().__init__(chain)
        self.rpc = rpc or RpcClient()
        self.batch_size = batch_size
        self._pair_cache = pair_cache or PairCache(chain)

        # Latest batch of pair creation events for deployer tracking
        self._last_pair_events: list[dict[str, Any]] = []

        # HTTP client for eth_getLogs batch calls
        self._client: httpx.AsyncClient | None = None

    @property
    def pair_cache(self) -> PairCache:
        return self._pair_cache

    # ── Feature Flags ────────────────────────────────────────────────

    def supports_dex(self) -> bool:
        return True

    def supports_deployers(self) -> bool:
        return True

    # ── HTTP Client ──────────────────────────────────────────────────

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

    # ── Endpoint Verification ────────────────────────────────────────

    async def verify_endpoints(self) -> list[str]:
        """Verify eth_getLogs endpoints are healthy for this chain."""
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return []

        tip = await self.rpc.eth_block_number(get_best_endpoint(endpoints))
        return await verify_endpoints_for_chain(
            endpoints,
            self.chain,
            latest_block=tip,
        )

    # ── Block Number ─────────────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get latest block number via eth_blockNumber."""
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            return None

        endpoint = get_best_endpoint(endpoints)
        return await self.rpc.eth_block_number(endpoint)

    # ── Block Timestamps ─────────────────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch ISO-8601 timestamps for a set of EVM block numbers."""
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return {}

        return await fetch_block_timestamps(self.rpc, endpoints, block_numbers)

    # ── Transfer Events ──────────────────────────────────────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch ERC-20 Transfer events via eth_getLogs.

        Calls eth_getLogs with only the Transfer event topic filter
        (no address filter) so we catch every transfer on the chain.

        Uses a curated list of endpoints known to support eth_getLogs.
        Falls through errors to try the next endpoint.
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

    # ── DEX Swap Events ──────────────────────────────────────────────

    async def fetch_swap_events(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch DEX Swap events (V2 + V3) via eth_getLogs.

        First fetches PairCreated events from known factories to build a
        pair→(token0, token1) mapping, then fetches swap events.
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

    # ── PairCreated Fetch ────────────────────────────────────────────

    async def _fetch_pair_created_events(
        self,
        from_block: int,
        to_block: int,
    ) -> None:
        """Fetch PairCreated (V2) + PoolCreated (V3) events from known DEX factories."""
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

    # ── Pool Token Resolution ────────────────────────────────────────

    async def _resolve_pool_tokens(self, pair_address: str) -> tuple[str, str]:
        """Resolve token0/token1 via eth_call on the pool contract."""
        # 1. Try ClickHouse rugdex.pairs lookup (via ch_writer if available)
        # (handled by caller via batch_resolve)
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
        """Batch-resolve token0/token1 for unknown pool addresses."""
        if not pair_addresses:
            return {}

        # eth_call fallback
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_POOL_RESOLVE)
        resolved: dict[str, tuple[str, str]] = {}
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

        tasks = [_resolve_one(p) for p in pair_addresses]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, tuple) and len(outcome) == 3:
                pair, t0, t1 = outcome
                if t0 and t1:
                    resolved[pair] = (t0, t1)
                    self.pair_cache.set(pair, (t0, t1))

        if resolved:
            self.pair_cache.save()

        return resolved
