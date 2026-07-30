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
import json
from typing import Any

import httpx

from lite_indexer.chains import (
    CHAIN_BLOCK_RANGE_LIMITS,
    GET_LOGS_ENDPOINTS,
)
from lite_indexer.config import (
    BATCH_SIZE,
    FACTORY_ADDRESSES,
    PAIR_CREATED_TOPIC,
    POOL_CREATED_TOPIC,
    RPC_TIMEOUT,
    SWAP_V2_TOPIC,
    SWAP_V3_TOPIC,
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
from lite_indexer.strategies.pool_resolver import batch_resolve_pool_tokens

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

    # ── Block Range Enforcement ──────────────────────────────────────

    def enforce_block_range(self, from_block: int, to_block: int) -> tuple[int, int]:
        """Apply chain-specific block range limits + batch size cap.

        Uses CHAIN_BLOCK_RANGE_LIMITS to cap the range per chain, then
        applies batch_size as a second safety limit. This prevents
        "response too large" errors from restrictive RPCs.

        Args:
            from_block: Requested start block.
            to_block: Requested end block.

        Returns:
            (from_block, capped_to_block) tuple.
        """
        max_range = CHAIN_BLOCK_RANGE_LIMITS.get(self.chain, 10000)
        range_size = to_block - from_block
        effective_cap = min(max_range, self.batch_size)
        if range_size > effective_cap:
            logger.debug(
                "capping_block_range",
                chain=self.chain,
                requested=range_size,
                cap=effective_cap,
            )
            return (from_block, from_block + effective_cap)
        return (from_block, to_block)

    def detect_reorg(self, from_block: int, to_block: int) -> bool:
        """Detect chain reorgs where block number goes backwards.

        When from_block > to_block, it means either:
        - The chain experienced a reorganization (blocks unwound)
        - There was an RPC consistency issue

        Args:
            from_block: Previously indexed block.
            to_block: Current chain tip.

        Returns:
            True if a reorg was detected.
        """
        if from_block > to_block:
            logger.warning(
                "chain_reorg_detected",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
            )
            return True
        return False

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
        """Get latest block number via eth_blockNumber.

        Returns None on all errors (timeout, connection, empty response,
        malformed JSON). The caller handles None by treating it as
        "no RPC available" and aborting the sync cycle.
        """
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            return None

        # Try multiple endpoints for redundancy
        for attempt in range(3):
            endpoint = get_best_endpoint(endpoints)
            try:
                result = await self.rpc.eth_block_number(endpoint)
                if result is not None:
                    return result
            except Exception as exc:
                logger.debug(
                    "fetch_block_number_failed",
                    chain=self.chain,
                    endpoint=endpoint[:50],
                    error=str(exc)[:60],
                )
                record_endpoint_failure(endpoint)
                await asyncio.sleep(1)
                continue

        logger.error("fetch_block_number_all_failed", chain=self.chain)
        return None

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

        Error handling:
        - Chain reorg: from_block > to_block resets state
        - Empty RPC response: returns []
        - Malformed JSON: caught by generic except, logs + continues
        - Rate limit (429): exponential backoff
        - Block range too large: capped by enforce_block_range
        - Network timeout: caught, logs + tries next endpoint
        """
        # Chain reorg detection
        if self.detect_reorg(from_block, to_block):
            return ["__CHAIN_REORG__"]

        # Enforce chain-specific block range
        from_block, to_block = self.enforce_block_range(from_block, to_block)

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

                # Empty or too-small response
                if resp.status_code == 200:
                    raw_text = resp.text
                    if not raw_text or len(raw_text) < 10:
                        last_error = "empty_response"
                        logger.debug("empty_rpc_response", endpoint=url[:50], chain=self.chain)
                        record_endpoint_failure(url)
                        continue

                    try:
                        data = resp.json()
                    except (json.JSONDecodeError, ValueError) as exc:
                        last_error = f"malformed_json: {str(exc)[:40]}"
                        logger.debug(
                            "rpc_malformed_json",
                            endpoint=url[:50],
                            chain=self.chain,
                            error=str(exc)[:60],
                        )
                        record_endpoint_failure(url)
                        continue

                    if not isinstance(data, dict):
                        last_error = "non_dict_response"
                        record_endpoint_failure(url)
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
                        else:
                            # result is null — endpoint works, no logs in range
                            if logs is None:
                                if best_result is None:
                                    best_result = []
                                break
                            last_error = "unexpected_result_type"
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
            except (httpx.RemoteProtocolError, httpx.LocalProtocolError) as exc:
                last_error = f"protocol_error: {str(exc)[:40]}"
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

        Error handling:
        - Chain reorg: from_block > to_block resets state
        - All RPC errors (timeout, rate limit, malformed, empty) fall
          through to the next endpoint
        - Block range is capped by enforce_block_range
        """
        # Chain reorg detection
        if self.detect_reorg(from_block, to_block):
            return ["__CHAIN_REORG__"]

        # Enforce chain-specific block range
        from_block, to_block = self.enforce_block_range(from_block, to_block)

        # Build pair→tokens mapping from PairCreated events first
        try:
            await self._fetch_pair_created_events(from_block, to_block)
        except Exception as exc:
            logger.warning("pair_created_fetch_failed", chain=self.chain, error=str(exc)[:100])
            return []

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
                        # Empty response check
                        if not resp.text or len(resp.text) < 10:
                            record_endpoint_failure(url)
                            continue

                        try:
                            data = resp.json()
                        except (json.JSONDecodeError, ValueError):
                            record_endpoint_failure(url)
                            continue

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
                except (httpx.RemoteProtocolError, httpx.LocalProtocolError):
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
            resolved = await batch_resolve_pool_tokens(self.rpc, self.chain, list(unknown_pairs), self.pair_cache)
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
                await apply_endpoint_backoff(url)
                try:
                    resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
                    if resp.status_code == 200:
                        # Empty response check
                        if not resp.text or len(resp.text) < 10:
                            record_endpoint_failure(url)
                            continue

                        try:
                            data = resp.json()
                        except (json.JSONDecodeError, ValueError):
                            record_endpoint_failure(url)
                            continue

                        if isinstance(data, dict) and "result" in data:
                            logs = data["result"]
                            if isinstance(logs, list):
                                pair_tokens, deployer_events = parse_pair_created_logs(logs, self.chain)
                                self.pair_cache.update(pair_tokens)
                                self._last_pair_events.extend(deployer_events)
                                break
                    elif resp.status_code == 429:
                        await handle_rate_limit(url)
                        record_endpoint_failure(url)
                        continue
                    else:
                        record_endpoint_failure(url)
                        continue
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
                    record_endpoint_failure(url)
                    continue
                except Exception as exc:
                    logger.debug("factory_events_error", factory=factory_addr[:20], error=str(exc)[:60])
                    record_endpoint_failure(url)
                    continue

        if len(self.pair_cache) > pairs_before:
            self.pair_cache.save()

    # ── Pool Token Resolution ────────────────────────────────────────
    # (delegated to pool_resolver.py — see batch_resolve_pool_tokens import)
