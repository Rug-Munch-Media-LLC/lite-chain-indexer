"""
Tron Strategy — TRC20 transfer indexing via TronGrid event API.

Tron is TRC20-based (ERC20-compatible) but uses its own RPC API:
  - /wallet/getnowblock → latest block number
  - /v1/contracts/{address}/events?event_name=Transfer → Transfer events
  - Block timestamps from block_header.raw_data.timestamp

TRC20 events map directly to the EVM transfer schema (wallet_memory.evm_transfers)
with chain='tron', making them queryable alongside EVM transfers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcFetchFailedError
from lite_indexer.strategies.base import AbstractChainStrategy

from .launch import (
    SUNPUMP_CONTRACT,
    SUNSWAP_FACTORY,
    detect_sunpump_launch,
    detect_sunswap_pair,
)
from .models import TRC20_TOKENS, TRON_MAX_BLOCK_RANGE
from .parsing import parse_tron_transfer_event
from .rpc import tron_get, tron_get_with_range, tron_post

logger = get_logger(__name__)


class TronStrategy(AbstractChainStrategy):
    """Strategy for Tron — indexes TRC20 Transfer events via TronGrid.

    TRC20 is ERC20-compatible. Transfer events are fetched via TronGrid's
    /v1/contracts/{address}/events API and mapped to the same schema as
    EVM transfers (wallet_memory.evm_transfers).

    The Tron Indexer supports:
      - Multiple RPC endpoints with failover
      - Paginated event queries
      - Per-token tracking (USDT, USDC, WTRX by default)
    """

    def __init__(
        self,
        chain: str = "tron",
        tokens: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(chain)
        self.tokens = tokens or TRC20_TOKENS
        self._client: httpx.AsyncClient | None = None
        # Launch detection (SunPump TokenCreate + SunSwap PairCreated):
        # buffered here, flushed in indexer.py's sync cycle. Best-effort.
        self._last_launch_events: list[Any] = []

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                limits=limits,
            )
        return self._client

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Make a GET request to TronGrid with endpoint failover.

        Args:
            path: API path (e.g., '/wallet/getnowblock').
            params: Optional query parameters.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        client = await self._get_client()
        return await tron_get(client, path, params)

    async def _post(self, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Make a POST request to TronGrid with endpoint failover.

        Args:
            path: API path (e.g., '/wallet/getnowblock').
            json_body: Optional JSON body.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        client = await self._get_client()
        return await tron_post(client, path, json_body)

    # ── Block Number ─────────────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get latest Tron block number via /wallet/getnowblock."""
        result = await self._post("/wallet/getnowblock")
        if result and "block_header" in result:
            raw_data = result["block_header"].get("raw_data", {})
            block_num = raw_data.get("number")
            if block_num is not None:
                return int(block_num)

        # Fallback: try full node RPC
        result2 = await self._post(
            "/jsonrpc",
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_blockNumber",
            },
        )
        if result2 and "result" in result2:
            return int(result2["result"], 16)

        return None

    # ── Block Timestamps ─────────────────────────────────────────────

    async def _block_to_timestamp_ms(self, block_num: int) -> int | None:
        """Resolve a Tron block number to its timestamp in milliseconds.

        Uses /wallet/getblockbynum for the exact block header timestamp,
        falling back to an estimate anchored at the chain tip (~3s blocks).
        TronGrid's events API filters on min/max_block_timestamp, so we
        must convert block numbers to timestamps before querying.

        Args:
            block_num: Tron block number.

        Returns:
            Epoch timestamp in milliseconds, or None if unresolvable.
        """
        try:
            result = await self._post("/wallet/getblockbynum", {"num": int(block_num)})
            if result and "block_header" in result:
                raw_data = result["block_header"].get("raw_data", {})
                ts = raw_data.get("timestamp")
                if ts:
                    return int(ts)
        except Exception as exc:
            logger.debug("tron_block_ts_failed", block=block_num, error=str(exc)[:60])

        # Fallback: estimate from chain tip (~3s blocks)
        try:
            latest = await self.fetch_block_number()
            now_res = await self._post("/wallet/getnowblock")
            if latest and now_res and "block_header" in now_res:
                latest_ts = now_res["block_header"].get("raw_data", {}).get("timestamp")
                if latest_ts and block_num <= latest:
                    return int(latest_ts) - (int(latest) - int(block_num)) * 3000
        except Exception as exc:
            logger.debug("tron_block_ts_estimate_failed", block=block_num, error=str(exc)[:60])

        return None

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch block timestamps via TronGrid /wallet/getblockbyid or approximate.

        Tron blocks have timestamps in block_header.raw_data.timestamp (ms).
        Since TRC20 events already include block_timestamp from the event
        endpoint, this fallback is rarely needed.
        """
        if not block_numbers:
            return {}

        # TRC20 events already come with timestamps from the event API,
        # so we use approximate timestamps as a lightweight fallback.
        timestamps: dict[int, str] = {}
        latest = await self.fetch_block_number()
        if latest and latest in block_numbers:
            # Fetch latest block to get its timestamp
            result = await self._post("/wallet/getnowblock")
            if result and "block_header" in result:
                raw_data = result["block_header"].get("raw_data", {})
                latest_ts_ms = raw_data.get("timestamp", 0)
                if latest_ts_ms:
                    try:
                        avg_block_time = 3.0  # Tron has ~3s blocks
                        for bn in block_numbers:
                            if bn not in timestamps:
                                offset = latest_ts_ms // 1000 - int((latest - bn) * avg_block_time)
                                ts = datetime.fromtimestamp(offset, tz=UTC)
                                timestamps[bn] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except (ValueError, TypeError, OSError):
                        pass

        return timestamps

    # ── Transfer Events ──────────────────────────────────────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch TRC20 Transfer events for all tracked tokens.

        Queries /v1/contracts/{token}/events for each tracked token
        and aggregates results. Respects block range limits.

        Args:
            from_block: Start block (inclusive).
            to_block: End block (inclusive).

        Returns:
            List of parsed TRC20 transfer event dicts (EVM-compatible schema).
        """
        if from_block > to_block:
            return []

        if to_block - from_block > TRON_MAX_BLOCK_RANGE:
            to_block = from_block + TRON_MAX_BLOCK_RANGE

        # TronGrid's events API filters on block timestamps, not block
        # numbers. Resolve the range once — timestamps are token-agnostic.
        min_ts = await self._block_to_timestamp_ms(from_block)
        max_ts = await self._block_to_timestamp_ms(to_block)
        if min_ts is None or max_ts is None:
            # Cannot constrain the query → do not advance state past it.
            raise RpcFetchFailedError(f"tron: cannot resolve timestamps for blocks {from_block}-{to_block}")

        all_events: list[dict[str, Any]] = []
        failures = 0

        for token in self.tokens:
            try:
                events, ok = await self._fetch_token_events(
                    token,
                    from_block,
                    to_block,
                    min_ts,
                    max_ts,
                )
                if not ok:
                    failures += 1
                all_events.extend(events)
            except Exception as exc:
                failures += 1
                logger.warning(
                    "tron_token_fetch_failed",
                    token=token["symbol"],
                    error=str(exc)[:80],
                )

        # Data-loss guard: if every tracked token's query failed, the empty
        # result does NOT mean "no events" — raise so the indexer retries
        # this block range instead of advancing past it.
        if self.tokens and failures == len(self.tokens):
            raise RpcFetchFailedError(f"tron: all endpoints failed for blocks {from_block}-{to_block}")

        # Deduplicate by (txID, logIndex) — TronGrid returns transaction_id
        # (txID) and event_index; using them prevents duplicates from the
        # per-token queries (a Transfer event touches two tracked tokens).
        seen_keys: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for evt in all_events:
            tx_id = evt.get("txID") or evt.get("txHash") or ""
            key = f"{tx_id}:{evt.get('logIndex', 0)}"
            if key not in seen_keys:
                seen_keys.add(key)
                deduped.append(evt)

        if len(deduped) < len(all_events):
            logger.debug("tron_dedup_removed", chain=self.chain, count=len(all_events) - len(deduped))

        # Launch detection (best-effort): poll SunPump + SunSwap factory for
        # TokenCreate / PairCreated events in the same block window. Results
        # are buffered in self._last_launch_events and flushed in the sync
        # cycle. Failures are contained — launch detection never blocks or
        # aborts transfer indexing.
        try:
            await self._fetch_launch_events(min_ts, max_ts)
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.warning("tron_launch_poll_failed", error=str(exc)[:80])

        return deduped

    async def _fetch_launch_events(
        self,
        min_ts: int,
        max_ts: int,
    ) -> None:
        """Poll SunPump + SunSwap factory for launch events (best-effort).

        Maps TronGrid polled events into EVM-style receipts
        (``{"logs": [{address, topics, data, transactionHash, blockNumber,
        logIndex, blockTimestamp}]}``) and runs the launch/pair detectors,
        buffering results in ``self._last_launch_events``. Tron has no
        WebSocket events — this is polling-only (protocol constraint).
        """
        launch_contracts = (
            (SUNPUMP_CONTRACT, "sunpump"),
            (SUNSWAP_FACTORY, "sunswap"),
        )
        for address, name in launch_contracts:
            try:
                result = await self._get(
                    f"/v1/contracts/{address}/events",
                    {
                        "min_block_timestamp": min_ts,
                        "max_block_timestamp": max_ts,
                        "only_confirmed": "true",
                        "limit": 200,
                        "order_by": "block_timestamp,asc",
                    },
                )
                if not result:
                    continue
                raw_events = result.get("data") or []
                for raw in raw_events if isinstance(raw_events, list) else []:
                    if not isinstance(raw, dict):
                        continue
                    receipt = {
                        "logs": [
                            {
                                "address": raw.get("address", address),
                                "topics": raw.get("topics") or [],
                                "data": raw.get("data", ""),
                                "transactionHash": raw.get("transaction_id", ""),
                                "blockNumber": raw.get("block_number", 0),
                                "logIndex": raw.get("event_index", 0),
                                "blockTimestamp": raw.get("block_timestamp", 0),
                            }
                        ]
                    }
                    evt = detect_sunpump_launch(receipt)
                    if evt is None:
                        evt = detect_sunswap_pair(receipt)
                    if evt is not None:
                        self._last_launch_events.append(evt)
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.warning(
                    "tron_launch_contract_poll_failed",
                    contract=name,
                    error=str(exc)[:80],
                )

    async def _fetch_token_events(
        self,
        token: dict[str, Any],
        from_block: int,
        to_block: int,
        min_ts: int,
        max_ts: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch TRC20 Transfer events for a single token.

        Uses TronGrid's paginated event API:
          GET /v1/contracts/{address}/events?
              event_name=Transfer&
              min_block_timestamp={from_ts}&
              max_block_timestamp={to_ts}&
              only_confirmed=true&
              limit=200&
              order_by=block_timestamp,desc

        Args:
            token: Token metadata dict with 'address' key.
            from_block: Start block (logging only).
            to_block: End block (logging only).
            min_ts: Inclusive start timestamp (ms).
            max_ts: Inclusive end timestamp (ms).

        Returns:
            Tuple of (parsed event dicts, rpc_ok). rpc_ok is False when
            the first-page query failed on every endpoint.
        """
        events: list[dict[str, Any]] = []
        address = token["address"]

        # TronGrid pagination
        cursor = None
        max_pages = 5  # Safety limit per token per batch
        page = 0
        rpc_ok = False

        path = f"/v1/contracts/{address}/events"

        while page < max_pages:
            client = await self._get_client()
            result = await tron_get_with_range(
                client,
                path,
                from_block,
                to_block,
                cursor,
                min_block_timestamp=min_ts,
                max_block_timestamp=max_ts,
            )

            if not result:
                break

            rpc_ok = True

            data_list = result.get("data") or []
            if not isinstance(data_list, list):
                break

            for raw_event in data_list:
                parsed = parse_tron_transfer_event(raw_event, token, self.chain)
                if parsed:
                    events.append(parsed)

            # Check for pagination
            meta = result.get("meta") or {}
            cursor = meta.get("fingerprint") or meta.get("cursor")
            if not cursor:
                break

            page += 1

        return events, rpc_ok
