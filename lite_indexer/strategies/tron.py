"""
Tron strategy — TRC20 transfer indexing via TronGrid event API.

Tron is TRC20-based (ERC20-compatible) but uses its own RPC API:
  - /wallet/getnowblock → latest block number
  - /v1/contracts/{address}/events?event_name=Transfer → Transfer events
  - Block timestamps from block_header.raw_data.timestamp

TRC20 events map directly to the EVM transfer schema (wallet_memory.evm_transfers)
with chain='tron', making them queryable alongside EVM transfers.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.strategies.base import AbstractChainStrategy

logger = get_logger(__name__)

# ── Tron RPC Endpoints ───────────────────────────────────────────────

TRON_ENDPOINTS: list[str] = [
    "https://api.trongrid.io",
    "https://rpc.tronlink.org",
    "https://api.tronstack.io",
]

# ── Tracked TRC20 Tokens ─────────────────────────────────────────────

TRC20_TOKENS: list[dict[str, Any]] = [
    {"address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "symbol": "USDT", "decimals": 6, "name": "Tether USD"},
    {"address": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8", "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    {"address": "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR", "symbol": "WTRX", "decimals": 6, "name": "Wrapped TRX"},
]

# Max events per page from TronGrid (200 is max)
TRON_PAGE_SIZE = 200
# Max block range per query (TronGrid limits)
TRON_MAX_BLOCK_RANGE = 2000


def _parse_tron_transfer_event(
    raw: dict[str, Any],
    token: dict[str, Any],
    chain: str,
) -> dict[str, Any] | None:
    """Parse a single TRC20 Transfer event from TronGrid response.

    TronGrid event format:
    {
      "block_number": "12345",
      "transaction_id": "0x...",
      "from": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
      "to": "...",
      "result": {
        "value": "1000000",
        "from": "...",
        "to": "..."
      },
      "block_timestamp": 1700000000000
    }

    Args:
        raw: Raw event dict from TronGrid.
        token: Tracked token metadata dict.
        chain: Chain name.

    Returns:
        Parsed event dict compatible with evm_transfers schema,
        or None if the event cannot be parsed.
    """
    try:
        result = raw.get("result") or {}
        from_addr = result.get("from", "") or raw.get("from", "")
        to_addr = result.get("to", "") or raw.get("to", "")

        if not from_addr or not to_addr:
            return None

        value = result.get("value", "0")
        if isinstance(value, str) and value.startswith("0x"):
            try:
                value = str(int(value, 16))
            except ValueError:
                value = "0"

        block_str = raw.get("block_number", "0")
        if isinstance(block_str, str):
            block_num = int(block_str)
        else:
            block_num = int(block_str)

        tx_hash = raw.get("transaction_id", "")

        log_index = raw.get("log_index", 0)
        if isinstance(log_index, str):
            log_index = int(log_index)

        # Parse timestamp from TronGrid (milliseconds since epoch)
        ts_raw = raw.get("block_timestamp", 0)
        if ts_raw:
            try:
                ts_sec = int(ts_raw) / 1000.0
                ts = datetime.fromtimestamp(ts_sec, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError, OSError):
                ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "type": "transfer",
            "chain": chain,
            "blockNumber": block_num,
            "blockTimestamp": ts,
            "txHash": tx_hash,
            "logIndex": log_index,
            "from": from_addr,
            "to": to_addr,
            "tokenAddress": token["address"],
            "value": str(value),
            "tokenSymbol": token["symbol"],
            "tokenName": token.get("name"),
            "source": "lite_indexer",
        }
    except Exception as exc:
        logger.debug("tron_event_parse_failed", error=str(exc)[:80])
        return None


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
        last_error: str | None = None

        for base_url in TRON_ENDPOINTS:
            url = f"{base_url}{path}"
            try:
                resp = await client.get(url, params=params, timeout=30.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
                    last_error = "non_dict_response"
                elif resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                else:
                    last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = str(exc)[:60]
                continue
            except Exception as exc:
                last_error = str(exc)[:60]
                continue

        logger.debug("tron_request_failed", path=path, error=last_error)
        return None

    async def _post(self, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Make a POST request to TronGrid with endpoint failover.

        Args:
            path: API path (e.g., '/wallet/getnowblock').
            json_body: Optional JSON body.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        client = await self._get_client()
        last_error: str | None = None

        for base_url in TRON_ENDPOINTS:
            url = f"{base_url}{path}"
            try:
                resp = await client.post(url, json=json_body or {}, timeout=30.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
                    last_error = "non_dict_response"
                elif resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                else:
                    last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = str(exc)[:60]
                continue
            except Exception as exc:
                last_error = str(exc)[:60]
                continue

        logger.debug("tron_request_failed", path=path, error=last_error)
        return None

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

        all_events: list[dict[str, Any]] = []

        for token in self.tokens:
            try:
                events = await self._fetch_token_events(token, from_block, to_block)
                all_events.extend(events)
            except Exception as exc:
                logger.warning(
                    "tron_token_fetch_failed",
                    token=token["symbol"],
                    error=str(exc)[:80],
                )

        # Deduplicate by (txHash, logIndex)
        seen_keys: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for evt in all_events:
            key = f"{evt['txHash']}:{evt['logIndex']}"
            if key not in seen_keys:
                seen_keys.add(key)
                deduped.append(evt)

        if len(deduped) < len(all_events):
            logger.debug("tron_dedup_removed", chain=self.chain, count=len(all_events) - len(deduped))

        return deduped

    async def _fetch_token_events(
        self,
        token: dict[str, Any],
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch TRC20 Transfer events for a single token.

        Uses TronGrid's paginated event API:
          GET /v1/contracts/{address}/events?
              event_name=Transfer&
              block_number={from}&
              block_number={to}&
              limit=200&
              order_by=block_timestamp,asc

        Args:
            token: Token metadata dict with 'address' key.
            from_block: Start block.
            to_block: End block.

        Returns:
            List of parsed event dicts.
        """
        events: list[dict[str, Any]] = []
        address = token["address"]

        # TronGrid pagination
        cursor = None
        max_pages = 5  # Safety limit per token per batch
        page = 0

        path = f"/v1/contracts/{address}/events"

        while page < max_pages:
            result = await self._tron_get_with_range(path, address, from_block, to_block, cursor)

            if not result:
                break

            data_list = result.get("data") or []
            if not isinstance(data_list, list):
                break

            for raw_event in data_list:
                parsed = _parse_tron_transfer_event(raw_event, token, self.chain)
                if parsed:
                    events.append(parsed)

            # Check for pagination
            meta = result.get("meta") or {}
            cursor = meta.get("fingerprint") or meta.get("cursor")
            if not cursor:
                break

            page += 1

        return events

    async def _tron_get_with_range(
        self,
        path: str,
        address: str,
        from_block: int,
        to_block: int,
        cursor: str | None = None,
    ) -> dict[str, Any] | None:
        """Make a GET request to TronGrid with block number range.

        TronGrid uses min_block_number and max_block_number for range queries.
        We build the query string manually for custom parameter formatting.

        Args:
            path: API path.
            address: Token contract address (unused, for logging context).
            from_block: Start block.
            to_block: End block.
            cursor: Pagination cursor.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        query_parts = [
            f"event_name=Transfer",
            f"from_block_number={from_block}",
            f"to_block_number={to_block}",
            f"limit={TRON_PAGE_SIZE}",
            "order_by=block_timestamp,desc",
        ]
        if cursor:
            query_parts.append(f"fingerprint={cursor}")

        query_string = "&".join(query_parts)
        full_url = f"?{query_string}"

        client = await self._get_client()
        last_error: str | None = None

        for base_url in TRON_ENDPOINTS:
            url = f"{base_url}{path}{full_url}"
            try:
                resp = await client.get(url, timeout=30.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        return data
                    last_error = "non_dict_response"
                elif resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                else:
                    last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = str(exc)[:60]
                continue
            except Exception as exc:
                last_error = str(exc)[:60]
                continue

        logger.debug("tron_get_range_failed", path=path[:50], error=last_error)
        return None
