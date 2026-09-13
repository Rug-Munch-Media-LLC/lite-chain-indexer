"""Free real-time Solana ingestion over a public RPC websocket.

Subscribes to Solana RPC ``logsSubscribe`` (program-scoped via a
``mentions`` filter) and/or ``accountSubscribe`` (watchlist-scoped), fetches
each notified transaction, and indexes it into
``wallet_memory.address_activity`` — the same keystone table the batch
indexer writes. This is the free real-time half of the SolanaProvider-parity story:
no API key, no paid webhook, just the public ``wss://`` endpoint plus our own
ClickHouse.

Scope discipline
----------------
The subscription is deliberately **watchlist/program-scoped** (a short list
of program ids and/or accounts). It is never a full-fidelity index of every
transaction on the chain — the free public websocket would not survive that
and it is not the goal. ``on_transaction`` is an optional hook so a caller
can also route the fetched transaction into the existing transfer/event
parsers.

Robustness
----------
The connection loop reconnects with exponential backoff, honours ping/pong
keepalive, and is cancel-safe (``stop()`` drains cleanly). The ``websockets``
package is imported lazily so the rest of the indexer runs without it; the
``ws_connect`` injectable lets tests drive the loop with a fake socket.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from lite_indexer.config import (
    SOLANA_WS_ACCOUNTS,
    SOLANA_WS_COMMITMENT,
    SOLANA_WS_MAX_RECONNECTS,
    SOLANA_WS_PING_INTERVAL,
    SOLANA_WS_PROGRAMS,
    SOLANA_WS_RECONNECT_BASE_SECONDS,
    SOLANA_WS_RECONNECT_MAX_SECONDS,
    SOLANA_WS_URL,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.address_activity import (
    ensure_address_activity_table,
    parse_address_activity,
    write_address_activity,
)
from lite_indexer.util.http import http_limits

logger = get_logger(__name__)

TransactionFetcher = Callable[[str], Awaitable[dict[str, Any] | None]]
TransactionCallback = Callable[[dict[str, Any]], Any]
AccountCallback = Callable[[dict[str, Any]], Any]

_TX_OPTIONS: dict[str, Any] = {
    "encoding": "jsonParsed",
    "maxSupportedTransactionVersion": 0,
    "commitment": "finalized",
}


@dataclass
class SolanaWebsocketConfig:
    """Runtime configuration for :class:`SolanaWebsocketIngestor`."""

    url: str = SOLANA_WS_URL
    programs: tuple[str, ...] = SOLANA_WS_PROGRAMS
    accounts: tuple[str, ...] = SOLANA_WS_ACCOUNTS
    commitment: str = SOLANA_WS_COMMITMENT
    ping_interval: float = SOLANA_WS_PING_INTERVAL
    reconnect_base_seconds: float = SOLANA_WS_RECONNECT_BASE_SECONDS
    reconnect_max_seconds: float = SOLANA_WS_RECONNECT_MAX_SECONDS
    max_reconnects: int = SOLANA_WS_MAX_RECONNECTS


class SolanaWebsocketIngestor:
    """Watchlist-scoped real-time ingestor for ``address_activity``.

    Args:
        ch_client: ClickHouse client (anything with ``execute``) used to
            persist address-activity rows. None disables persistence.
        config: Websocket configuration (defaults from env).
        fetch_transaction: Async callable ``(signature) -> tx | None``.
            Injectable for tests; defaults to a public-RPC getTransaction.
        on_transaction: Optional callback invoked with each fetched
            transaction (route it into transfer/event parsers).
        on_account_update: Optional callback invoked with each
            ``accountNotification`` value (watchlist liveness).
        ws_connect: Optional callable ``(url) -> async context manager``
            returning a connected websocket. Injectable for tests.
    """

    def __init__(
        self,
        *,
        ch_client: Any | None = None,
        config: SolanaWebsocketConfig | None = None,
        fetch_transaction: TransactionFetcher | None = None,
        on_transaction: TransactionCallback | None = None,
        on_account_update: AccountCallback | None = None,
        ws_connect: Callable[[str], Any] | None = None,
    ) -> None:
        self._ch_client = ch_client
        self.config = config or SolanaWebsocketConfig()
        self._fetch_transaction = fetch_transaction or self._default_fetch_transaction
        self._on_transaction = on_transaction
        self._on_account_update = on_account_update
        self._ws_connect = ws_connect
        self._http_client: Any = None
        self._stop = asyncio.Event()
        self._next_id = 0
        self.events_indexed = 0

    # ── Subscription plumbing ────────────────────────────────────────

    def _allocate_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def subscription_requests(self) -> list[dict[str, Any]]:
        """Build the ``logsSubscribe`` / ``accountSubscribe`` requests.

        One ``logsSubscribe`` per configured program (with a ``mentions``
        filter so the node only pushes transactions touching it) and one
        ``accountSubscribe`` per configured watchlist account.
        """
        requests: list[dict[str, Any]] = []
        for program in self.config.programs:
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": self._allocate_id(),
                    "method": "logsSubscribe",
                    "params": [{"mentions": [program]}, {"commitment": self.config.commitment}],
                }
            )
        for account in self.config.accounts:
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": self._allocate_id(),
                    "method": "accountSubscribe",
                    "params": [account, {"encoding": "jsonParsed", "commitment": self.config.commitment}],
                }
            )
        return requests

    # ── Connection loop ──────────────────────────────────────────────

    def _connect(self) -> Any:
        """Return an async context manager yielding a connected websocket."""
        if self._ws_connect is not None:
            return self._ws_connect(self.config.url)
        import websockets

        return websockets.connect(
            self.config.url,
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_interval * 2,
            max_size=2**20,
        )

    async def run(self) -> None:
        """Connect, subscribe and process notifications until ``stop()``.

        Reconnects with exponential backoff on any disconnect; gives up
        after ``config.max_reconnects`` attempts when that is non-zero.
        """
        attempt = 0
        while not self._stop.is_set():
            try:
                async with self._connect() as ws:
                    attempt = 0
                    for request in self.subscription_requests():
                        await ws.send(json.dumps(request))
                    logger.info(
                        "solana_ws_subscribed",
                        url=self.config.url,
                        programs=len(self.config.programs),
                        accounts=len(self.config.accounts),
                    )
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — reconnect on any socket error
                logger.warning("solana_ws_disconnected", error=str(exc)[:120])
            if self._stop.is_set():
                break
            attempt += 1
            if self.config.max_reconnects and attempt >= self.config.max_reconnects:
                logger.warning("solana_ws_reconnect_exhausted", attempts=attempt)
                break
            delay = min(
                self.config.reconnect_base_seconds * (2 ** max(0, attempt - 1)),
                self.config.reconnect_max_seconds,
            )
            metrics_incr("solana_ws_reconnects")
            await asyncio.sleep(delay)

    def stop(self) -> None:
        """Signal the connection loop to exit after the current message."""
        self._stop.set()

    # ── Notification handling ────────────────────────────────────────

    async def _handle_raw(self, raw: Any) -> int:
        """Dispatch one raw websocket frame; return rows indexed."""
        try:
            message = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        except (ValueError, TypeError):
            metrics_incr("solana_ws_bad_frame")
            return 0
        if not isinstance(message, dict):
            return 0
        method = message.get("method")
        if method == "logsNotification":
            return await self._handle_logs(message)
        if method == "accountNotification":
            return await self._handle_account(message)
        return 0

    async def _handle_logs(self, message: dict[str, Any]) -> int:
        """Fetch the notified transaction and index its address-activity rows."""
        params = message.get("params") or {}
        value = (params.get("result") or {}).get("value") or {}
        if value.get("err") is not None:
            metrics_incr("solana_ws_failed_tx_skipped")
            return 0
        signature = value.get("signature")
        if not signature:
            return 0
        metrics_incr("solana_ws_logs_events")
        transaction = await self._fetch_transaction(str(signature))
        if not isinstance(transaction, dict):
            return 0
        return await self.index_transaction(transaction)

    async def _handle_account(self, message: dict[str, Any]) -> int:
        """Handle an ``accountSubscribe`` update (watchlist liveness).

        Account notifications carry account state, not a signature, so they
        cannot yield address-activity rows directly; the optional callback
        lets a caller react (and, if it knows the transaction, index it).
        """
        params = message.get("params") or {}
        value = (params.get("result") or {}).get("value")
        metrics_incr("solana_ws_account_events")
        if self._on_account_update is not None and isinstance(value, dict):
            outcome = self._on_account_update(value)
            if asyncio.iscoroutine(outcome):
                await outcome
        return 0

    async def index_transaction(self, transaction: dict[str, Any]) -> int:
        """Expand + persist one parsed transaction into ``address_activity``.

        Best-effort: a parse or write failure is logged and returns 0. The
        optional ``on_transaction`` hook runs after the address rows are
        written so it can reuse the same payload for transfer/event parsing.
        """
        try:
            rows = parse_address_activity(transaction)
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            logger.warning("solana_ws_parse_failed", error=str(exc)[:100])
            return 0
        written = 0
        if rows and self._ch_client is not None:
            await asyncio.to_thread(ensure_address_activity_table, self._ch_client)
            written = await asyncio.to_thread(write_address_activity, self._ch_client, rows)
            if written:
                self.events_indexed += 1
        if self._on_transaction is not None:
            outcome = self._on_transaction(transaction)
            if asyncio.iscoroutine(outcome):
                await outcome
        return written

    # ── Default transaction fetcher ──────────────────────────────────

    async def _get_http_client(self) -> Any:
        import httpx

        if self._http_client is None or getattr(self._http_client, "is_closed", True):
            self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=http_limits())
        return self._http_client

    async def _default_fetch_transaction(self, signature: str) -> dict[str, Any] | None:
        """Fetch one transaction via the free Solana RPC pool."""
        from lite_indexer.strategies.solana.rpc import rpc_call

        client = await self._get_http_client()
        response = await rpc_call(client, "getTransaction", [signature, _TX_OPTIONS])
        if response is None:
            metrics_incr("solana_ws_tx_fetch_failed")
            return None
        result = response.get("result")
        return result if isinstance(result, dict) else None

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent)."""
        self.stop()
        if self._http_client is not None and not getattr(self._http_client, "is_closed", True):
            await self._http_client.aclose()
        self._http_client = None


__all__ = ["SolanaWebsocketConfig", "SolanaWebsocketIngestor"]
