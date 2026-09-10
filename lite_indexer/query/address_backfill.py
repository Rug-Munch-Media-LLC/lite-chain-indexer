"""On-demand Solana address backfill — free getSignaturesForAddress parity.

Recovers a wallet's transaction history once, from free RPC, and indexes it
into ``wallet_memory.address_activity`` so every later read is a ClickHouse
hit. This is the free replacement for Helius's paid webhook +
``getSignaturesForAddress``: the same history, fetched on first touch and
never re-fetched.

Provider order (all free tiers, keys from env, never hardcoded):

1. ``SOLANA_PUBLIC_RPC``         — official public node (keyless)
2. ``SOLANA_PUBLIC_RPC_FALLBACK``— publicnode (keyless)
3. Alchemy                       — ``${ALCHEMY_KEY}`` when set
4. Helius                        — ``${HELIUS_KEY}`` when set

The first provider that returns a signature list answers; a provider that
errors (HTTP error / JSON-RPC error / timeout) falls through to the next.
When every ``getSignaturesForAddress`` provider returns an empty history and
a Helius key is configured, the Helius-only ``getTransactionsForAddress``
(``filters.tokenAccounts = "all"``) is tried — it is the one method that
retains *closed* account history (see docs/closed-account-history.md).

Never-re-fetch guard
--------------------
A dedicated ``wallet_memory.address_backfill_state`` row records each
``(chain, address)`` that has been attempted successfully — including a
legitimately empty result, so a closed account is never re-dialled. A
process-local attempt cache additionally stops a failed or unwritable
attempt (e.g. a read-only API user) from re-spending free-tier RPC on every
request within ``ADDRESS_BACKFILL_ATTEMPT_TTL``.

All RPC I/O goes through an injectable :class:`RpcTransport`, so tests use a
recording fake instead of monkeypatching module internals.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlparse

from lite_indexer.config import (
    ADDRESS_BACKFILL_ATTEMPT_TTL,
    ADDRESS_BACKFILL_CONCURRENCY,
    ADDRESS_BACKFILL_ENABLED,
    ADDRESS_BACKFILL_MAX_SIGNATURES,
    ADDRESS_BACKFILL_MAX_TX,
    ADDRESS_BACKFILL_PAGE_SIZE,
    ADDRESS_BACKFILL_TIMEOUT,
    CH_ADDRESS_BACKFILL_STATE_TABLE,
    SOLANA_ALCHEMY_RPC_TEMPLATE,
    SOLANA_HELIUS_RPC_TEMPLATE,
    SOLANA_PUBLIC_RPC,
    SOLANA_PUBLIC_RPC_FALLBACK,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.address_activity import (
    ensure_address_activity_table,
    parse_address_activity,
    write_address_activity,
)

logger = get_logger(__name__)

# Solana JSON-RPC getTransaction options — jsonParsed so the pure
# address-activity parser can expand account keys without a second call.
_TX_OPTIONS: dict[str, Any] = {
    "encoding": "jsonParsed",
    "maxSupportedTransactionVersion": 0,
    "commitment": "finalized",
}

_BACKFILL_STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_ADDRESS_BACKFILL_STATE_TABLE} (
    address String CODEC(ZSTD(3)),
    chain LowCardinality(String),
    status LowCardinality(String),
    signatures_seen UInt64,
    rows_written UInt64,
    provider LowCardinality(String),
    fetched_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (chain, address)
"""


class RpcTransport(Protocol):
    """Minimal JSON-RPC transport (injectable for tests)."""

    async def call(self, endpoint: str, method: str, params: list[Any]) -> dict[str, Any] | None:
        """POST a JSON-RPC request; return the parsed response or None."""
        ...


class HttpRpcTransport:
    """Default transport — a shared httpx client over the free RPC pool."""

    def __init__(self, timeout: float = ADDRESS_BACKFILL_TIMEOUT) -> None:
        self._timeout = float(timeout)
        self._client: Any = None

    async def _get_client(self) -> Any:
        import httpx

        if self._client is None or getattr(self._client, "is_closed", True):
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(self._timeout))
        return self._client

    async def call(self, endpoint: str, method: str, params: list[Any]) -> dict[str, Any] | None:
        import httpx

        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            client = await self._get_client()
            resp = await client.post(endpoint, json=payload)
        except (httpx.HTTPError, OSError, TimeoutError) as exc:
            logger.debug(
                "address_backfill_rpc_error",
                endpoint=urlparse(endpoint).netloc,  # host only — never the key
                error=str(exc)[:80],
            )
            return None
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent)."""
        if self._client is not None and not getattr(self._client, "is_closed", True):
            await self._client.aclose()
        self._client = None


def default_solana_providers() -> list[tuple[str, str]]:
    """Return the ordered (name, url) free-provider fallback list.

    ``${ENV_VAR}`` placeholders are expanded; templates whose key is unset
    are dropped (never dialled). Public nodes always come first.
    """
    candidates = [
        ("public", SOLANA_PUBLIC_RPC),
        ("public-fallback", SOLANA_PUBLIC_RPC_FALLBACK),
        ("alchemy", SOLANA_ALCHEMY_RPC_TEMPLATE),
        ("helius", SOLANA_HELIUS_RPC_TEMPLATE),
    ]
    providers: list[tuple[str, str]] = []
    for name, template in candidates:
        url = os.path.expandvars(template)
        if "${" in url or not url.startswith("http"):
            continue
        providers.append((name, url))
    return providers


@dataclass
class BackfillResult:
    """Outcome of one :meth:`AddressBackfill.backfill_address` call."""

    address: str
    chain: str = "solana"
    skipped: bool = False
    reason: str = ""
    provider: str = ""
    signatures: int = 0
    rows_written: int = 0


@dataclass
class AddressBackfill:
    """Fetch + index one address's history, guarded against re-fetching.

    Args:
        ch_client: A ClickHouse client (anything with ``execute``).
        ch_client_factory: Zero-arg callable resolving a client lazily
            (used by the query router / API so no connection is forced).
        transport: Injectable JSON-RPC transport (defaults to HTTP).
        providers: Ordered ``(name, url)`` fallback list (defaults to
            :func:`default_solana_providers`).
        max_signatures: Signature cap per backfill.
        max_tx: Transaction fetch cap per backfill.
        concurrency: Concurrent ``getTransaction`` calls.
    """

    ch_client: Any | None = None
    ch_client_factory: Callable[[], Any] | None = None
    transport: RpcTransport | None = None
    providers: list[tuple[str, str]] = field(default_factory=default_solana_providers)
    max_signatures: int = ADDRESS_BACKFILL_MAX_SIGNATURES
    max_tx: int = ADDRESS_BACKFILL_MAX_TX
    page_size: int = ADDRESS_BACKFILL_PAGE_SIZE
    concurrency: int = ADDRESS_BACKFILL_CONCURRENCY
    enabled: bool = ADDRESS_BACKFILL_ENABLED
    attempt_ttl: float = ADDRESS_BACKFILL_ATTEMPT_TTL
    _attempts: dict[tuple[str, str], float] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.transport is None:
            self.transport = HttpRpcTransport()
        self.providers = list(self.providers)

    @property
    def _transport(self) -> RpcTransport:
        """The active RPC transport (never None after ``__post_init__``)."""
        assert self.transport is not None  # noqa: S101 — set in __post_init__
        return self.transport

    # ── Client resolution ────────────────────────────────────────────

    def _resolve_client(self) -> Any | None:
        """Resolve the ClickHouse client (lazily via the factory)."""
        if self.ch_client is not None:
            return self.ch_client
        if self.ch_client_factory is not None:
            try:
                self.ch_client = self.ch_client_factory()
            except Exception as exc:  # noqa: BLE001 — lazy connect is best-effort
                logger.warning("address_backfill_ch_resolve_failed", error=str(exc)[:100])
                return None
        return self.ch_client

    # ── Never-re-fetch guard ─────────────────────────────────────────

    def _recently_attempted(self, chain: str, address: str) -> bool:
        """True when a failed attempt for this address is still cooling down."""
        if self.attempt_ttl <= 0:
            return False
        ts = self._attempts.get((chain, address))
        return ts is not None and (time.monotonic() - ts) < self.attempt_ttl

    def _record_attempt(self, chain: str, address: str) -> None:
        self._attempts[(chain, address)] = time.monotonic()

    async def is_backfilled(self, chain: str, address: str) -> bool:
        """True when the persistent guard table already holds this address.

        A row with ``status='complete'`` means a prior backfill finished —
        including a legitimately empty (closed-account) result — so the
        address must never be re-fetched.
        """
        client = self._resolve_client()
        if client is None:
            return False
        sql = (
            f"SELECT count() FROM {CH_ADDRESS_BACKFILL_STATE_TABLE} FINAL "
            "WHERE chain = %(chain)s AND address = %(address)s AND status = 'complete'"
        )
        try:
            rows = await asyncio.to_thread(client.execute, sql, {"chain": chain, "address": address})
        except Exception as exc:  # noqa: BLE001 — guard is best-effort
            logger.debug("address_backfill_guard_read_failed", error=str(exc)[:100])
            return False
        try:
            return bool(rows and int(rows[0][0]) > 0)
        except (TypeError, ValueError, IndexError):
            return False

    async def _ensure_state_table(self) -> None:
        client = self._resolve_client()
        if client is None:
            return
        try:
            await asyncio.to_thread(client.execute, _BACKFILL_STATE_DDL)
        except Exception as exc:  # noqa: BLE001 — DDL is best-effort
            logger.debug("address_backfill_state_ddl_failed", error=str(exc)[:100])

    async def mark_backfilled(
        self,
        chain: str,
        address: str,
        *,
        status: str,
        signatures_seen: int,
        rows_written: int,
        provider: str,
    ) -> bool:
        """Record a completed backfill in the guard table."""
        client = self._resolve_client()
        if client is None:
            return False
        row = (address, chain, status, int(signatures_seen), int(rows_written), provider, datetime.now(UTC))
        sql = (
            f"INSERT INTO {CH_ADDRESS_BACKFILL_STATE_TABLE} "
            "(address, chain, status, signatures_seen, rows_written, provider, fetched_at) VALUES"
        )
        try:
            await asyncio.to_thread(client.execute, sql, [row])
            return True
        except Exception as exc:  # noqa: BLE001 — guard write is best-effort
            logger.warning("address_backfill_guard_write_failed", error=str(exc)[:100])
            return False

    # ── Multi-provider fetch ─────────────────────────────────────────

    async def _paginate_signatures(self, url: str, address: str, max_signatures: int) -> list[dict[str, Any]] | None:
        """Page one provider's ``getSignaturesForAddress`` (None = provider failed)."""
        collected: list[dict[str, Any]] = []
        before: str | None = None
        while len(collected) < max_signatures:
            limit = max(1, min(self.page_size, max_signatures - len(collected)))
            options: dict[str, Any] = {"limit": limit, "commitment": "finalized"}
            if before:
                options["before"] = before
            resp = await self._transport.call(url, "getSignaturesForAddress", [address, options])
            if resp is None or "result" not in resp:
                return None
            result = resp.get("result")
            if not isinstance(result, list):
                return None
            collected.extend(item for item in result if isinstance(item, dict))
            if len(result) < limit:
                break
            before = str(result[-1].get("signature") or "")
            if not before:
                break
        return collected[:max_signatures]

    async def _fetch_signatures(self, address: str, max_signatures: int) -> tuple[list[dict[str, Any]] | None, str]:
        """Walk the provider fallback list; first success wins."""
        for name, url in self.providers:
            sigs = await self._paginate_signatures(url, address, max_signatures)
            if sigs is not None:
                return sigs, name
            metrics_incr("address_backfill_provider_failed")
            logger.debug("address_backfill_provider_failed", provider=name)
        return None, ""

    async def _fetch_helius_enhanced(self, address: str, max_signatures: int) -> list[dict[str, Any]]:
        """Helius-only closed-account recovery (``getTransactionsForAddress``)."""
        helius_url = next((url for name, url in self.providers if name == "helius"), "")
        if not helius_url:
            return []
        options = {
            "transactionDetails": "signatures",
            "sortOrder": "desc",
            "limit": min(max_signatures, 1000),
            "maxSupportedTransactionVersion": 0,
            "filters": {"tokenAccounts": "all"},
        }
        resp = await self._transport.call(helius_url, "getTransactionsForAddress", [address, options])
        if resp is None or "result" not in resp:
            return []
        result = resp.get("result")
        data = result.get("data") if isinstance(result, dict) else result
        if not isinstance(data, list):
            return []
        out: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            sig = item.get("signature")
            if not sig:
                tx = item.get("transaction") or {}
                sigs = tx.get("signatures") or []
                sig = sigs[0] if sigs else ""
            if sig:
                out.append({"signature": str(sig), "slot": int(item.get("slot") or 0)})
        return out[:max_signatures]

    async def _call_with_fallback(self, method: str, params: list[Any]) -> dict[str, Any] | None:
        """Call ``method`` trying each provider until one answers."""
        for _name, url in self.providers:
            resp = await self._transport.call(url, method, params)
            if resp is not None and "result" in resp:
                return resp
        return None

    async def _fetch_and_parse(self, sig_infos: list[dict[str, Any]], max_tx: int) -> list[dict[str, Any]]:
        """Fetch each signature's transaction and expand address-activity rows."""
        semaphore = asyncio.Semaphore(max(1, self.concurrency))

        async def _one(info: dict[str, Any]) -> list[dict[str, Any]]:
            sig = str(info.get("signature") or "")
            if not sig:
                return []
            async with semaphore:
                resp = await self._call_with_fallback("getTransaction", [sig, _TX_OPTIONS])
            if not resp:
                return []
            tx = resp.get("result")
            if not isinstance(tx, dict):
                return []
            try:
                return parse_address_activity(tx)
            except (ValueError, TypeError, KeyError, AttributeError):
                return []

        results = await asyncio.gather(*(_one(info) for info in sig_infos[:max_tx]))
        return [row for batch in results for row in batch]

    # ── Public entry point ───────────────────────────────────────────

    async def backfill_address(
        self,
        address: str,
        chain: str = "solana",
        *,
        force: bool = False,
        max_signatures: int | None = None,
        max_tx: int | None = None,
    ) -> BackfillResult:
        """Backfill one address's history into ``address_activity`` (once).

        Args:
            address: Base58 Solana address (case-sensitive).
            chain: Chain name; only ``"solana"`` is supported today.
            force: Bypass both guards (operator/tests only).
            max_signatures: Override the signature cap.
            max_tx: Override the transaction-fetch cap.

        Returns:
            A :class:`BackfillResult`; ``skipped`` is True when a guard
            short-circuited the fetch, ``reason`` explains why.
        """
        addr = str(address or "").strip()
        result = BackfillResult(address=addr, chain=chain)
        if not addr:
            result.skipped, result.reason = True, "empty_address"
            return result
        if not self.enabled:
            result.skipped, result.reason = True, "disabled"
            return result
        if chain != "solana":
            result.skipped, result.reason = True, "unsupported_chain"
            return result

        if not force and await self.is_backfilled(chain, addr):
            metrics_incr("address_backfill_skipped")
            result.skipped, result.reason = True, "already_indexed"
            return result
        if not force and self._recently_attempted(chain, addr):
            result.skipped, result.reason = True, "recent_attempt"
            return result
        self._record_attempt(chain, addr)

        sig_cap = int(max_signatures if max_signatures is not None else self.max_signatures)
        tx_cap = int(max_tx if max_tx is not None else self.max_tx)

        sig_infos, provider = await self._fetch_signatures(addr, sig_cap)
        if sig_infos is None:
            metrics_incr("address_backfill_failed")
            result.reason = "all_providers_failed"
            return result

        enhanced = False
        if not sig_infos:
            sig_infos = await self._fetch_helius_enhanced(addr, sig_cap)
            enhanced = bool(sig_infos)

        rows = await self._fetch_and_parse(sig_infos, tx_cap)
        written = 0
        client = self._resolve_client()
        if rows and client is not None:
            await asyncio.to_thread(ensure_address_activity_table, client)
            written = await asyncio.to_thread(write_address_activity, client, rows)

        final_provider = "helius-enhanced" if enhanced else provider
        await self._ensure_state_table()
        await self.mark_backfilled(
            chain,
            addr,
            status="complete",
            signatures_seen=len(sig_infos),
            rows_written=written,
            provider=final_provider,
        )
        metrics_incr("address_backfill_completed")
        result.provider = final_provider
        result.signatures = len(sig_infos)
        result.rows_written = written
        logger.info(
            "address_backfill_done",
            chain=chain,
            address=addr[:12],
            provider=final_provider,
            signatures=len(sig_infos),
            rows=written,
        )
        return result


__all__ = [
    "AddressBackfill",
    "BackfillResult",
    "HttpRpcTransport",
    "RpcTransport",
    "default_solana_providers",
]
