"""
ClickHouse writer — inserts events via ClickHouse native protocol.

Handles lazy connections, column existence checks, and batch inserts
for transfer events, DEX swaps, and deployer events.

Deployer events resolve the real deployer EOA (the transaction `from`
address) via eth_getTransactionByHash before inserting — the PairCreated/
PoolCreated log itself only exposes the factory address, not the EOA that
submitted the transaction.

Column pruning (Improvement 3): old rows keep their summary columns
(from/to/token/value/block_ts for transfers, swap identity for DEX)
while heavy detail columns degrade to type defaults via column-level
TTL — cutting storage 30-50% while keeping every scanner query
answerable.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import (
    BALANCE_CACHE_ENABLED,
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_TABLE,
    CH_TTL_ENABLED,
    HOLDER_BALANCES_MV_ENABLED,
    TOKEN_META_ENABLED,
    WALLET_PROJECTION_ENABLED,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.pricing import compute_swap_usd, detect_whale, get_price
from lite_indexer.storage.balance_cache import (
    ensure_balance_cache_table,
    ensure_holder_balances_mv,
    update_holder_balances_bulk,
)
from lite_indexer.storage.projections import ensure_wallet_projection
from lite_indexer.storage.token_meta import ensure_token_meta_table, upsert_token_meta

logger = get_logger(__name__)

# Max concurrent eth_getTransactionByHash calls while resolving deployer
# EOAs (keeps RPC overhead low while batching unique tx hashes).
_DEPLOYER_RPC_CONCURRENCY = 8

# How many endpoints to try per tx hash before giving up on it. The
# previous implementation queried only the single best endpoint — when that
# node was down or rate-limited, every deployer event in the batch landed
# with an empty deployer_eoa. Trying the whole pool (bounded) recovers the
# rows that a transient best-endpoint failure would otherwise lose.
_DEPLOYER_RPC_MAX_ENDPOINTS = 3


async def _resolve_deployer_eoas(chain: str, events: list[dict[str, Any]]) -> dict[str, str]:
    """Resolve the deployer EOA (tx `from`) for each unique tx hash.

    Groups deployer events by unique ``transactionHash`` so each tx is
    queried via ``eth_getTransactionByHash``, then maps tx_hash → from
    address. Returns {} if resolution is unavailable.

    Endpoint resilience: instead of a single ``get_best_endpoint`` call, the
    pool is rotated so each tx hash is retried against up to
    ``_DEPLOYER_RPC_MAX_ENDPOINTS`` endpoints. A transient failure on the
    healthiest node no longer blanks the whole batch — the fallback nodes
    recover the rows. Failure feedback (``record_endpoint_failure``) flows
    back into the shared pool so future batches prefer healthier endpoints.

    Args:
        chain: Chain name (used to pick RPC endpoints).
        events: Deployer event dicts (each with a ``transactionHash``).

    Returns:
        Dict mapping tx_hash → lowercased `from` address (EOA).
    """
    tx_hashes = {e.get("transactionHash", "") for e in events if isinstance(e, dict) and e.get("transactionHash")}
    if not tx_hashes:
        return {}

    # Deferred imports avoid any import-cycle risk with the storage package.
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.rpc import RpcClient, get_best_endpoint, record_endpoint_failure

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.warning("deployer_eoa_no_endpoints", chain=chain)
        return {}

    rpc = RpcClient()
    semaphore = asyncio.Semaphore(_DEPLOYER_RPC_CONCURRENCY)

    async def _try_endpoint(endpoint: str, tx_hash: str) -> str | None:
        try:
            result = await rpc.call(endpoint, "eth_getTransactionByHash", [tx_hash])
            if result and isinstance(result.get("result"), dict):
                from_addr = result["result"].get("from")
                if from_addr:
                    return str(from_addr).lower()
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.debug("deployer_eoa_fetch_failed", tx=tx_hash[:16], error=str(exc)[:60])
        record_endpoint_failure(endpoint)
        return None

    async def _get_from(tx_hash: str) -> tuple[str, str]:
        # Rotate the pool starting from the best endpoint so healthy nodes
        # are preferred, but the batch survives a dead best node.
        start_idx = endpoints.index(get_best_endpoint(endpoints))
        rotated = endpoints[start_idx:] + endpoints[:start_idx]
        for endpoint in rotated[:_DEPLOYER_RPC_MAX_ENDPOINTS]:
            async with semaphore:
                from_addr = await _try_endpoint(endpoint, tx_hash)
            if from_addr:
                return tx_hash, from_addr
        return tx_hash, ""

    results = await asyncio.gather(*(_get_from(h) for h in tx_hashes), return_exceptions=True)

    mapping: dict[str, str] = {}
    for res in results:
        if isinstance(res, tuple) and res[1]:
            mapping[res[0]] = res[1]
    return mapping


def _serialize_raw_payload(event: dict[str, Any]) -> str:
    """Serialize the raw RPC dict carried on an event (best-effort).

    Events may carry the raw RPC payload under ``raw_payload`` (or ``raw``)
    as a dict, so it can be re-decoded later without a re-fetch (see
    migrations/010_raw_payload_parent_hash.sql). Absent or unserializable
    payloads degrade to ``''`` — the column default — so parsers that do
    not emit the raw payload never break the writer.

    Args:
        event: Parsed event dict.

    Returns:
        Compact JSON string, or ``''`` when no payload is in hand.
    """
    raw = event.get("raw_payload") or event.get("raw")
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw
    try:
        return json.dumps(raw, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        logger.debug("raw_payload_serialize_skip")
        return ""


from lite_indexer.storage._schema import SchemaMixin  # noqa: E402


class ClickHouseWriter(SchemaMixin):
    """Insert events into ClickHouse tables.

    Connects lazily on first write. Handles column existence checks.
    """

    def __init__(self) -> None:
        # Legacy/shared client slot: set by tests (fake injection) and by
        # the main thread on first connect. Production worker threads use
        # ``_local`` below so no two threads share one connection.
        self._client: Any = None
        # Thread-local ClickHouse client — clickhouse_driver connections are
        # NOT safe for concurrent use, so each write-pool worker keeps its
        # own (bounded by CH_WRITE_THREADS).
        self._local = threading.local()
        # Wallet-access DDL is applied once per process, only after the
        # corresponding ensure_*() call reports success (so a table that
        # appears later is picked up on the next cycle).
        self._wallet_projection_ready = False
        self._token_meta_ready = False
        self._balance_cache_ready = False
        self._holder_mv_ready = False
        self._trace_table_ready = False
        # Schema probe cache for dex_events amount columns (None = unknown).
        # See _dex_amounts_numeric / write_dex_events (2026-08-19 incident).
        self._dex_numeric_amounts: bool | None = None

    def _connect(self) -> Any | None:
        """Lazy-init a ClickHouse native protocol client for this thread.

        A shared ``self._client`` (test-injected fake, or the main thread's
        connection) wins when present; otherwise each write-pool worker
        thread gets its own client via thread-local storage. Returns None
        when ClickHouse is unreachable.
        """
        if self._client is not None:
            return self._client
        local_client = getattr(self._local, "client", None)
        if local_client is not None:
            return local_client

        ch_host = os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
        ch_port_raw = os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"
        ch_port = int(ch_port_raw)
        ch_user = os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default")
        ch_pass = os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
        ch_db = os.getenv("CH_DATABASE", "wallet_memory")

        try:
            from clickhouse_driver import Client as CHClient

            client = CHClient(
                host=ch_host,
                port=ch_port,
                user=ch_user,
                password=ch_pass,
                database=ch_db,
                connect_timeout=5,
                send_receive_timeout=30,
            )
            client.execute("SELECT 1")
            self._local.client = client
            logger.info("clickhouse_connected", host=ch_host, database=ch_db)
            return client
        except Exception as exc:
            logger.warning("clickhouse_unavailable", error=str(exc)[:100])
            return None

    def client(self) -> Any | None:
        """Return the underlying ClickHouse client (lazily connected).

        Exposes the raw client for modules that need direct SQL access
        (reconciliation, reorg staging). Returns None when ClickHouse is
        unavailable.

        Returns:
            ClickHouse client, or None when unavailable.
        """
        return self._connect()

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert transfer events into ClickHouse. Returns rows inserted.

        Only the hot-path transfer columns are written: chain, block_number,
        tx_hash, from_address, to_address, token_address, value,
        block_timestamp, log_index, ingested_at.

        ``token_symbol``/``token_name`` and ``source`` are deliberately NOT
        part of the INSERT — the columns stay in the schema (with DEFAULT '')
        for backward compatibility, but every new row stores empty strings.
        Token metadata lives in ``wallet_memory.token_meta_ref`` (one row per
        token, populated once per new token per batch in
        ``write_wallet_features``). ``source`` was always ``'lite_indexer'``
        — constant dead weight across 278M rows.

        Args:
            chain: Chain name.
            events: Parsed transfer event dicts.

        Returns:
            Number of rows inserted (0 on failure/unavailable).
        """
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("clickhouse_skipped", chain=chain)
            return 0

        # Backward compatibility: older deployments may lack the `source`
        # column. The INSERT no longer names it (so it defaults to ''), but
        # ensure the column exists for any scanner that still reads it.
        self._ensure_source_column(client, CH_TABLE)
        self._ensure_raw_payload_column(client, CH_TABLE)
        if CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        for event in events:
            ts_str = event.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            rows.append(
                (
                    chain,
                    event["blockNumber"],
                    event["txHash"],
                    event["from"],
                    event["to"],
                    event["tokenAddress"],
                    event["value"],
                    ts,
                    event["logIndex"],
                    now,
                    _serialize_raw_payload(event),
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_TABLE} "
                "(chain, block_number, tx_hash, from_address, to_address, "
                "token_address, value, block_timestamp, log_index, "
                "ingested_at, raw_payload) VALUES",
                rows,
            )
            logger.info("clickhouse_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    def _dex_amounts_numeric(self, client: Any) -> bool:
        """Return True when dex_events amount columns are Float (not String).

        Schema-adaptive probe for the 2026-08-19 dex_events incident: the
        canonical schema (bootstrap.py) stores raw uint256 swap amounts as
        String, but an ad-hoc ALTER (no repo source) converted the six
        amount columns to Nullable(Float64) on 08-18 ~21:39. Every string
        INSERT since failed with Code 53 ("argument is not a float") and
        dex_events silently stalled until this probe let the writer match
        the live schema. Migration 013 restores the canonical String
        columns; this probe makes the writer correct under either schema.

        Result is cached per process; the schema never changes mid-run
        (migration 013 is applied while the indexer is stopped).
        """
        if self._dex_numeric_amounts is not None:
            return self._dex_numeric_amounts
        try:
            types = self._column_types(client, CH_DEX_TABLE)
            amount_type = types.get("amount0_in", "")
            self._dex_numeric_amounts = "Float" in amount_type or "Decimal" in amount_type
            logger.info(
                "dex_amount_schema_probed",
                column_type=amount_type or "unknown",
                numeric=self._dex_numeric_amounts,
            )
        except Exception:  # noqa: BLE001 — probe failure falls back to canonical
            self._dex_numeric_amounts = False
        return self._dex_numeric_amounts

    def write_dex_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert DEX swap events into ClickHouse. Returns rows inserted.

        Whale detection + USD normalization happen here, at ingestion time:

          - ``whale`` is computed from the cached token_prices feed (USD
            notional >= ``WHALE_THRESHOLD_USD``) with the per-chain raw
            amount thresholds as fallback (see
            :func:`lite_indexer.pricing.detect_whale`).
          - ``price_usd`` is the current USD price of token0 (best-effort,
            0 when unpriced).
          - ``volume_usd`` is the estimated USD notional of the swap
            (max leg, best-effort, 0 when unpriced).
        """
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("dex_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEX_TABLE)
        self._ensure_dex_usd_columns(client)
        self._ensure_raw_payload_column(client, CH_DEX_TABLE)
        if CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        # Match the live amount-column types (String canonical vs the
        # 08-18 ad-hoc Float64 ALTER). Sending strings into Float64
        # columns is what stalled dex_events on 08-19 (Code 53).
        numeric_amounts = self._dex_amounts_numeric(client)

        def _amount(value: Any) -> Any:
            if numeric_amounts:
                if value in (None, ""):
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None
            return "" if value is None else str(value)

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        for event in events:
            ts_str = event.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            version = event.get("dex_type", "v2")
            amt0 = event.get("amount0In", "0") or "0"
            amt1 = event.get("amount1In", "0") or "0"

            whale = detect_whale(event)
            price_usd = get_price(chain, str(event.get("token0", ""))) or 0.0
            volume_usd = compute_swap_usd(event)

            rows.append(
                (
                    "swap",
                    chain,
                    event["blockNumber"],
                    ts,
                    event["transactionHash"],
                    event["logIndex"],
                    event.get("pairAddress", ""),
                    event.get("dex", ""),
                    version,
                    event.get("token0", ""),
                    event.get("token1", ""),
                    event.get("sender", ""),
                    event.get("recipient", ""),
                    _amount(event.get("amount0In", "0")),
                    _amount(event.get("amount1In", "0")),
                    _amount(event.get("amount0Out", "0")),
                    _amount(event.get("amount1Out", "0")),
                    _amount(amt0),
                    _amount(amt1),
                    whale,
                    price_usd,
                    volume_usd,
                    now,
                    event.get("source", "lite_indexer"),
                    _serialize_raw_payload(event),
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEX_TABLE} "
                "(type, chain, block_number, block_timestamp, tx_hash, log_index, "
                "contract_address, dex, version, token0, token1, sender, recipient, "
                "amount0_in, amount1_in, amount0_out, amount1_out, "
                "amount0, amount1, whale, price_usd, volume_usd, ingested_at, source, "
                "raw_payload) VALUES",
                rows,
            )
            logger.info("dex_clickhouse_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("dex_clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    async def write_deployer_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert deployer events into ClickHouse. Returns rows inserted.

        F0 data-quality fix — resolves the real deployer EOA via
        ``eth_getTransactionByHash`` (batched per unique tx hash) and maps:

          deployer       ← tx `from` (the EOA)      [was: factory]
          deployer_eoa   ← tx `from` (the EOA)
          token_address  ← token0 (first token)
          pair_address   ← pair/pool address        [restored column]
          token0         ← token0
          token1         ← token1
          factory        ← factory

        If EOA resolution fails the deployer fields stay empty rather than
        being mislabeled with the factory address.
        """
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("deployer_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)
        self._ensure_deployer_columns(client)

        # Resolve real deployer EOA per unique tx hash (batched RPC calls).
        try:
            eoa_map = await _resolve_deployer_eoas(chain, events)
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.warning("deployer_eoa_resolution_failed", chain=chain, error=str(exc)[:100])
            eoa_map = {}

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        for e in events:
            ts_str = e.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            version = e.get("version", "v2")
            pool_type = f"{version}_pool"
            factory = e.get("factory", "")

            # Correct mapping: deployer ← tx `from` (EOA); token_address ←
            # token0; pair_address ← pair/pool address (kept separate so
            # scanners can join on the pair without mislabeling).
            tx_hash = e.get("transactionHash", "")
            deployer = eoa_map.get(tx_hash, "")
            token0 = e.get("token0", "") or ""
            token1 = e.get("token1", "") or ""
            token_address = token0
            pair_address = e.get("pair_address", "") or ""

            rows.append(
                (
                    deployer,
                    deployer,
                    token_address,
                    token0,
                    token1,
                    pair_address,
                    chain,
                    e.get("blockNumber", 0),
                    ts,
                    tx_hash,
                    pool_type,
                    factory,
                    now,
                    "lite_indexer",
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEPLOYER_TABLE} "
                "(deployer, deployer_eoa, token_address, token0, token1, pair_address, "
                "chain, block_number, block_timestamp, tx_hash, type, "
                "factory, ingested_at, source) VALUES",
                rows,
            )
            logger.info("deployer_events_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    async def write_solana_deployer_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert Solana deployer events into ClickHouse. Returns rows inserted.

        Solana deployer events are captured structurally by the Solana
        strategy: the fee payer of the mint's creation transaction is the real
        creator EOA. The strategy already resolved ``deployer`` /
        ``deployer_eoa`` (no ``eth_getTransactionByHash`` round-trip needed),
        so this method inserts the rows as-is.

        Args:
            chain: Chain name (``solana``).
            events: Deployer event dicts (see SolanaStrategy._capture_deployer_event).

        Returns:
            Number of rows inserted (0 on failure/unavailable).
        """
        if not events:
            return 0

        client = self._connect()
        if not client:
            logger.warning("deployer_clickhouse_skipped", chain=chain)
            return 0

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)
        self._ensure_deployer_columns(client)

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        for e in events:
            ts_str = e.get("block_timestamp", "") or e.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            deployer = e.get("deployer", "") or ""
            token_address = e.get("token_address", "") or e.get("token0", "") or ""
            rows.append(
                (
                    deployer,
                    deployer,  # deployer_eoa — already resolved by the strategy
                    token_address,
                    e.get("token0", "") or token_address,
                    e.get("token1", "") or "",
                    e.get("pair_address", "") or "",
                    e.get("chain", chain),
                    int(e.get("block_number", 0) or 0),
                    ts,
                    e.get("tx_hash", "") or "",
                    e.get("type", "solana_token"),
                    e.get("factory", "") or "",
                    now,
                    "lite_indexer",
                )
            )

        try:
            client.execute(
                f"INSERT INTO {CH_DEPLOYER_TABLE} "
                "(deployer, deployer_eoa, token_address, token0, token1, pair_address, "
                "chain, block_number, block_timestamp, tx_hash, type, "
                "factory, ingested_at, source) VALUES",
                rows,
            )
            logger.info("solana_deployer_events_inserted", chain=chain, rows=len(rows))
            metrics_incr("clickhouse_inserts")
            return len(rows)
        except Exception as exc:
            metrics_incr("clickhouse_errors")
            logger.error("solana_deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
            return 0

    # ── Internal-Transfer Traces (wallet-memory blind spot) ─────────────
    # Internal ETH flows (WETH unwraps, bridge deposits, fee flows,
    # SELFDESTRUCT payouts) never appear as ERC-20 Transfer logs. Rows are
    # collected from free trace_* RPCs by lite_indexer.traces.TraceCollector
    # and inserted here. Best-effort + gated by TRACE_ENABLED: a trace
    # insert failure never blocks the main transfer sync (the table is
    # enrichment, not the durable transfer record).

    async def write_internal_transfers(self, chain: str, rows: list[dict[str, Any]]) -> int:
        """Insert internal-transfer rows into ClickHouse.

        Delegates to :func:`lite_indexer.storage.trace_store.write_internal_transfers`,
        ensuring the table exists first. Returns rows inserted (0 when
        ClickHouse is unavailable or the insert fails).

        Args:
            chain: Chain name.
            rows: Standardized internal-transfer row dicts from
                ``TraceCollector``.

        Returns:
            Number of rows inserted (0 on failure/unavailable).
        """
        if not rows:
            return 0
        client = self._connect()
        if not client:
            logger.warning("trace_clickhouse_skipped", chain=chain)
            return 0

        from lite_indexer.storage.trace_store import (
            ensure_internal_transfers_table,
        )
        from lite_indexer.storage.trace_store import (
            write_internal_transfers as _write,
        )

        if not self._trace_table_ready and await ensure_internal_transfers_table(client):
            self._trace_table_ready = True
        if not self._trace_table_ready:
            return 0
        return _write(client, chain, rows)

    # ── Wallet Access Path (competitive features) ──────────────────────
    # Address-first projections, token metadata reference table, and the
    # per-token incremental balance cache. Each is gated by its own config
    # flag and independently usable; every DDL is idempotent and non-fatal.
    # Called from the indexer's async sync loop right after transfer
    # writes succeed, so the caches never diverge from evm_transfers.

    async def write_wallet_features(self, chain: str, events: list[dict[str, Any]]) -> dict[str, int]:
        """Apply wallet-access optimizations for a batch of transfers.

        Runs only when the corresponding config flag is enabled:

        1. ``WALLET_PROJECTION_ENABLED`` — address-first projections on
           evm_transfers (electrs key-as-record pattern).
        2. Token metadata reference table — always enabled
           (``TOKEN_META_ENABLED``), upserts one row per unique token into
           ``wallet_memory.token_meta_ref`` (cryo reference table).
        3. ``BALANCE_CACHE_ENABLED`` — incrementally update holder balances
           (electrs lazy cache); deltas are accumulated per holder so each
           holder gets at most one read-modify-write per batch.
        4. ``HOLDER_BALANCES_MV_ENABLED`` — auto-maintained SummingMergeTree
           materialized view over evm_transfers.

        Best-effort: when ClickHouse is unreachable or a DDL fails, the
        method degrades gracefully and returns whatever stats were
        collected (callers surface them in the sync summary).

        Args:
            chain: Chain name.
            events: Parsed transfer event dicts (must match the events
                already inserted into evm_transfers).

        Returns:
            Stats dict, e.g. ``{"token_meta_upserted": 3,
            "holder_balance_updates": 5}``. Empty when ClickHouse is down.
        """
        client = self._connect()
        if not client:
            return {}

        stats: dict[str, int] = {}

        # 1. Address-first projections (once per process, on success).
        if WALLET_PROJECTION_ENABLED and not self._wallet_projection_ready and await ensure_wallet_projection(client):
            self._wallet_projection_ready = True
            stats["wallet_projection_ready"] = 1

        # 2. Token metadata reference table — once per unique token per
        # batch (ReplacingMergeTree makes re-upserts across batches a
        # harmless no-op, so no existence check is needed).
        if TOKEN_META_ENABLED:
            if not self._token_meta_ready and await ensure_token_meta_table(client):
                self._token_meta_ready = True
            seen_tokens: dict[str, dict[str, Any]] = {}
            for event in events:
                token_address = event.get("tokenAddress") or ""
                if token_address and token_address not in seen_tokens:
                    seen_tokens[token_address] = {
                        "symbol": event.get("tokenSymbol"),
                        "name": event.get("tokenName"),
                        "block": event.get("blockNumber") or 0,
                    }
            for token_address, meta in seen_tokens.items():
                await upsert_token_meta(
                    client,
                    chain,
                    token_address,
                    meta["symbol"],
                    meta["name"],
                    first_seen_block=meta["block"],
                )
            if seen_tokens:
                stats["token_meta_upserted"] = len(seen_tokens)

        # 3. Manual incremental balance cache — accumulate deltas per
        #    holder so each (chain, token, holder) is written at most once.
        #    2026-09-02: applied in ONE bulk read + ONE bulk insert (the
        #    old per-holder read-modify-write needed 2 round-trips per
        #    holder — 100K+ holders in a catch-up batch deadlocked every
        #    EVM sync cycle for 15+ min, zero sync_completes).
        if BALANCE_CACHE_ENABLED:
            if not self._balance_cache_ready and await ensure_balance_cache_table(client):
                self._balance_cache_ready = True
            deltas: dict[tuple[str, str, str], int] = {}
            for event in events:
                try:
                    amount = int(event.get("value") or "0")
                except (ValueError, TypeError):
                    continue
                token_address = event.get("tokenAddress") or ""
                to_address = event.get("to") or ""
                from_address = event.get("from") or ""
                if to_address:
                    key = (chain, token_address, to_address)
                    deltas[key] = deltas.get(key, 0) + amount
                if from_address:
                    key = (chain, token_address, from_address)
                    deltas[key] = deltas.get(key, 0) - amount
            balance_updates = await update_holder_balances_bulk(client, deltas)
            if balance_updates:
                stats["holder_balance_updates"] = balance_updates

        # 4. Auto-maintained materialized view (once per process).
        if HOLDER_BALANCES_MV_ENABLED and not self._holder_mv_ready and await ensure_holder_balances_mv(client):
            self._holder_mv_ready = True
            stats["holder_balances_mv_ready"] = 1

        return stats
