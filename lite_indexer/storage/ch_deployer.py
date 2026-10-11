"""ClickHouse deployer writes — EVM (RPC-resolved EOA) + Solana rows.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) so the
writer stays under the per-file size ratchet.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_DEPLOYER_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.ch_errors import (
    ClickHouseUnavailable,
    is_unavailable_error,
    require_client,
)
from lite_indexer.storage.ch_rows import _parse_ts, _resolve_deployer_eoas
from lite_indexer.storage.writer_api import WriterApi

logger = get_logger(__name__)


def _insert_deployer(
    client: Any,
    chain: str,
    rows: list[tuple[Any, ...]],
    log_event: str,
    fail_event: str,
) -> int:
    """Execute the deployer INSERT; fail CLOSED on an outage."""
    try:
        client.execute(
            f"INSERT INTO {CH_DEPLOYER_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
            "(deployer, deployer_eoa, token_address, token0, token1, pair_address, "
            "chain, block_number, block_timestamp, tx_hash, type, "
            "factory, ingested_at, source) VALUES",
            rows,
        )
        logger.info(log_event, chain=chain, rows=len(rows))
        metrics_incr("clickhouse_inserts")
        return len(rows)
    except Exception as exc:
        metrics_incr("clickhouse_errors")
        logger.error(fail_event, chain=chain, rows=len(rows), error=str(exc)[:150])
        if is_unavailable_error(exc):
            raise ClickHouseUnavailable(f"ClickHouse unreachable during deployer insert (chain={chain})") from exc
        return 0


def _deployer_row(chain: str, event: dict[str, Any], eoa_map: dict[str, str], now: datetime) -> tuple[Any, ...]:
    """Build one EVM deployer INSERT row.

    Correct mapping: deployer ← tx `from` (EOA); token_address ←
    token0; pair_address ← pair/pool address (kept separate so
    scanners can join on the pair without mislabeling).
    """
    version = event.get("version", "v2")
    tx_hash = event.get("transactionHash", "")
    token0 = event.get("token0", "") or ""
    token1 = event.get("token1", "") or ""
    deployer = eoa_map.get(tx_hash, "")
    # 2026-10-11: some launch events arrive without a block timestamp under the
    # camelCase key; accept snake_case too, and make a MISSING one visible —
    # silently stamping ``now`` produced rows whose block_timestamp was the
    # ingest time (data-quality bug, ~2 rows/cycle).
    ts_raw = event.get("blockTimestamp") or event.get("block_timestamp") or ""
    if not ts_raw:
        logger.warning("deployer_ts_missing", chain=chain, tx_hash=tx_hash[:20])
    return (
        deployer,
        deployer,
        token0,
        token0,
        token1,
        event.get("pair_address", "") or "",
        chain,
        event.get("blockNumber", 0),
        _parse_ts(ts_raw, now),
        tx_hash,
        f"{version}_pool",
        event.get("factory", ""),
        now,
        "lite_indexer",
    )


def _solana_deployer_row(chain: str, event: dict[str, Any], now: datetime) -> tuple[Any, ...]:
    """Build one Solana deployer INSERT row (EOA already resolved)."""
    deployer = event.get("deployer", "") or ""
    token_address = event.get("token_address", "") or event.get("token0", "") or ""
    return (
        deployer,
        deployer,  # deployer_eoa — already resolved by the strategy
        token_address,
        event.get("token0", "") or token_address,
        event.get("token1", "") or "",
        event.get("pair_address", "") or "",
        event.get("chain", chain),
        int(event.get("block_number", 0) or 0),
        _parse_ts(event.get("block_timestamp", "") or event.get("blockTimestamp", ""), now),
        event.get("tx_hash", "") or "",
        event.get("type", "solana_token"),
        event.get("factory", "") or "",
        now,
        "lite_indexer",
    )


class DeployerMixin:
    """``deployer_events`` writes for EVM and Solana strategies."""

    async def write_deployer_events(self: WriterApi, chain: str, events: list[dict[str, Any]]) -> int:
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

        client = require_client(self._connect(), chain)

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)
        self._ensure_deployer_columns(client)

        # Resolve real deployer EOA per unique tx hash (batched RPC calls).
        try:
            eoa_map = await _resolve_deployer_eoas(chain, events)
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.warning("deployer_eoa_resolution_failed", chain=chain, error=str(exc)[:100])
            eoa_map = {}

        now = datetime.now(UTC)
        rows = [_deployer_row(chain, e, eoa_map, now) for e in events]
        return _insert_deployer(client, chain, rows, "deployer_events_inserted", "deployer_events_insert_failed")

    async def write_solana_deployer_events(self: WriterApi, chain: str, events: list[dict[str, Any]]) -> int:
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

        client = require_client(self._connect(), chain)

        self._ensure_source_column(client, CH_DEPLOYER_TABLE)
        self._ensure_deployer_columns(client)

        now = datetime.now(UTC)
        rows = [_solana_deployer_row(chain, e, now) for e in events]
        return _insert_deployer(
            client,
            chain,
            rows,
            "solana_deployer_events_inserted",
            "solana_deployer_events_insert_failed",
        )
