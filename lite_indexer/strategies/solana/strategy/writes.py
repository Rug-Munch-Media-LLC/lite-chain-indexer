"""Solana ClickHouse write path (SPL balance-change rows).

Split out of sync.py so the write contract (timestamp coercion, schema
mapping) owns one module. Composed into SolanaStrategy as a mixin.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.dedup import dedup_balance_events, existing_balance_keys
from lite_indexer.strategies.solana.timestamps import coerce_event_ts

from ._base import SolanaMixinBase

logger = get_logger(__name__)


class WritesMixin(SolanaMixinBase):
    """ClickHouse persistence for Solana strategy events."""

    def write_clickhouse(
        self,
        events: list[dict[str, Any]],
        ch_writer: Any,
    ) -> int:
        """Write Solana SPL balance changes to ClickHouse.

        Uses the solana_spl_balance_changes table schema:
          chain, slot, signature, mint, owner, token_account,
          amount_pre, amount_post, change, decimals, symbol,
          token_name, block_timestamp, source, ingested_at
        """
        if not events:
            return 0

        client = ch_writer._connect()
        if not client:
            logger.warning("solana_clickhouse_skipped")
            return 0

        # F1 wallet resolution: ensure the owner column exists (idempotent)
        # before inserting. Old deployments degrade gracefully — the DDL
        # helper reports success once the column is in place, historical
        # rows keep DEFAULT '' until backfilled.
        ensure_owner = getattr(ch_writer, "_ensure_solana_owner_column", None)
        if ensure_owner is not None:
            ensure_owner(client)

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        # Idempotency: collapse in-batch duplicates, then drop keys
        # already durable. Without this a re-run of the same slot window
        # double-inserted every balance-change row (this table has no
        # ReplacingMergeTree dedup key, unlike evm_transfers).
        unique = dedup_balance_events(events)
        already = existing_balance_keys(client, list(unique))
        skipped_in_batch = len(events) - len(unique)
        skipped_existing = 0

        for key, event in unique.items():
            if key in already:
                skipped_existing += 1
                continue
            raw_ts = event.get("blockTimestamp") or event.get("block_timestamp")
            ts = coerce_event_ts(raw_ts, now)

            rows.append(
                (
                    event.get("slot", 0),
                    ts,
                    event.get("signature", ""),
                    event.get("token_account", ""),
                    event.get("mint", ""),
                    event.get("owner", ""),
                    event.get("amount_pre", "0"),
                    event.get("amount_post", "0"),
                    event.get("change", "0"),
                    event.get("decimals", 0),
                    event.get("symbol", ""),
                    event.get("token_name", ""),
                    now,
                )
            )

        if skipped_in_batch or skipped_existing:
            logger.info(
                "solana_dedup_skipped",
                in_batch=skipped_in_batch,
                existing=skipped_existing,
                kept=len(rows),
            )

        if not rows:
            # Everything was already durable (post-rewind re-run).
            return len(unique)

        solana_table = "wallet_memory.solana_spl_balance_changes"

        try:
            client.execute(
                f"INSERT INTO {solana_table} "
                "(slot, block_timestamp, tx_hash, token_account, "
                "mint, owner, pre_amount, post_amount, delta, "
                "decimals, token_symbol, token_name, "
                "ingested_at) VALUES",
                rows,
            )
            logger.info("solana_clickhouse_inserted", rows=len(rows))
            return len(rows)
        except Exception as exc:
            logger.error("solana_clickhouse_insert_failed", rows=len(rows), error=str(exc)[:150])
            return 0
        finally:
            # 4b. Trace-level rows (SOL transfers, CPIs, account state
            # changes) collected from the same getTransaction calls. Written
            # best-effort after the SPL balance events; on failure the rows
            # are kept (bounded by _SOLANA_TRACE_ROW_CAP) and retried on the
            # next write cycle. Runs in a finally so trace rows are drained
            # even when the SPL insert fails (e.g. CH hiccup).
            self._drain_trace_rows(client)
            # 4c. Address-activity keystone rows (wallet_memory.address_activity)
            # buffered from the same getTransaction calls — drained alongside
            # trace rows so a CH hiccup on the SPL insert never strands them.
            self._drain_address_activity_rows(client)
