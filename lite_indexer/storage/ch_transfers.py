"""ClickHouse transfer writes — idempotent-rewind dedup + insert.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) for the
per-file size ratchet. Facade config flags are read at call time.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import lite_indexer.storage.clickhouse as _facade
from lite_indexer.config import CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.ch_dedup_windows import (
    existing_keys_in_windows,
    iter_block_windows,
)
from lite_indexer.storage.ch_errors import (
    ClickHouseUnavailable,
    is_unavailable_error,
    require_client,
)
from lite_indexer.storage.ch_rows import (
    _coerce_int,
    _parse_ts,
    _serialize_raw_payload,
    _transfer_dedup_key,
)

logger = get_logger(__name__)


def _dedup_transfer_events(
    events: list[dict[str, Any]],
) -> dict[tuple[int, str, int], dict[str, Any]]:
    """In-batch dedup keyed on (block_number, tx_hash, log_index).

    Dedup key: (chain, block_number, tx_hash, log_index) — mirrored by
    the appended tx_log_key sorting column in
    migrations/018_evm_transfers_dedup_key_v2.sql (018 supersedes the
    invalid 017).
    """
    unique: dict[tuple[int, str, int], dict[str, Any]] = {}
    for event in events:
        key = (
            _coerce_int(event.get("blockNumber")),
            str(event.get("txHash") or event.get("txID") or ""),
            _coerce_int(event.get("logIndex")),
        )
        unique.setdefault(key, event)
    return unique


def _transfer_rows(
    chain: str,
    unique: dict[tuple[int, str, int], dict[str, Any]],
    existing: set[tuple[int, str, int]],
    now: datetime,
) -> tuple[list[tuple[Any, ...]], int]:
    """Build INSERT rows for the deduped events; returns ``(rows, skipped)``."""
    rows: list[tuple[Any, ...]] = []
    skipped = 0
    for key, event in unique.items():
        if key in existing:
            skipped += 1
            continue
        rows.append(
            (
                chain,
                event["blockNumber"],
                event["txHash"],
                event["from"],
                event["to"],
                event["tokenAddress"],
                event["value"],
                _parse_ts(event.get("blockTimestamp", ""), now),
                event["logIndex"],
                now,
                _serialize_raw_payload(event),
                _transfer_dedup_key(key[0], key[1], key[2]),
            )
        )
    return rows, skipped


def _insert_transfers(client: Any, chain: str, rows: list[tuple[Any, ...]]) -> int:
    """Execute the transfer INSERT; fail CLOSED on an outage."""
    try:
        client.execute(
            f"INSERT INTO {CH_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
            "(chain, block_number, tx_hash, from_address, to_address, "
            "token_address, value, block_timestamp, log_index, "
            "ingested_at, raw_payload, tx_log_key) VALUES",
            rows,
        )
        logger.info("clickhouse_inserted", chain=chain, rows=len(rows))
        metrics_incr("clickhouse_inserts")
        return len(rows)
    except Exception as exc:
        metrics_incr("clickhouse_errors")
        logger.error("clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
        if is_unavailable_error(exc):
            raise ClickHouseUnavailable(f"ClickHouse unreachable during transfer insert (chain={chain})") from exc
        return 0


class TransfersMixin:
    """``evm_transfers`` writes with idempotent-rewind dedup."""

    def _existing_transfer_keys(
        self,
        client: Any,
        chain: str,
        keys: list[tuple[int, str, int]],
    ) -> set[tuple[int, str, int]]:
        """Return the (block_number, tx_hash, log_index) keys already in CH.

        Makes a re-extracted range idempotent: the ReplacingMergeTree only
        collapses duplicates at merge time, so an explicit existence check
        is the immediate guard against a rewind compounding the row count.

        The range is WALKED in ``_DEDUP_MAX_BLOCK_SPAN``-sized windows (one
        query each, results unioned), never abandoned when the span is
        wide: the earlier bail-out returned an empty set above the cap,
        which reads as "nothing exists" and re-inserted the whole batch —
        disabling the guard on exactly the large rewind it exists for. A
        narrow batch still issues exactly ONE query, so the per-block hot
        path is unchanged.
        """
        if not keys:
            return set()
        lo = min(k[0] for k in keys)
        hi = max(k[0] for k in keys)
        existing, gaps = existing_keys_in_windows(client, chain, lo, hi)
        windows = sum(1 for _ in iter_block_windows(lo, hi))
        if windows > 1:
            metrics_incr("clickhouse_dedup_chunked")
            logger.warning("clickhouse_dedup_span_chunked", chain=chain, span=hi - lo, windows=windows)
        if gaps:
            metrics_incr("clickhouse_dedup_window_gaps", float(gaps))
            logger.warning("clickhouse_dedup_degraded", chain=chain, gap_windows=gaps, windows=windows)
        return existing

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert transfer events into ClickHouse. Returns rows inserted.

        Only the hot-path transfer columns are written: chain, block_number,
        tx_hash, from_address, to_address, token_address, value,
        block_timestamp, log_index, ingested_at.

        ``token_symbol``/``token_name``/``source`` are deliberately NOT part
        of the INSERT (columns stay with DEFAULT ''; token metadata lives in
        ``wallet_memory.token_meta_ref`` via ``write_wallet_features``).

        Args:
            chain: Chain name.
            events: Parsed transfer event dicts.

        Returns:
            Number of rows inserted (0 on failure/unavailable).
        """
        if not events:
            return 0

        # Fail CLOSED: an outage aborts the cycle, never returns 0 (0 was read
        # as "nothing to insert" and checkpointed as progress → the 2026-09-20
        # 80-min fleet-wide hole).
        client = require_client(self._connect(), chain)

        # Backward compat: ensure the legacy `source` column still exists.
        self._ensure_source_column(client, CH_TABLE)
        self._ensure_raw_payload_column(client, CH_TABLE)
        self._ensure_dedup_key_column(client, CH_TABLE)
        if _facade.CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        now = datetime.now(UTC)

        # Idempotent rewind: dedup in-batch + skip rows already durable
        # in CH (ReplacingMergeTree collapses only at merge time).
        unique = _dedup_transfer_events(events)
        existing = self._existing_transfer_keys(client, chain, list(unique))
        rows, skipped = _transfer_rows(chain, unique, existing, now)

        if skipped:
            metrics_incr("clickhouse_dedup_skipped", float(skipped))
            logger.info("clickhouse_dedup_skipped", chain=chain, skipped=skipped, kept=len(rows))

        if not rows:
            # All durable (post-rewind replay): falsy count == "handled", so the
            # range advances without falsely claiming a row was written.
            return 0

        return _insert_transfers(client, chain, rows)
