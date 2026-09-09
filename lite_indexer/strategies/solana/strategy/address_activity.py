"""Address-activity mixin — buffer/drain rows for the tracing keystone table.

Rows are expanded from the same getTransaction (jsonParsed) payloads that
already feed SPL balance changes, DEX swaps and traces — zero extra RPC
calls. Buffered on the strategy instance (``_address_activity_rows``) and
drained in ``write_clickhouse`` alongside trace rows.

Follow-up hook site (not wired here, scope-tight): the launchpad block
watcher (``SyncMixin.poll_launchpad_blocks``) also walks every transaction
of every new block; calling :meth:`_ingest_address_activity` there would
extend address_activity from "transactions that touch tracked mints" to
"every transaction on the chain" — the full free-Helius ambition. The
historical backfill task will consume the same pure parser.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.storage.address_activity import (
    ensure_address_activity_table,
    parse_address_activity,
    write_address_activity,
)

logger = get_logger(__name__)

# Upper bound on buffered address-activity rows awaiting a ClickHouse
# write. Rows are collected from the same getTransaction calls as SPL
# events (zero extra RPC cost) and drained in write_clickhouse; the cap
# bounds memory when CH is down between cycles (oldest rows drop first).
_ADDRESS_ACTIVITY_ROW_CAP = 20_000


class AddressActivityMixin:
    """Buffering and draining of address-activity rows (best-effort)."""

    def _ingest_address_activity(self, tx_data: dict[str, Any]) -> int:
        """Expand one parsed transaction and buffer its address rows.

        Best-effort and non-fatal: a parse failure loses only this
        transaction's address rows, never the SPL balance events.

        Args:
            tx_data: Parsed getTransaction (jsonParsed) result.

        Returns:
            Number of rows buffered (0 when the tx yields none).
        """
        rows = parse_address_activity(tx_data)
        if not rows:
            return 0
        self._address_activity_rows.extend(rows)
        if len(self._address_activity_rows) > _ADDRESS_ACTIVITY_ROW_CAP:
            self._address_activity_rows = self._address_activity_rows[-_ADDRESS_ACTIVITY_ROW_CAP:]
        return len(rows)

    def _drain_address_activity_rows(self, client: Any) -> None:
        """Write buffered address-activity rows to ClickHouse (best-effort).

        On failure the rows are kept (bounded by
        ``_ADDRESS_ACTIVITY_ROW_CAP``) and retried on the next write cycle.
        """
        rows = self._address_activity_rows
        self._address_activity_rows = []
        if not rows:
            return
        try:
            ensure_address_activity_table(client)
            written = write_address_activity(client, rows)
            logger.info("address_activity_clickhouse_inserted", rows=written)
            if not written:
                # write_address_activity swallows failures and returns 0 — requeue
                self._address_activity_rows = rows
        except Exception as exc:  # noqa: BLE001 — best-effort write
            logger.error(
                "address_activity_clickhouse_insert_failed",
                rows=len(rows),
                error=str(exc)[:150],
            )
            self._address_activity_rows = rows
