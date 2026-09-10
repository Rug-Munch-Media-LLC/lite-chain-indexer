"""Cross-chain address-activity mixin — buffer/drain for EVM + TRON.

Mirrors the Solana ``AddressActivityMixin``
(``strategies/solana/strategy/address_activity.py``): rows are expanded
from the same event payloads the strategy already fetched (zero extra RPC
calls), buffered on the strategy instance, and drained in
``write_clickhouse``.

The Solana mixin works from full ``getTransaction`` (jsonParsed) payloads;
the EVM/TRON pipeline is log-based, so each strategy supplies its parser
by overriding :meth:`_parse_address_activity_event`. The parser signature
is identical for both chains, so the buffering/draining logic is shared.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.storage.address_activity import (
    ensure_address_activity_chain_column,
    ensure_address_activity_table,
    write_address_activity,
)

logger = get_logger(__name__)

# Upper bound on buffered address-activity rows awaiting a ClickHouse
# write (same cap as the Solana mixin). Bounds memory when CH is down
# between cycles — oldest rows drop first.
_ADDRESS_ACTIVITY_ROW_CAP = 20_000


class CrossChainAddressActivityMixin:
    """Buffering and draining of address-activity rows (best-effort).

    Concrete strategies must call :meth:`_init_address_activity` in their
    ``__init__`` and override :meth:`_parse_address_activity_event` with a
    chain-specific parser (EVM or TRON).
    """

    def _init_address_activity(self) -> None:
        """Initialise the per-instance address-activity row buffer."""
        self._address_activity_rows: list[dict[str, Any]] = []

    def _parse_address_activity_event(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Expand one event into address-activity rows (chain-specific).

        Subclasses override this with ``parse_evm_address_activity`` or
        ``parse_tron_address_activity``.
        """
        return []

    def _ingest_address_activity_events(self, events: list[dict[str, Any]]) -> int:
        """Expand a batch of events and buffer their address rows.

        Best-effort and non-fatal: a parse failure loses only that event's
        address rows, never the transfer/dex write.

        Args:
            events: Parsed event dicts (transfers, swaps, or raw logs).

        Returns:
            Number of rows buffered.
        """
        buffered = 0
        for event in events or []:
            rows = self._parse_address_activity_event(event)
            if rows:
                self._address_activity_rows.extend(rows)
                buffered += len(rows)
        if len(self._address_activity_rows) > _ADDRESS_ACTIVITY_ROW_CAP:
            self._address_activity_rows = self._address_activity_rows[-_ADDRESS_ACTIVITY_ROW_CAP:]
        return buffered

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
            ensure_address_activity_chain_column(client)
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


__all__ = [
    "CrossChainAddressActivityMixin",
]
