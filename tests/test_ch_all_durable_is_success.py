"""Regression: an all-durable rewind must not read as a write failure.

``write_transfers`` used to return 0 both when the INSERT failed and when every
event in the batch was already durable. The sync loop treats ``ch_inserted == 0``
as "ClickHouse write failed" and aborts the cycle without advancing the cursor,
so an idempotent replay re-fetched the same range forever. Observed on
arbitrum 2026-10-23: ``dedup_skipped kept=0`` immediately before each
``sync_aborted_ch_write_failed``, and no ``clickhouse_insert_failed`` line
anywhere — nothing had actually failed.

The contract pinned here: >0 means handled (inserted OR already durable),
0 means the INSERT failed.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.storage import ch_transfers as ct

EVENTS: list[dict[str, Any]] = [
    {"blockNumber": 7, "txHash": "0xaa", "logIndex": 0},
    {"blockNumber": 8, "txHash": "0xbb", "logIndex": 1},
]


class _Writer(ct.TransfersMixin):
    """TransfersMixin with the schema/connection siblings stubbed out."""

    def _connect(self) -> object:
        return object()

    def _ensure_source_column(self, *_: Any) -> None: ...

    def _ensure_raw_payload_column(self, *_: Any) -> None: ...

    def _ensure_dedup_key_column(self, *_: Any) -> None: ...

    def _existing_transfer_keys(self, *_: Any) -> set[tuple[int, str, int]]:
        return {(7, "0xaa", 0), (8, "0xbb", 1)}


def _writer(monkeypatch: Any, rows: list[Any], inserted: int) -> _Writer:
    unique = {key: event for key, event in zip(((7, "0xaa", 0), (8, "0xbb", 1)), EVENTS, strict=False)}
    monkeypatch.setattr(ct, "require_client", lambda client, chain: client)
    monkeypatch.setattr(ct, "_dedup_transfer_events", lambda events: unique)
    monkeypatch.setattr(ct, "_transfer_rows", lambda chain, u, existing, now: (rows, len(u) - len(rows)))
    monkeypatch.setattr(ct, "_insert_transfers", lambda client, chain, r: inserted)
    monkeypatch.setattr(ct._facade, "CH_TTL_ENABLED", False)
    return _Writer()


def test_all_durable_counts_as_handled(monkeypatch: Any) -> None:
    writer = _writer(monkeypatch, rows=[], inserted=0)
    assert writer.write_transfers("arbitrum", EVENTS) == len(EVENTS)


def test_insert_failure_still_reports_zero(monkeypatch: Any) -> None:
    writer = _writer(monkeypatch, rows=[("row",), ("row",)], inserted=0)
    assert writer.write_transfers("arbitrum", EVENTS) == 0


def test_rows_written_reports_inserted_count(monkeypatch: Any) -> None:
    writer = _writer(monkeypatch, rows=[("row",), ("row",)], inserted=2)
    assert writer.write_transfers("arbitrum", EVENTS) == 2
