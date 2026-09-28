"""Tests for idempotent transfer writes (reconciliation-rewind dedup).

A re-extracted range must not append duplicate rows. The writer dedups
within the batch and skips (chain, block_number, tx_hash, log_index) keys
already present in ClickHouse. Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from lite_indexer.storage import clickhouse as ch
from lite_indexer.storage.ch_dedup_windows import _DEDUP_MAX_ROWS, iter_block_windows
from lite_indexer.storage.ch_rows import _DEDUP_MAX_BLOCK_SPAN
from lite_indexer.strategies.solana.dedup import (
    MAX_DEDUP_SLOT_SPAN,
    existing_balance_keys,
)

#: A span far past the one-window width — the rewind shape the old
#: bail-out mishandled (it returned an empty set, re-inserting everything).
WIDE_SPAN = _DEDUP_MAX_BLOCK_SPAN * 3 + 7


class FakeClient:
    """Minimal ClickHouse client: records INSERTs, answers the dedup SELECT.

    ``existing_by_window`` maps a window's ``(lo, hi)`` to the rows that
    window should report, so a test can prove each window was asked
    separately and that an out-of-window row is NOT seen.
    """

    def __init__(
        self,
        existing: list[tuple[Any, ...]] | None = None,
        existing_by_window: dict[tuple[int, int], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self.existing = existing or []
        self.existing_by_window = existing_by_window
        self.inserts: list[tuple[str, Any]] = []
        self.dedup_queries: list[dict[str, Any]] = []

    def execute(self, query: str, params: Any = None) -> Any:
        if query.strip().upper().startswith("INSERT"):
            self.inserts.append((query, params))
            return None
        if "SELECT block_number, tx_hash, log_index" in query:
            self.dedup_queries.append(params)
            if self.existing_by_window is not None:
                lo, hi = int(params["lo"]), int(params["hi"])
                return self.existing_by_window.get((lo, hi), [])
            return self.existing
        return []


def _event(block: int, tx: str, idx: int) -> dict[str, Any]:
    return {
        "blockNumber": block,
        "txHash": tx,
        "logIndex": idx,
        "from": "TFrom",
        "to": "TTo",
        "tokenAddress": "TToken",
        "value": "1000",
        "blockTimestamp": "2023-11-14T22:13:20Z",
    }


def _writer(monkeypatch, existing=None, existing_by_window=None) -> tuple[ch.ClickHouseWriter, FakeClient]:
    monkeypatch.setattr(ch, "CH_TTL_ENABLED", False)
    writer = ch.ClickHouseWriter()
    client = FakeClient(existing=existing, existing_by_window=existing_by_window)
    writer._client = client
    return writer, client


def test_write_transfers_skips_rows_already_in_clickhouse(monkeypatch) -> None:
    writer, client = _writer(monkeypatch, existing=[(100, "0xaa", 0)])
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xbb", 1)])

    assert written == 1
    assert len(client.inserts) == 1
    rows = client.inserts[0][1]
    assert len(rows) == 1
    assert rows[0][1] == 100  # block_number
    assert rows[0][2] == "0xbb"  # tx_hash
    assert rows[0][8] == 1  # log_index
    assert rows[0][11] == "100:0xbb:1"  # tx_log_key (storage dedup key)


def test_tx_log_key_matches_app_dedup_key(monkeypatch) -> None:
    """The storage key must equal the app dedup key (block, tx_hash, log)."""
    writer, client = _writer(monkeypatch)
    writer.write_transfers("tron", [_event(100, "0xaa", 0)])

    rows = client.inserts[0][1]
    assert rows[0][11] == "100:0xaa:0"
    # Ordering contract: block:tx_hash:log_index — same as _existing_transfer_keys.
    assert rows[0][11].split(":") == ["100", "0xaa", "0"]


def test_tx_log_key_distinguishes_same_log_index_different_tx(monkeypatch) -> None:
    """Same (block, log_index) but different tx_hash => distinct dedup keys."""
    writer, client = _writer(monkeypatch)
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xbb", 0)])

    assert written == 2
    keys = {r[11] for r in client.inserts[0][1]}
    assert keys == {"100:0xaa:0", "100:0xbb:0"}


def test_migration_dedup_key_contract() -> None:
    """Schema contract: the dedup migration appends tx_log_key to the sort key.

    Guards against a regression to the invalid form (appending the EXISTING
    tx_hash column), which ClickHouse 26.7 rejects with Code 36. Located by
    pattern, not number, so the migration set can be renumbered freely.
    """
    from pathlib import Path

    mig = Path(__file__).resolve().parents[1] / "migrations"
    v2 = sorted(mig.glob("*_evm_transfers_dedup_key_v2.sql"))
    assert v2, "dedup_key_v2 migration is missing"
    text = v2[0].read_text()
    assert "MODIFY ORDER BY (chain, block_number, log_index, tx_log_key)" in text
    # CH 26.7 accepts a new key column only when it is ADDed in the same ALTER.
    # The runtime fallback may pre-create tx_log_key, so the migration MUST
    # DROP-then-ADD (a bare `ADD COLUMN IF NOT EXISTS` leaves it existing and
    # the MODIFY ORDER BY then fails with Code 36 — the original failure mode).
    assert "DROP COLUMN IF EXISTS tx_log_key" in text
    assert "ADD COLUMN tx_log_key String" in text
    old = sorted(mig.glob("*_evm_transfers_dedup_key.sql"))
    old = [p for p in old if "v2" not in p.name]
    assert old, "the superseded dedup_key migration should be kept for history"
    old_text = old[0].read_text()
    assert "INVALID" in old_text and "SUPERSEDED" in old_text and "DO NOT APPLY" in old_text


def test_write_transfers_dedups_identical_events_in_batch(monkeypatch) -> None:
    writer, client = _writer(monkeypatch)
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xaa", 0)])

    assert written == 1
    rows = client.inserts[0][1]
    assert len(rows) == 1


def test_write_transfers_all_duplicates_writes_nothing(monkeypatch) -> None:
    writer, client = _writer(monkeypatch, existing=[(100, "0xaa", 0)])
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0)])

    assert written == 0
    assert client.inserts == []


# ── Wide-rewind windowing (the >50k-block span bug) ──────────────────


def test_iter_block_windows_single_window_for_narrow_batch() -> None:
    assert list(iter_block_windows(100, 900)) == [(100, 900)]
    assert list(iter_block_windows(7, 7)) == [(7, 7)]
    # Exactly the width is still ONE window, not two.
    assert list(iter_block_windows(0, _DEDUP_MAX_BLOCK_SPAN)) == [(0, _DEDUP_MAX_BLOCK_SPAN)]


def _logged_events(caplog) -> list[dict[str, Any]]:
    """Decoded log payloads — the JSON logger puts ``event`` in ``msg``."""
    return [json.loads(r.msg) for r in caplog.records if r.msg.startswith("{")]


def _events_named(caplog, name: str) -> list[dict[str, Any]]:
    return [e for e in _logged_events(caplog) if e.get("event") == name]


def test_iter_block_windows_covers_wide_range_without_gaps_or_overlap() -> None:
    windows = list(iter_block_windows(0, WIDE_SPAN))
    pairs = list(zip(windows, windows[1:], strict=False))

    assert len(windows) == 4
    assert windows[0][0] == 0
    assert windows[-1][1] == WIDE_SPAN  # hi always included
    for (_, prev_hi), (next_lo, _) in pairs:
        assert next_lo == prev_hi + 1  # contiguous: no block is skipped
    for win_lo, win_hi in windows:
        assert win_hi - win_lo <= _DEDUP_MAX_BLOCK_SPAN


def test_narrow_batch_issues_exactly_one_dedup_query(monkeypatch) -> None:
    """Hot path regression guard: this write runs on EVERY indexed block."""
    writer, client = _writer(monkeypatch)
    writer.write_transfers("base", [_event(100, "0xaa", 0), _event(200, "0xbb", 1)])

    assert len(client.dedup_queries) == 1
    assert client.dedup_queries[0]["lo"] == 100
    assert client.dedup_queries[0]["hi"] == 200


def test_wide_batch_issues_one_query_per_window(monkeypatch) -> None:
    writer, client = _writer(monkeypatch)
    writer.write_transfers("base", [_event(0, "0xaa", 0), _event(WIDE_SPAN, "0xbb", 1)])

    assert len(client.dedup_queries) == 4
    assert [q["lo"] for q in client.dedup_queries] == [0, 50_001, 100_002, 150_003]
    assert client.dedup_queries[-1]["hi"] == WIDE_SPAN


def test_wide_batch_still_skips_rows_already_durable(monkeypatch) -> None:
    """The regression itself: a >50k-block rewind must NOT re-insert.

    The durable row sits in the FIRST window; the old bail-out returned an
    empty set for the whole batch and re-inserted it verbatim.
    """
    windows = list(iter_block_windows(10, WIDE_SPAN))
    writer, client = _writer(
        monkeypatch,
        existing_by_window={windows[0]: [(10, "0xaa", 0)]},
    )
    written = writer.write_transfers("base", [_event(10, "0xaa", 0), _event(WIDE_SPAN, "0xbb", 1)])

    assert written == 1
    assert len(client.inserts) == 1
    rows = client.inserts[0][1]
    assert [r[2] for r in rows] == ["0xbb"]  # 0xaa was durable and skipped


def test_wide_batch_does_not_read_beyond_its_windows(monkeypatch) -> None:
    """A durable row OUTSIDE the walked windows must not suppress a write."""
    writer, client = _writer(monkeypatch, existing=[(10, "0xaa", 0), (WIDE_SPAN + 500, "0xzz", 0)])
    written = writer.write_transfers("base", [_event(10, "0xaa", 0), _event(WIDE_SPAN, "0xbb", 1)])

    assert written == 1
    assert [r[2] for r in client.inserts[0][1]] == ["0xbb"]


def test_capped_window_is_logged_and_counted_not_silently_trusted(monkeypatch, caplog) -> None:
    """A window that returns the cap has a PARTIAL answer — say so loudly."""
    writer, client = _writer(monkeypatch, existing=[(1, "0xaa", 0)] * _DEDUP_MAX_ROWS)
    with caplog.at_level(logging.WARNING):
        written = writer.write_transfers("base", [_event(1, "0xaa", 0)])

    assert _events_named(caplog, "clickhouse_dedup_window_capped")
    assert _events_named(caplog, "clickhouse_dedup_degraded")
    # Skipped count comes from what WAS returned; the write still proceeds
    # (availability beats dedup) and the gap is reported, not hidden.
    assert written == 0


def test_uncapped_window_emits_no_degradation_log(monkeypatch, caplog) -> None:
    writer, client = _writer(monkeypatch, existing=[(1, "0xaa", 0)])
    with caplog.at_level(logging.WARNING):
        writer.write_transfers("base", [_event(1, "0xaa", 0)])

    assert not _events_named(caplog, "clickhouse_dedup_window_capped")
    assert not _events_named(caplog, "clickhouse_dedup_degraded")
    assert not _events_named(caplog, "clickhouse_dedup_span_chunked")


def test_wide_batch_logs_that_it_took_the_chunked_path(monkeypatch, caplog) -> None:
    writer, client = _writer(monkeypatch)
    with caplog.at_level(logging.WARNING):
        writer.write_transfers("base", [_event(0, "0xaa", 0), _event(WIDE_SPAN, "0xbb", 1)])

    chunked = _events_named(caplog, "clickhouse_dedup_span_chunked")
    assert len(chunked) == 1
    assert chunked[0]["windows"] == 4
    assert chunked[0]["span"] == WIDE_SPAN


# ── Solana: the same bail-out, and the per-key-loop regression ─────────


class _SolanaFake:
    """Records the slot-range queries and answers per window."""

    def __init__(self, by_window: dict[tuple[int, int], list[tuple[Any, ...]]] | None = None) -> None:
        self.by_window = by_window
        self.queries: list[dict[str, Any]] = []

    def execute(self, query: str, params: Any = None) -> Any:
        if "SELECT slot" in query:
            self.queries.append(params)
            return (self.by_window or {}).get((int(params["lo"]), int(params["hi"])), [])
        return []


def test_solana_narrow_batch_issues_one_query() -> None:
    client = _SolanaFake()
    keys = [(100, "sigA", "acct1"), (300, "sigB", "acct2")]
    assert existing_balance_keys(client, keys) == set()
    assert len(client.queries) == 1


def test_solana_wide_batch_walks_windows_and_still_dedups() -> None:
    """A wide slot range must NOT return "nothing exists" and re-insert."""
    span = 3 * MAX_DEDUP_SLOT_SPAN + 5
    windows = list(iter_block_windows(10, span, MAX_DEDUP_SLOT_SPAN))
    client = _SolanaFake({windows[0]: [(10, "sigA", "acct1")]})
    keys = [(10, "sigA", "acct1"), (span, "sigZ", "acctZ")]

    assert existing_balance_keys(client, keys) == {(10, "sigA", "acct1")}
    assert len(client.queries) == len(windows)  # one per window, not per key
    assert len(client.queries) == 3
    assert client.queries[-1]["hi"] == span
