"""Tests for BtcSource — the AWS public Bitcoin lake read tier.

Covered, in the order the contract depends on them:
1. date validation rejects malformed partitions instead of skipping them,
2. the row mapper is honest about UTXO (values in sats, NULLs preserved),
3. a missing partition returns empty rather than raising.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.query import btc_read
from lite_indexer.query.btc_rowmap import SATS_PER_BTC, UTXO_EDGE_LOSS, map_row, sats
from lite_indexer.query.btc_source import BtcSource


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], cols: list[str]) -> None:
        self._rows = rows
        self.description = [(c,) for c in cols]
        self._served = False

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._served:
            return []
        self._served = True
        return list(self._rows)


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None, cols: list[str] | None = None) -> None:
        self.rows = rows or []
        self.cols = cols or []
        self.calls: list[tuple[str, list[Any]]] = []
        self.boom: Exception | None = None

    def execute(self, sql: str, params: list[Any] | None = None) -> FakeCursor:
        self.calls.append((sql, list(params or [])))
        if self.boom is not None:
            raise self.boom
        return FakeCursor(self.rows, self.cols)


TX_COLS = [
    "block_number",
    "block_hash",
    "block_timestamp",
    "tx_hash",
    "tx_index",
    "is_coinbase",
    "input_count",
    "output_count",
    "fee",
    "input_addresses",
    "input_values",
    "input_spent_tx",
    "input_spent_index",
    "output_addresses",
    "output_values",
    "output_types",
]


def _tx_row(**over: Any) -> tuple[Any, ...]:
    base = {
        "block_number": 976000,
        "block_hash": "00aeb8",
        "block_timestamp": "2026-11-15 18:25:56",
        "tx_hash": "934df9da",
        "tx_index": 1,
        "is_coinbase": False,
        "input_count": 1,
        "output_count": 1,
        "fee": 0.00142022,
        "input_addresses": ["bc1qsender"],
        "input_values": [0.30595074],
        "input_spent_tx": ["8bdc3170"],
        "input_spent_index": [1],
        "output_addresses": ["bc1qrecipient"],
        "output_values": [0.30453052],
        "output_types": ["witness_v0_keyhash"],
    }
    base.update(over)
    return tuple(base[c] for c in TX_COLS)


def _source(
    rows: list[tuple[Any, ...]] | None = None, cols: list[str] | None = None
) -> tuple[BtcSource, FakeConnection]:
    fake = FakeConnection(rows, cols)
    return BtcSource(connect=lambda: fake), fake


# ── 1. Date validation ───────────────────────────────────────────────


@pytest.mark.parametrize("bad", ["2026-11-1", "20261115", "2026/11/15", "", "2026-11-15/*", "29-11-2026"])
def test_validate_dates_rejects_malformed(bad: str) -> None:
    """A malformed partition must raise, never be skipped into an empty read."""
    with pytest.raises(ValueError, match="invalid date"):
        BtcSource.validate_dates([bad])


def test_validate_dates_accepts_exact_days_and_passes_none_through() -> None:
    assert BtcSource.validate_dates(["2026-11-15", "2026-11-16"]) == ["2026-11-15", "2026-11-16"]
    assert BtcSource.validate_dates(None) is None
    assert BtcSource.validate_dates([]) is None


def test_query_rejects_malformed_date_before_touching_the_lake() -> None:
    src, fake = _source()
    with pytest.raises(ValueError, match="invalid date"):
        src.query("btc_transactions", dates=["2026-11-1"])
    assert fake.calls == [], "validation must happen before any SQL is executed"


def test_iter_rows_rejects_malformed_date() -> None:
    src, _ = _source()
    with pytest.raises(ValueError, match="invalid date"):
        list(src.iter_rows("btc_transactions", "15-11-2026"))


def test_paths_are_day_partitioned_and_month_scope_is_never_a_wildcard_history_scan() -> None:
    src, _ = _source()
    assert src.paths("btc_transactions", ["2026-11-15"]) == [
        "s3://aws-public-blockchain/v1.0/btc/transactions/date=2026-11-15/*.parquet"
    ]
    scoped = BtcSource(scope="2026-11-*")
    assert scoped.paths("btc_blocks", None) == ["s3://aws-public-blockchain/v1.0/btc/blocks/date=2026-11-*/*.parquet"]


# ── 2. Row mapper ────────────────────────────────────────────────────


def test_sats_is_a_unit_change_not_a_rounding() -> None:
    assert sats(0.30595074) == 30_595_074
    assert sats(1.0) == SATS_PER_BTC
    assert sats(0) == 0
    assert sats(None) is None


def test_map_row_keeps_utxo_shape_and_does_not_invent_from_to() -> None:
    row = map_row("btc_transactions", dict(zip(TX_COLS, _tx_row(), strict=True)))
    assert row["chain"] == "btc"
    assert row["table"] == "btc_transactions"
    # The honest part: lists, not a fabricated (from, to) pair.
    assert "from_address" not in row and "to_address" not in row
    assert row["input_addresses"] == ["bc1qsender"]
    assert row["output_addresses"] == ["bc1qrecipient"]
    assert row["input_spent_tx"] == ["8bdc3170"]
    # The spent-output index is what makes an edge-exact join possible.
    assert row["input_spent_index"] == [1]
    assert row["utxo_edge_loss"] == UTXO_EDGE_LOSS
    assert row["fee_sats"] == 142_022
    assert row["input_value_sats"] == [30_595_074]
    assert row["output_value_sats"] == [30_453_052]


def test_map_row_preserves_nulls_instead_of_fabricating_zeroes() -> None:
    """An OP_RETURN output has no address; a coinbase has no inputs."""
    raw = dict(
        zip(
            TX_COLS,
            _tx_row(
                input_addresses=None,
                input_values=None,
                input_spent_tx=None,
                input_spent_index=None,
                is_coinbase=True,
                input_count=0,
                fee=None,
                output_addresses=[None, "bc1qx"],
                output_values=None,
            ),
            strict=True,
        )
    )
    row = map_row("btc_transactions", raw)
    assert row["input_addresses"] is None
    assert row["input_spent_index"] is None
    assert row["input_value_sats"] is None
    assert row["output_value_sats"] is None
    assert row["output_addresses"] == [None, "bc1qx"]
    assert row["is_coinbase"] is True
    assert row["fee_sats"] is None


def test_map_row_rejects_an_unmapped_table() -> None:
    with pytest.raises(ValueError, match="unmapped btc table"):
        map_row("evm_transfers", {})


def test_map_row_block_shape() -> None:
    raw = {
        "block_number": 975868,
        "block_hash": "00fb43",
        "block_timestamp": "2026-11-15 00:59:04",
        "previousblockhash": "00bd0f",
        "transaction_count": 7283,
        "size": 1644405,
        "weight": 3997413,
        "difficulty": 134823837323272.7,
        "bits": "17021673",
        "chainwork": "00ff",
        "merkle_root": "124ec1",
        "version": 537362432,
        "nonce": 1292761633,
    }
    row = map_row("btc_blocks", raw)
    assert row["table"] == "btc_blocks"
    assert row["block_number"] == 975868
    assert row["transaction_count"] == 7283
    assert row["bits"] == "17021673"


# ── 3. Missing partition is empty, not an exception ──────────────────


def test_missing_partition_returns_empty() -> None:
    """httpfs raises for an absent ``date=`` prefix; the reader reports empty."""
    src, fake = _source()
    fake.boom = OSError("No files found that match the pattern")
    rows, truncated = src.query("btc_transactions", dates=["2020-01-01"])
    assert rows == []
    assert truncated is False


def test_missing_partition_on_iter_rows_still_raises_to_the_caller() -> None:
    """Streaming must not swallow it: write_unit has to see the failure."""
    src, fake = _source()
    fake.boom = OSError("No files found")
    with pytest.raises(OSError, match="No files found"):
        list(src.iter_rows("btc_transactions", "2020-01-01"))


def test_query_maps_rows_and_detects_truncation_via_the_probe_row() -> None:
    src, fake = _source([_tx_row(tx_hash="a"), _tx_row(tx_hash="b"), _tx_row(tx_hash="c")], TX_COLS)
    rows, truncated = src.query("btc_transactions", dates=["2026-11-15"], limit=2)
    assert truncated is True
    assert [r["tx_hash"] for r in rows] == ["a", "b"]
    sql, params = fake.calls[0]
    assert "LIMIT ?" in sql and params[-1] == 3, "the probe row is what makes truncation provable"


def test_query_unknown_table_is_empty_not_a_crash() -> None:
    src, fake = _source()
    assert src.query("dogecoin_transactions") == ([], False)
    assert fake.calls == []


def test_query_disabled_tier_returns_empty() -> None:
    src = BtcSource(enabled=False)
    assert src.available() is False
    assert src.query("btc_transactions") == ([], False)


def test_oversize_limit_raises_rather_than_truncating_silently() -> None:
    src, _ = _source()
    with pytest.raises(ValueError, match="exceeds ceiling"):
        src.query("btc_transactions", dates=["2026-11-15"], limit=10_000_000)


def test_count_query_targets_one_partition() -> None:
    src, _ = _source()
    sql, params = src.count_query("btc_transactions", "2026-11-15")
    assert "count(*)" in sql
    assert "date=2026-11-15" in sql
    assert params == []


def test_build_query_pushes_block_bounds_as_parameters() -> None:
    sql, params = btc_read.build_query(
        ["s3://b/x/date=2026-11-15/*.parquet"],
        columns="block_number",
        from_block=976000,
        to_block=976100,
        limit=50,
    )
    assert "block_number >= ?" in sql and "block_number <= ?" in sql
    assert params == [976000, 976100, 51]
