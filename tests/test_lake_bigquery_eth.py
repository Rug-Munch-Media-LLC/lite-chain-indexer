"""fill-eth: day SQL bounds, lake→warehouse mapping, NULL-value discipline."""

from __future__ import annotations

from lite_indexer.ingest.lake_bigquery_eth import (
    EthFillRequest,
    _scan_estimate,
    day_sql,
    map_trace,
    map_transfer,
)

DAY = "2026-10-20"


def test_day_sql_is_bounded_and_unlimited() -> None:
    sql = day_sql("token_transfers", DAY)
    assert 'TIMESTAMP "2026-10-20 00:00:00"' in sql
    assert 'TIMESTAMP "2026-10-21 00:00:00"' in sql
    assert "LIMIT" not in sql.upper()


def test_day_sql_selects_trace_columns() -> None:
    sql = day_sql("traces", DAY)
    assert "transaction_hash AS tx_hash" in sql
    assert "trace_address" in sql
    assert "CAST(value AS STRING) AS value" in sql


def test_map_transfer_shape_is_writer_ready() -> None:
    row = map_transfer(
        {
            "block_number": 20000000,
            "block_timestamp": 1_727_000_000_000,
            "tx_hash": "0xabc",
            "from_address": "0xAA",
            "to_address": "0xBB",
            "token_address": "0xCC",
            "value": "1234",
            "log_index": 7,
        }
    )
    assert row is not None
    assert row["blockNumber"] == 20000000
    assert row["from"] == "0xaa"
    assert row["value"] == "1234"
    assert row["logIndex"] == 7
    assert row["blockTimestamp"].startswith("2024-09-22")


def test_map_transfer_never_fabricates_zero_for_null_value() -> None:
    """A NULL value is a defect, not a 0 transfer — the row is dropped."""
    assert map_transfer({"block_number": 1, "value": None, "log_index": 0}) is None


def test_map_trace_parses_address_path_and_null_value() -> None:
    row = map_trace(
        {
            "block_number": 5,
            "block_timestamp": 1_727_000_000_000,
            "tx_hash": "0xdead",
            "trace_address": "0,1",
            "from_address": "0xAA",
            "to_address": "0xBB",
            "value": None,
            "call_type": "call",
            "gas_used": 21,
            "input": "0xa9059cbb00",
        }
    )
    assert row["trace_address"] == [0, 1]
    assert row["value_wei"] == 0  # zero-value call, counted as null upstream
    assert row["input_method_id"] == "0xa9059cbb"
    assert row["chain"] == "ethereum"


def test_scan_estimate_matches_measured_day_costs() -> None:
    req = EthFillRequest(dates=["2026-10-20", "2026-10-21"], tables=("token_transfers", "traces"))
    assert round(_scan_estimate(req), 2) == 0.74
