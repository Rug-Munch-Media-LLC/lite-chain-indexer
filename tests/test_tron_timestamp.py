"""Unit tests for the TRON timestamp parse boundary.

TronGrid reports epoch **milliseconds**; the durable store holds ISO-8601
seconds. Parsing ms as seconds lands rows at 1970-01-01, so the conversion
must happen at the parse boundary (``ms_to_iso_utc``). Hermetic — no network.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lite_indexer.strategies.tron.parsing import ms_to_iso_utc, parse_tron_transfer_event

FROM_ADDR = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TO_ADDR = "TLa2f6VPqDgRE67v1736s7bJ8Ray5wYjU7"
TOKEN = {"address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "symbol": "USDT", "name": "Tether"}


def test_ms_to_iso_utc_converts_milliseconds_not_1970() -> None:
    assert ms_to_iso_utc(1700000000000) == "2023-11-14T22:13:20Z"


def test_ms_to_iso_utc_zero_and_none_fall_back() -> None:
    fallback = datetime(2026, 1, 1, tzinfo=UTC)
    assert ms_to_iso_utc(0, fallback=fallback) == "2026-01-01T00:00:00Z"
    assert ms_to_iso_utc(None, fallback=fallback) == "2026-01-01T00:00:00Z"
    assert ms_to_iso_utc("not-a-number", fallback=fallback) == "2026-01-01T00:00:00Z"


def test_ms_to_iso_utc_accepts_numeric_string() -> None:
    assert ms_to_iso_utc("1700000000000") == "2023-11-14T22:13:20Z"


def test_parse_tron_transfer_event_keeps_real_timestamp() -> None:
    raw = {
        "block_number": "68000000",
        "transaction_id": "0xdeadbeef",
        "event_index": 3,
        "from_address": FROM_ADDR,
        "to_address": TO_ADDR,
        "block_timestamp": 1700000000000,
    }
    event = parse_tron_transfer_event(raw, TOKEN, "tron")
    assert event is not None
    assert event["blockTimestamp"] == "2023-11-14T22:13:20Z"
    assert not event["blockTimestamp"].startswith("1970")
    assert event["blockNumber"] == 68000000
    assert event["logIndex"] == 3
