"""Unit tests for the solana block-time coercion contract."""
from datetime import UTC, datetime

from lite_indexer.strategies.solana.timestamps import coerce_event_ts, iso_from_epoch

NOW = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)


def test_iso_from_epoch_roundtrip():
    expected = datetime.fromtimestamp(1789970219, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert iso_from_epoch(1789970219) == expected
    assert iso_from_epoch(1789970219.9) == datetime.fromtimestamp(1789970219.9, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_iso_from_epoch_rejects_unusable():
    for bad in (None, "", "123", True, [], {}):
        assert iso_from_epoch(bad) is None


def test_coerce_accepts_iso_and_epoch():
    assert coerce_event_ts("2026-09-21T08:58:30Z", NOW) == datetime(2026, 9, 21, 8, 58, 30, tzinfo=UTC)
    assert coerce_event_ts(1789970219, NOW) == datetime.fromtimestamp(1789970219, tz=UTC)
    assert coerce_event_ts(1789970219.5, NOW).timestamp() == 1789970219.5


def test_coerce_falls_back_to_now():
    for bad in (None, "", "not-a-date", True, [], float("nan")):
        assert coerce_event_ts(bad, NOW) == NOW
