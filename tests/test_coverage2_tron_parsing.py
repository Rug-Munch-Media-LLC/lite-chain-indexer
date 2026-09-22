"""Coverage tests for ``lite_indexer.strategies.tron.parsing``.

Hermetic — pure parsing, no network, no TronGrid. Complements
``tests/test_tron_timestamp.py`` (ms→ISO) and ``tests/test_strategies.py``
(transfer-event happy path) by pinning the address-normalization contract
and the malformed-input degradation paths: hex→base58check conversion,
mixed address formats, hex-encoded values, and the None-on-garbage rule.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lite_indexer.strategies.tron.parsing import (
    _hex_to_base58check,
    _normalize_tron_address,
    _parse_tron_transfer_event,
    ms_to_iso_utc,
    parse_tron_transfer_event,
)

# USDT TRC20 contract — a real, well-known base58check address.
USDT_B58 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
# The same address in 0x41-prefixed hex (version byte + 20-byte body).
USDT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
TOKEN = {"address": USDT_B58, "symbol": "USDT", "name": "Tether USD"}


# ── _hex_to_base58check ─────────────────────────────────────────────────


def test_hex_to_base58check_roundtrips_known_address() -> None:
    assert _hex_to_base58check(USDT_HEX) == USDT_B58


def test_hex_to_base58check_rejects_non_hex() -> None:
    assert _hex_to_base58check("not-hex-at-all") is None


def test_hex_to_base58check_handles_leading_zero_bytes() -> None:
    # A payload starting with 0x00 must gain a leading '1' per base58check.
    encoded = _hex_to_base58check("00" + "11" * 20)
    assert encoded is not None
    assert encoded.startswith("1")


# ── _normalize_tron_address ─────────────────────────────────────────────


def test_normalize_passes_through_base58check() -> None:
    assert _normalize_tron_address(USDT_B58) == USDT_B58


def test_normalize_converts_0x41_hex_to_base58() -> None:
    assert _normalize_tron_address("0x" + USDT_HEX) == USDT_B58


def test_normalize_converts_bare_41_hex_to_base58() -> None:
    assert _normalize_tron_address(USDT_HEX) == USDT_B58


def test_normalize_empty_and_none_return_empty_string() -> None:
    assert _normalize_tron_address("") == ""
    assert _normalize_tron_address(None) == ""


def test_normalize_whitespace_only_returns_empty_string() -> None:
    assert _normalize_tron_address("   ") == ""


def test_normalize_unknown_shape_returned_unchanged() -> None:
    # Not 34-char T-address, not 42-char 41-hex → returned verbatim.
    assert _normalize_tron_address("0xdeadbeef") == "0xdeadbeef"


# ── ms_to_iso_utc ───────────────────────────────────────────────────────


def test_ms_to_iso_utc_uses_fallback_for_zero() -> None:
    fallback = datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert ms_to_iso_utc(0, fallback=fallback) == "2020-01-02T03:04:05Z"


def test_ms_to_iso_utc_uses_fallback_for_garbage() -> None:
    fallback = datetime(2021, 6, 7, 8, 9, 10, tzinfo=UTC)
    assert ms_to_iso_utc("not-a-number", fallback=fallback) == "2021-06-07T08:09:10Z"


def test_ms_to_iso_utc_overflow_falls_back() -> None:
    fallback = datetime(2022, 1, 1, 0, 0, 0, tzinfo=UTC)
    # A value far beyond datetime's range must not raise.
    assert ms_to_iso_utc(10**20, fallback=fallback) == "2022-01-01T00:00:00Z"


# ── parse_tron_transfer_event ───────────────────────────────────────────


def _event(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "block_number": "12345",
        "transaction_id": "0xabc",
        "event_index": 2,
        "from_address": USDT_B58,
        "to_address": USDT_B58,
        "result": {"value": "1000000"},
        "block_timestamp": 1_700_000_000_000,
    }
    base.update(overrides)
    return base


def test_parse_returns_none_when_from_missing() -> None:
    assert (
        parse_tron_transfer_event(_event(from_address="", result={"value": "1"}), TOKEN, "tron")
        is None
    )


def test_parse_returns_none_when_to_missing() -> None:
    assert parse_tron_transfer_event(_event(to_address=""), TOKEN, "tron") is None


def test_parse_decodes_hex_value_to_decimal() -> None:
    evt = parse_tron_transfer_event(_event(result={"value": "0x10"}), TOKEN, "tron")
    assert evt is not None
    assert evt["value"] == "16"


def test_parse_bad_hex_value_degrades_to_zero() -> None:
    evt = parse_tron_transfer_event(_event(result={"value": "0xzz"}), TOKEN, "tron")
    assert evt is not None
    assert evt["value"] == "0"


def test_parse_accepts_int_block_number() -> None:
    evt = parse_tron_transfer_event(_event(block_number=99), TOKEN, "tron")
    assert evt is not None
    assert evt["blockNumber"] == 99


def test_parse_accepts_string_event_index() -> None:
    evt = parse_tron_transfer_event(_event(event_index="7"), TOKEN, "tron")
    assert evt is not None
    assert evt["logIndex"] == 7


def test_parse_falls_back_to_log_index_key_when_event_index_absent() -> None:
    # ``raw.get("event_index", raw.get("log_index", 0))`` only falls back
    # when the key is ABSENT — an explicit None is returned as-is.
    event = _event(log_index=5)
    del event["event_index"]
    evt = parse_tron_transfer_event(event, TOKEN, "tron")
    assert evt is not None
    assert evt["logIndex"] == 5


def test_parse_explicit_none_event_index_is_not_fallback() -> None:
    # Pins the real (surprising) behaviour: a present-but-None event_index
    # short-circuits the log_index fallback and yields None.
    evt = parse_tron_transfer_event(_event(event_index=None, log_index=5), TOKEN, "tron")
    assert evt is not None
    assert evt["logIndex"] is None


def test_parse_normalizes_hex_addresses_from_result() -> None:
    evt = parse_tron_transfer_event(
        _event(
            from_address="", to_address="", result={"value": "1", "from": USDT_HEX, "to": USDT_HEX}
        ),
        TOKEN,
        "tron",
    )
    assert evt is not None
    assert evt["from"] == USDT_B58
    assert evt["to"] == USDT_B58


def test_parse_returns_none_on_unparseable_block_number() -> None:
    # int("garbage") raises → the broad guard returns None (never raises).
    assert parse_tron_transfer_event(_event(block_number="garbage"), TOKEN, "tron") is None


def test_parse_alias_is_same_function() -> None:
    assert _parse_tron_transfer_event is parse_tron_transfer_event
