"""Tests for LP (Mint/Burn) ingest: type derivation + V2/V3 decode.

The topic hashes are recomputed here from their canonical signatures.
A typo in a topic constant produces an *empty* getLogs result, never an
error, so this is the only guard that catches a wrong hash.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.config import (
    BURN_V2_TOPIC,
    BURN_V3_TOPIC,
    LP_ADD_TYPE,
    LP_REMOVE_TYPE,
    LP_TOPIC_TYPES,
    MINT_V2_TOPIC,
    MINT_V3_TOPIC,
    PAIR_CREATED_TOPIC,
    SWAP_V2_TOPIC,
    SWAP_V3_TOPIC,
)
from lite_indexer.dex import parse_dex_logs, parse_lp_logs, parse_swap_logs
from lite_indexer.dex.log_context import LogDecodeContext
from lite_indexer.dex.lp import lp_event_type
from lite_indexer.storage.ch_dex import _dex_row

# ── Fixtures ──────────────────────────────────────────────────────────

_LP = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_T0 = "0x1111111111111111111111111111111111111111"
_T1 = "0x2222222222222222222222222222222222222222"
_SENDER = "0x" + "ab" * 20
_OWNER = "0x" + "cd" * 20
_TO = "0x" + "ef" * 20
PAIRS = {_LP: (_T0, _T1)}


def _word(value: int) -> str:
    """One 32-byte data word."""
    return f"{value:064x}"


def _v2_mint_log(block: int = 100, log_index: int = 3) -> dict[str, Any]:
    return {
        "address": _LP,
        "topics": [MINT_V2_TOPIC, "0x" + "00" * 12 + "ab" * 20],
        "data": "0x" + _word(10**18) + _word(2 * 10**18),
        "blockNumber": hex(block),
        "transactionHash": "0x" + "11" * 32,
        "logIndex": hex(log_index),
    }


def _v2_burn_log(block: int = 100, log_index: int = 4) -> dict[str, Any]:
    return {
        "address": _LP,
        "topics": [
            BURN_V2_TOPIC,
            "0x" + "00" * 12 + "ab" * 20,
            "0x" + "00" * 12 + "ef" * 20,
        ],
        "data": "0x" + _word(5 * 10**17) + _word(10**18),
        "blockNumber": hex(block),
        "transactionHash": "0x" + "22" * 32,
        "logIndex": hex(log_index),
    }


def _v3_mint_log(block: int = 100, log_index: int = 5) -> dict[str, Any]:
    # data = tickLower, tickUpper, amount, amount0, amount1, fee
    data = _word(-887272) + _word(887272) + _word(12345) + _word(3 * 10**18) + _word(7 * 10**18) + _word(0)
    return {
        "address": _LP,
        "topics": [
            MINT_V3_TOPIC,
            "0x" + "00" * 12 + "cd" * 20,
            "0x" + "00" * 12 + "ab" * 20,
        ],
        "data": "0x" + data,
        "blockNumber": hex(block),
        "transactionHash": "0x" + "33" * 32,
        "logIndex": hex(log_index),
    }


def _v3_burn_log(block: int = 100, log_index: int = 6) -> dict[str, Any]:
    # V3 burn amounts are int256: amount0 negative, amount1 positive.
    neg = (2**256) - (4 * 10**18)
    data = _word(-887272) + _word(887272) + _word(999) + _word(neg) + _word(9 * 10**18) + _word(0)
    return {
        "address": _LP,
        "topics": [
            BURN_V3_TOPIC,
            "0x" + "00" * 12 + "cd" * 20,
            "0x" + "00" * 12 + "ab" * 20,
        ],
        "data": "0x" + data,
        "blockNumber": hex(block),
        "transactionHash": "0x" + "44" * 32,
        "logIndex": hex(log_index),
    }


# ── Topic hash derivation (the anti-silent-failure guard) ─────────────


def test_lp_topics_match_keccak_of_canonical_signatures() -> None:
    """Every topic constant equals keccak256(signature) — no hand-typed hash."""
    from Crypto.Hash import keccak  # nosec B413: pycryptodome, not the dead pycrypto (see [tool.bandit])

    def k(sig: str) -> str:
        digest = keccak.new(digest_bits=256)
        digest.update(sig.encode())
        return "0x" + digest.hexdigest()

    assert k("Mint(address,uint256,uint256)") == MINT_V2_TOPIC
    assert k("Burn(address,uint256,uint256,address)") == BURN_V2_TOPIC
    assert k("Mint(address,address,int24,int24,uint128,uint256,uint256)") == MINT_V3_TOPIC
    assert k("Burn(address,address,int24,int24,uint128,uint256,uint256)") == BURN_V3_TOPIC


def test_preexisting_topics_unchanged_by_lp_split() -> None:
    """The LP config split must not perturb the existing topic constants."""
    from Crypto.Hash import keccak  # nosec B413: pycryptodome, not the dead pycrypto (see [tool.bandit])

    def k(sig: str) -> str:
        digest = keccak.new(digest_bits=256)
        digest.update(sig.encode())
        return "0x" + digest.hexdigest()

    assert k("Swap(address,uint256,uint256,uint256,uint256,address)") == SWAP_V2_TOPIC
    assert k("Swap(address,address,int256,int256,uint160,uint128,int24)") == SWAP_V3_TOPIC
    assert k("PairCreated(address,address,address,uint256)") == PAIR_CREATED_TOPIC


# ── Type derivation ───────────────────────────────────────────────────


def test_lp_event_type_derives_from_topic() -> None:
    assert lp_event_type(MINT_V2_TOPIC) == LP_ADD_TYPE
    assert lp_event_type(MINT_V3_TOPIC) == LP_ADD_TYPE
    assert lp_event_type(BURN_V2_TOPIC) == LP_REMOVE_TYPE
    assert lp_event_type(BURN_V3_TOPIC) == LP_REMOVE_TYPE


def test_lp_event_type_is_case_insensitive() -> None:
    assert lp_event_type(MINT_V2_TOPIC.upper().replace("0X", "0x")) == LP_ADD_TYPE


def test_lp_event_type_rejects_non_lp_topics() -> None:
    """A swap topic must never be classified as LP activity."""
    assert lp_event_type(SWAP_V2_TOPIC) == ""
    assert lp_event_type(SWAP_V3_TOPIC) == ""
    assert lp_event_type("0xdeadbeef") == ""


def test_emitted_types_are_exactly_the_scanner_filter_values() -> None:
    """The scanner's liquidity_drain filter matches these four literals."""
    scanner_values = {"add_liquidity", "remove_liquidity", "lp_add", "lp_remove"}
    assert set(LP_TOPIC_TYPES[t][0] for t in LP_TOPIC_TYPES) <= scanner_values


# ── V2 decode ─────────────────────────────────────────────────────────


def test_v2_mint_decodes_add_liquidity() -> None:
    events = parse_lp_logs([_v2_mint_log()], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS))
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "add_liquidity"
    assert event["dex_type"] == "v2"
    assert event["dex"] == "uniswap_v2"
    assert event["sender"] == _SENDER
    assert event["token0"] == _T0
    assert event["token1"] == _T1
    assert event["amount0"] == str(10**18)
    assert event["amount1"] == str(2 * 10**18)
    assert event["pairAddress"] == _LP


def test_v2_burn_decodes_remove_liquidity_with_destination() -> None:
    events = parse_lp_logs([_v2_burn_log()], _ctx("ethereum", 0, 200, BURN_V2_TOPIC, PAIRS))
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "remove_liquidity"
    assert event["sender"] == _SENDER
    assert event["recipient"] == _TO
    assert event["amount0"] == str(5 * 10**17)
    assert event["amount1"] == str(10**18)


def test_v2_lp_legs_have_no_swap_legs() -> None:
    """amountN_in/out stay 0 so volume math never reads an LP leg as a trade."""
    event = parse_lp_logs([_v2_mint_log()], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS))[0]
    assert event["amount0In"] == "0"
    assert event["amount1In"] == "0"
    assert event["amount0Out"] == "0"
    assert event["amount1Out"] == "0"


# ── V3 decode ─────────────────────────────────────────────────────────


def test_v3_mint_decodes_add_liquidity() -> None:
    events = parse_lp_logs([_v3_mint_log()], _ctx("ethereum", 0, 200, MINT_V3_TOPIC, PAIRS))
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "add_liquidity"
    assert event["dex_type"] == "v3"
    assert event["dex"] == "uniswap_v3"
    # topics[1] is the owner for V3 — that is the LP identity.
    assert event["sender"] == _OWNER
    assert event["amount0"] == str(3 * 10**18)
    assert event["amount1"] == str(7 * 10**18)


def test_v3_burn_uses_int256_magnitude() -> None:
    """A negative int256 leg must store its magnitude, not ~2^256."""
    events = parse_lp_logs([_v3_burn_log()], _ctx("ethereum", 0, 200, BURN_V3_TOPIC, PAIRS))
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "remove_liquidity"
    assert event["amount0"] == str(4 * 10**18)
    assert event["amount1"] == str(9 * 10**18)
    assert int(event["amount0"]) < 2**255


# ── Filtering: reorg, range, dedup, caps ──────────────────────────────


def test_out_of_range_block_is_dropped() -> None:
    assert parse_lp_logs([_v2_mint_log(block=500)], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS)) == []


def test_removed_log_is_dropped() -> None:
    log = {**_v2_mint_log(), "removed": True}
    assert parse_lp_logs([log], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS)) == []


def test_duplicate_tx_logindex_is_deduped() -> None:
    events = parse_lp_logs([_v2_mint_log(), _v2_mint_log()], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS))
    assert len(events) == 1


def test_truncated_data_yields_zero_not_crash() -> None:
    log = {**_v2_mint_log(), "data": "0x" + _word(1)}
    event = parse_lp_logs([log], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS))[0]
    assert event["amount0"] == "0"
    assert event["amount1"] == "0"


def test_non_lp_topic_returns_no_events() -> None:
    assert parse_lp_logs([_v2_mint_log()], _ctx("ethereum", 0, 200, SWAP_V2_TOPIC, PAIRS)) == []


# ── Dispatcher ────────────────────────────────────────────────────────


def test_parse_dex_logs_routes_swap_and_lp_topics() -> None:
    assert parse_dex_logs([_v2_mint_log()], _ctx("ethereum", 0, 200, MINT_V2_TOPIC, PAIRS))
    assert parse_dex_logs([], _ctx("ethereum", 0, 200, SWAP_V2_TOPIC, PAIRS)) == []
    assert parse_dex_logs([_v2_mint_log()], _ctx("ethereum", 0, 200, PAIR_CREATED_TOPIC, PAIRS)) == []


def test_parse_swap_logs_still_works_after_dispatch_split() -> None:
    """Regression: the swap entry point must be unchanged by the refactor."""
    log = {
        "address": _LP,
        "topics": [SWAP_V2_TOPIC, "0x" + "00" * 12 + "ab" * 20, "0x" + "00" * 12 + "ef" * 20],
        "data": "0x" + _word(1) + _word(2) + _word(3) + _word(4),
        "blockNumber": hex(100),
        "transactionHash": "0x" + "55" * 32,
        "logIndex": hex(0),
    }
    events = parse_swap_logs([log], _ctx("ethereum", 0, 200, SWAP_V2_TOPIC, PAIRS))
    assert events[0]["type"] == "swap"
    assert events[0]["amount0In"] == "1"
    assert events[0]["amount1Out"] == "4"


# ── Writer: type is derived, not hardcoded ────────────────────────────


@pytest.mark.parametrize(
    ("topic", "log"),
    [
        (MINT_V2_TOPIC, _v2_mint_log()),
        (BURN_V2_TOPIC, _v2_burn_log()),
        (MINT_V3_TOPIC, _v3_mint_log()),
        (BURN_V3_TOPIC, _v3_burn_log()),
    ],
)
def test_dex_row_derives_type_from_event(topic: str, log: dict[str, Any]) -> None:
    from datetime import UTC, datetime

    event = parse_lp_logs([log], _ctx("ethereum", 0, 200, topic, PAIRS))[0]
    row = _dex_row("ethereum", event, False, datetime.now(UTC))
    assert row[0] == event["type"]
    assert row[0] in {LP_ADD_TYPE, LP_REMOVE_TYPE}


def test_dex_row_falls_back_to_swap_without_explicit_type() -> None:
    """Hand-built swap payloads (no 'type') keep the pre-existing value."""
    from datetime import UTC, datetime

    event = {
        "blockNumber": 1,
        "transactionHash": "0xabc",
        "logIndex": 0,
        "amount0": "1",
        "amount1": "2",
    }
    assert _dex_row("ethereum", event, False, datetime.now(UTC))[0] == "swap"


def _ctx(
    chain: str,
    from_block: int,
    to_block: int,
    topic: str,
    pair_tokens: dict[str, tuple[str, str]],
) -> LogDecodeContext:
    """Decode context for one fetch — the decoders take this, not five args."""
    return LogDecodeContext(
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        topic=topic,
        pair_tokens=pair_tokens,
    )
