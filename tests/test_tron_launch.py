"""Unit tests for Tron launch detection (lite_indexer/strategies/tron/launch.py).

Pure-function tests with fixture receipts / tx dicts — no live TronGrid
calls. Covers ``detect_sunpump_launch`` (TokenCreate log decode), the
SunSwap ``detect_sunswap_pair`` decoder and ``detect_graduation``
(launchToDEX selector), plus the None/edge paths.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.strategies.tron.launch import (
    LAUNCH_TO_DEX_SELECTOR,
    SUNPUMP_CONTRACT,
    SUNSWAP_FACTORY,
    TOKEN_CREATE_TOPIC,
    detect_graduation,
    detect_sunpump_launch,
    detect_sunswap_pair,
)
from lite_indexer.strategies.tron.parsing import _hex_to_base58check

TOKEN_HEX = "411234567890abcdef1234567890abcdef12345678"  # 21-byte tron address
TOKEN_B58 = _hex_to_base58check(TOKEN_HEX) or ""
CREATOR_HEX = "41" + "22" * 20
CREATOR_B58 = _hex_to_base58check(CREATOR_HEX) or ""
PAIR_HEX = "41" + "33" * 20
PAIR_B58 = _hex_to_base58check(PAIR_HEX) or ""


def _padded_address(address_hex: str) -> str:
    """32-byte word with the 0x41 address in the rightmost 21 bytes."""
    return "00" * 11 + address_hex


def _token_create_log(
    *,
    indexed_creator: bool = True,
    token_index: int = 7,
    tx_hash: str = "0xabc123",
    block_number: int = 42_000_000,
    log_index: int = 3,
) -> dict[str, Any]:
    data = _padded_address(TOKEN_HEX)  # word 0: token address (rightmost bytes)
    if indexed_creator:
        # creator lives in topics[1]; token index is the SECOND data word.
        data += int(token_index).to_bytes(32, "big").hex()
    else:
        # non-indexed layout: creator is the second data word, index third.
        data += _padded_address(CREATOR_HEX)
        data += int(token_index).to_bytes(32, "big").hex()
    topics = [TOKEN_CREATE_TOPIC]
    if indexed_creator:
        topics.append("0x" + _padded_address(CREATOR_HEX))
    return {
        "address": SUNPUMP_CONTRACT,
        "topics": topics,
        "data": "0x" + data,
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "logIndex": log_index,
    }


def _receipt(logs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"logs": logs}


class TestDetectSunpumpLaunch:
    def test_indexed_creator_layout(self):
        evt = detect_sunpump_launch(_receipt([_token_create_log()]))
        assert evt is not None
        assert evt.token_address == TOKEN_B58
        assert evt.creator == CREATOR_B58
        assert evt.token_index == 7
        assert evt.tx_hash == "0xabc123"
        assert evt.block_number == 42_000_000
        assert evt.log_index == 3
        assert evt.contract_address == SUNPUMP_CONTRACT

    def test_non_indexed_creator_layout(self):
        evt = detect_sunpump_launch(_receipt([_token_create_log(indexed_creator=False)]))
        assert evt is not None
        assert evt.token_address == TOKEN_B58
        assert evt.creator == CREATOR_B58
        assert evt.token_index == 7

    def test_wrong_contract_ignored(self):
        log = _token_create_log()
        log["address"] = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"  # some other contract
        assert detect_sunpump_launch(_receipt([log])) is None

    def test_wrong_topic_ignored(self):
        log = _token_create_log()
        log["topics"][0] = "0x" + "ab" * 32
        assert detect_sunpump_launch(_receipt([log])) is None

    def test_short_data_ignored(self):
        log = _token_create_log()
        log["data"] = "0x" + "00" * 20
        assert detect_sunpump_launch(_receipt([log])) is None

    def test_empty_logs_returns_none(self):
        assert detect_sunpump_launch(_receipt([])) is None

    def test_non_dict_log_skipped(self):
        assert detect_sunpump_launch({"logs": ["junk"]}) is None

    def test_hex_string_indexes(self):
        log = _token_create_log()
        log["blockNumber"] = f"0x{42_000_000:x}"
        log["logIndex"] = "0x5"
        evt = detect_sunpump_launch(_receipt([log]))
        assert evt is not None
        assert evt.block_number == 42_000_000
        assert evt.log_index == 5


class TestDetectSunswapPair:
    def _pair_log(self) -> dict[str, Any]:
        return {
            "address": SUNSWAP_FACTORY,
            "topics": [
                "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9",
                "0x" + _padded_address(TOKEN_HEX),
                "0x" + _padded_address(CREATOR_HEX),
            ],
            "data": "0x" + _padded_address(PAIR_HEX),
            "transactionHash": "0xdef456",
            "blockNumber": 42_000_001,
        }

    def test_pair_created_decoded(self):
        evt = detect_sunswap_pair(_receipt([self._pair_log()]))
        assert evt is not None
        assert evt.pair_address == PAIR_B58
        assert evt.token0 == TOKEN_B58
        assert evt.token1 == CREATOR_B58
        assert evt.factory == SUNSWAP_FACTORY
        assert evt.tx_hash == "0xdef456"

    def test_wrong_factory_ignored(self):
        log = self._pair_log()
        log["address"] = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
        assert detect_sunswap_pair(_receipt([log])) is None

    def test_fewer_than_three_topics_ignored(self):
        log = self._pair_log()
        log["topics"] = log["topics"][:2]
        assert detect_sunswap_pair(_receipt([log])) is None


class TestDetectGraduation:
    def _launch_to_dex_tx(self, token_hex: str = TOKEN_HEX) -> dict[str, Any]:
        # Single address arg: 32-byte word with the 0x41 address right-aligned.
        data = LAUNCH_TO_DEX_SELECTOR + ("00" * 11 + token_hex)
        return {
            "raw_data": {"contract": [{"parameter": {"value": {"to_address": SUNPUMP_CONTRACT, "data": "0x" + data}}}]}
        }

    def test_launch_to_dex_returns_token(self):
        assert detect_graduation(self._launch_to_dex_tx()) == TOKEN_B58

    def test_other_contract_not_matched(self):
        tx = self._launch_to_dex_tx()
        tx["raw_data"]["contract"][0]["parameter"]["value"]["to_address"] = "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
        assert detect_graduation(tx) is None

    def test_non_launch_selector_not_matched(self):
        tx = self._launch_to_dex_tx()
        tx["raw_data"]["contract"][0]["parameter"]["value"]["data"] = "0x1cc2c911" + "00" * 64
        assert detect_graduation(tx) is None

    def test_no_contracts_returns_none(self):
        assert detect_graduation({}) is None

    def test_flat_contract_list_fallback(self):
        # ``_tx_contracts`` falls back to the flattened ``contract`` list
        # only when ``raw_data.contract`` is not itself a list.
        tx = self._launch_to_dex_tx()
        real_contract = tx["raw_data"]["contract"]
        tx["raw_data"]["contract"] = {"unexpected": "shape"}
        tx["contract"] = real_contract
        assert detect_graduation(tx) == TOKEN_B58

    def test_selector_available(self):
        assert LAUNCH_TO_DEX_SELECTOR == "5a47db1d"
