"""
Tests for LiteChainIndexer.

Tests core functionality: DEX event parsing, RPC utilities,
state management, and the storage layer.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from lite_indexer.dex.log_context import LogDecodeContext

# ── Parse/Decoder Tests ──────────────────────────────────────────────


class TestUnpadAddress:
    """Test the address unpadding utility."""

    def test_unpad_ethereum_address(self):
        from lite_indexer.dex import unpad_address

        padded = "0x" + "00" * 12 + "dead00000000000000000000000000000000beef"
        result = unpad_address(padded)
        assert result == "0xdead00000000000000000000000000000000beef"

    def test_unpad_already_short(self):
        from lite_indexer.dex import unpad_address

        result = unpad_address("0xdead00000000000000000000000000000000beef")
        assert result == "0xdead00000000000000000000000000000000beef"

    def test_unpad_empty(self):
        from lite_indexer.dex import unpad_address

        assert unpad_address("") == "0x0000000000000000000000000000000000000000"
        assert unpad_address("0x") == "0x0000000000000000000000000000000000000000"


class TestParseTransferLogs:
    """Test parsing raw eth_getLogs results into transfer events."""

    def test_basic_transfer(self):
        from lite_indexer.dex import parse_transfer_logs

        logs = [
            {
                "blockNumber": "0x100000",
                "logIndex": "0x1",
                "transactionHash": "0xabc",
                "address": "0xtoken",
                "topics": [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0x" + "00" * 12 + "dead00000000000000000000000000000000beef",
                    "0x" + "00" * 12 + "cafe00000000000000000000000000000000babe",
                ],
                "data": "0x" + "00" * 31 + "01",  # value = 1
                "removed": False,
            }
        ]

        events = parse_transfer_logs(logs, "ethereum", 0x100000, 0x100000)
        assert len(events) == 1
        assert events[0]["from"] == "0xdead00000000000000000000000000000000beef"
        assert events[0]["to"] == "0xcafe00000000000000000000000000000000babe"
        assert events[0]["value"] == "1"
        assert events[0]["chain"] == "ethereum"
        assert events[0]["type"] == "transfer"

    def test_skips_removed_logs(self):
        from lite_indexer.dex import parse_transfer_logs

        logs = [
            {
                "blockNumber": "0x100000",
                "logIndex": "0x1",
                "transactionHash": "0xabc",
                "address": "0xtoken",
                "topics": ["0xtopic", "0xfrom", "0xto"],
                "data": "0x1",
                "removed": True,
            }
        ]
        events = parse_transfer_logs(logs, "ethereum", 0x100000, 0x100001)
        assert len(events) == 0

    def test_skips_nft_transfers(self):
        """4 topics + value=0 → NFT, not ERC-20."""
        from lite_indexer.dex import parse_transfer_logs

        logs = [
            {
                "blockNumber": "0x100000",
                "logIndex": "0x1",
                "transactionHash": "0xabc",
                "address": "0xnft",
                "topics": [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "0xfrom",
                    "0xto",
                    "0xtokenid",
                ],
                "data": "0x0",
                "removed": False,
            }
        ]
        events = parse_transfer_logs(logs, "ethereum", 0x100000, 0x100001)
        assert len(events) == 0

    def test_dedup_by_tx_hash_log_index(self):
        from lite_indexer.dex import parse_transfer_logs

        log = {
            "blockNumber": "0x100000",
            "logIndex": "0x1",
            "transactionHash": "0xabc",
            "address": "0xtoken",
            "topics": ["0x" + "d" * 64, "0xfrom", "0xto"],
            "data": "0x1",
            "removed": False,
        }
        events = parse_transfer_logs([log, log], "ethereum", 0x100000, 0x100001)
        assert len(events) == 1  # Deduplicated


class TestParseSwapLogs:
    """Test parsing V2 and V3 swap events."""

    def test_v2_swap_basic(self):
        from lite_indexer.dex import SWAP_V2_TOPIC, parse_swap_logs

        pair = "0xpair"
        pair_tokens = {pair: ("0xtoken0", "0xtoken1")}

        logs = [
            {
                "blockNumber": "0x100000",
                "logIndex": "0x1",
                "transactionHash": "0xabc",
                "address": pair,
                "topics": [
                    SWAP_V2_TOPIC,
                    "0x" + "00" * 12 + "sender0000000000000000000000000000",
                    "0x" + "00" * 12 + "recipnt0000000000000000000000000000",
                ],
                "data": "0x"
                + "01" * 32  # amount0In
                + "02" * 32  # amount1In
                + "03" * 32  # amount0Out
                + "04" * 32,  # amount1Out
                "removed": False,
            }
        ]

        events = parse_swap_logs(logs, _ctx("ethereum", 0x100000, 0x100000, SWAP_V2_TOPIC, pair_tokens))
        assert len(events) == 1
        assert events[0]["dex_type"] == "v2"
        assert events[0]["token0"] == "0xtoken0"
        assert events[0]["token1"] == "0xtoken1"
        assert events[0]["pairAddress"] == pair

    def test_v2_swap_without_pair_tokens_falls_back_empty(self):
        from lite_indexer.dex import SWAP_V2_TOPIC, parse_swap_logs

        logs = [
            {
                "blockNumber": "0x100000",
                "logIndex": "0x1",
                "transactionHash": "0xabc",
                "address": "0xunknown",
                "topics": [SWAP_V2_TOPIC, "0xsender", "0xrecip"],
                "data": "0x" + "01" * 128,
                "removed": False,
            }
        ]

        events = parse_swap_logs(logs, _ctx("ethereum", 0x100000, 0x100000, SWAP_V2_TOPIC, {}))
        assert len(events) == 1
        assert events[0]["token0"] == ""
        assert events[0]["token1"] == ""


class TestParsePairCreatedLogs:
    """Test parsing PairCreated (V2) and PoolCreated (V3) events."""

    def test_v2_pair_created(self):
        from lite_indexer.dex import PAIR_CREATED_TOPIC, parse_pair_created_logs

        # 20-byte addresses = 40 hex chars
        token0_addr = "dead00000000000000000000000000000000beef"
        token1_addr = "cafe00000000000000000000000000000000babe"
        pair_addr_raw = "aaaa00000000000000000000000000000000bbbb"

        log_entry = {
            "blockNumber": "0x100000",
            "transactionHash": "0xabc",
            "address": "0xfactory",
            "topics": [
                PAIR_CREATED_TOPIC,
                "0x" + "00" * 12 + token0_addr,  # 24 + 40 = 64 hex chars = 32 bytes
                "0x" + "00" * 12 + token1_addr,
            ],
            # V2 data: 12 zero bytes padding + 20 bytes pair address = 64 hex chars
            "data": "0x" + "00" * 12 + pair_addr_raw,
            "removed": False,
        }

        pair_tokens, deployer_events = parse_pair_created_logs([log_entry], "ethereum")
        assert len(pair_tokens) == 1
        assert len(deployer_events) == 1
        pair_addr = "0x" + pair_addr_raw.lower()
        assert pair_addr in pair_tokens
        assert pair_tokens[pair_addr] == (
            "0x" + token0_addr.lower(),
            "0x" + token1_addr.lower(),
        )
        assert deployer_events[0]["factory"] == "0xfactory"
        assert deployer_events[0]["version"] == "v2"


# ── State Tests ──────────────────────────────────────────────────────


class TestStateManager:
    """Test state file management."""

    def test_save_and_load(self):
        from lite_indexer.state import STATE_DIR, StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(type(STATE_DIR), "parent", Path(tmpdir)):
                manager = StateManager("ethereum")
                manager.save(last_block=100, events_count=50)

                loaded = manager.load()
                assert loaded.get("last_block") == 100
                assert loaded.get("events_indexed") == 50
                assert loaded.get("chain") == "ethereum"


class TestPairCache:
    """Test pair cache persistence."""

    def test_cache_persistence(self):
        from lite_indexer.state import PairCache

        with tempfile.TemporaryDirectory() as tmpdir:
            # Patch STATE_DIR to use temp dir by replacing its path
            patched_state_dir = Path(tmpdir) / "state"
            patched_state_dir.mkdir(parents=True, exist_ok=True)

            with patch("lite_indexer.state.STATE_DIR", patched_state_dir):
                cache = PairCache("ethereum")
                cache.set("0xpair1", ("0xtoken0", "0xtoken1"))
                cache.set("0xpair2", ("0xtoken2", "0xtoken3"))
                cache.save()

                # Create new cache instance — should load from disk
                cache2 = PairCache("ethereum")
                assert cache2.get("0xpair1") == ("0xtoken0", "0xtoken1")
                assert cache2.get("0xpair2") == ("0xtoken2", "0xtoken3")
                assert len(cache2) == 2


# ── Config Tests ─────────────────────────────────────────────────────


class TestChainConfig:
    """Test chain configuration."""

    def test_endpoints_exist(self):
        from lite_indexer.chains import GET_LOGS_ENDPOINTS

        assert "ethereum" in GET_LOGS_ENDPOINTS
        assert len(GET_LOGS_ENDPOINTS["ethereum"]) >= 1

    def test_poll_intervals_exist(self):
        from lite_indexer.chains import CHAIN_POLL_INTERVALS

        assert "ethereum" in CHAIN_POLL_INTERVALS
        assert CHAIN_POLL_INTERVALS["ethereum"] == 12.0

    def test_all_chains_have_endpoints(self):
        from lite_indexer.chains import GET_LOGS_ENDPOINTS

        for chain in GET_LOGS_ENDPOINTS:
            assert len(GET_LOGS_ENDPOINTS[chain]) > 0, f"{chain} has no endpoints"


# ── Config Constants Tests ───────────────────────────────────────────


class TestConfig:
    """Test config constants are consistent."""

    def test_required_topics(self):
        from lite_indexer.config import (
            PAIR_CREATED_TOPIC,
            POOL_CREATED_TOPIC,
            SWAP_V2_TOPIC,
            SWAP_V3_TOPIC,
            TRANSFER_EVENT_TOPIC,
        )

        for topic in [
            TRANSFER_EVENT_TOPIC,
            SWAP_V2_TOPIC,
            SWAP_V3_TOPIC,
            PAIR_CREATED_TOPIC,
            POOL_CREATED_TOPIC,
        ]:
            assert topic.startswith("0x")
            assert len(topic) == 66  # 0x + 64 hex chars

    def test_disk_cap_sane(self):
        from lite_indexer.config import BATCH_SIZE, DISK_CAP_BYTES

        assert DISK_CAP_BYTES > 0
        assert 0 < BATCH_SIZE <= 10000


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
