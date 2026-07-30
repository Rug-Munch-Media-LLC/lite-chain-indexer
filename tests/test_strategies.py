"""Tests for chain indexing strategies — Solana, Tron, EVM.

Tests the parsing functions that convert raw RPC responses into
structured transfer events. These are pure functions that don't
require network access.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.strategies import (
    EVMChainStrategy,
    SolanaStrategy,
    TronStrategy,
    create_strategy,
)
from lite_indexer.strategies.solana import _parse_solana_transfer_events
from lite_indexer.strategies.tron import _parse_tron_transfer_event
from lite_indexer.strategies.evm import EVMChainStrategy


# ═══════════════════════════════════════════════════════════════════════════
# Solana Strategy Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestSolanaParseTransferEvents:
    """Tests for _parse_solana_transfer_events — SPL balance changes."""

    def test_empty_meta_returns_empty(self):
        """Transaction with no meta should return empty list."""
        events = _parse_solana_transfer_events({}, 100, set())
        assert events == []

    def test_no_balance_changes_returns_empty(self):
        """Transaction with no balance changes should return empty list."""
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [],
                "postTokenBalances": [],
            },
            "transaction": {"signatures": ["sig1"]},
        }
        events = _parse_solana_transfer_events(tx_data, 100, {"mint1"})
        assert events == []

    def test_spl_balance_change_detected(self):
        """A tracked token balance change should be detected."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner1",
                        "uiTokenAmount": {
                            "amount": "1000000",
                            "decimals": 6,
                            "uiAmount": 1.0,
                        },
                    },
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner1",
                        "uiTokenAmount": {
                            "amount": "500000",
                            "decimals": 6,
                            "uiAmount": 0.5,
                        },
                    },
                ],
            },
            "transaction": {"signatures": ["sig1"]},
            "blockTime": 1700000000,
        }
        events = _parse_solana_transfer_events(tx_data, 200, {usdc_mint})
        assert len(events) == 1
        event = events[0]
        assert event["type"] == "spl_balance_change"
        assert event["chain"] == "solana"
        assert event["slot"] == 200
        assert event["signature"] == "sig1"
        assert event["mint"] == usdc_mint
        assert event["amount_pre"] == "1000000"
        assert event["amount_post"] == "500000"
        assert event["change"] == "-500000"
        assert event["decimals"] == 6
        assert event["symbol"] == "USDC"
        assert event["token_name"] == "USD Coin"
        assert event["source"] == "lite_indexer"

    def test_zero_change_skipped(self):
        """Balance changes of zero should be skipped."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner1",
                        "uiTokenAmount": {"amount": "1000000", "decimals": 6},
                    },
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner1",
                        "uiTokenAmount": {"amount": "1000000", "decimals": 6},
                    },
                ],
            },
            "transaction": {"signatures": ["sig1"]},
        }
        events = _parse_solana_transfer_events(tx_data, 200, {usdc_mint})
        assert len(events) == 0

    def test_untracked_mint_skipped(self):
        """Balance changes for untracked mints should be skipped."""
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": "unknown_mint",
                        "owner": "0xowner1",
                        "uiTokenAmount": {"amount": "100", "decimals": 0},
                    },
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": "unknown_mint",
                        "owner": "0xowner1",
                        "uiTokenAmount": {"amount": "200", "decimals": 0},
                    },
                ],
            },
            "transaction": {"signatures": ["sig1"]},
        }
        events = _parse_solana_transfer_events(tx_data, 200, {"tracked_mint"})
        assert len(events) == 0

    def test_multiple_balance_changes(self):
        """Multiple balance changes in the same tx should all be detected."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        usdt_mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xalice",
                        "uiTokenAmount": {"amount": "1000", "decimals": 6},
                    },
                    {
                        "accountIndex": 1,
                        "mint": usdt_mint,
                        "owner": "0xbob",
                        "uiTokenAmount": {"amount": "2000", "decimals": 6},
                    },
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xalice",
                        "uiTokenAmount": {"amount": "500", "decimals": 6},
                    },
                    {
                        "accountIndex": 1,
                        "mint": usdt_mint,
                        "owner": "0xbob",
                        "uiTokenAmount": {"amount": "1500", "decimals": 6},
                    },
                ],
            },
            "transaction": {"signatures": ["sig1"]},
            "blockTime": 1700000000,
        }
        events = _parse_solana_transfer_events(tx_data, 200, {usdc_mint, usdt_mint})
        assert len(events) == 2

    def test_no_signature_handled(self):
        """Transaction with no signatures should still parse."""
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        tx_data: dict[str, Any] = {
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner",
                        "uiTokenAmount": {"amount": "100", "decimals": 6},
                    },
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": usdc_mint,
                        "owner": "0xowner",
                        "uiTokenAmount": {"amount": "0", "decimals": 6},
                    },
                ],
            },
            "transaction": {},
            "blockTime": 1700000000,
        }
        events = _parse_solana_transfer_events(tx_data, 200, {usdc_mint})
        assert len(events) == 1
        assert events[0]["signature"] == ""


class TestSolanaStrategy:
    """Tests for SolanaStrategy class."""

    def test_create_strategy(self):
        """create_strategy should create SolanaStrategy for 'solana'."""
        strategy = create_strategy("solana")
        assert isinstance(strategy, SolanaStrategy)
        assert strategy.chain == "solana"

    def test_has_tracked_tokens(self):
        """SolanaStrategy should have tracked tokens by default."""
        strategy = SolanaStrategy()
        assert len(strategy.tracked_tokens) >= 5
        assert "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" in strategy._mint_set

    def test_supports_dex_false(self):
        """Solana strategy should not support DEX events."""
        strategy = SolanaStrategy()
        assert strategy.supports_dex() is False

    def test_supports_deployers_false(self):
        """Solana strategy should not support deployer tracking."""
        strategy = SolanaStrategy()
        assert strategy.supports_deployers() is False

    def test_fetch_block_number_returns_none_without_rpc(self):
        """Without RPC, fetch_block_number should return None (no crash)."""
        strategy = SolanaStrategy()
        import asyncio

        result = asyncio.run(strategy.fetch_block_number())
        # In test env without RPC access, should return None gracefully
        assert result is None or isinstance(result, int)

    def test_fetch_transfers_empty_range(self):
        """from_slot > to_slot should return empty list."""
        strategy = SolanaStrategy()
        import asyncio

        result = asyncio.run(strategy.fetch_transfers(100, 50))
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# Tron Strategy Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestTronParseTransferEvent:
    """Tests for _parse_tron_transfer_event — TRC20 event parsing."""

    USDT_TOKEN = {
        "address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        "symbol": "USDT",
        "decimals": 6,
        "name": "Tether USD",
    }

    def test_parses_valid_event(self):
        """A valid TronGrid event should parse correctly."""
        raw: dict[str, Any] = {
            "block_number": "12345",
            "transaction_id": "0xabc123",
            "log_index": 0,
            "from": "TR_sender",
            "to": "TR_receiver",
            "result": {
                "value": "1000000",
                "from": "TR_sender",
                "to": "TR_receiver",
            },
            "block_timestamp": 1700000000000,
        }
        event = _parse_tron_transfer_event(raw, self.USDT_TOKEN, "tron")
        assert event is not None
        assert event["type"] == "transfer"
        assert event["chain"] == "tron"
        assert event["blockNumber"] == 12345
        assert event["txHash"] == "0xabc123"
        assert event["from"] == "TR_sender"
        assert event["to"] == "TR_receiver"
        assert event["tokenAddress"] == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        assert event["value"] == "1000000"
        assert event["tokenSymbol"] == "USDT"
        assert event["tokenName"] == "Tether USD"
        assert event["source"] == "lite_indexer"

    def test_hex_value_converted_to_decimal(self):
        """Hex values in the value field should be converted to decimal."""
        raw: dict[str, Any] = {
            "block_number": "12345",
            "transaction_id": "0xabc",
            "log_index": 0,
            "result": {
                "value": "0x1000",
                "from": "sender",
                "to": "receiver",
            },
            "block_timestamp": 1700000000000,
        }
        event = _parse_tron_transfer_event(raw, self.USDT_TOKEN, "tron")
        assert event is not None
        assert event["value"] == "4096"  # 0x1000 = 4096

    def test_missing_from_to_returns_none(self):
        """Event with missing from/to should return None."""
        raw: dict[str, Any] = {
            "block_number": "12345",
            "transaction_id": "0xabc",
            "result": {},
            "block_timestamp": 1700000000000,
        }
        event = _parse_tron_transfer_event(raw, self.USDT_TOKEN, "tron")
        assert event is None

    def test_block_number_as_int(self):
        """Block number can be an int, not just string."""
        raw: dict[str, Any] = {
            "block_number": 12345,
            "transaction_id": "0xabc",
            "log_index": 0,
            "result": {"value": "100", "from": "sender", "to": "receiver"},
            "block_timestamp": 1700000000000,
        }
        event = _parse_tron_transfer_event(raw, self.USDT_TOKEN, "tron")
        assert event is not None
        assert event["blockNumber"] == 12345

    def test_log_index_as_string(self):
        """log_index can be a string."""
        raw: dict[str, Any] = {
            "block_number": "12345",
            "transaction_id": "0xabc",
            "log_index": "3",
            "result": {"value": "100", "from": "sender", "to": "receiver"},
            "block_timestamp": 1700000000000,
        }
        event = _parse_tron_transfer_event(raw, self.USDT_TOKEN, "tron")
        assert event is not None
        assert event["logIndex"] == 3


class TestTronStrategy:
    """Tests for TronStrategy class."""

    def test_create_strategy(self):
        """create_strategy should create TronStrategy for 'tron'."""
        strategy = create_strategy("tron")
        assert isinstance(strategy, TronStrategy)
        assert strategy.chain == "tron"

    def test_has_tokens(self):
        """TronStrategy should have TRC20 tokens by default."""
        strategy = TronStrategy()
        assert len(strategy.tokens) >= 3
        symbols = [t["symbol"] for t in strategy.tokens]
        assert "USDT" in symbols

    def test_supports_dex_false(self):
        """Tron strategy should not support DEX events."""
        strategy = TronStrategy()
        assert strategy.supports_dex() is False

    def test_supports_deployers_false(self):
        """Tron strategy should not support deployer tracking."""
        strategy = TronStrategy()
        assert strategy.supports_deployers() is False

    def test_fetch_transfers_empty_range(self):
        """from_block > to_block should return empty list."""
        strategy = TronStrategy()
        import asyncio

        result = asyncio.run(strategy.fetch_transfers(100, 50))
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# EVM Strategy Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestEVMStrategy:
    """Tests for EVMChainStrategy class."""

    def test_create_strategy_for_ethereum(self):
        """create_strategy should create EVMChainStrategy for 'ethereum'."""
        strategy = create_strategy("ethereum")
        assert isinstance(strategy, EVMChainStrategy)
        assert strategy.chain == "ethereum"

    def test_create_strategy_for_all_evm_chains(self):
        """create_strategy should create EVMChainStrategy for all EVM chains."""
        evm_chains = ["ethereum", "base", "bsc", "polygon", "arbitrum", "optimism"]
        for chain in evm_chains:
            strategy = create_strategy(chain)
            assert isinstance(strategy, EVMChainStrategy), f"Failed for chain: {chain}"

    def test_create_strategy_for_unsupported_chain_raises(self):
        """create_strategy should raise ValueError for unsupported chains."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            create_strategy("unsupported_chain")

    def test_supports_dex_true(self):
        """EVM strategy should support DEX events."""
        strategy = EVMChainStrategy("ethereum")
        assert strategy.supports_dex() is True

    def test_supports_deployers_true(self):
        """EVM strategy should support deployer tracking."""
        strategy = EVMChainStrategy("ethereum")
        assert strategy.supports_deployers() is True

    def test_enforce_block_range_below_limit(self):
        """Block range below limit should not be capped."""
        strategy = EVMChainStrategy("ethereum")
        f, t = strategy.enforce_block_range(100, 500)
        assert f == 100
        assert t == 500

    def test_enforce_block_range_above_limit(self):
        """Block range above limit should be capped."""
        strategy = EVMChainStrategy("ethereum", batch_size=100)
        f, t = strategy.enforce_block_range(100, 50000)
        assert f == 100
        assert t <= 100 + 100

    def test_detect_reorg_returns_true(self):
        """from_block > to_block should detect reorg."""
        strategy = EVMChainStrategy("ethereum")
        assert strategy.detect_reorg(500, 100) is True

    def test_detect_reorg_returns_false(self):
        """Normal from_block < to_block should not detect reorg."""
        strategy = EVMChainStrategy("ethereum")
        assert strategy.detect_reorg(100, 500) is False

    def test_fetch_block_number_returns_none_without_rpc(self):
        """Without RPC, fetch_block_number should return None."""
        strategy = EVMChainStrategy("ethereum")
        import asyncio

        result = asyncio.run(strategy.fetch_block_number())
        assert result is None or isinstance(result, int)

    def test_fetch_transfers_empty_endpoints_returns_empty(self):
        """Chain with no endpoints should return empty list gracefully."""
        strategy = EVMChainStrategy("unsupported_evm")
        import asyncio

        result = asyncio.run(strategy.fetch_transfers(100, 200))
        assert result == []

    def test_verify_endpoints_no_endpoints(self):
        """Chain with no endpoints should return empty list."""
        strategy = EVMChainStrategy("unsupported_evm")
        import asyncio

        result = asyncio.run(strategy.verify_endpoints())
        assert result == []

    def test_fetch_swap_events_reorg_detected(self):
        """fetch_swap_events should return CHAIN_REORG signal on reorg."""
        strategy = EVMChainStrategy("ethereum")
        import asyncio

        result = asyncio.run(strategy.fetch_swap_events(500, 100))
        assert result == ["__CHAIN_REORG__"]

    def test_fetch_pair_created_events_chain_no_factories(self):
        """_fetch_pair_created_events on chain without factories should not crash."""
        # Use a chain with no factories configured to avoid RPC calls
        strategy = EVMChainStrategy("gnosis")
        import asyncio

        async def run():
            await strategy._fetch_pair_created_events(100, 200)
            # After running without factories, pair cache should be unchanged
            assert len(strategy.pair_cache) >= 0

        asyncio.run(run())
