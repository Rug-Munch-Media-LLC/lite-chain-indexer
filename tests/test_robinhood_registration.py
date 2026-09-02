"""Tests for the Robinhood chain registration (Arbitrum Orbit L2, chainId 78600).

Covers every registration point added 2026-08-13: the RPC endpoint pool
(``GET_LOGS_ENDPOINTS`` + last-resort), the polling interval, the
``eth_getLogs`` block-range cap, the config budget/priority, the
Chainlist chainId mapping and the strategy registry. No live RPC — all
assertions are against the static registration tables.
"""

from __future__ import annotations

from lite_indexer.chains.endpoints import (
    GET_LOGS_ENDPOINTS,
    LAST_RESORT_ENDPOINTS,
    last_resort_endpoints,
)
from lite_indexer.chains.limits import CHAIN_BLOCK_RANGE_LIMITS
from lite_indexer.chains.polling import CHAIN_POLL_INTERVALS
from lite_indexer.config import CHAIN_MEMORY_BUDGET_MB, CHAIN_PRIORITY
from lite_indexer.rpc.discovery import chain_id_for
from lite_indexer.strategies import SUPPORTED_CHAINS, create_strategy
from lite_indexer.strategies.evm import EVMChainStrategy

ROBINHOOD_ENDPOINT = "https://rpc.mainnet.chain.robinhood.com"  # official, fully indexable (verified 2026-09-01)
ROBINHOOD_CHAIN_ID = 4663  # 0x1237 — old spec 78600 was wrong (chainlist mapped it to Vanguard)


class TestRobinhoodEndpoints:
    def test_official_endpoint_registered(self):
        """Official endpoint is T1 (first) in the expanded 2026-09-01 pool."""
        assert GET_LOGS_ENDPOINTS["robinhood"][0] == ROBINHOOD_ENDPOINT

    def test_endpoint_is_https_and_placeholder_free(self):
        urls = GET_LOGS_ENDPOINTS["robinhood"]
        assert urls
        for url in urls:
            assert url.startswith("https://")
            assert "${" not in url

    def test_last_resort_includes_official_endpoint(self):
        # LAST_RESORT still lists the legacy rpc.robinhood.org host.
        assert LAST_RESORT_ENDPOINTS["robinhood"] == ["https://rpc.robinhood.org"]
        assert "https://rpc.robinhood.org" in last_resort_endpoints("robinhood")

    def test_no_duplicate_endpoints(self):
        assert len(GET_LOGS_ENDPOINTS["robinhood"]) == len(set(GET_LOGS_ENDPOINTS["robinhood"]))


class TestRobinhoodPollingAndLimits:
    def test_poll_interval_registered(self):
        # ~0.25s block time on an Orbit L2 → 1s poll.
        assert CHAIN_POLL_INTERVALS["robinhood"] == 1.0

    def test_block_range_cap_registered(self):
        assert CHAIN_BLOCK_RANGE_LIMITS["robinhood"] == 1000

    def test_memory_budget_registered(self):
        assert CHAIN_MEMORY_BUDGET_MB["robinhood"] == 384

    def test_priority_registered(self):
        assert CHAIN_PRIORITY["robinhood"] == 5


class TestRobinhoodChainId:
    def test_chain_id_for_robinhood(self):
        assert chain_id_for("robinhood") == ROBINHOOD_CHAIN_ID


class TestRobinhoodStrategyRegistry:
    def test_supported_chains_entry(self):
        assert SUPPORTED_CHAINS["robinhood"] == {"type": "evm", "label": "Robinhood"}

    def test_create_strategy_returns_evm(self):
        strategy = create_strategy("robinhood")
        assert isinstance(strategy, EVMChainStrategy)
