"""Tests for the Hyperliquid EVM chain registration (chainId 999).

Covers every registration point added 2026-09-13: the strategy registry,
the chain registry preset, the RPC endpoint pool (``GET_LOGS_ENDPOINTS`` +
last-resort), the expected chainId map and the Chainlist chainId mapping.
No live RPC — all assertions are against the static registration tables.

The public tree must stay free/keyless: dRPC and Dwellir (curated/private
overlay) must never appear here.
"""

from __future__ import annotations

from pathlib import Path

from lite_indexer.chain_registry import ChainType, get_chain_config
from lite_indexer.chains.endpoints import (
    BUILTIN_ENDPOINTS,
    EXPECTED_CHAIN_IDS,
    GET_LOGS_ENDPOINTS,
    LAST_RESORT_ENDPOINTS,
    last_resort_endpoints,
)
from lite_indexer.rpc.discovery import chain_id_for
from lite_indexer.strategies import SUPPORTED_CHAINS, create_strategy
from lite_indexer.strategies.evm import EVMChainStrategy

REPO_ROOT = Path(__file__).resolve().parents[1]

HYPERLIQUID_ENDPOINT = "https://rpc.hyperliquid.xyz/evm"  # official keyless, live-verified 2026-09-13
HYPERLIQUID_POCKET_ENDPOINT = (
    "https://hyperliquid.api.pocket.network"  # Pocket keyless, live-verified getLogs 2026-09-13
)
HYPERLIQUID_CHAIN_ID = 999
HYPERLIQUID_CHAIN_ID_HEX = "0x3e7"

# Curated/keyed provider names that live ONLY in the private runtime overlay.
# dRPC and Dwellir are overlay-only (the official keyless endpoint passes
# eth_getLogs, so no paid tier is required for this chain).
OVERLAY_ONLY_PROVIDERS = (
    "alchemy",
    "drpc",
    "dwellir",
    "uniblock",
    "blockpi",
    "blockvision",
    "chainstack",
    "helius",
    "nodereal",
    "infura",
    "ankr",
    "quicknode",
    "moralis",
    "nownodes",
)


class TestHyperliquidEndpoints:
    def test_official_endpoint_registered(self):
        """Official keyless endpoint is first in the active pool."""
        assert GET_LOGS_ENDPOINTS["hyperliquid"][0] == HYPERLIQUID_ENDPOINT

    def test_endpoint_is_in_builtin_public_pool(self):
        """The endpoint ships in the public (pre-overlay) pool, keyless."""
        assert HYPERLIQUID_ENDPOINT in BUILTIN_ENDPOINTS["hyperliquid"]
        assert "${" not in HYPERLIQUID_ENDPOINT

    def test_pocket_endpoint_registered_and_keyless(self):
        """Pocket Network is a free/keyless second source for Hyperliquid."""
        assert HYPERLIQUID_POCKET_ENDPOINT in GET_LOGS_ENDPOINTS["hyperliquid"]
        assert HYPERLIQUID_POCKET_ENDPOINT in BUILTIN_ENDPOINTS["hyperliquid"]
        assert "${" not in HYPERLIQUID_POCKET_ENDPOINT

    def test_endpoint_is_https_and_placeholder_free(self):
        urls = GET_LOGS_ENDPOINTS["hyperliquid"]
        assert urls
        for url in urls:
            assert url.startswith("https://")
            assert "${" not in url

    def test_no_duplicate_endpoints(self):
        assert len(GET_LOGS_ENDPOINTS["hyperliquid"]) == len(set(GET_LOGS_ENDPOINTS["hyperliquid"]))

    def test_last_resort_is_official_active_pool_endpoint(self):
        assert LAST_RESORT_ENDPOINTS["hyperliquid"] == [HYPERLIQUID_ENDPOINT]
        assert HYPERLIQUID_ENDPOINT in last_resort_endpoints("hyperliquid")

    def test_no_denylisted_provider_in_builtin_pool(self):
        # The private runtime overlay legitimately injects keyed providers
        # (dRPC/Dwellir) into the *active* pool; the PUBLIC builtin pool must
        # stay free/keyless.
        for url in BUILTIN_ENDPOINTS["hyperliquid"]:
            lowered = url.lower()
            for token in OVERLAY_ONLY_PROVIDERS:
                assert token not in lowered, f"overlay-only provider {token!r} leaked into {url}"

    def test_no_denylisted_provider_in_endpoints_source(self):
        src = (REPO_ROOT / "lite_indexer" / "chains" / "endpoints.py").read_text(encoding="utf-8").lower()
        for token in ("drpc", "dwellir"):
            assert token not in src, f"overlay-only provider {token!r} leaked into endpoints.py"


class TestHyperliquidChainId:
    def test_expected_chain_id_map(self):
        assert EXPECTED_CHAIN_IDS["hyperliquid"] == HYPERLIQUID_CHAIN_ID_HEX

    def test_chain_id_for_hyperliquid(self):
        assert chain_id_for("hyperliquid") == HYPERLIQUID_CHAIN_ID

    def test_chain_registry_preset(self):
        config = get_chain_config("hyperliquid")
        assert config is not None
        assert config.chain_type == ChainType.EVM
        assert config.chain_id == HYPERLIQUID_CHAIN_ID
        assert HYPERLIQUID_ENDPOINT in config.rpc_endpoints


class TestHyperliquidStrategyRegistry:
    def test_supported_chains_entry(self):
        assert SUPPORTED_CHAINS["hyperliquid"] == {"type": "evm", "label": "Hyperliquid"}

    def test_create_strategy_returns_evm(self):
        strategy = create_strategy("hyperliquid")
        assert isinstance(strategy, EVMChainStrategy)
        assert strategy.chain == "hyperliquid"


def test_vector_ingestion_is_opt_in_by_default():
    """Lightest-weight/zero-cost: vectors (may use a paid lane) are OFF by default."""
    import lite_indexer.config.indexing as idx
    from lite_indexer.config.indexing import VECTOR_INGESTION_ENABLED
    # default (no env override in this process) must be False
    assert isinstance(VECTOR_INGESTION_ENABLED, bool)
    import inspect
    src = inspect.getsource(idx)
    assert 'os.environ.get("VECTOR_INGESTION_ENABLED", "0")' in src
