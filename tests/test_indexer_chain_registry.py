"""Tests for chain registry integration with indexer."""

from __future__ import annotations

import pytest

from lite_indexer.chain_registry import (
    ChainConfig,
    ChainType,
    EVM_PRESETS,
    add_chain_config,
    get_chain_config,
    list_supported_chains,
)
from lite_indexer.indexer import LiteIndexer


class TestIndexerChainRegistry:
    """Tests that LiteIndexer integrates with chain registry."""

    def test_indexer_has_chain_config(self):
        """Indexer should have chain_config property."""
        indexer = LiteIndexer(chain="ethereum")
        assert indexer._chain_config is not None
        assert isinstance(indexer._chain_config, ChainConfig)

    def test_indexer_chain_config_has_rpc_endpoints(self):
        """Indexer chain config should have RPC endpoints."""
        indexer = LiteIndexer(chain="ethereum")
        config = indexer._chain_config
        assert config is not None
        assert len(config.rpc_endpoints) > 0

    def test_indexer_unknown_chain_no_config(self):
        """Indexer with unknown chain should have None config."""
        indexer = LiteIndexer(chain="unknown_chain")
        assert indexer._chain_config is None

    def test_indexer_uses_chain_registry(self):
        """Indexer should use chain registry for configuration."""
        indexer = LiteIndexer(chain="base")
        config = indexer._chain_config
        assert config is not None
        assert config.chain_type == ChainType.EVM
        assert config.chain_id == 8453

    def test_list_supported_chains_in_indexer(self):
        """list_supported_chains should return chains usable by indexer."""
        chains = list_supported_chains()
        assert "ethereum" in chains
        assert "solana" in chains
        assert "tron" in chains

    def test_add_chain_config_runtime(self):
        """Should be able to add chain config at runtime."""
        new_config = ChainConfig(
            name="testnet_1",
            chain_type=ChainType.EVM,
            chain_id=9999,
            rpc_endpoints=["https://rpc.testnet1.com"],
        )
        add_chain_config("testnet_1", new_config)
        retrieved = get_chain_config("testnet_1")
        assert retrieved is not None
        assert retrieved.chain_id == 9999
