"""Tests for Chain Configuration Registry."""

from __future__ import annotations

from lite_indexer.chain_registry import (
    EVM_PRESETS,
    ChainConfig,
    ChainType,
    add_chain_config,
    get_chain_config,
    list_supported_chains,
)


class TestChainConfig:
    """Tests for ChainConfig dataclass."""

    def test_create_chain_config(self):
        config = ChainConfig(
            name="testnet",
            chain_type=ChainType.EVM,
            chain_id=1337,
            rpc_endpoints=["https://rpc.testnet.com"],
            poll_interval_seconds=5,
            batch_size=1000,
            native_currency="TEST",
            block_time_seconds=2.0,
        )
        assert config.name == "testnet"
        assert config.chain_id == 1337
        assert config.native_currency == "TEST"

    def test_chain_config_defaults(self):
        config = ChainConfig(
            name="minimal",
            chain_type=ChainType.EVM,
        )
        assert config.chain_id == 0
        assert config.poll_interval_seconds == 12
        assert config.batch_size == 2000
        assert config.supports_dex is True


class TestChainType:
    """Tests for ChainType enum."""

    def test_evm_type(self):
        assert ChainType.EVM.value == "evm"

    def test_solana_type(self):
        assert ChainType.SOLANA.value == "solana"

    def test_tron_type(self):
        assert ChainType.TRON.value == "tron"

    def test_cosmos_type(self):
        assert ChainType.COSMOS.value == "cosmos"

    def test_aptos_type(self):
        assert ChainType.APTOS.value == "aptos"

    def test_sui_type(self):
        assert ChainType.SUI.value == "sui"

    def test_bitcoin_type(self):
        assert ChainType.BITCOIN.value == "bitcoin"


class TestGetChainConfig:
    """Tests for get_chain_config function."""

    def test_get_ethereum(self):
        config = get_chain_config("ethereum")
        assert config is not None
        assert config.chain_type == ChainType.EVM
        assert config.chain_id == 1
        assert len(config.rpc_endpoints) > 0

    def test_get_base(self):
        config = get_chain_config("base")
        assert config is not None
        assert config.chain_id == 8453

    def test_get_solana(self):
        config = get_chain_config("solana")
        assert config is not None
        assert config.chain_type == ChainType.SOLANA

    def test_get_tron(self):
        config = get_chain_config("tron")
        assert config is not None
        assert config.chain_type == ChainType.TRON

    def test_get_unknown_returns_none(self):
        config = get_chain_config("nonexistent_chain")
        assert config is None

    def test_case_insensitive(self):
        config_lower = get_chain_config("ethereum")
        config_upper = get_chain_config("ETHEREUM")
        assert config_lower is not None
        assert config_upper is not None
        assert config_lower.name == config_upper.name


class TestListSupportedChains:
    """Tests for list_supported_chains function."""

    def test_returns_list(self):
        chains = list_supported_chains()
        assert isinstance(chains, list)
        assert len(chains) > 0

    def test_includes_ethereum(self):
        chains = list_supported_chains()
        assert "ethereum" in chains

    def test_includes_solana(self):
        chains = list_supported_chains()
        assert "solana" in chains

    def test_includes_tron(self):
        chains = list_supported_chains()
        assert "tron" in chains

    def test_all_lowercase(self):
        chains = list_supported_chains()
        for chain in chains:
            assert chain == chain.lower()


class TestAddChainConfig:
    """Tests for add_chain_config function."""

    def test_add_new_chain(self):
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

    def test_overwrite_existing_chain(self):
        original = get_chain_config("ethereum")
        assert original is not None
        modified = ChainConfig(
            name="ethereum_custom",
            chain_type=ChainType.EVM,
            chain_id=1,
            rpc_endpoints=["https://custom.rpc.com"],
        )
        add_chain_config("ethereum", modified)
        retrieved = get_chain_config("ethereum")
        assert retrieved is not None
        assert retrieved.rpc_endpoints == ["https://custom.rpc.com"]
        # Restore original
        add_chain_config("ethereum", original)


class TestEVMPresets:
    """Tests for EVM_PRESETS dictionary."""

    def test_has_multiple_chains(self):
        assert len(EVM_PRESETS) >= 10

    def test_all_configs_are_chain_config(self):
        for name, config in EVM_PRESETS.items():
            assert isinstance(config, ChainConfig)
            assert config.name == name

    def test_ethereum_has_dex_factories(self):
        eth = EVM_PRESETS.get("ethereum")
        assert eth is not None
        assert len(eth.dex_factory_addresses) > 0

    def test_solana_has_correct_type(self):
        sol = EVM_PRESETS.get("solana")
        assert sol is not None
        assert sol.chain_type == ChainType.SOLANA

    def test_tron_has_correct_type(self):
        tron = EVM_PRESETS.get("tron")
        assert tron is not None
        assert tron.chain_type == ChainType.TRON

    def test_all_chains_have_rpc_endpoints(self):
        for name, config in EVM_PRESETS.items():
            assert len(config.rpc_endpoints) > 0, f"{name} has no RPC endpoints"

    def test_all_chains_have_poll_interval(self):
        for name, config in EVM_PRESETS.items():
            assert config.poll_interval_seconds > 0, f"{name} has invalid poll interval"

    def test_all_chains_have_block_time(self):
        for name, config in EVM_PRESETS.items():
            assert config.block_time_seconds > 0, f"{name} has invalid block time"
