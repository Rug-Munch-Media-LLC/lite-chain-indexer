"""
Tests for lazy-indexing demand tracking (lite_indexer/chains/demand.py).

Covers the per-chain event type gating: top chains always index DEX +
deployer, long-tail chains start transfers-only and enable DEX/deployer
after the scanner signals demand (file touch), and enabled event types
persist in the state file across restarts.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer.indexer import LiteIndexer


@pytest.fixture(autouse=True)
def _clear_demand_registry():
    """The demand registry is a module global — clear it per test."""
    from lite_indexer.chains.demand import CHAIN_EVENT_DEMAND

    CHAIN_EVENT_DEMAND.clear()
    yield
    CHAIN_EVENT_DEMAND.clear()


# ── demand.py unit tests ───────────────────────────────────────────────


class TestDemandRegistry:
    """In-memory scanner demand registry."""

    def test_record_and_get(self):
        from lite_indexer.chains.demand import get_chain_demand, record_scan_demand

        record_scan_demand("scroll", "dex")
        assert get_chain_demand("scroll") == {"dex"}
        record_scan_demand("scroll", "deployer")
        assert get_chain_demand("scroll") == {"dex", "deployer"}
        assert get_chain_demand("ethereum") == set()

    def test_chain_normalized_lowercase(self):
        from lite_indexer.chains.demand import get_chain_demand, record_scan_demand

        record_scan_demand("Scroll", "dex")
        assert get_chain_demand("scroll") == {"dex"}

    def test_unknown_event_type_ignored(self):
        from lite_indexer.chains.demand import get_chain_demand, record_scan_demand

        record_scan_demand("scroll", "transfers")
        record_scan_demand("scroll", "")
        record_scan_demand("scroll", "DEX")  # not in DEMANDABLE_EVENT_TYPES
        assert get_chain_demand("scroll") == set()

    def test_get_returns_copy(self):
        from lite_indexer.chains.demand import get_chain_demand, record_scan_demand

        record_scan_demand("scroll", "dex")
        demand = get_chain_demand("scroll")
        demand.add("deployer")
        # Mutating the returned set must not affect the registry
        assert get_chain_demand("scroll") == {"dex"}


class TestDemandFiles:
    """File-touch demand signaling (scanner → indexer)."""

    def test_read_demand_files(self, monkeypatch, tmp_path: Path):
        from lite_indexer.chains import demand as demand_mod

        demand_dir = tmp_path / "indexer-demand"
        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", demand_dir)

        assert demand_mod.read_demand_files("scroll") == set()

        (demand_dir / "scroll").mkdir(parents=True)
        (demand_dir / "scroll" / "dex").touch()
        assert demand_mod.read_demand_files("scroll") == {"dex"}
        assert demand_mod.read_demand_files("SCROLL") == {"dex"}

        (demand_dir / "scroll" / "deployer").touch()
        assert demand_mod.read_demand_files("scroll") == {"dex", "deployer"}

    def test_demand_file_path(self, monkeypatch, tmp_path: Path):
        from lite_indexer.chains import demand as demand_mod

        demand_dir = tmp_path / "indexer-demand"
        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", demand_dir)
        assert demand_mod.demand_file_path("Scroll", "dex") == demand_dir / "scroll" / "dex"

    def test_read_demand_missing_dir_ok(self, monkeypatch, tmp_path: Path):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", tmp_path / "does-not-exist")
        assert demand_mod.read_demand_files("scroll") == set()

    def test_read_demand_files_ignores_directories(self, monkeypatch, tmp_path: Path):
        """A directory named 'dex' is NOT a demand signal (only files count)."""
        from lite_indexer.chains import demand as demand_mod

        demand_dir = tmp_path / "indexer-demand"
        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", demand_dir)
        (demand_dir / "scroll").mkdir(parents=True)
        (demand_dir / "scroll" / "dex").mkdir()  # directory, not a file

        assert demand_mod.read_demand_files("scroll") == set()

        (demand_dir / "scroll" / "deployer").touch()
        assert demand_mod.read_demand_files("scroll") == {"deployer"}


class TestDefaultEnabledEvents:
    """Default event types per chain under the lazy policy."""

    def test_top_chain_always_enabled(self, monkeypatch):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "LAZY_INDEXING_ENABLED", True)
        for chain in ("ethereum", "bsc", "polygon", "arbitrum", "base", "solana"):
            assert demand_mod.default_enabled_events(chain) == {"dex", "deployer"}, chain

    def test_long_tail_transfers_only(self, monkeypatch):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "LAZY_INDEXING_ENABLED", True)
        for chain in ("scroll", "mantle", "linea", "fantom", "avalanche", "gnosis"):
            assert demand_mod.default_enabled_events(chain) == set(), chain

    def test_lazy_disabled_legacy_behavior(self, monkeypatch):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "LAZY_INDEXING_ENABLED", False)
        assert demand_mod.default_enabled_events("scroll") == {"dex", "deployer"}
        assert demand_mod.default_enabled_events("ethereum") == {"dex", "deployer"}

    def test_default_enabled_events_case_insensitive_chain(self, monkeypatch):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "LAZY_INDEXING_ENABLED", True)
        assert demand_mod.default_enabled_events("Ethereum") == {"dex", "deployer"}
        assert demand_mod.default_enabled_events("SCROLL") == set()

    def test_default_enabled_events_unknown_chain_is_lazy(self, monkeypatch):
        from lite_indexer.chains import demand as demand_mod

        monkeypatch.setattr(demand_mod, "LAZY_INDEXING_ENABLED", True)
        # A chain not in TOP_CHAINS is treated as long-tail (transfers only).
        assert demand_mod.default_enabled_events("newchain") == set()

    def test_top_chains_and_lazy_chains_are_known(self):
        from lite_indexer.chains.demand import LAZY_CHAINS, TOP_CHAINS
        from lite_indexer.strategies import SUPPORTED_CHAINS

        # Every configured chain is either a top chain or a lazily-gated chain
        # (optimism/zksync/celo default to transfers-only like lazy chains)
        assert set() == TOP_CHAINS & LAZY_CHAINS
        for chain in TOP_CHAINS | LAZY_CHAINS:
            assert chain in SUPPORTED_CHAINS, f"{chain} not in SUPPORTED_CHAINS"


# ── LiteIndexer integration: _refresh_demand_enabled ────────────────────


@pytest.fixture()
def _indexer_env(monkeypatch, tmp_path: Path):
    """Point STATE_DIR + demand dir at tmp so LiteIndexer construction is hermetic."""
    from lite_indexer import state as state_mod

    monkeypatch.setattr(state_mod, "STATE_DIR", tmp_path / "state")
    return tmp_path


class TestLazyIndexingRefresh:
    """LiteIndexer._refresh_demand_enabled() lazy gating."""

    def test_top_chain_enabled_by_default(self, _indexer_env, tmp_path: Path):
        indexer = LiteIndexer(chain="ethereum", lazy_indexing=True, data_dir=tmp_path)
        assert indexer.dex_enabled is False
        assert indexer.deployers_enabled is False

        indexer._refresh_demand_enabled()

        assert indexer.dex_enabled is True
        assert indexer.deployers_enabled is True

    def test_long_tail_stays_transfers_only_without_demand(self, _indexer_env, tmp_path: Path):
        indexer = LiteIndexer(chain="scroll", lazy_indexing=True, data_dir=tmp_path)
        indexer._refresh_demand_enabled()
        assert indexer.dex_enabled is False
        assert indexer.deployers_enabled is False

    def test_long_tail_enabled_by_demand_file(self, monkeypatch, _indexer_env, tmp_path: Path):
        from lite_indexer.chains import demand as demand_mod

        demand_dir = tmp_path / "indexer-demand"
        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", demand_dir)
        (demand_dir / "scroll").mkdir(parents=True)
        (demand_dir / "scroll" / "dex").touch()

        indexer = LiteIndexer(chain="scroll", lazy_indexing=True, data_dir=tmp_path)
        indexer._refresh_demand_enabled()

        assert indexer.dex_enabled is True
        assert indexer.deployers_enabled is False
        assert demand_mod.get_chain_demand("scroll") == {"dex"}

    def test_long_tail_enabled_by_persisted_state(self, _indexer_env, tmp_path: Path):
        # First run: demand enables dex, state persists it
        from lite_indexer.chains import demand as demand_mod

        indexer = LiteIndexer(chain="mantle", lazy_indexing=True, data_dir=tmp_path)
        indexer.dex_enabled = True
        indexer.state.save(100, 0, enabled_events=indexer._enabled_event_types())

        # New process: no demand files, but state says dex was enabled
        demand_mod.CHAIN_EVENT_DEMAND.clear()
        fresh = LiteIndexer(chain="mantle", lazy_indexing=True, data_dir=tmp_path)
        assert fresh._persisted_enabled_events == {"dex"}
        fresh._refresh_demand_enabled()
        assert fresh.dex_enabled is True

    def test_lazy_disabled_is_noop(self, monkeypatch, _indexer_env, tmp_path: Path):
        from lite_indexer.chains import demand as demand_mod

        demand_dir = tmp_path / "indexer-demand"
        monkeypatch.setattr(demand_mod, "INDEXER_DEMAND_DIR", demand_dir)
        (demand_dir / "scroll").mkdir(parents=True)
        (demand_dir / "scroll" / "dex").touch()

        indexer = LiteIndexer(chain="scroll", lazy_indexing=False, data_dir=tmp_path)
        indexer._refresh_demand_enabled()

        # Legacy: CLI flags are the authority, demand files ignored
        assert indexer.dex_enabled is False
        assert indexer.deployers_enabled is False


class TestStatePersistence:
    """enabled_events round-trips through the state file."""

    def test_save_and_load_enabled_events(self, _indexer_env, tmp_path: Path):
        from lite_indexer.state import StateManager

        manager = StateManager("scroll")
        manager.save(100, 50, enabled_events=["dex", "deployer"])
        state = manager.load()
        # StateManager.save sorts the enabled set for deterministic writes
        assert state.get("enabled_events") == ["deployer", "dex"]

    def test_save_without_enabled_events(self, _indexer_env, tmp_path: Path):
        from lite_indexer.state import StateManager

        manager = StateManager("scroll")
        manager.save(100, 50)
        state = manager.load()
        assert "enabled_events" not in state
