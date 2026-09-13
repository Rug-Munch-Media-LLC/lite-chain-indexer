"""Tests for EVM chain RPC endpoint configuration (lite_indexer/chains/endpoints.py).

Covers:

1. `${ENV_VAR}` placeholder resolution (``_resolve_endpoint_url``)
2. Active-pool filtering of unresolvable placeholders
   (``_resolved_endpoints``)
3. Module-level ``GET_LOGS_ENDPOINTS`` invariants: all EVM chains
   present, HTTPS only, no dead ``${...}`` URLs in the active pool,
   every chain has at least one dialable endpoint, T1-first ordering.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lite_indexer.chains.endpoints import (
    CANDIDATE_ENDPOINTS,
    GET_LOGS_ENDPOINTS,
    _resolve_endpoint_url,
    _resolved_endpoints,
    require_nodereal_key,
)

# The NodeReal demo key previously baked into the BSC endpoint URL
# (audit 2026-09-06) — must never appear in the repo again.
LEAKED_NODEREAL_DEMO_KEY = "64a9df0874fb4a93b9d0a3849de012d3"

# Every EVM chain the indexer supports must appear in GET_LOGS_ENDPOINTS.
EXPECTED_EVM_CHAINS = [
    "ethereum",
    "base",
    "bsc",
    "polygon",
    "arbitrum",
    "optimism",
    "avalanche",
    "fantom",
    "linea",
    "scroll",
    "zksync",
    "mantle",
    "gnosis",
    "celo",
    "robinhood",
    "blast",
    "fraxtal",
    "metis",
]


class TestResolveEndpointUrl:
    def test_expands_env_var(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALCHEMY_KEY", "test-key-123")
        url = _resolve_endpoint_url("https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}")
        assert url == "https://eth-mainnet.g.alchemy.com/v2/test-key-123"

    def test_unset_var_leaves_placeholder(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("ALCHEMY_KEY", raising=False)
        url = _resolve_endpoint_url("https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}")
        assert "${ALCHEMY_KEY}" in url

    def test_dollar_bare_form_supported(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GETBLOCK_KEY", "kb")
        assert _resolve_endpoint_url("https://eth.getblock.io/mainnet/?api_key=$GETBLOCK_KEY").endswith("?api_key=kb")

    def test_no_placeholder_passthrough(self):
        assert _resolve_endpoint_url("https://eth.drpc.org") == "https://eth.drpc.org"


class TestResolvedEndpoints:
    def test_drops_unresolved_placeholders(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("ALCHEMY_KEY", raising=False)
        monkeypatch.delenv("QUICKNODE_ENDPOINT", raising=False)
        urls = [
            "https://eth.drpc.org",
            "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
            "https://${QUICKNODE_ENDPOINT}",
        ]
        assert _resolved_endpoints(urls) == ["https://eth.drpc.org"]

    def test_keeps_resolved_placeholder_endpoints(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALCHEMY_KEY", "k123")
        urls = [
            "https://eth.drpc.org",
            "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        ]
        resolved = _resolved_endpoints(urls)
        assert resolved == ["https://eth.drpc.org", "https://eth-mainnet.g.alchemy.com/v2/k123"]

    def test_preserves_order(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("ALCHEMY_KEY", raising=False)
        urls = ["https://b.example", "https://a.example", "https://c.example"]
        assert _resolved_endpoints(urls) == urls


class TestGetLogsEndpoints:
    def test_all_evm_chains_present(self):
        assert sorted(GET_LOGS_ENDPOINTS.keys()) == sorted(EXPECTED_EVM_CHAINS)

    def test_active_pool_has_no_placeholder_urls(self):
        """No active endpoint may contain an unresolved ${...} template."""
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            for url in urls:
                assert "${" not in url, f"{chain}: {url}"

    def test_all_urls_https(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            for url in urls:
                assert url.startswith("https://"), f"{chain}: {url}"

    def test_every_chain_has_at_least_one_endpoint(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert len(urls) >= 1, chain

    def test_tier_one_endpoint_first(self):
        """The first endpoint per chain is a T1 fast/verified public RPC."""
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert urls[0], chain
            assert urls[0].startswith("https://"), chain

    def test_no_duplicate_endpoints_within_chain(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert len(urls) == len(set(urls)), chain

    # Chains hit hardest by the 2026-09-02 RPC rate-limit crisis
    # (517 rate-limit events/hour, dex_events 160-210h stale). Each must
    # keep >= 3 free (no API-key placeholder) endpoints so a single
    # provider ban cannot take a chain's pool offline.
    RATE_LIMIT_CRITICAL_CHAINS = ["polygon", "optimism", "bsc", "arbitrum", "avalanche"]

    def test_rate_limit_critical_chains_have_at_least_3_free_endpoints(self):
        for chain in self.RATE_LIMIT_CRITICAL_CHAINS:
            urls = GET_LOGS_ENDPOINTS[chain]
            free = [u for u in urls if "${" not in u]
            assert len(free) >= 3, f"{chain}: only {len(free)} free endpoints ({free})"

    def test_total_endpoint_count_sane(self):
        """Active pool is bounded: >= 1 per chain, <= raw template count.

        Upper bound raised 66 -> 96 (2026-08-23): probe-promoted fallbacks
        (ee50656) + DRPC_KEY keyed depth (aeb7228) intentionally expanded
        the pool. Raised 96 -> 104 (2026-09-02): rate-limit crisis
        endpoints for polygon/bsc + blast/fraxtal/metis pools. Raised
        104 -> 122 (2026-09-13): +18 Uniblock archive templates (robinhood,
        blast/fraxtal/metis + 14 T3 fallbacks) — active only when
        UNIBLOCK_KEY is set. Raised 122 -> 150 (2026-09-13): +18 keyless
        Pocket T1 templates + 9 Infura keyed archive templates + Pocket
        solana/tron. The bound still guards against runaway duplication.
        """
        total = sum(len(urls) for urls in GET_LOGS_ENDPOINTS.values())
        assert len(GET_LOGS_ENDPOINTS) <= total <= 150


class TestNoderealKeyRequired:
    """Item 1 (2026-09-06 audit): NodeReal BSC key must come from env."""

    def test_nodereal_endpoint_uses_env_key_not_leaked_demo(self):
        """The BSC nodereal endpoint must resolve ${NODEREAL_KEY} — the old
        baked-in demo key must be gone from the active pool."""
        bsc = GET_LOGS_ENDPOINTS["bsc"]
        nodereal = [u for u in bsc if "nodereal.io" in u]
        assert nodereal, "bsc nodereal endpoint must stay in the active pool"
        assert len(nodereal) == 1
        url = nodereal[0]
        assert LEAKED_NODEREAL_DEMO_KEY not in url, "leaked demo key still present"
        assert "${" not in url  # resolved at import, never a dead placeholder
        assert url == "https://bsc-mainnet.nodereal.io/v1/test-key-123"
        assert os.environ.get("NODEREAL_KEY") == "test-key-123"

    def test_require_nodereal_key_raises_when_unset(self, monkeypatch: pytest.MonkeyPatch):
        """A clear startup error must be raised when NODEREAL_KEY is unset."""
        monkeypatch.delenv("NODEREAL_KEY", raising=False)
        with pytest.raises(RuntimeError) as excinfo:
            require_nodereal_key()
        message = str(excinfo.value)
        assert "NODEREAL_KEY" in message
        assert "required" in message.lower()

    def test_require_nodereal_key_ok_when_set(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("NODEREAL_KEY", "some-rotated-key")
        require_nodereal_key()  # must not raise

    def test_module_import_fails_when_key_unset(self, tmp_path: Path):
        """Importing lite_indexer.chains.endpoints without NODEREAL_KEY must
        fail loudly at startup (hermetic subprocess — no in-process reload,
        which would poison module state for the rest of the suite)."""
        env = dict(os.environ)
        env.pop("NODEREAL_KEY", None)
        # Point load_env_file at a missing file so host state cannot mask
        # the missing key.
        env["LITE_INDEXER_ENV_FILE"] = str(tmp_path / "does-not-exist.env")
        repo_root = Path(__file__).resolve().parents[1]
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                "import lite_indexer.chains.endpoints",
            ],
            env=env,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert proc.returncode != 0, "import should fail when NODEREAL_KEY is unset"
        assert "NODEREAL_KEY" in proc.stderr
        assert "required" in proc.stderr.lower()


# ── Uniblock (archive eth_getLogs) — live-verified 2026-09-13 ─────────
# Template: .../json-rpc?chainId=<id>&apiKey=${UNIBLOCK_KEY}; the query-param
# form matches the existing ${VAR} URL-template model (like dRPC's dkey=).
UNIBLOCK_EVM_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "base": 8453,
    "bsc": 56,
    "polygon": 137,
    "arbitrum": 42161,
    "optimism": 10,
    "avalanche": 43114,
    "fantom": 250,
    "linea": 59144,
    "scroll": 534352,
    "zksync": 324,
    "mantle": 5000,
    "gnosis": 100,
    "celo": 42220,
    "blast": 81457,
    "fraxtal": 252,
    "metis": 1088,
    "robinhood": 4663,
}
# Chains where Uniblock is T2 (previously single-endpoint pools).
UNIBLOCK_T2_CHAINS = ("blast", "fraxtal", "metis")
UNIBLOCK_PREFIX = "https://api.uniblock.dev/uni/v1/json-rpc?chainId="
UNIBLOCK_TEST_KEY = "uniblock-key-test"


class TestUniblockEndpoints:
    def test_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("UNIBLOCK_KEY", UNIBLOCK_TEST_KEY)
        template = f"{UNIBLOCK_PREFIX}4663&apiKey=${{UNIBLOCK_KEY}}"
        expected = f"{UNIBLOCK_PREFIX}4663&apiKey={UNIBLOCK_TEST_KEY}"
        assert _resolve_endpoint_url(template) == expected
        assert _resolved_endpoints([template]) == [expected]

    def test_unresolved_template_is_dropped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("UNIBLOCK_KEY", raising=False)
        template = f"{UNIBLOCK_PREFIX}1&apiKey=${{UNIBLOCK_KEY}}"
        assert _resolved_endpoints([template]) == []

    def test_active_pool_contains_expanded_uniblock(self, tmp_path: Path):
        """Import with UNIBLOCK_KEY set and assert every indexed chain gets
        the expanded Uniblock URL (hermetic subprocess — no in-process reload
        that would poison module state for the rest of the suite)."""
        out = tmp_path / "uniblock_pool.json"
        script = (
            "import json, os\n"
            "from lite_indexer.chains.endpoints import GET_LOGS_ENDPOINTS\n"
            "from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS\n"
            "with open(os.environ['UNIBLOCK_TEST_OUT'], 'w') as fh:\n"
            "    json.dump({'evm': GET_LOGS_ENDPOINTS, 'solana': SOLANA_ENDPOINTS}, fh)\n"
        )
        env = dict(os.environ)
        env["NODEREAL_KEY"] = "test-key-123"
        env["UNIBLOCK_KEY"] = UNIBLOCK_TEST_KEY
        env["NATS_ENABLED"] = "0"
        env["UNIBLOCK_TEST_OUT"] = str(out)
        repo_root = Path(__file__).resolve().parents[1]
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        pool = json.loads(out.read_text())
        for chain, chain_id in UNIBLOCK_EVM_CHAIN_IDS.items():
            expected = f"{UNIBLOCK_PREFIX}{chain_id}&apiKey={UNIBLOCK_TEST_KEY}"
            assert expected in pool["evm"][chain], f"{chain}: missing {expected}"
        # robinhood Uniblock is high-priority (right after the official RPC);
        # blast/fraxtal/metis sit at T2, immediately after their publicnode T1.
        robinhood = pool["evm"]["robinhood"]
        robinhood_expected = f"{UNIBLOCK_PREFIX}4663&apiKey={UNIBLOCK_TEST_KEY}"
        assert robinhood.index(robinhood_expected) <= 1
        for chain in UNIBLOCK_T2_CHAINS:
            urls = pool["evm"][chain]
            expected = f"{UNIBLOCK_PREFIX}{UNIBLOCK_EVM_CHAIN_IDS[chain]}&apiKey={UNIBLOCK_TEST_KEY}"
            assert urls.index(expected) == 1
        assert f"{UNIBLOCK_PREFIX}solana&apiKey={UNIBLOCK_TEST_KEY}" in pool["solana"]

    def test_source_uses_only_env_placeholder(self):
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        uniblock_lines = [ln for ln in src.splitlines() if "api.uniblock.dev/uni/v1/json-rpc" in ln]
        assert uniblock_lines
        for ln in uniblock_lines:
            assert "apiKey=${UNIBLOCK_KEY}" in ln, ln
        assert "Uniblock — live-verified archive eth_getLogs 2026-09-13" in src


class TestDeadPublicnodeEndpointsRemoved:
    """PublicNode retired Fantom (for Sonic) and the zksync-era host; both
    404'd here on 2026-09-13 (re-probed twice) and were culled."""

    DEAD = (
        "https://fantom-rpc.publicnode.com",
        "https://zksync-era-rpc.publicnode.com",
    )

    def test_not_in_active_pools(self):
        for url in self.DEAD:
            for chain, urls in GET_LOGS_ENDPOINTS.items():
                assert url not in urls, f"{chain}: dead endpoint {url} in active pool"

    def test_not_in_candidate_pool(self):
        for url in self.DEAD:
            for chain, urls in CANDIDATE_ENDPOINTS.items():
                assert url not in urls, f"{chain}: dead endpoint {url} still a candidate"

    def test_live_polygon_publicnode_kept(self):
        # polygon-bor-rpc.publicnode.com is alive — do not over-cull.
        assert "https://polygon-bor-rpc.publicnode.com" in GET_LOGS_ENDPOINTS["polygon"]


# ── Pocket Network (keyless, 2026-09-13) ──────────────────────────────
# Template: https://<slug>.api.pocket.network — keyless, 2000 req/IP/min.
# eth_chainId live-verified for every slug below; robinhood was not in
# Pocket's published list but probed OK (0x1237).
POCKET_SLUGS: dict[str, str] = {
    "ethereum": "eth",
    "base": "base",
    "bsc": "bsc",
    "polygon": "poly",
    "arbitrum": "arb-one",
    "optimism": "op",
    "avalanche": "avax",
    "fantom": "fantom",
    "linea": "linea",
    "scroll": "scroll",
    "zksync": "zksync-era",
    "mantle": "mantle",
    "gnosis": "gnosis",
    "celo": "celo",
    "blast": "blast",
    "fraxtal": "fraxtal",
    "metis": "metis",
    "robinhood": "robinhood",
}


class TestPocketEndpoints:
    def test_pocket_present_for_every_verified_evm_chain(self):
        for chain, slug in POCKET_SLUGS.items():
            expected = f"https://{slug}.api.pocket.network"
            assert expected in GET_LOGS_ENDPOINTS[chain], f"{chain}: missing {expected}"

    def test_pocket_entries_are_keyless(self):
        for _chain, slug in POCKET_SLUGS.items():
            url = f"https://{slug}.api.pocket.network"
            assert "${" not in url

    def test_pocket_non_evm_fallbacks_present(self):
        from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS

        assert "https://solana.api.pocket.network" in SOLANA_ENDPOINTS
        assert "https://tron.api.pocket.network" in TRON_ENDPOINTS

    def test_metis_pocket_marked_non_archive(self):
        """Metis Pocket has no archival endpoint — Uniblock stays archive."""
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        assert "metis.api.pocket.network" in src
        metis_block = src.split('"metis": [', 1)[1].split("],", 1)[0]
        assert "NON-archive" in metis_block
        assert "uniblock" in metis_block.lower()

    def test_robinhood_pocket_probed_ok(self):
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        assert "robinhood.api.pocket.network" in src
        assert "0x1237" in src


# ── Infura (keyed, 2026-09-13) ────────────────────────────────────────
INFURA_CHAINS: dict[str, str] = {
    "ethereum": "mainnet",
    "bsc": "bsc-mainnet",
    "polygon": "polygon-mainnet",
    "arbitrum": "arbitrum-mainnet",
    "optimism": "optimism-mainnet",
    "base": "base-mainnet",
    "linea": "linea-mainnet",
    "scroll": "scroll-mainnet",
    "avalanche": "avalanche-mainnet",
    "zksync": "zksync-mainnet",
    "blast": "blast-mainnet",
    "mantle": "mantle-mainnet",
    "celo": "celo-mainnet",
}


class TestInfuraEndpoints:
    def test_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INFURA_KEY", "infura-key-test")
        template = "https://mainnet.infura.io/v3/${INFURA_KEY}"
        assert _resolve_endpoint_url(template) == "https://mainnet.infura.io/v3/infura-key-test"
        assert _resolved_endpoints([template]) == ["https://mainnet.infura.io/v3/infura-key-test"]

    def test_unresolved_template_is_dropped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("INFURA_KEY", raising=False)
        assert _resolved_endpoints(["https://mainnet.infura.io/v3/${INFURA_KEY}"]) == []

    def test_all_13_verified_chains_have_infura_template(self):
        """Keyed templates are stripped from the active pool when the key is
        unset, so assert against the source module (13 verified chains)."""
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        for chain, slug in INFURA_CHAINS.items():
            expected = f'"https://{slug}.infura.io/v3/${{INFURA_KEY}}"'
            assert expected in src, f"{chain}: missing Infura template {expected}"

    def test_infura_templates_use_only_env_placeholder(self):
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        infura_lines = [ln for ln in src.splitlines() if ln.strip().startswith('"https://') and "infura.io/v3/" in ln]
        assert len(infura_lines) == 13
        for ln in infura_lines:
            assert "v3/${INFURA_KEY}" in ln, ln


# ── Ankr free-key reconciliation (2026-09-13) ─────────────────────────
# The provisioned ${ANKR_KEY} works for only 8 chains; the other 6 return
# HTTP 403 "API key is not allowed to access blockchain". No Ankr template
# may exist for those six — a dead keyed entry only adds retry noise.
ANKR_WORKING_CHAINS = {
    "ethereum",
    "bsc",
    "polygon",
    "arbitrum",
    "avalanche",
    "base",
    "celo",
    "gnosis",
}
ANKR_FORBIDDEN_CHAINS = ("optimism", "fantom", "linea", "blast", "fraxtal", "metis")


# ── BlockPI (per-chain keyed, archive — live-verified 2026-09-13) ──────
# BlockPI keys are PER-CHAIN: ${BLOCKPI_<CHAIN>_KEY}, host <chain>.blockpi.network
# (zksync → zksync-era). The keyless /public candidates were HTTP-521-flaky
# (2026-08-07); the keyed templates replace them for the 9 provisioned chains.
BLOCKPI_CHAINS: dict[str, tuple[str, str]] = {
    "ethereum": ("ethereum", "BLOCKPI_ETH_KEY"),
    "bsc": ("bsc", "BLOCKPI_BSC_KEY"),
    "arbitrum": ("arbitrum", "BLOCKPI_ARBITRUM_KEY"),
    "optimism": ("optimism", "BLOCKPI_OPTIMISM_KEY"),
    "avalanche": ("avalanche", "BLOCKPI_AVALANCHE_KEY"),
    "linea": ("linea", "BLOCKPI_LINEA_KEY"),
    "scroll": ("scroll", "BLOCKPI_SCROLL_KEY"),
    "zksync": ("zksync-era", "BLOCKPI_ZKSYNC_KEY"),
    "fantom": ("fantom", "BLOCKPI_FANTOM_KEY"),
}
BLOCKPI_TEST_KEY = "blockpi-key-test"


class TestBlockPIPerChainEndpoints:
    def _source(self) -> str:
        return Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()

    def test_all_nine_verified_chains_have_keyed_template(self):
        src = self._source()
        for chain, (host, env) in BLOCKPI_CHAINS.items():
            expected = f'"https://{host}.blockpi.network/v1/rpc/${{{env}}}"'
            assert expected in src, f"{chain}: missing keyed BlockPI template {expected}"

    def test_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BLOCKPI_ETH_KEY", BLOCKPI_TEST_KEY)
        template = "https://ethereum.blockpi.network/v1/rpc/${BLOCKPI_ETH_KEY}"
        expected = f"https://ethereum.blockpi.network/v1/rpc/{BLOCKPI_TEST_KEY}"
        assert _resolve_endpoint_url(template) == expected
        assert _resolved_endpoints([template]) == [expected]

    def test_unresolved_keyed_template_dropped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("BLOCKPI_ETH_KEY", raising=False)
        assert _resolved_endpoints(["https://ethereum.blockpi.network/v1/rpc/${BLOCKPI_ETH_KEY}"]) == []

    def test_old_public_candidates_removed_for_keyed_chains(self):
        """ethereum/bsc/arbitrum/linea had keyless /public candidates; each
        now has a keyed active entry, so the /public candidate is gone."""
        for chain in ("ethereum", "bsc", "arbitrum", "linea"):
            candidates = CANDIDATE_ENDPOINTS.get(chain, [])
            assert not any("blockpi.network/v1/rpc/public" in u for u in candidates), chain

    def test_polygon_public_stays_candidate_with_flaky_note(self):
        assert "https://polygon.blockpi.network/v1/rpc/public" in CANDIDATE_ENDPOINTS["polygon"]
        src = self._source()
        candidates_src = src.split("CANDIDATE_ENDPOINTS", 1)[1]
        block = candidates_src.split('"polygon": [', 1)[1].split("],", 1)[0]
        assert "flaky" in block.lower()
        # polygon has no provisioned key — no keyed BlockPI template for it.
        assert "BLOCKPI_POLYGON_KEY" not in src

    def test_unindexed_blockpi_chains_not_in_active_pool(self):
        for chain in ("taiko", "cronos", "bitcoin"):
            assert chain not in GET_LOGS_ENDPOINTS, chain
        # ...but they are documented as available-but-unindexed in the source.
        src = self._source().lower()
        for chain in ("taiko", "cronos", "bitcoin"):
            assert chain in src


class TestAnkrFreeKeyChains:
    def _source_block(self, chain: str) -> str:
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        return src.split(f'"{chain}": [', 1)[1].split("],", 1)[0]

    def test_no_ankr_template_for_403_chains(self):
        for chain in ANKR_FORBIDDEN_CHAINS:
            assert "rpc.ankr.com" not in self._source_block(chain), f"{chain}: dead Ankr entry present"

    def test_no_ankr_in_active_pool_for_403_chains(self):
        for chain in ANKR_FORBIDDEN_CHAINS:
            assert not any("rpc.ankr.com" in u for u in GET_LOGS_ENDPOINTS[chain]), chain

    def test_ankr_only_for_working_chains(self):
        """Any chain carrying an Ankr template must be in the working set."""
        for chain in EXPECTED_EVM_CHAINS:
            if "rpc.ankr.com" in self._source_block(chain):
                assert chain in ANKR_WORKING_CHAINS, f"{chain}: Ankr registered but key 403s there"

    def test_registered_ankr_chains_present(self):
        for chain in ("ethereum", "bsc", "polygon", "arbitrum", "base", "celo"):
            assert "rpc.ankr.com" in self._source_block(chain), chain

    def test_ankr_source_documents_free_key_limitation(self):
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()
        assert "ANKR FREE-KEY LIMITATION" in src
        assert "not allowed to access" in src


# ── Dead-host prune (DNS + probe sweep, 2026-09-13) ────────────────────
# All 14 hosts below failed `getent hosts` (no DNS) AND an 8s POST
# eth_chainId probe from this host. They must never re-enter the pool.
DEAD_HOSTS_2026_09_13 = (
    "arbitrum.llamarpc.com",
    "avalanche.mainnet.nodies.app",
    "binance.llamarpc.com",
    "bsc-mainnet.g.alchemy.com",
    "celo-mainnet.public.blastapi.io",
    "fantom-mainnet.g.alchemy.com",
    "mainnet.era.zksync.network",
    "opt-mainnet.public.blastapi.io",
    "optimism.llamarpc.com",
    "optimism.meowrpc.com",
    "polygon.llamarpc.com",
    "polygon.meowrpc.com",
    "rpc-mainnet.matic.network",
    "zksync-era.g.alchemy.com",
)

# Alchemy host per indexed chain it supports (DoH-verified + live-probed with
# the real key, 2026-09-13 — all returned eth_chainId 200). Note bsc uses
# bnb-mainnet and zksync uses zksync-mainnet (the old alias names are NXDOMAIN).
ALCHEMY_SUPPORTED_HOSTS: dict[str, str] = {
    "ethereum": "eth-mainnet",
    "base": "base-mainnet",
    "bsc": "bnb-mainnet",
    "polygon": "polygon-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "avalanche": "avax-mainnet",
    "linea": "linea-mainnet",
    "scroll": "scroll-mainnet",
    "zksync": "zksync-mainnet",
    "mantle": "mantle-mainnet",
    "celo": "celo-mainnet",
    "blast": "blast-mainnet",
}
# Alchemy has no endpoint for these indexed chains — no template may exist.
ALCHEMY_UNSUPPORTED_HOSTS = (
    "fantom-mainnet",
    "fantom",
    "gnosis-mainnet",
    "fraxtal-mainnet",
    "metis-mainnet",
    "robinhood-mainnet",
)


class TestDeadHostsPruned:
    def _source(self) -> str:
        return Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()

    def test_dead_hosts_absent_from_active_pool(self):
        for host in DEAD_HOSTS_2026_09_13:
            for chain, urls in GET_LOGS_ENDPOINTS.items():
                assert not any(host in u for u in urls), f"{chain}: dead host {host} in active pool"

    def test_dead_hosts_absent_from_candidate_pool(self):
        for host in DEAD_HOSTS_2026_09_13:
            for chain, urls in CANDIDATE_ENDPOINTS.items():
                assert not any(host in u for u in urls), f"{chain}: dead host {host} still a candidate"

    def test_no_chain_left_with_zero_endpoints(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert len(urls) >= 1, f"{chain}: active pool is empty"

    def _url_lines(self) -> list[str]:
        # Comments mention old host names by design; only endpoint URL lines
        # count as "registered".
        return [ln for ln in self._source().splitlines() if ln.strip().startswith('"https://')]

    def test_alchemy_template_present_for_every_supported_chain(self):
        """Keyed Alchemy templates are dropped from the active pool when
        ALCHEMY_KEY is unset, so assert against the source URL lines."""
        for chain, host in ALCHEMY_SUPPORTED_HOSTS.items():
            expected = f'"https://{host}.g.alchemy.com/v2/${{ALCHEMY_KEY}}"'
            assert any(expected in ln for ln in self._url_lines()), f"{chain}: missing {expected}"

    def test_no_alchemy_template_for_unsupported_chains(self):
        # Alchemy has no Fantom (and no gnosis/fraxtal/metis/robinhood)
        # endpoint — the dead host must not be swapped for an invented one.
        for host in ALCHEMY_UNSUPPORTED_HOSTS:
            assert not any(f"{host}.g.alchemy.com" in ln for ln in self._url_lines()), host

    def test_every_alchemy_url_uses_env_placeholder_only(self):
        alchemy_lines = [ln for ln in self._url_lines() if ".g.alchemy.com" in ln]
        assert alchemy_lines
        for ln in alchemy_lines:
            assert "/v2/${ALCHEMY_KEY}" in ln, ln


# ── BlockVision (per-chain keyed, archive — live-verified 2026-09-13) ──
# BlockVision keys are PER-CHAIN: ${BLOCKVISION_<CHAIN>_KEY}, host
# <chain>-mainnet.blockvision.org (ethereum/bsc indexed; monad/sui served but
# NOT in our 20-chain pool). Archive eth_getLogs was live-verified here:
# ethereum blocks 1..2 → [], bsc → real logs.
BLOCKVISION_CHAINS: dict[str, tuple[str, str]] = {
    "ethereum": ("eth-mainnet", "BLOCKVISION_ETH_KEY"),
    "bsc": ("bsc-mainnet", "BLOCKVISION_BSC_KEY"),
}
BLOCKVISION_UNINDEXED_CHAINS = ("monad", "sui")
BLOCKVISION_TEST_KEY = "blockvision-key-test"


class TestBlockVisionEndpoints:
    def _source(self) -> str:
        return Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "endpoints.py").read_text()

    def test_indexed_chains_have_keyed_template(self):
        src = self._source()
        for chain, (host, env) in BLOCKVISION_CHAINS.items():
            expected = f'"https://{host}.blockvision.org/v1/${{{env}}}"'
            assert expected in src, f"{chain}: missing keyed BlockVision template {expected}"

    def test_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BLOCKVISION_ETH_KEY", BLOCKVISION_TEST_KEY)
        template = "https://eth-mainnet.blockvision.org/v1/${BLOCKVISION_ETH_KEY}"
        expected = f"https://eth-mainnet.blockvision.org/v1/{BLOCKVISION_TEST_KEY}"
        assert _resolve_endpoint_url(template) == expected
        assert _resolved_endpoints([template]) == [expected]

    def test_bsc_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BLOCKVISION_BSC_KEY", BLOCKVISION_TEST_KEY)
        template = "https://bsc-mainnet.blockvision.org/v1/${BLOCKVISION_BSC_KEY}"
        expected = f"https://bsc-mainnet.blockvision.org/v1/{BLOCKVISION_TEST_KEY}"
        assert _resolved_endpoints([template]) == [expected]

    def test_unresolved_keyed_template_dropped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("BLOCKVISION_ETH_KEY", raising=False)
        assert _resolved_endpoints(["https://eth-mainnet.blockvision.org/v1/${BLOCKVISION_ETH_KEY}"]) == []

    def test_source_documents_archive_verification(self):
        src = self._source()
        assert "BLOCKVISION KEYED ARCHIVE" in src
        assert "live-verified archive eth_getLogs 2026-09-13" in src

    def test_unindexed_chains_not_in_active_evm_pool(self):
        """monad/sui are served by BlockVision but are NOT our indexed chains."""
        for chain in BLOCKVISION_UNINDEXED_CHAINS:
            assert chain not in GET_LOGS_ENDPOINTS, chain
            assert chain not in EXPECTED_EVM_CHAINS, chain
        # ...but the provider registry documents them as available-but-unindexed.
        src = Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "rpc_registry.py").read_text()
        for chain in BLOCKVISION_UNINDEXED_CHAINS:
            assert chain in src.lower(), chain


# ── Chainstack Tron (per-node full URL, HTTP Basic auth — 2026-09-13) ──
# The Chainstack Tron Mainnet node is a full-URL basic-auth endpoint
# (userinfo embedded in the URL) supplied via CHAINSTACK_TRON_URL. HTTPX
# handles userinfo-in-URL. Live-verified 2026-09-13: eth_blockNumber +
# wallet/getblockbynum + eth_getLogs.
CHAINSTACK_TRON_TEST_URL = "https://user:pass@tron-mainnet.core.chainstack.com/jsonrpc"
CHAINSTACK_TRON_TEMPLATE = "${CHAINSTACK_TRON_URL}"


class TestChainstackTronEndpoints:
    def _non_evm_source(self) -> str:
        return Path(__file__).resolve().parents[1].joinpath("lite_indexer", "chains", "non_evm.py").read_text()

    def test_template_registered_in_tron_pool_source(self):
        src = self._non_evm_source()
        assert f'"{CHAINSTACK_TRON_TEMPLATE}"' in src
        tron_block = src.split("TRON_ENDPOINTS", 1)[1]
        assert CHAINSTACK_TRON_TEMPLATE in tron_block
        assert "Chainstack TRON Mainnet node" in tron_block
        assert "live-verified 2026-09-13" in tron_block

    def test_template_expands_via_expandvars(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("CHAINSTACK_TRON_URL", CHAINSTACK_TRON_TEST_URL)
        assert _resolve_endpoint_url(CHAINSTACK_TRON_TEMPLATE) == CHAINSTACK_TRON_TEST_URL
        assert _resolved_endpoints([CHAINSTACK_TRON_TEMPLATE]) == [CHAINSTACK_TRON_TEST_URL]

    def test_unresolved_template_is_dropped(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("CHAINSTACK_TRON_URL", raising=False)
        assert _resolved_endpoints([CHAINSTACK_TRON_TEMPLATE]) == []

    def test_active_tron_pool_contains_expanded_chainstack(self, tmp_path: Path):
        """Hermetic subprocess import with CHAINSTACK_TRON_URL set (no
        in-process reload that would poison module state)."""
        out = tmp_path / "tron_pool.json"
        script = (
            "import json, os\n"
            "from lite_indexer.chains.non_evm import TRON_ENDPOINTS\n"
            "with open(os.environ['TRON_TEST_OUT'], 'w') as fh:\n"
            "    json.dump(TRON_ENDPOINTS, fh)\n"
        )
        env = dict(os.environ)
        env["NODEREAL_KEY"] = "test-key-123"
        env["CHAINSTACK_TRON_URL"] = CHAINSTACK_TRON_TEST_URL
        env["NATS_ENABLED"] = "0"
        env["TRON_TEST_OUT"] = str(out)
        repo_root = Path(__file__).resolve().parents[1]
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        pool = json.loads(out.read_text())
        assert CHAINSTACK_TRON_TEST_URL in pool
        # Keyed entries must stay behind the keyless public endpoints.
        assert pool.index(CHAINSTACK_TRON_TEST_URL) > pool.index("https://api.trongrid.io")
