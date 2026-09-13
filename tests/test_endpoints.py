"""Tests for the PUBLIC free/keyless endpoint pool + private overlay hook.

The public repository must expose only free, keyless endpoints plus the
overlay extension point. Curated/keyed providers live in a private runtime
overlay (``lite_indexer/chains/_overlay.py``).

Covers:

* ``_resolve_endpoint_url`` / ``_resolved_endpoints`` env-template handling
* builtin pool shape (all chains present, https, no placeholders, ordered)
* the freemium denylist is ABSENT from every public ``chains/*.py`` source
* overlay load / merge / missing-file / no-dupe / order semantics
* with NO overlay, only keyless endpoints remain
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lite_indexer.chains._overlay import load_endpoint_overlay, merge_endpoints
from lite_indexer.chains.endpoints import (
    BUILTIN_ENDPOINTS,
    CANDIDATE_ENDPOINTS,
    GET_LOGS_ENDPOINTS,
    _resolve_endpoint_url,
    _resolved_endpoints,
)
from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS

REPO_ROOT = Path(__file__).resolve().parents[1]
CHAINS_DIR = REPO_ROOT / "lite_indexer" / "chains"

# Provider hostnames that MUST NOT appear anywhere in the public chains
# package (they are part of the private curated stack). Mirrors the task's
# `git grep` denylist.
FREEMIUM_DENYLIST = (
    "alchemy",
    "drpc",
    "uniblock",
    "blockpi",
    "blockvision",
    "chainstack",
    "helius",
    "nodereal",
    "infura",
    "ankr",
)

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
        monkeypatch.setenv("YOUR_PROVIDER_KEY", "test-key-123")
        url = _resolve_endpoint_url("https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}")
        assert url == "https://mainnet.example-rpc.com/v1/test-key-123"

    def test_unset_var_leaves_placeholder(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("YOUR_PROVIDER_KEY", raising=False)
        url = _resolve_endpoint_url("https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}")
        assert "${YOUR_PROVIDER_KEY}" in url

    def test_dollar_bare_form_supported(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("YOUR_PROVIDER_KEY", "kb")
        assert _resolve_endpoint_url("https://eth.example-rpc.com/?api_key=$YOUR_PROVIDER_KEY").endswith("?api_key=kb")

    def test_no_placeholder_passthrough(self):
        assert _resolve_endpoint_url("https://mainnet.example-rpc.com") == "https://mainnet.example-rpc.com"


class TestResolvedEndpoints:
    def test_drops_unresolved_placeholders(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("YOUR_PROVIDER_KEY", raising=False)
        urls = [
            "https://mainnet.example-rpc.com",
            "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}",
            "https://${YOUR_DEDICATED_ENDPOINT}",
        ]
        assert _resolved_endpoints(urls) == ["https://mainnet.example-rpc.com"]

    def test_keeps_resolved_placeholder_endpoints(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("YOUR_PROVIDER_KEY", "k123")
        urls = [
            "https://mainnet.example-rpc.com",
            "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}",
        ]
        assert _resolved_endpoints(urls) == [
            "https://mainnet.example-rpc.com",
            "https://mainnet.example-rpc.com/v1/k123",
        ]

    def test_preserves_order(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("YOUR_PROVIDER_KEY", raising=False)
        urls = ["https://b.example", "https://a.example", "https://c.example"]
        assert _resolved_endpoints(urls) == urls


class TestGetLogsEndpoints:
    def test_all_evm_chains_present(self):
        assert sorted(GET_LOGS_ENDPOINTS.keys()) == sorted(EXPECTED_EVM_CHAINS)

    def test_active_pool_has_no_placeholder_urls(self):
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
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert urls[0] and urls[0].startswith("https://"), chain

    def test_no_duplicate_endpoints_within_chain(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert len(urls) == len(set(urls)), chain

    def test_builtin_pool_is_free_keyless_only(self):
        """The builtin (pre-overlay) pool carries no ${...} templates."""
        for chain, urls in BUILTIN_ENDPOINTS.items():
            assert urls, chain
            for url in urls:
                assert "${" not in url, f"{chain}: {url}"

    # Chains hit hardest by the 2026-09-02 RPC rate-limit crisis. Each must
    # keep >= 3 free (builtin/keyless) endpoints so a single provider ban
    # cannot take a chain's pool offline.
    RATE_LIMIT_CRITICAL_CHAINS = ["polygon", "optimism", "bsc", "arbitrum", "avalanche"]

    def test_rate_limit_critical_chains_have_at_least_3_free_endpoints(self):
        for chain in self.RATE_LIMIT_CRITICAL_CHAINS:
            free = [u for u in BUILTIN_ENDPOINTS[chain] if "${" not in u]
            assert len(free) >= 3, f"{chain}: only {len(free)} free endpoints ({free})"

    def test_total_endpoint_count_sane(self):
        """Builtin pool is bounded; the active pool is builtin + overlay."""
        builtin_total = sum(len(urls) for urls in BUILTIN_ENDPOINTS.values())
        active_total = sum(len(urls) for urls in GET_LOGS_ENDPOINTS.values())
        assert 50 <= builtin_total <= 120
        assert builtin_total <= active_total <= 500


class TestPublicModulesHaveNoFreemium:
    """The public chains package must not expose the curated provider stack."""

    def test_no_freemium_token_in_any_public_chain_module(self):
        for src in sorted(CHAINS_DIR.glob("*.py")):
            text = src.read_text(encoding="utf-8").lower()
            for token in FREEMIUM_DENYLIST:
                assert token not in text, f"{src.name}: leaked provider token {token!r}"

    def test_no_refresh_of_removed_provider_surfaces(self):
        init_src = (CHAINS_DIR / "__init__.py").read_text(encoding="utf-8")
        assert "rpc_registry" not in init_src

    def test_no_freemium_token_in_any_lite_indexer_module(self):
        """Full-tree gate: no curated provider name may appear anywhere in the
        public ``lite_indexer/`` source (tests may assert this absence)."""
        for src in sorted((REPO_ROOT / "lite_indexer").rglob("*.py")):
            text = src.read_text(encoding="utf-8").lower()
            for token in FREEMIUM_DENYLIST:
                assert token not in text, f"{src.relative_to(REPO_ROOT)}: leaked provider token {token!r}"


class TestOverlayLoad:
    def test_missing_file_returns_empty(self, tmp_path: Path):
        assert load_endpoint_overlay(tmp_path / "absent.json") == {}

    def test_unreadable_or_bad_json_returns_empty(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        assert load_endpoint_overlay(bad) == {}

    def test_non_dict_returns_empty(self, tmp_path: Path):
        arr = tmp_path / "arr.json"
        arr.write_text("[1, 2, 3]", encoding="utf-8")
        assert load_endpoint_overlay(arr) == {}

    def test_flat_list_form_parses(self, tmp_path: Path):
        f = tmp_path / "flat.json"
        f.write_text(json.dumps({"solana": ["https://sol.example-rpc.com/?x=${YOUR_PROVIDER_KEY}"]}), encoding="utf-8")
        overlay = load_endpoint_overlay(f)
        assert overlay["solana"]["endpoints"] == ["https://sol.example-rpc.com/?x=${YOUR_PROVIDER_KEY}"]

    def test_dict_form_with_notes_parses(self, tmp_path: Path):
        f = tmp_path / "dict.json"
        payload = {
            "ethereum": {
                "endpoints": ["https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"],
                "notes": {"https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}": "T4 — mine"},
            }
        }
        f.write_text(json.dumps(payload), encoding="utf-8")
        overlay = load_endpoint_overlay(f)
        assert overlay["ethereum"]["endpoints"] == ["https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"]
        assert overlay["ethereum"]["notes"]["https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"] == "T4 — mine"

    def test_env_var_path_is_honored(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        f = tmp_path / "env.json"
        f.write_text(json.dumps({"base": ["https://base.example-rpc.com/${YOUR_PROVIDER_KEY}"]}), encoding="utf-8")
        monkeypatch.setenv("LITE_INDEXER_ENDPOINT_OVERLAY", str(f))
        assert load_endpoint_overlay()["base"]["endpoints"] == ["https://base.example-rpc.com/${YOUR_PROVIDER_KEY}"]

    def test_env_var_missing_file_falls_back_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("LITE_INDEXER_ENDPOINT_OVERLAY", str(tmp_path / "nope.json"))
        assert load_endpoint_overlay() == {}


class TestOverlayMerge:
    BUILTIN = {
        "ethereum": ["https://a.example", "https://b.example"],
        "base": ["https://c.example"],
    }

    def test_appends_preserving_order_and_dedupes(self):
        overlay = {
            "ethereum": {
                "endpoints": [
                    "https://b.example",  # duplicate -> dropped
                    "https://d.example",
                    "https://e.example",
                ],
                "notes": {},
            }
        }
        merged = merge_endpoints(self.BUILTIN, overlay)
        assert merged["ethereum"] == [
            "https://a.example",
            "https://b.example",
            "https://d.example",
            "https://e.example",
        ]

    def test_builtin_order_preserved_and_overlay_after(self):
        overlay = {"ethereum": ["https://z.example"]}
        merged = merge_endpoints(self.BUILTIN, overlay)
        assert merged["ethereum"][:2] == ["https://a.example", "https://b.example"]
        assert merged["ethereum"][-1] == "https://z.example"

    def test_overlay_only_chain_is_included(self):
        overlay = {"solana": ["https://sol.example-rpc.com/?x=${YOUR_PROVIDER_KEY}"]}
        merged = merge_endpoints(self.BUILTIN, overlay)
        assert merged["solana"] == ["https://sol.example-rpc.com/?x=${YOUR_PROVIDER_KEY}"]

    def test_none_or_empty_overlay_is_identity(self):
        assert merge_endpoints(self.BUILTIN, None) == self.BUILTIN
        assert merge_endpoints(self.BUILTIN, {}) == self.BUILTIN

    def test_does_not_mutate_builtin(self):
        merge_endpoints(self.BUILTIN, {"ethereum": ["https://z.example"]})
        assert self.BUILTIN["ethereum"] == ["https://a.example", "https://b.example"]

    def test_with_overlay_yields_expected_union_for_representative_chain(self):
        """Production parity: builtin + overlay == the recorded union, in order."""
        builtin = {"ethereum": ["https://a.example", "https://b.example"]}
        overlay = {"ethereum": {"endpoints": ["https://c.example", "https://d.example"], "notes": {}}}
        merged = merge_endpoints(builtin, overlay)
        assert merged["ethereum"] == [
            "https://a.example",
            "https://b.example",
            "https://c.example",
            "https://d.example",
        ]


class TestNoOverlayOnlyKeyless:
    """Hermetic subprocess import with the overlay pointed at a missing file."""

    def test_active_pool_is_free_keyless_only(self, tmp_path: Path):
        out = tmp_path / "pool.json"
        script = (
            "import json, os\n"
            "from lite_indexer.chains.endpoints import GET_LOGS_ENDPOINTS, BUILTIN_ENDPOINTS\n"
            "from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS\n"
            "json.dump({'evm': list(GET_LOGS_ENDPOINTS.values()), "
            "'builtin': list(BUILTIN_ENDPOINTS.values()), "
            "'solana': SOLANA_ENDPOINTS, 'tron': TRON_ENDPOINTS}, open(os.environ['OUT'], 'w'))\n"
        )
        env = dict(os.environ)
        env["NODEREAL_KEY"] = "test-key-123"
        env["NATS_ENABLED"] = "0"
        env["LITE_INDEXER_ENDPOINT_OVERLAY"] = str(tmp_path / "nonexistent-overlay.json")
        env["OUT"] = str(out)
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        data = json.loads(out.read_text())
        every = data["evm"] + data["builtin"] + [data["solana"], data["tron"]]
        for urls in every:
            for url in urls:
                assert "${" not in url, url
                assert not any(tok in url.lower() for tok in FREEMIUM_DENYLIST), url


class TestDeadPublicnodeEndpointsRemoved:
    """PublicNode retired Fantom (for Sonic) and the zksync-era host."""

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
        assert "https://polygon-bor-rpc.publicnode.com" in GET_LOGS_ENDPOINTS["polygon"]


# ── Pocket Network (keyless, 2026-09-13) — public free tier ──────────
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
            assert f"https://{slug}.api.pocket.network" in GET_LOGS_ENDPOINTS[chain], chain

    def test_pocket_entries_are_keyless(self):
        for slug in POCKET_SLUGS.values():
            assert "${" not in f"https://{slug}.api.pocket.network"

    def test_pocket_non_evm_fallbacks_present(self):
        assert "https://solana.api.pocket.network" in SOLANA_ENDPOINTS
        assert "https://tron.api.pocket.network" in TRON_ENDPOINTS

    def test_no_chain_left_with_zero_endpoints(self):
        for chain, urls in GET_LOGS_ENDPOINTS.items():
            assert urls, chain
        assert SOLANA_ENDPOINTS
        assert TRON_ENDPOINTS


# ── Blockscout (keyless archive getLogs, 2026-09-13) ───────────────────
# Public keyless endpoints verified live with eth_chainId + an old-block
# eth_getLogs (USDC transfer filter). 8 chains passed; base is 429 and the
# rest have no blockscout eth-rpc host. Explicit hosts where the canonical
# <chain>.blockscout.com 301-redirects.
BLOCKSCOUT_HOSTS: dict[str, str] = {
    "ethereum": "eth.blockscout.com",
    "polygon": "polygon.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
    "zksync": "zksync.blockscout.com",
    "celo": "celo.blockscout.com",
    "optimism": "explorer.optimism.io",
    "scroll": "scrollscan.com",
    "gnosis": "gnosisscan.io",
}
# Chains with NO usable Blockscout eth-rpc host (base is rate-limited).
BLOCKSCOUT_UNAVAILABLE = (
    "base",
    "bsc",
    "avalanche",
    "fantom",
    "linea",
    "mantle",
    "robinhood",
    "blast",
    "fraxtal",
    "metis",
)


class TestBlockscoutEndpoints:
    def test_present_and_keyless_for_every_verified_chain(self):
        for chain, host in BLOCKSCOUT_HOSTS.items():
            url = f"https://{host}/api/eth-rpc"
            assert url in BUILTIN_ENDPOINTS[chain], f"{chain}: missing {url}"
            assert "${" not in url
            # Public free/keyless tier — not a keyed template.
            assert url in GET_LOGS_ENDPOINTS[chain], f"{chain}: dropped from active pool"

    def test_logs_only_comment_present(self):
        src = (REPO_ROOT / "lite_indexer" / "chains" / "endpoints.py").read_text(encoding="utf-8")
        assert "Blockscout" in src
        assert "logs-only (no archive state)" in src

    def test_expands_via_normal_resolution_path(self):
        for host in BLOCKSCOUT_HOSTS.values():
            url = f"https://{host}/api/eth-rpc"
            assert _resolved_endpoints([url]) == [url]

    def test_unavailable_chains_have_no_blockscout_entry(self):
        for chain in BLOCKSCOUT_UNAVAILABLE:
            assert not any(
                "blockscout" in u or "scrollscan" in u or "gnosisscan" in u for u in BUILTIN_ENDPOINTS[chain]
            ), chain


# ── Tenderly keyless public gateway (2026-09-13) ───────────────────────
# https://gateway.tenderly.co/public/<net> — keyless, archive getLogs
# verified live. 13 chains pass; bsc/scroll/metis/fantom/robinhood 404.
TENDERLY_NETS: dict[str, str] = {
    "ethereum": "mainnet",
    "base": "base",
    "polygon": "polygon",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "avalanche": "avalanche",
    "gnosis": "gnosis",
    "linea": "linea",
    "zksync": "zksync",
    "mantle": "mantle",
    "celo": "celo",
    "blast": "blast",
    "fraxtal": "fraxtal",
}
TENDERLY_UNSUPPORTED = ("bsc", "scroll", "metis", "fantom", "robinhood")


class TestTenderlyGateway:
    def test_present_and_keyless_for_verified_chains(self):
        for chain, net in TENDERLY_NETS.items():
            url = f"https://gateway.tenderly.co/public/{net}"
            assert url in BUILTIN_ENDPOINTS[chain], f"{chain}: missing {url}"
            assert "${" not in url
            assert url in GET_LOGS_ENDPOINTS[chain], f"{chain}: dropped from active pool"

    def test_absent_for_unsupported_chains(self):
        for chain in TENDERLY_UNSUPPORTED:
            assert not any("gateway.tenderly.co/public/" in u for u in BUILTIN_ENDPOINTS[chain]), chain

    def test_expands_via_normal_resolution_path(self):
        for net in TENDERLY_NETS.values():
            url = f"https://gateway.tenderly.co/public/{net}"
            assert _resolved_endpoints([url]) == [url]

    def test_no_keyed_tenderly_form_wired(self):
        src = (REPO_ROOT / "lite_indexer" / "chains" / "endpoints.py").read_text(encoding="utf-8")
        assert "TENDERLY_ACCESS_KEY" not in src
        assert "tenderly.co/public/${" not in src
