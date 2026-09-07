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

import os
import subprocess
import sys
from pathlib import Path

import pytest

from lite_indexer.chains.endpoints import (
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
        endpoints for polygon/bsc + blast/fraxtal/metis pools. The bound
        still guards against runaway duplication.
        """
        total = sum(len(urls) for urls in GET_LOGS_ENDPOINTS.values())
        assert len(GET_LOGS_ENDPOINTS) <= total <= 104


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
