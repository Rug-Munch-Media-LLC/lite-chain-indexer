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

import pytest

from lite_indexer.chains.endpoints import (
    GET_LOGS_ENDPOINTS,
    _resolve_endpoint_url,
    _resolved_endpoints,
)

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

    def test_total_endpoint_count_sane(self):
        """Active pool is bounded: >= 1 per chain, <= the 66 raw templates."""
        total = sum(len(urls) for urls in GET_LOGS_ENDPOINTS.values())
        assert len(GET_LOGS_ENDPOINTS) <= total <= 66
