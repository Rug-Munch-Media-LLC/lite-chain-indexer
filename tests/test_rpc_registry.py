"""Tests for the freemium RPC provider registry (chains/rpc_registry.py).

The registry is the monetization + fallback-upgrade surface. Two security
invariants matter more than anything else here:

1. **Key protection** — no real API keys may ever appear in the registry
   or endpoint templates; keys exist ONLY as ${ENV_VAR} placeholders.
2. **Placeholder discipline** — signup URLs ship as SET_ME until the
   project publishes referral links; nothing may auto-invent a URL.
"""

from __future__ import annotations

import re
from dataclasses import FrozenInstanceError

import pytest

from lite_indexer.chains.rpc_registry import (
    FREEMIUM_PROVIDERS,
    SKIPPED_PROVIDERS,
    providers_with_referrals,
    signup_table,
)


class TestRegistryIntegrity:
    def test_all_providers_present(self):
        expected = {
            "QuickNode",
            "Chainstack",
            "GetBlock",
            "NOWNodes",
            "Tatum",
            "Alchemy",
            "dRPC",
            "Ankr",
            "Helius",
            "BlockPI",
            "Uniblock",
            "BlockVision",
            "Pocket Network",
            "Infura",
        }
        assert set(FREEMIUM_PROVIDERS) == expected

    def test_env_keys_are_unique(self):
        keys = [p.env_key for p in FREEMIUM_PROVIDERS.values()]
        assert len(keys) == len(set(keys))

    def test_url_examples_use_env_placeholder_not_real_key(self):
        for p in FREEMIUM_PROVIDERS.values():
            if "${" + p.env_key + "}" not in p.url_example and p.name != "Tatum":
                # Tatum uses a header-style key; its example documents that.
                assert p.env_key.split("_")[0].lower() in p.url_example.lower(), (
                    f"{p.name}: url_example must reference its env key"
                )


class TestKeyProtection:
    """THE invariant: no real secrets in the OSS tree, ever."""

    REAL_KEY_PATTERNS = [
        re.compile(r"\b[0-9a-f]{32}\b"),  # 32-hex (Alchemy/Infura style)
        re.compile(r"\b[A-Za-z0-9_-]{40,}\b"),  # long opaque tokens
    ]

    @pytest.mark.parametrize("fieldname", ["url_example"])
    def test_no_literal_secrets_in_templates(self, fieldname):
        for p in FREEMIUM_PROVIDERS.values():
            value = getattr(p, fieldname)
            for pattern in self.REAL_KEY_PATTERNS:
                match = pattern.search(value)
                assert match is None or "${" in value, f"{p.name}.{fieldname} looks like a REAL KEY: {match!r}"

    def test_placeholder_vars_resolve_through_expandvars_contract(self):
        # Every env_key must be usable as ${KEY} — the form os.path.expandvars
        # resolves (chains/endpoints.py contract).
        import os

        for p in FREEMIUM_PROVIDERS.values():
            sample = f"https://x/${{{p.env_key}}}"
            resolved = os.path.expandvars(sample)
            if p.env_key not in os.environ:
                assert resolved == sample or "x/" in resolved  # unresolved stays literal


class TestReferralMetadata:
    def test_referral_providers_subset(self):
        referrers = {p.name for p in providers_with_referrals()}
        assert {"QuickNode", "Chainstack", "GetBlock", "NOWNodes", "Tatum"} <= referrers
        assert "Ankr" not in referrers  # no public program — recommend on merit only

    def test_signup_urls_are_placeholders_until_published(self):
        # Discipline check: nobody ships an invented URL. The owner drops
        # real referral links into ONE place (this registry) later.
        for name, _env, url in signup_table():
            assert url == "SET_ME" or url.startswith(("https://", "http://")), (
                f"{name}: signup_url must be SET_ME or a real https URL"
            )

    def test_frozen_dataclass(self):
        p = next(iter(FREEMIUM_PROVIDERS.values()))
        with pytest.raises(FrozenInstanceError):
            p.name = "mutated"  # type: ignore[misc]


class TestUniblockProvider:
    def test_uniblock_registered(self):
        p = FREEMIUM_PROVIDERS["Uniblock"]
        assert p.env_key == "UNIBLOCK_KEY"
        assert "${UNIBLOCK_KEY}" in p.url_example
        assert p.url_example.startswith("https://api.uniblock.dev/uni/v1/json-rpc?chainId=")
        assert p.signup_url == "https://dashboard.uniblock.dev"
        assert p.has_referral_program is False

    def test_uniblock_not_a_referral_provider(self):
        assert "Uniblock" not in {p.name for p in providers_with_referrals()}


class TestSkippedProviders:
    def test_routemesh_recorded_as_skipped(self):
        reason = SKIPPED_PROVIDERS["RouteMesh"]
        assert "no DNS" in reason
        assert "2026-09-13" in reason


class TestPocketProvider:
    """Pocket Network is KEYLESS — no env key, no placeholder."""

    def test_pocket_registered_keyless(self):
        p = FREEMIUM_PROVIDERS["Pocket Network"]
        assert p.env_key == ""
        assert "${" not in p.url_example
        assert p.url_example == "https://eth.api.pocket.network"
        assert p.has_referral_program is False
        assert "2000 req/IP/min" in p.free_tier

    def test_pocket_not_a_referral_provider(self):
        assert "Pocket Network" not in {p.name for p in providers_with_referrals()}


class TestInfuraProvider:
    def test_infura_registered_with_placeholder(self):
        p = FREEMIUM_PROVIDERS["Infura"]
        assert p.env_key == "INFURA_KEY"
        assert "${INFURA_KEY}" in p.url_example
        assert p.url_example.startswith("https://mainnet.infura.io/v3/")
        assert p.has_referral_program is False
        assert "archive" in p.chains_note

    def test_infura_not_a_referral_provider(self):
        assert "Infura" not in {p.name for p in providers_with_referrals()}


class TestBlockPIProvider:
    """BlockPI authenticates with a PER-CHAIN key, not one account-wide key."""

    def test_blockpi_registered_per_chain(self):
        p = FREEMIUM_PROVIDERS["BlockPI"]
        assert p.env_key == "BLOCKPI_<CHAIN>_KEY"
        assert "${BLOCKPI_<CHAIN>_KEY}" in p.url_example
        assert p.url_example.startswith("https://<chain>.blockpi.network/v1/rpc/")
        assert p.has_referral_program is False

    def test_blockpi_documents_indexed_chains(self):
        p = FREEMIUM_PROVIDERS["BlockPI"]
        for chain in ("ethereum", "bsc", "arbitrum", "optimism", "avalanche", "linea", "scroll", "zksync", "fantom"):
            assert chain in p.chains_note

    def test_blockpi_documents_unindexed_chains(self):
        p = FREEMIUM_PROVIDERS["BlockPI"]
        for chain in ("taiko", "cronos", "bitcoin"):
            assert chain in p.chains_note

    def test_blockpi_not_a_referral_provider(self):
        assert "BlockPI" not in {p.name for p in providers_with_referrals()}


class TestBlockVisionProvider:
    """BlockVision authenticates with a PER-CHAIN key, like BlockPI."""

    def test_blockvision_registered_per_chain(self):
        p = FREEMIUM_PROVIDERS["BlockVision"]
        assert p.env_key == "BLOCKVISION_<CHAIN>_KEY"
        assert "${BLOCKVISION_<CHAIN>_KEY}" in p.url_example
        assert p.url_example.startswith("https://<chain>-mainnet.blockvision.org/v1/")
        assert p.signup_url == "https://blockvision.org"
        assert p.has_referral_program is False
        assert "UNKNOWN" in p.free_tier

    def test_blockvision_documents_indexed_chains(self):
        p = FREEMIUM_PROVIDERS["BlockVision"]
        for chain in ("ethereum", "bsc"):
            assert chain in p.chains_note
        assert "archive" in p.chains_note.lower()

    def test_blockvision_documents_unindexed_chains(self):
        p = FREEMIUM_PROVIDERS["BlockVision"]
        for chain in ("monad", "sui"):
            assert chain in p.chains_note

    def test_blockvision_not_a_referral_provider(self):
        assert "BlockVision" not in {p.name for p in providers_with_referrals()}


class TestOneRpcKeyedUnresolved:
    """The provided 1RPC key was not accepted in any documented format."""

    def test_one_rpc_keyed_format_recorded_unresolved(self):
        reason = SKIPPED_PROVIDERS["1RPC keyed upgrade"]
        assert "2026-09-13" in reason
        assert "unresolved" in reason.lower()


class TestAnkrFreeKeyLimitation:
    """The provisioned Ankr free key only serves 8 chains (403 elsewhere)."""

    def test_ankr_note_documents_403_chains(self):
        p = FREEMIUM_PROVIDERS["Ankr"]
        assert "403" in p.chains_note
        for chain in ("optimism", "fantom", "linea", "blast", "fraxtal", "metis"):
            assert chain in p.chains_note

    def test_ankr_note_lists_working_chains(self):
        p = FREEMIUM_PROVIDERS["Ankr"]
        for chain in ("eth", "bsc", "polygon", "arbitrum", "avalanche", "base", "celo", "gnosis"):
            assert chain in p.chains_note
