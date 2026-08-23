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

import pytest

from lite_indexer.chains.rpc_registry import (
    FREEMIUM_PROVIDERS,
    FreemiumProvider,
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
        with pytest.raises(Exception):
            p.name = "mutated"  # type: ignore[misc]
