"""Quota is pooled PER ACCOUNT; the residential proxy is a key-less lever.

Contract (2026-09-14 capacity correction):

* N API keys of ONE provider account draw ONE budget — never N copies of the
  same free quota. The quota key is ``account_id`` (default: the provider id),
  distinct from ``key_id``.
* Two distinct accounts pool independently.
* Per-KEY rate limits (rps/concurrency) stay separate from the pooled quota.
* Key-less endpoints (per-IP limits) route through the proxy pool; keyed
  endpoints stay direct-first with the proxy only as a transport fallback.
* The daily/monthly reset + 70/85/95% degrade ladder still fire.

Hermetic — synthetic hosts/tokens only, no network, no real provider names.
"""

from __future__ import annotations

from pathlib import Path

from lite_indexer.config import ProviderBudget
from lite_indexer.rpc import ledger as ledger_mod
from lite_indexer.rpc.budgets import ProviderBudgetTracker
from lite_indexer.rpc.ledger import (
    ProviderLedger,
    account_id_for_endpoint,
    key_id_for_endpoint,
)
from lite_indexer.rpc.proxy import ProxyRouter, ProxySpec

# Synthetic, non-secret tokens (never real API keys).
KEY_A = "a" * 32
KEY_B = "b" * 32
PROVIDER = "vendor-test.example"
KEYED_A = f"https://rpc.vendor-test.example/v2/{KEY_A}"
KEYED_B = f"https://rpc.vendor-test.example/v2/{KEY_B}"
KEYLESS = "https://public.vendor-test.example"

NOW = 1_700_000_000.0


def _seed_account(
    ledger: ProviderLedger,
    provider: str,
    *,
    budget_units: int = 100,
    account_id: str | None = None,
    key_id: str | None = None,
    period: str = "day",
    now: float = NOW,
) -> None:
    """Seed one account budget with a flat 1-unit tariff (test seam)."""
    budget = ledger._ensure_budget(provider, key_id, account_id=account_id)  # noqa: SLF001
    budget.budget_units = budget_units
    budget.unit_type = "raw"
    budget.method_tariff = {"eth_chainId": 1.0, "default": 1.0}
    budget.period = period
    budget.reset_at = ledger._compute_reset_at(period, now)  # noqa: SLF001
    budget.updated_at = now


class TestAccountPooling:
    def test_n_keys_share_one_account_budget(self, tmp_path: Path) -> None:
        led = ProviderLedger(path=tmp_path / "ledger.json")
        _seed_account(led, PROVIDER)

        for key in (KEY_A, KEY_B, KEY_A):
            led.record_call(PROVIDER, "eth_chainId", key, now=NOW)

        state = led.state_for(PROVIDER, now=NOW)
        assert state["consumed_units"] == 3.0  # three calls, one budget
        assert state["consumed_pct"] == 3.0
        # Exactly ONE budget exists for the provider (not one per key).
        assert len([k for k in led.snapshot(NOW) if k.startswith(PROVIDER)]) == 1

    def test_default_account_is_provider_id(self) -> None:
        assert account_id_for_endpoint(KEYED_A) == PROVIDER
        assert account_id_for_endpoint(KEYED_B) == PROVIDER
        assert account_id_for_endpoint(KEYED_A) == account_id_for_endpoint(KEYED_B)

    def test_configured_override_names_one_account(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.setitem(ledger_mod.KNOWN_ACCOUNT_IDS, key_id_for_endpoint(KEYED_A), "named-acct")
        assert account_id_for_endpoint(KEYED_A) == "named-acct"
        # A key without an override still defaults to one account per provider.
        assert account_id_for_endpoint(KEYED_B) == PROVIDER

    def test_two_accounts_pool_independently(self, tmp_path: Path) -> None:
        led = ProviderLedger(path=tmp_path / "ledger.json")
        _seed_account(led, PROVIDER, account_id="acct-1")
        _seed_account(led, PROVIDER, account_id="acct-2")

        led.record_call(PROVIDER, "eth_chainId", KEY_A, now=NOW, account_id="acct-1")
        led.record_call(PROVIDER, "eth_chainId", KEY_B, now=NOW, account_id="acct-2")
        led.record_call(PROVIDER, "eth_chainId", KEY_B, now=NOW, account_id="acct-2")

        assert led.state_for(PROVIDER, now=NOW, account_id="acct-1")["consumed_units"] == 1.0
        assert led.state_for(PROVIDER, now=NOW, account_id="acct-2")["consumed_units"] == 2.0
        assert len(led.snapshot(NOW)) == 2

    def test_degrade_ladder_counts_all_keys_against_one_budget(self, tmp_path: Path) -> None:
        led = ProviderLedger(path=tmp_path / "ledger.json")
        _seed_account(led, PROVIDER, budget_units=100)
        keys = (KEY_A, KEY_B)
        for i in range(70):
            led.record_call(PROVIDER, "eth_chainId", keys[i % 2], now=NOW)
        # 70 calls spread over two keys == 70% of the ONE account budget.
        assert led.state_for(PROVIDER, now=NOW)["degrade_level"] == 1

    def test_monthly_reset_zeroes_the_account_budget(self, tmp_path: Path) -> None:
        led = ProviderLedger(path=tmp_path / "ledger.json")
        _seed_account(led, PROVIDER, budget_units=100, period="month")
        led.record_call(PROVIDER, "eth_chainId", KEY_A, now=NOW)

        before = led.state_for(PROVIDER, now=NOW)
        assert before["consumed_units"] == 1.0
        rolled = led.state_for(PROVIDER, now=before["reset_at"] + 1.0)
        assert rolled["consumed_units"] == 0.0
        assert rolled["consumed_pct"] == 0.0


class TestRateVsQuota:
    def test_per_key_rate_separate_from_account_quota(self) -> None:
        tracker = ProviderBudgetTracker(
            provider_budgets={
                PROVIDER: ProviderBudget(rps=1.0, max_concurrency=1, daily_budget=10, explicit=True),
            },
        )
        # rps/concurrency identity is per key ...
        assert tracker.budget_key(KEYED_A) != tracker.budget_key(KEYED_B)
        # ... while the daily quota is per account.
        assert tracker.quota_key(KEYED_A) == tracker.quota_key(KEYED_B)

        tracker.record_dispatch(KEYED_A)
        assert tracker.used_today(KEYED_A) == 1
        assert tracker.used_today(KEYED_B) == 1  # same account budget, shared
        assert tracker.wait_seconds(KEYED_A) > 0.0  # key A paced
        assert tracker.wait_seconds(KEYED_B) == 0.0  # key B independent pacing


class TestProxyKeylessOnly:
    def _router(self, **kw: object) -> ProxyRouter:
        spec = ProxySpec("http://proxy.example:8080", "proxy.example:8080")
        return ProxyRouter([spec], enabled=True, keyless_only=True, **kw)  # type: ignore[arg-type]

    def test_keyless_proxied_keyed_direct(self) -> None:
        router = self._router()
        spec = router.proxy_for(KEYLESS)
        assert spec is not None  # key-less host → proxy (per-IP limit)
        assert router.proxy_for(KEYED_A) is None  # keyed → direct-first
        assert router.fallback_proxy_for(KEYED_A) is spec  # proxy only as fallback
        assert router.fallback_proxy_for(KEYLESS) is None  # keyless already proxied

    def test_default_off_keeps_legacy_proxy_primary(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.setenv("WEBSHARE_PROXY_LIST", "proxy.example:8080")
        monkeypatch.setenv("RPC_PROXY_ENABLED", "1")
        monkeypatch.delenv("RPC_PROXY_KEYLESS_ONLY", raising=False)
        router = ProxyRouter.from_env()
        assert router.keyless_only is False
        assert router.proxy_for(KEYED_A) is not None  # legacy behaviour unchanged

    def test_env_enables_keyless_only(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.setenv("WEBSHARE_PROXY_LIST", "proxy.example:8080")
        monkeypatch.setenv("RPC_PROXY_ENABLED", "1")
        monkeypatch.setenv("RPC_PROXY_KEYLESS_ONLY", "1")
        router = ProxyRouter.from_env()
        assert router.keyless_only is True
        assert router.proxy_for(KEYLESS) is not None
        assert router.proxy_for(KEYED_A) is None
