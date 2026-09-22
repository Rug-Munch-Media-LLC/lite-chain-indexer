"""Coverage tests for ``lite_indexer.config.rpc`` budget parsing edges.

Hermetic — pure parsing, no network. Complements ``test_rpc_budgets.py``
by pinning the malformed-input degradation contract: a bad env var must
never stop the indexer from starting, and every field is clamped to a
non-negative value.
"""

from __future__ import annotations

from lite_indexer.config.rpc import (
    DEFAULT_PROVIDER_BUDGET,
    ProviderBudget,
    _budget_from_mapping,
    parse_provider_budgets,
    resolve_provider_budget,
)


# ── _budget_from_mapping ────────────────────────────────────────────────


def test_mapping_parses_all_fields() -> None:
    budget = _budget_from_mapping(
        {"rps": 5, "max_concurrency": 2, "daily_budget": 100, "preemptive_pct": 50}
    )
    assert budget is not None
    assert budget.rps == 5.0
    assert budget.max_concurrency == 2
    assert budget.daily_budget == 100
    assert budget.preemptive_pct == 50.0
    assert budget.explicit is True


def test_mapping_non_dict_returns_none() -> None:
    assert _budget_from_mapping("not-a-dict") is None
    assert _budget_from_mapping(42) is None
    assert _budget_from_mapping(None) is None


def test_mapping_bad_numeric_returns_none() -> None:
    assert _budget_from_mapping({"rps": "fast"}) is None
    assert _budget_from_mapping({"max_concurrency": "many"}) is None


def test_mapping_clamps_negatives_to_zero() -> None:
    budget = _budget_from_mapping({"rps": -5, "max_concurrency": -2, "daily_budget": -10})
    assert budget is not None
    assert budget.rps == 0.0
    assert budget.max_concurrency == 0
    assert budget.daily_budget == 0


def test_mapping_defaults_when_fields_absent() -> None:
    budget = _budget_from_mapping({})
    assert budget is not None
    assert budget.explicit is True


# ── parse_provider_budgets ──────────────────────────────────────────────


def test_parse_empty_string_returns_empty_map() -> None:
    assert parse_provider_budgets("") == {}


def test_parse_invalid_json_returns_empty_map() -> None:
    assert parse_provider_budgets("{not json") == {}


def test_parse_non_object_json_returns_empty_map() -> None:
    assert parse_provider_budgets("[1, 2, 3]") == {}


def test_parse_skips_malformed_entries_keeps_valid() -> None:
    raw = '{"good.com": {"rps": 5}, "bad.com": "nope"}'
    out = parse_provider_budgets(raw)
    assert set(out) == {"good.com"}
    assert out["good.com"].rps == 5.0


def test_parse_skips_empty_identity_key() -> None:
    assert parse_provider_budgets('{"": {"rps": 5}}') == {}


# ── resolve_provider_budget ─────────────────────────────────────────────


def test_resolve_falls_back_to_default() -> None:
    budget = resolve_provider_budget(
        "unknown.com", "https://x/v1", provider_budgets={}, endpoint_budgets={}
    )
    assert budget is DEFAULT_PROVIDER_BUDGET
    assert budget.explicit is False


def test_resolve_endpoint_wins_over_provider() -> None:
    provider = ProviderBudget(rps=5.0, max_concurrency=2, explicit=True)
    endpoint = ProviderBudget(rps=1.0, max_concurrency=1, explicit=True)
    budget = resolve_provider_budget(
        "example.com",
        "https://x/v1",
        provider_budgets={"example.com": provider},
        endpoint_budgets={"https://x/v1": endpoint},
    )
    assert budget is endpoint


def test_resolve_provider_wins_over_default() -> None:
    provider = ProviderBudget(rps=5.0, max_concurrency=2, explicit=True)
    budget = resolve_provider_budget(
        "example.com", None, provider_budgets={"example.com": provider}, endpoint_budgets={}
    )
    assert budget is provider


def test_resolve_without_endpoint_uses_provider() -> None:
    provider = ProviderBudget(rps=5.0, max_concurrency=2, explicit=True)
    budget = resolve_provider_budget(
        "example.com", None, provider_budgets={"example.com": provider}, endpoint_budgets={}
    )
    assert budget is provider
