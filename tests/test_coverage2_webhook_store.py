"""Coverage tests for ``lite_indexer.webhooks.storage`` + ``models``.

Hermetic — SQLite in a tmp dir, no network, no NATS. Complements
``tests/test_webhooks.py`` (which drives the full delivery lifecycle) by
pinning the store's CRUD edges and the derived-health contract directly:
failure counting, success reset, log ordering/limit clamping, and the
NATS subject-pattern matcher.
"""

from __future__ import annotations

from pathlib import Path

from lite_indexer.webhooks.models import (
    HEALTH_BREAK,
    HEALTH_DISABLE,
    HEALTH_WARN,
    WebhookSubscription,
)
from lite_indexer.webhooks.storage import WebhookStore, _subject_matches


def _store(tmp_path: Path) -> WebhookStore:
    return WebhookStore(tmp_path / "webhooks.db")


# ── health derivation ───────────────────────────────────────────────────


def test_health_thresholds_are_ordered() -> None:
    assert HEALTH_WARN < HEALTH_BREAK < HEALTH_DISABLE


def test_health_healthy_below_warn() -> None:
    assert WebhookSubscription(id="a", url="https://x", failure_count=0).health == "healthy"
    assert (
        WebhookSubscription(id="a", url="https://x", failure_count=HEALTH_WARN - 1).health
        == "healthy"
    )


def test_health_warn_at_threshold() -> None:
    assert WebhookSubscription(id="a", url="https://x", failure_count=HEALTH_WARN).health == "warn"


def test_health_broken_at_threshold() -> None:
    assert (
        WebhookSubscription(id="a", url="https://x", failure_count=HEALTH_BREAK).health == "broken"
    )


def test_health_disabled_at_threshold() -> None:
    assert (
        WebhookSubscription(id="a", url="https://x", failure_count=HEALTH_DISABLE).health
        == "disabled"
    )


# ── subscription CRUD ───────────────────────────────────────────────────


def test_create_then_get_roundtrips(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription(
        "https://example.com/hook", ["chain.ethereum.transfer"], secret="s3cr3t"
    )
    fetched = store.get_subscription(sub.id)
    assert fetched is not None
    assert fetched.url == "https://example.com/hook"
    assert fetched.subjects == ["chain.ethereum.transfer"]
    assert fetched.secret == "s3cr3t"
    assert fetched.failure_count == 0
    assert fetched.last_success is None


def test_get_unknown_returns_none(tmp_path: Path) -> None:
    assert _store(tmp_path).get_subscription("nope") is None


def test_delete_unknown_returns_false(tmp_path: Path) -> None:
    assert _store(tmp_path).delete_subscription("nope") is False


def test_delete_existing_returns_true_and_removes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription("https://example.com/hook", ["chain.>"], secret="")
    assert store.delete_subscription(sub.id) is True
    assert store.get_subscription(sub.id) is None


def test_list_subscriptions_returns_all(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.create_subscription("https://a.example.com/h", ["chain.>"], secret="")
    store.create_subscription("https://b.example.com/h", ["alerts.>"], secret="")
    assert len(store.list_subscriptions()) == 2


# ── failure counting / success reset ────────────────────────────────────


def test_record_attempt_increments_failure_count(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription("https://example.com/hook", ["chain.>"], secret="")
    store.record_attempt(sub.id, "chain.ethereum.transfer", "failed", 500, 12)
    store.record_attempt(sub.id, "chain.ethereum.transfer", "error", None, 3)
    fetched = store.get_subscription(sub.id)
    assert fetched is not None
    assert fetched.failure_count == 2


def test_record_success_resets_failure_count_and_sets_last_success(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription("https://example.com/hook", ["chain.>"], secret="")
    store.record_attempt(sub.id, "chain.ethereum.transfer", "failed", 500, 12)
    store.record_success(sub.id, "chain.ethereum.transfer", 200, 8)
    fetched = store.get_subscription(sub.id)
    assert fetched is not None
    assert fetched.failure_count == 0
    assert fetched.last_success is not None


def test_delivery_log_returns_attempts_newest_first(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription("https://example.com/hook", ["chain.>"], secret="")
    store.record_attempt(sub.id, "chain.ethereum.transfer", "failed", 500, 1)
    store.record_success(sub.id, "chain.ethereum.transfer", 200, 2)
    log = store.delivery_log(sub.id)
    assert len(log) == 2
    assert log[0].status == "success"


def test_delivery_log_limit_is_clamped(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sub = store.create_subscription("https://example.com/hook", ["chain.>"], secret="")
    for _ in range(3):
        store.record_attempt(sub.id, "chain.ethereum.transfer", "failed", 500, 1)
    assert len(store.delivery_log(sub.id, limit=0)) == 1  # clamped up to 1
    assert len(store.delivery_log(sub.id, limit=10_000)) == 3  # clamped down to 1000


# ── matching_subscriptions ──────────────────────────────────────────────


def test_matching_subscriptions_filters_by_pattern(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.create_subscription("https://a.example.com/h", ["chain.ethereum.>"], secret="")
    store.create_subscription("https://b.example.com/h", ["alerts.>"], secret="")
    matches = store.matching_subscriptions("chain.ethereum.transfer")
    assert len(matches) == 1
    assert matches[0].url == "https://a.example.com/h"


# ── _subject_matches ────────────────────────────────────────────────────


def test_subject_matches_exact() -> None:
    assert _subject_matches("chain.ethereum.transfer", "chain.ethereum.transfer") is True


def test_subject_matches_single_token_wildcard() -> None:
    assert _subject_matches("chain.*.transfer", "chain.ethereum.transfer") is True


def test_subject_matches_tail_wildcard() -> None:
    assert _subject_matches("chain.>", "chain.ethereum.dex.swap") is True


def test_subject_matches_rejects_token_count_mismatch() -> None:
    assert _subject_matches("chain.ethereum", "chain.ethereum.transfer") is False


def test_subject_matches_rejects_literal_mismatch() -> None:
    assert _subject_matches("chain.ethereum.transfer", "chain.base.transfer") is False


def test_subject_matches_wildcard_does_not_span_missing_tokens() -> None:
    assert _subject_matches("chain.*", "chain.ethereum.transfer") is False
