"""Coverage tests for ``lite_indexer.webhooks.delivery`` DLQ persistence.

Hermetic — SQLite in a tmp dir, no network, no NATS. Complements
``tests/test_webhooks.py`` (full delivery lifecycle) and
``tests/test_coverage_webhook_retry.py`` (pure backoff helpers) by pinning
the ``QueueMixin`` DLQ state machine directly: insert/get, the
pending→in_flight claim guard, reschedule, dead-letter, delete, and the
due-row scan.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path

from lite_indexer.webhooks.delivery import WebhookDelivery
from lite_indexer.webhooks.models import DlqEntry
from lite_indexer.webhooks.storage import WebhookStore


def _delivery(tmp_path: Path) -> WebhookDelivery:
    return WebhookDelivery(enabled=False, store=WebhookStore(tmp_path / "webhooks.db"))


def _entry(
    entry_id: str, *, next_attempt_at: float | None = None, status: str = "pending"
) -> DlqEntry:
    return DlqEntry(
        id=entry_id,
        subscription_id="sub-1",
        subject="chain.ethereum.transfer",
        payload=b'{"a":1}',
        event_type="transfer",
        chain="ethereum",
        timestamp=1_700_000_000,
        attempts=1,
        max_attempts=8,
        next_attempt_at=time.time() if next_attempt_at is None else next_attempt_at,
        status=status,
        last_error=None,
        last_status_code=None,
        created_at=datetime.now(UTC).isoformat(),
    )


def test_insert_then_get_roundtrips(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    fetched = delivery._get_dlq_entry("e1")
    assert fetched is not None
    assert fetched.id == "e1"
    assert fetched.payload == b'{"a":1}'
    assert fetched.attempts == 1
    assert fetched.max_attempts == 8
    assert fetched.status == "pending"


def test_get_unknown_returns_none(tmp_path: Path) -> None:
    assert _delivery(tmp_path)._get_dlq_entry("missing") is None


def test_claim_pending_entry_succeeds_once(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    assert delivery._claim_dlq_entry("e1") is True
    # Second claim must fail — the entry is now in_flight.
    assert delivery._claim_dlq_entry("e1") is False


def test_claim_unknown_entry_returns_false(tmp_path: Path) -> None:
    assert _delivery(tmp_path)._claim_dlq_entry("nope") is False


def test_reschedule_sets_pending_and_new_attempt_count(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    delivery._claim_dlq_entry("e1")
    delivery._reschedule_dlq(
        "e1", attempts=2, next_attempt_at=time.time() + 5, status_code=503, error="boom"
    )
    fetched = delivery._get_dlq_entry("e1")
    assert fetched is not None
    assert fetched.status == "pending"
    assert fetched.attempts == 2
    assert fetched.last_status_code == 503
    assert fetched.last_error == "boom"


def test_mark_dead_sets_terminal_status(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    delivery._mark_dlq_dead("e1", attempts=8, reason="retries exhausted")
    fetched = delivery._get_dlq_entry("e1")
    assert fetched is not None
    assert fetched.status == "dead"
    assert fetched.attempts == 8
    assert fetched.last_error == "retries exhausted"


def test_delete_removes_entry(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    delivery._delete_dlq_entry("e1")
    assert delivery._get_dlq_entry("e1") is None


def test_due_rows_returns_only_pending_and_due(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    now = time.time()
    delivery._dlq_insert(_entry("due", next_attempt_at=now - 10))
    delivery._dlq_insert(_entry("future", next_attempt_at=now + 3600))
    delivery._dlq_insert(_entry("dead", next_attempt_at=now - 10, status="dead"))
    due = [str(row["id"]) for row in delivery._dlq_due_rows(now)]
    assert due == ["due"]


def test_due_rows_ordered_by_next_attempt(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    now = time.time()
    delivery._dlq_insert(_entry("later", next_attempt_at=now - 1))
    delivery._dlq_insert(_entry("sooner", next_attempt_at=now - 100))
    due = [str(row["id"]) for row in delivery._dlq_due_rows(now)]
    assert due == ["sooner", "later"]


def test_insert_or_replace_updates_existing_row(tmp_path: Path) -> None:
    delivery = _delivery(tmp_path)
    delivery._dlq_insert(_entry("e1"))
    delivery._dlq_insert(_entry("e1", status="dead"))
    fetched = delivery._get_dlq_entry("e1")
    assert fetched is not None
    assert fetched.status == "dead"
