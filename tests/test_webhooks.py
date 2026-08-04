"""
Tests for the webhook delivery platform (lite_indexer.webhooks).

Covers:
  1. NATS subject matching + event/chain parsing + HMAC signing
  2. SQLite store CRUD (subscriptions + delivery log)
  3. WebhookDelivery subscribe / unsubscribe / list
  4. deliver(): success, non-2xx retry, transport-error retry,
     signature headers, budget exhaustion
  5. NATS start/stop lifecycle with a fake connection
  6. Indexer wiring (webhook_delivery property + lazy start)
  7. API endpoints (register, list, delete, logs, validation)
"""

from __future__ import annotations

import hashlib

# Test methods intentionally omit return annotations (repo-wide test
# convention); scope mypy's no-untyped-def to this file only.
# mypy: disable-error-code="no-untyped-def"
import hmac
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from lite_indexer.webhooks import WebhookDelivery
from lite_indexer.webhooks.delivery import _parse_subject, _sign
from lite_indexer.webhooks.storage import WebhookStore, _subject_matches

# ── Helpers ─────────────────────────────────────────────────────────


def _make_delivery(
    tmp_path,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    max_retries: int = 3,
    backoffs: tuple[float, ...] = (0.0, 0.0, 0.0),
    total_timeout: float = 30.0,
    timeout: float = 10.0,
    clock: Any = time.monotonic,
    enabled: bool = True,
) -> WebhookDelivery:
    store = WebhookStore(tmp_path / "webhooks.db")
    return WebhookDelivery(
        enabled=enabled,
        store=store,
        max_retries=max_retries,
        backoffs=backoffs,
        total_timeout_seconds=total_timeout,
        timeout_seconds=timeout,
        transport=transport,
        clock=clock,
    )


def _echo_handler(secret: str = ""):
    """MockTransport handler that verifies headers + signature and returns 200."""

    def handler(request: httpx.Request) -> httpx.Response:
        sig = request.headers.get("X-LCI-Signature", "")
        assert sig.startswith("sha256=")
        expected = hmac.new(secret.encode(), request.content, hashlib.sha256).hexdigest()
        assert sig == f"sha256={expected}"
        assert request.headers["X-LCI-Event"]
        assert request.headers["X-LCI-Chain"]
        return httpx.Response(200, json={"received": True})

    return handler


def _status_handler(statuses: list[int]):
    """Return the next status from ``statuses`` on each call (last repeats)."""

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses[0] if len(statuses) == 1 else statuses.pop(0)
        return httpx.Response(status, json={"status": status})

    return handler


class FakeMsg:
    """Stand-in for a NATS Msg."""

    def __init__(self, subject: str, data: bytes) -> None:
        self.subject = subject
        self.data = data


class FakeNCSimple:
    """Minimal NATS client fake: records subscriptions, supports close."""

    is_connected = True

    def __init__(self) -> None:
        self.subs: list[tuple[str, Any]] = []
        self.closed = False

    async def subscribe(self, subject: str, cb: Any = None, **kwargs: Any) -> Any:
        self.subs.append((subject, cb))
        return SimpleNamespace(subject=subject)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_webhook_singleton():
    """Keep the process-wide singleton clean between tests."""
    WebhookDelivery._shared = None
    yield
    WebhookDelivery._shared = None


# ── Subject parsing / matching / signing ────────────────────────────


class TestSubjectParsing:
    def test_evm_subjects(self):
        assert _parse_subject("chain.ethereum.transfer") == ("transfer", "ethereum")
        assert _parse_subject("chain.base.dex.swap") == ("swap", "base")
        assert _parse_subject("chain.bsc.deploy") == ("deploy", "bsc")

    def test_whale_alerts(self):
        assert _parse_subject("alerts.whale.solana") == ("whale", "solana")

    def test_unknown_subject(self):
        assert _parse_subject("something.else") == ("something.else", "")


class TestSubjectMatching:
    def test_exact(self):
        assert _subject_matches("chain.ethereum.transfer", "chain.ethereum.transfer")

    def test_chain_wildcard(self):
        assert _subject_matches("chain.*.transfer", "chain.base.transfer")
        assert not _subject_matches("chain.*.transfer", "chain.base.dex.swap")

    def test_tail_wildcard(self):
        assert _subject_matches("chain.ethereum.>", "chain.ethereum.dex.swap")
        assert _subject_matches("alerts.whale.>", "alerts.whale.tron")

    def test_no_match(self):
        assert not _subject_matches("chain.ethereum.transfer", "chain.ethereum.dex.swap")
        assert not _subject_matches("chain.ethereum.transfer", "alerts.whale.ethereum")


class TestSigning:
    def test_hmac_sha256_hex(self):
        sig = _sign("s3cret", b"hello")
        assert len(sig) == 64
        expected = hmac.new(b"s3cret", b"hello", hashlib.sha256).hexdigest()
        assert sig == expected

    def test_empty_secret_consistent(self):
        assert _sign("", b"x") == _sign("", b"x")


# ── Store CRUD ──────────────────────────────────────────────────────


class TestStore:
    def test_create_get_list_delete(self, tmp_path):
        store = WebhookStore(tmp_path / "webhooks.db")
        sub = store.create_subscription("https://hook.example.com/ev", ["chain.ethereum.transfer"], "sek")
        assert sub.id
        assert sub.url == "https://hook.example.com/ev"
        assert sub.subjects == ["chain.ethereum.transfer"]
        assert sub.secret == "sek"
        assert sub.failure_count == 0

        fetched = store.get_subscription(sub.id)
        assert fetched is not None and fetched.id == sub.id
        assert fetched.secret == "sek"
        assert store.list_subscriptions() == [sub]
        assert store.delete_subscription(sub.id) is True
        assert store.get_subscription(sub.id) is None
        assert store.delete_subscription(sub.id) is False

    def test_matching_subscriptions(self, tmp_path):
        store = WebhookStore(tmp_path / "webhooks.db")
        store.create_subscription("https://a.example.com", ["chain.ethereum.transfer"])
        store.create_subscription("https://b.example.com", ["alerts.whale.>"])
        store.create_subscription("https://c.example.com", ["chain.*.dex.swap"])

        matches = store.matching_subscriptions("chain.ethereum.transfer")
        assert [m.url for m in matches] == ["https://a.example.com"]
        matches = store.matching_subscriptions("alerts.whale.solana")
        assert [m.url for m in matches] == ["https://b.example.com"]
        matches = store.matching_subscriptions("chain.base.dex.swap")
        assert [m.url for m in matches] == ["https://c.example.com"]
        assert store.matching_subscriptions("chain.ethereum.deploy") == []

    def test_delivery_log_and_failure_tracking(self, tmp_path):
        store = WebhookStore(tmp_path / "webhooks.db")
        sub = store.create_subscription("https://a.example.com", ["chain.ethereum.transfer"])

        store.record_attempt(sub.id, "chain.ethereum.transfer", "failed", 500, 12)
        store.record_attempt(sub.id, "chain.ethereum.transfer", "error", None, 7)
        updated = store.get_subscription(sub.id)
        assert updated is not None and updated.failure_count == 2

        store.record_success(sub.id, "chain.ethereum.transfer", 200, 5)
        updated = store.get_subscription(sub.id)
        assert updated is not None and updated.failure_count == 0
        assert updated.last_success is not None

        log = store.delivery_log(sub.id)
        assert len(log) == 3
        assert log[0].status == "success"
        assert log[0].status_code == 200
        assert log[0].duration_ms == 5
        assert log[1].status == "error"
        assert log[1].status_code is None


# ── WebhookDelivery subscribe / unsubscribe ─────────────────────────


class TestSubscriptionManagement:
    async def test_subscribe_returns_id_and_persists(self, tmp_path):
        delivery = _make_delivery(tmp_path)
        sub_id = await delivery.subscribe("https://hook.example.com", ["chain.ethereum.transfer"], "secret")
        assert sub_id
        subs = delivery.list_subscriptions()
        assert len(subs) == 1
        assert subs[0].id == sub_id
        assert subs[0].url == "https://hook.example.com"
        assert subs[0].secret == "secret"

    async def test_unsubscribe_unknown_returns_false(self, tmp_path):
        delivery = _make_delivery(tmp_path)
        assert await delivery.unsubscribe("nope") is False

    async def test_unsubscribe_removes(self, tmp_path):
        delivery = _make_delivery(tmp_path)
        sub_id = await delivery.subscribe("https://hook.example.com", ["chain.ethereum.transfer"])
        assert await delivery.unsubscribe(sub_id) is True
        assert delivery.list_subscriptions() == []


# ── deliver() ───────────────────────────────────────────────────────


class TestDeliver:
    async def test_deliver_success_with_signature(self, tmp_path):
        secret = "whale-secret"
        transport = httpx.MockTransport(_echo_handler(secret))
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"], secret)

        results = await delivery.deliver("chain.ethereum.transfer", b'{"value":"1"}')
        assert results == [True]

        log = delivery.delivery_log(delivery.list_subscriptions()[0].id)
        assert len(log) == 1
        assert log[0].status == "success"
        assert log[0].status_code == 200

    async def test_deliver_matches_only_configured_subjects(self, tmp_path):
        transport = httpx.MockTransport(_echo_handler())
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])
        await delivery.subscribe("https://hook2.example.com/ev", ["alerts.whale.>"])

        results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [True]
        # The whale-only webhook must NOT receive the transfer event.
        log = delivery.delivery_log(delivery.list_subscriptions()[1].id)
        assert log == []

    async def test_deliver_no_matching_subscription(self, tmp_path):
        transport = httpx.MockTransport(_echo_handler())
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])
        assert await delivery.deliver("chain.ethereum.deploy", b"{}") == []

    async def test_deliver_retries_non_2xx_then_succeeds(self, tmp_path):
        transport = httpx.MockTransport(_status_handler([500, 500, 200]))
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        with patch("lite_indexer.webhooks.delivery.asyncio.sleep", new=AsyncMock()):
            results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [True]

        sub_id = delivery.list_subscriptions()[0].id
        log = delivery.delivery_log(sub_id)
        # delivery_log returns newest-first.
        assert [a.status for a in log] == ["success", "failed", "failed"]

    async def test_deliver_retries_transport_error(self, tmp_path):
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        delivery = _make_delivery(tmp_path, transport=httpx.MockTransport(handler))
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        with patch("lite_indexer.webhooks.delivery.asyncio.sleep", new=AsyncMock()):
            results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [False]

        sub_id = delivery.list_subscriptions()[0].id
        log = delivery.delivery_log(sub_id)
        assert len(log) == 4  # initial + 3 retries
        assert all(a.status == "error" for a in log)

    async def test_deliver_stops_retrying_after_max_retries(self, tmp_path):
        transport = httpx.MockTransport(_status_handler([500]))
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        with patch("lite_indexer.webhooks.delivery.asyncio.sleep", new=AsyncMock()):
            results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [False]

        sub_id = delivery.list_subscriptions()[0].id
        log = delivery.delivery_log(sub_id)
        assert len(log) == 4  # initial + 3 retries, never more
        assert all(a.status == "failed" for a in log)
        sub = delivery.list_subscriptions()[0]
        assert sub.failure_count == 4

    async def test_deliver_budget_exhaustion_drops(self, tmp_path):
        # total_timeout=0 → the first deadline check fails immediately.
        delivery = _make_delivery(tmp_path, transport=httpx.MockTransport(_status_handler([500])), total_timeout=0.0)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [False]
        # No HTTP attempt: the budget check precedes any request. One
        # "dropped" row records that the delivery was abandoned.
        log = delivery.delivery_log(delivery.list_subscriptions()[0].id)
        assert len(log) == 1
        assert log[0].status == "dropped"
        assert log[0].status_code is None

    async def test_deliver_uses_exponential_backoff_schedule(self, tmp_path, monkeypatch):
        sleeps: list[float] = []
        now = [1000.0]

        def clock() -> float:
            return now[0]

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        monkeypatch.setattr("lite_indexer.webhooks.delivery.asyncio.sleep", fake_sleep)
        transport = httpx.MockTransport(_status_handler([500, 500, 500]))
        delivery = _make_delivery(
            tmp_path,
            transport=transport,
            max_retries=3,
            backoffs=(1.0, 4.0, 16.0),
            clock=clock,
        )
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [False]
        assert sleeps == [1.0, 4.0, 16.0], "backoff schedule must be 1s, 4s, 16s"

    async def test_deliver_never_sleeps_past_budget(self, tmp_path, monkeypatch):
        sleeps: list[float] = []
        now = [1000.0]

        def clock() -> float:
            return now[0]

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        monkeypatch.setattr("lite_indexer.webhooks.delivery.asyncio.sleep", fake_sleep)
        transport = httpx.MockTransport(_status_handler([500]))
        delivery = _make_delivery(
            tmp_path,
            transport=transport,
            max_retries=3,
            backoffs=(1.0, 4.0, 16.0),
            total_timeout=5.0,  # 1s + 4s fit, 16s would blow the budget
            clock=clock,
        )
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"])

        results = await delivery.deliver("chain.ethereum.transfer", b"{}")
        assert results == [False]
        # 1s fits under the 5s budget; 4s would land exactly on the
        # deadline and 16s would exceed it — neither is slept.
        assert sleeps == [1.0]


# ── NATS lifecycle ──────────────────────────────────────────────────


class TestNatsLifecycle:
    async def test_start_subscribes_to_event_subjects(self, tmp_path):
        fake = FakeNCSimple()
        delivery = _make_delivery(tmp_path)
        with patch("nats.connect", new=AsyncMock(return_value=fake)):
            started = await delivery.start()
        assert started is True
        assert delivery.started is True
        subjects = {s for s, _ in fake.subs}
        assert subjects == {"chain.>", "alerts.>"}

    async def test_start_idempotent(self, tmp_path):
        fake = FakeNCSimple()
        delivery = _make_delivery(tmp_path)
        with patch("nats.connect", new=AsyncMock(return_value=fake)):
            assert await delivery.start() is True
            assert await delivery.start() is True
        assert len(fake.subs) == 2, "second start must be a no-op"

    async def test_start_failure_returns_false(self, tmp_path):
        delivery = _make_delivery(tmp_path)
        with patch("nats.connect", new=AsyncMock(side_effect=ConnectionError("no server"))):
            started = await delivery.start()
        assert started is False
        assert delivery.started is False

    async def test_disabled_never_starts(self, tmp_path):
        delivery = _make_delivery(tmp_path, enabled=False)
        with patch("nats.connect", new=AsyncMock()) as mock_connect:
            assert await delivery.start() is False
        mock_connect.assert_not_awaited()

    async def test_stop_closes_nats_and_is_idempotent(self, tmp_path):
        fake = FakeNCSimple()
        delivery = _make_delivery(tmp_path)
        with patch("nats.connect", new=AsyncMock(return_value=fake)):
            await delivery.start()
        await delivery.stop()
        assert fake.closed is True
        assert delivery.started is False
        await delivery.stop()  # second stop must be a no-op

    async def test_on_message_delivers_to_webhook(self, tmp_path):
        transport = httpx.MockTransport(_echo_handler("sec"))
        delivery = _make_delivery(tmp_path, transport=transport)
        await delivery.subscribe("https://hook.example.com/ev", ["chain.ethereum.transfer"], "sec")

        await delivery._on_message(FakeMsg("chain.ethereum.transfer", b'{"a":1}'))
        log = delivery.delivery_log(delivery.list_subscriptions()[0].id)
        assert log and log[0].status == "success"


# ── Indexer wiring ──────────────────────────────────────────────────


class TestIndexerWebhookWiring:
    def test_property_none_when_nats_disabled(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=False, webhook_enabled=True)
        assert indexer.webhook_delivery is None

    def test_property_none_when_webhooks_disabled(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True, webhook_enabled=False)
        assert indexer.webhook_delivery is None

    def test_property_uses_shared_singleton_when_enabled(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True, webhook_enabled=True)
        delivery = indexer.webhook_delivery
        assert delivery is not None
        assert delivery is WebhookDelivery.shared()
        assert indexer.webhook_delivery is delivery  # cached

    async def test_maybe_start_starts_delivery(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        fake = FakeNCSimple()
        delivery = _make_delivery(tmp_path)
        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True, webhook_enabled=True)
        indexer._webhook_delivery = delivery
        with patch("nats.connect", new=AsyncMock(return_value=fake)):
            await indexer._maybe_start_webhook_delivery()
        assert delivery.started is True

    async def test_maybe_start_disabled_noop(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=False, webhook_enabled=True)
        await indexer._maybe_start_webhook_delivery()  # must not raise

    async def test_maybe_start_swallows_start_errors(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        delivery = _make_delivery(tmp_path)
        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True, webhook_enabled=True)
        indexer._webhook_delivery = delivery
        with patch("nats.connect", new=AsyncMock(side_effect=ConnectionError("boom"))):
            await indexer._maybe_start_webhook_delivery()
        assert delivery.started is False


# ── API endpoints ───────────────────────────────────────────────────


@pytest.fixture()
def api_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """TestClient with the webhook router backed by an isolated store."""
    from api.app import create_app

    store = WebhookStore(tmp_path / "api-webhooks.db")
    delivery = WebhookDelivery(enabled=False, store=store)

    monkeypatch.setattr("lite_indexer.webhooks.router._delivery", delivery)
    monkeypatch.setattr("lite_indexer.webhooks.router._store", store)

    app = create_app()
    with TestClient(app) as c:
        yield c


class TestWebhookApi:
    def test_register_and_list(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"], "secret": "s3cret"},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["url"] == "https://hook.example.com/ev"
        assert body["subjects"] == ["chain.ethereum.transfer"]
        assert "secret" not in body  # never echoed
        sub_id = body["id"]

        resp = api_client.get("/api/v1/webhooks")
        assert resp.status_code == 200
        assert [w["id"] for w in resp.json()] == [sub_id]

    def test_register_rejects_http_url(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "http://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]},
        )
        assert resp.status_code == 422

    def test_register_rejects_empty_subjects(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": []},
        )
        assert resp.status_code == 422

    def test_register_rejects_invalid_subject(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain ethereum transfer"]},
        )
        assert resp.status_code == 422

    def test_unregister(self, api_client: TestClient):
        sub_id = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]},
        ).json()["id"]
        resp = api_client.delete(f"/api/v1/webhooks/{sub_id}")
        assert resp.status_code == 204
        assert api_client.get("/api/v1/webhooks").json() == []

    def test_unregister_unknown_404(self, api_client: TestClient):
        assert api_client.delete("/api/v1/webhooks/nope").status_code == 404

    def test_logs(self, api_client: TestClient):
        sub_id = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]},
        ).json()["id"]
        # Record an attempt directly in the shared store.
        from lite_indexer.webhooks.router import get_store

        get_store().record_attempt(sub_id, "chain.ethereum.transfer", "failed", 500, 12)

        resp = api_client.get(f"/api/v1/webhooks/{sub_id}/logs")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["status"] == "failed"
        assert body[0]["status_code"] == 500

    def test_logs_unknown_404(self, api_client: TestClient):
        assert api_client.get("/api/v1/webhooks/nope/logs").status_code == 404
