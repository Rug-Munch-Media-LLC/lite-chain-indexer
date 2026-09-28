"""SSRF protection + webhook management auth (audit SEC-3a/SEC-3b, 2026-09-19).

Covers:

* ``ssrf.resolve_and_check`` / ``is_safe_webhook_url`` — blocked IP classes
  (loopback, RFC1918, link-local, CGNAT, IPv6 loopback/link-local/ULA,
  unspecified) and the port rule (443 or >= 1024 only).
* Registration rejects private targets; public hosts pass.
* DNS-rebinding: public at register, private at send → blocked at send.
* ``WEBHOOK_API_KEY`` gate on POST/DELETE and GET logs — 401 when set and
  missing/wrong, passes with the right key, and **503 when unset** (the
  routes are fail-closed; only the explicit
  ``API_ALLOW_UNAUTHENTICATED_WRITE=1`` dev escape leaves them open).

Resolvers are injected via the ``ssrf._default_resolver`` seam — no
monkeypatching of call sites.
"""

from __future__ import annotations

import ipaddress
import logging
import socket

# Test methods intentionally omit return annotations (repo-wide test
# convention); scope mypy's no-untyped-def to this file only.
# mypy: disable-error-code="no-untyped-def"
import pytest
from fastapi.testclient import TestClient

from lite_indexer.webhooks.delivery import WebhookDelivery
from lite_indexer.webhooks.models import WebhookSubscription
from lite_indexer.webhooks.ssrf import is_safe_webhook_url, resolve_and_check
from lite_indexer.webhooks.storage import WebhookStore

PUBLIC = "93.184.216.34"


_UNSET = object()


def _setattr(target, name_or_value, value=_UNSET) -> None:
    """Set an attribute without the pytest monkeypatch fixture.

    Two forms: ``_setattr("module.attr", value)`` (dotted path) and
    ``_setattr(obj, "attr", value)`` (live object).
    """
    if value is _UNSET:
        import importlib

        mod_path, _, attr = str(target).rpartition(".")
        setattr(importlib.import_module(mod_path), attr, name_or_value)
    else:
        setattr(target, name_or_value, value)


def _fake_resolver(*addresses):
    """Build a resolver fn returning the given addresses for any host."""

    def resolve(host, port, *args, **kwargs):
        return [(socket.AF_INET, None, None, "", (addr, 0)) for addr in addresses]

    return resolve


def _literal_resolver(host, port, *args, **kwargs):
    """Resolve literal IPs to themselves; names to a public IP."""
    try:
        ipaddress.ip_address(host)
        addr = host
    except ValueError:
        logging.getLogger(__name__).debug("literal_resolver_name_host", host=host)
        addr = PUBLIC
    return [(socket.AF_INET, None, None, "", (addr, 0))]


# ── Pure helper ──────────────────────────────────────────────────────


class TestResolveAndCheck:
    @pytest.mark.parametrize(
        "addr",
        [
            "127.0.0.1",  # loopback
            "10.0.0.5",  # RFC1918
            "172.16.1.1",  # RFC1918
            "192.168.1.1",  # RFC1918
            "169.254.169.254",  # link-local (cloud metadata)
            "100.64.0.1",  # CGNAT
            # bandit B104: unspecified, in the SSRF deny-list under test, not a bind
            "0.0.0.0",  # nosec B104
            "::1",  # IPv6 loopback
            "fe80::1",  # IPv6 link-local
            "fc00::1",  # IPv6 unique-local
        ],
    )
    def test_blocked_addresses(self, addr: str):
        assert resolve_and_check("x", resolver=_fake_resolver(addr)) is False

    def test_public_allowed(self):
        assert resolve_and_check("x", resolver=_fake_resolver(PUBLIC)) is True

    def test_any_blocked_address_blocks(self):
        # One bad address in the set is enough (fail-closed).
        assert resolve_and_check("x", resolver=_fake_resolver(PUBLIC, "10.0.0.1")) is False

    def test_unresolvable_rejected(self):
        def fail(host, port, *a, **k):
            raise socket.gaierror("no such host")

        assert resolve_and_check("x", resolver=fail) is False

    @pytest.mark.parametrize(
        "url",
        [
            "https://127.0.0.1/x",
            "https://169.254.169.254/latest/meta-data",
            "https://10.0.0.1:8443/x",
            "http://example.com/x",  # non-https
            "https://example.com:80/x",  # port < 1024, not 443
            "https://example.com:22/x",
        ],
    )
    def test_unsafe_urls(self, url: str, monkeypatch: pytest.MonkeyPatch):
        _setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
        assert is_safe_webhook_url(url) is False

    def test_safe_urls(self, monkeypatch: pytest.MonkeyPatch):
        _setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
        assert is_safe_webhook_url("https://example.com/x") is True
        assert is_safe_webhook_url("https://example.com:8443/x") is True


# ── Registration + send-time re-check ────────────────────────────────


@pytest.fixture()
def api_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """TestClient with the webhook router backed by an isolated store."""
    from api.app import create_app

    store = WebhookStore(tmp_path / "ssrf-webhooks.db")
    delivery = WebhookDelivery(enabled=False, store=store)

    _setattr("lite_indexer.webhooks.router._delivery", delivery)
    _setattr("lite_indexer.webhooks.router._store", store)
    _setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
    # Auth gate: force the documented dev escape so the SSRF suite can
    # register without a secret. The fail-closed behaviour is asserted
    # explicitly in TestApiKeyGate.
    _setattr("lite_indexer.webhooks.auth.WEBHOOK_API_KEY", "")
    _setattr("lite_indexer.webhooks.auth.ALLOW_UNAUTHENTICATED_WRITE", True)

    app = create_app()
    with TestClient(app) as c:
        yield c


class TestRegistrationSsrf:
    def test_register_rejects_private_host(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://169.254.169.254/latest", "subjects": ["chain.ethereum.transfer"]},
        )
        assert resp.status_code == 422

    def test_register_rejects_loopback(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://127.0.0.1:9000/hook", "subjects": ["chain.ethereum.transfer"]},
        )
        assert resp.status_code == 422

    def test_register_public_ok(self, api_client: TestClient):
        resp = api_client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]},
        )
        assert resp.status_code == 201


class TestSendTimeRebind:
    async def test_rebind_blocked_at_send(self, tmp_path, monkeypatch: pytest.MonkeyPatch):
        """Host resolves public at register, private at send → POST blocked."""

        store = WebhookStore(tmp_path / "rebind.db")
        delivery = WebhookDelivery(enabled=False, store=store)
        sub = WebhookSubscription(
            id="rebind1",
            url="https://rebind.example.com/hook",
            subjects=["chain.ethereum.transfer"],
            secret="",
            created_at="2026-09-20T00:00:00Z",
        )
        _setattr(
            "lite_indexer.webhooks.ssrf._default_resolver",
            _fake_resolver("10.1.2.3"),  # rebinding: now private
        )
        posted = []

        class FakeHttp:
            async def post(self, url, **kwargs):
                posted.append(url)

        _setattr(delivery, "_http", lambda: FakeHttp())
        delivered, status, _dur, error = await delivery._attempt_post(
            sub, "wid", 1, "chain.ethereum.transfer", "transfer", "ethereum", b"{}"
        )
        assert delivered is False
        assert posted == []
        assert status is None
        assert "not allowed" in (error or "")


# ── API-key gate (SEC-3b) ────────────────────────────────────────────


class TestApiKeyGate:
    _BODY = {"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]}

    def _register(self, client: TestClient) -> str:
        return client.post("/api/v1/webhooks", json=self._BODY).json()["id"]

    def _register_authed(self, client: TestClient, key: str) -> str:
        return client.post("/api/v1/webhooks", json=self._BODY, headers={"X-API-Key": key}).json()["id"]

    def test_key_required_when_set(self, api_client: TestClient):
        _setattr("lite_indexer.webhooks.auth.WEBHOOK_API_KEY", "sekrit")
        sub_id = self._register_authed(api_client, "sekrit")
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}").status_code == 401
        assert api_client.get(f"/api/v1/webhooks/{sub_id}/logs").status_code == 401
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}", headers={"X-API-Key": "wrong"}).status_code == 401
        assert api_client.get(f"/api/v1/webhooks/{sub_id}/logs", headers={"X-API-Key": "sekrit"}).status_code == 200
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}", headers={"X-API-Key": "sekrit"}).status_code == 204

    def test_registration_refused_without_key(self, api_client: TestClient):
        """Registration is a write: unset key must refuse, not serve."""
        _setattr("lite_indexer.webhooks.auth.WEBHOOK_API_KEY", "")
        _setattr("lite_indexer.webhooks.auth.ALLOW_UNAUTHENTICATED_WRITE", False)
        resp = api_client.post("/api/v1/webhooks", json=self._BODY)
        assert resp.status_code == 503
        assert "WEBHOOK_API_KEY" in resp.json()["detail"]

    def test_registration_refused_with_wrong_key(self, api_client: TestClient):
        _setattr("lite_indexer.webhooks.auth.WEBHOOK_API_KEY", "sekrit")
        assert api_client.post("/api/v1/webhooks", json=self._BODY).status_code == 401

    def test_delete_refused_when_unset(self, api_client: TestClient):
        """Previously open-when-unset. Now 503 naming the env var."""
        sub_id = self._register(api_client)
        _setattr("lite_indexer.webhooks.auth.WEBHOOK_API_KEY", "")
        _setattr("lite_indexer.webhooks.auth.ALLOW_UNAUTHENTICATED_WRITE", False)
        resp = api_client.delete(f"/api/v1/webhooks/{sub_id}")
        assert resp.status_code == 503
        assert "WEBHOOK_API_KEY" in resp.json()["detail"]

    def test_open_only_under_dev_escape(self, api_client: TestClient):
        """The single documented escape: API_ALLOW_UNAUTHENTICATED_WRITE=1."""
        _setattr("lite_indexer.webhooks.auth.ALLOW_UNAUTHENTICATED_WRITE", True)
        sub_id = self._register(api_client)
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}").status_code == 204
