"""SSRF protection + webhook management auth (audit SEC-3a/SEC-3b, 2026-09-19).

Covers:

* ``ssrf.resolve_and_check`` / ``is_safe_webhook_url`` — blocked IP classes
  (loopback, RFC1918, link-local, CGNAT, IPv6 loopback/link-local/ULA,
  unspecified) and the port rule (443 or >= 1024 only).
* Registration rejects private targets; public hosts pass.
* DNS-rebinding: public at register, private at send → blocked at send.
* ``WEBHOOKS_API_KEY`` gate on DELETE and GET logs (401 when set and
  missing/wrong; passes with the right key; open when unset).

Resolvers are injected via the ``ssrf._default_resolver`` seam — no
monkeypatching of call sites.
"""

from __future__ import annotations

import ipaddress
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
            "0.0.0.0",  # unspecified
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
        monkeypatch.setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
        assert is_safe_webhook_url(url) is False

    def test_safe_urls(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
        assert is_safe_webhook_url("https://example.com/x") is True
        assert is_safe_webhook_url("https://example.com:8443/x") is True


# ── Registration + send-time re-check ────────────────────────────────


@pytest.fixture()
def api_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """TestClient with the webhook router backed by an isolated store."""
    from api.app import create_app

    store = WebhookStore(tmp_path / "ssrf-webhooks.db")
    delivery = WebhookDelivery(enabled=False, store=store)

    monkeypatch.setattr("lite_indexer.webhooks.router._delivery", delivery)
    monkeypatch.setattr("lite_indexer.webhooks.router._store", store)
    monkeypatch.setattr("lite_indexer.webhooks.ssrf._default_resolver", _literal_resolver)
    monkeypatch.setattr("lite_indexer.webhooks.auth.WEBHOOKS_API_KEY", None)

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
        monkeypatch.setattr(
            "lite_indexer.webhooks.ssrf._default_resolver",
            _fake_resolver("10.1.2.3"),  # rebinding: now private
        )
        posted = []

        class FakeHttp:
            async def post(self, url, **kwargs):
                posted.append(url)

        monkeypatch.setattr(delivery, "_http", lambda: FakeHttp())
        delivered, status, _dur, error = await delivery._attempt_post(
            sub, "wid", 1, "chain.ethereum.transfer", "transfer", "ethereum", b"{}"
        )
        assert delivered is False
        assert posted == []
        assert status is None
        assert "not allowed" in (error or "")


# ── API-key gate (SEC-3b) ────────────────────────────────────────────


class TestApiKeyGate:
    def _register(self, client: TestClient) -> str:
        return client.post(
            "/api/v1/webhooks",
            json={"url": "https://hook.example.com/ev", "subjects": ["chain.ethereum.transfer"]},
        ).json()["id"]

    def test_key_required_when_set(self, api_client: TestClient, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr("lite_indexer.webhooks.auth.WEBHOOKS_API_KEY", "sekrit")
        sub_id = self._register(api_client)
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}").status_code == 401
        assert api_client.get(f"/api/v1/webhooks/{sub_id}/logs").status_code == 401
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}", headers={"X-API-Key": "wrong"}).status_code == 401
        assert api_client.get(f"/api/v1/webhooks/{sub_id}/logs", headers={"X-API-Key": "sekrit"}).status_code == 200
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}", headers={"X-API-Key": "sekrit"}).status_code == 204

    def test_open_when_unset(self, api_client: TestClient):
        sub_id = self._register(api_client)
        assert api_client.delete(f"/api/v1/webhooks/{sub_id}").status_code == 204
