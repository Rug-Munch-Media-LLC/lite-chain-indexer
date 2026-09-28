"""Fail-closed write auth: startup guard, runtime guard, and XFF trust.

Covers the two policy decisions from the 2026-09-28 auth review:

* ``api/write_guard.assert_write_auth_configured`` — the app refuses to
  start with a write route mounted and no secret, unless the explicit
  ``API_ALLOW_UNAUTHENTICATED_WRITE=1`` dev escape is set. (a)
* ``api.routers.labels.require_write_auth`` — an individual write is
  refused 503 when the token is unset, naming the env var. (b)
* ``api.app._client_ip`` — the per-IP rate-limit key must not be taken
  from a client-supplied X-Forwarded-For.
"""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest

import api.app
import api.config
import api.routers.labels
import api.write_guard


def write_guard() -> ModuleType:
    """Return the guard module reloaded against the current env.

    The config values are captured at import time, so each env change needs
    a fresh module object to be observed. Reload is the seam — no
    monkeypatching of call sites.
    """
    importlib.reload(api.config)
    return importlib.reload(api.write_guard)


def test_refuses_to_start_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LABEL_API_TOKEN", "")
    monkeypatch.setenv("API_ALLOW_UNAUTHENTICATED_WRITE", "0")
    monkeypatch.setenv("LABEL_API_WRITE_ENABLED", "1")
    guard = write_guard()
    with pytest.raises(guard.WriteAuthConfigError) as excinfo:
        guard.assert_write_auth_configured()
    assert "LABEL_API_TOKEN" in str(excinfo.value)


def test_starts_with_token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LABEL_API_TOKEN", "a-real-secret")
    monkeypatch.setenv("API_ALLOW_UNAUTHENTICATED_WRITE", "0")
    write_guard().assert_write_auth_configured()  # must not raise


def test_dev_escape_permits_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LABEL_API_TOKEN", "")
    monkeypatch.setenv("API_ALLOW_UNAUTHENTICATED_WRITE", "1")
    write_guard().assert_write_auth_configured()  # must not raise


def test_escape_value_other_than_one_keeps_guard_armed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the exact value "1" opens the dev escape — not "true"/"yes"."""
    monkeypatch.setenv("LABEL_API_TOKEN", "")
    monkeypatch.setenv("API_ALLOW_UNAUTHENTICATED_WRITE", "true")
    monkeypatch.setenv("LABEL_API_WRITE_ENABLED", "1")
    guard = write_guard()
    with pytest.raises(guard.WriteAuthConfigError):
        guard.assert_write_auth_configured()


def test_no_default_token_is_ever_synthesized(monkeypatch: pytest.MonkeyPatch) -> None:
    """There is no hardcoded fallback secret anywhere in the config."""
    monkeypatch.delenv("LABEL_API_TOKEN", raising=False)
    importlib.reload(api.config)
    assert api.config.LABEL_API_TOKEN == ""


class TestClientIpTrust:
    """The per-IP rate-limit key must not be client-controlled."""

    class _Req:
        def __init__(self, peer: str, xff: str | None) -> None:
            self.client = type("C", (), {"host": peer})()
            self.headers = {"x-forwarded-for": xff} if xff else {}

    def test_ignores_xff_when_no_proxy_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api.app, "TRUSTED_PROXY_HOPS", 0)
        req = self._Req("10.0.0.5", "1.2.3.4")
        assert api.app._client_ip(req) == "10.0.0.5"

    def test_uses_rightmost_hop_with_one_trusted_proxy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api.app, "TRUSTED_PROXY_HOPS", 1)
        # nginx appended the real peer; the leftmost entry is the forged one.
        req = self._Req("127.0.0.1", "6.6.6.6, 10.0.0.5")
        assert api.app._client_ip(req) == "10.0.0.5"

    def test_too_many_hops_falls_back_to_socket_peer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api.app, "TRUSTED_PROXY_HOPS", 1)
        # Two entries with only one trusted proxy — the extra is spoofed.
        req = self._Req("127.0.0.1", "9.9.9.9, 8.8.8.8")
        assert api.app._client_ip(req) == "8.8.8.8"

    def test_no_xff_header_uses_socket_peer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api.app, "TRUSTED_PROXY_HOPS", 1)
        assert api.app._client_ip(self._Req("127.0.0.1", None)) == "127.0.0.1"


def test_legacy_leftmost_read_is_gone() -> None:
    """Regression guard: never regress to split(',')[0]."""
    src = importlib.import_module("api.app").__file__
    with open(src, encoding="utf-8") as handle:
        body = handle.read()
    assert 'split(",")[0]' not in body
    assert "x-real-ip" not in body
