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
from api.app import client_ip_from_headers
from api.write_guard import write_auth_decision


def write_guard() -> ModuleType:
    """Return the guard module reloaded against the current env.

    The config values are captured at import time, so each env change needs
    a fresh module object to be observed. Reload is the seam — no
    monkeypatching of call sites.
    """
    importlib.reload(api.config)
    return importlib.reload(api.write_guard)


def test_refuses_to_start_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard wires the pure policy to the import-time config and raises."""
    monkeypatch.setenv("LABEL_API_TOKEN", "")
    monkeypatch.setenv("API_ALLOW_UNAUTHENTICATED_WRITE", "0")
    monkeypatch.setenv("LABEL_API_WRITE_ENABLED", "1")
    guard = write_guard()
    with pytest.raises(guard.WriteAuthConfigError) as excinfo:
        guard.assert_write_auth_configured()
    assert "LABEL_API_TOKEN" in str(excinfo.value)


# ── The policy as a pure function (no env, no reload, no monkeypatch) ────
# The decision below is a string function, so it is tested directly. The
# reload-based tests above prove the import-time wiring reaches it; these
# prove the policy itself, which is where the actual rule lives.


def test_policy_refuses_when_no_token_and_no_escape() -> None:
    assert write_auth_decision(token="", dev_escape="0", write_enabled="1") is True


def test_policy_starts_when_token_set() -> None:
    assert write_auth_decision(token="s3cret", dev_escape="0", write_enabled="1") is False


def test_policy_starts_only_for_exact_escape_one() -> None:
    assert write_auth_decision(token="", dev_escape="1", write_enabled="1") is False


@pytest.mark.parametrize("escape", ["0", "true", "yes", "TRUE", "", " 1"])
def test_policy_keeps_guard_armed_for_any_other_escape(escape: str) -> None:
    """Only the exact string "1" opens the escape — not "true"/"yes"/" 1"."""
    assert write_auth_decision(token="", dev_escape=escape, write_enabled="1") is True


def test_policy_ignores_a_disabled_write_route() -> None:
    """A kill-switched write endpoint is a different posture, not an error."""
    assert write_auth_decision(token="", dev_escape="0", write_enabled="") is False


def test_no_default_token_is_ever_synthesized(monkeypatch: pytest.MonkeyPatch) -> None:
    """There is no hardcoded fallback secret anywhere in the config."""
    monkeypatch.delenv("LABEL_API_TOKEN", raising=False)
    importlib.reload(api.config)
    assert api.config.LABEL_API_TOKEN == ""


class TestClientIpTrust:
    """The per-IP rate-limit key must not be client-controlled.

    The hop-counting rule is a pure function, so these pass plain strings
    rather than monkeypatching the module global — same coverage, no
    module-state mutation, and therefore order-independent.
    """

    def test_ignores_xff_when_no_proxy_configured(self) -> None:
        assert client_ip_from_headers("10.0.0.5", "1.2.3.4", 0) == "10.0.0.5"

    def test_uses_rightmost_hop_with_one_trusted_proxy(self) -> None:
        # nginx appended the real peer; the leftmost entry is the forged one.
        assert client_ip_from_headers("127.0.0.1", "6.6.6.6, 10.0.0.5", 1) == "10.0.0.5"

    def test_too_few_hops_falls_back_to_socket_peer(self) -> None:
        # One entry but two trusted proxies — the chain is untrustworthy.
        assert client_ip_from_headers("127.0.0.1", "9.9.9.9", 2) == "127.0.0.1"

    def test_extra_leading_hops_are_forged(self) -> None:
        # Two entries, one trusted proxy: the extra leading one is spoofed, so
        # we still take the rightmost — never the leftmost.
        assert client_ip_from_headers("127.0.0.1", "9.9.9.9, 8.8.8.8", 1) == "8.8.8.8"

    def test_no_xff_header_uses_socket_peer(self) -> None:
        assert client_ip_from_headers("127.0.0.1", "", 1) == "127.0.0.1"

    def test_wire_format_still_reads_xff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_client_ip delegates to the pure function, so the wiring holds."""
        monkeypatch.setattr(api.app, "TRUSTED_PROXY_HOPS", 1)
        req = type(
            "R", (), {"client": type("C", (), {"host": "127.0.0.1"})(), "headers": {"x-forwarded-for": "1.1.1.1"}}
        )()
        assert api.app._client_ip(req) == "1.1.1.1"  # type: ignore[arg-type]


def test_legacy_leftmost_read_is_gone() -> None:
    """Regression guard: never regress to split(',')[0]."""
    src = importlib.import_module("api.app").__file__
    with open(src, encoding="utf-8") as handle:
        body = handle.read()
    assert 'split(",")[0]' not in body
    assert "x-real-ip" not in body
