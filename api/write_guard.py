"""Startup guard for the API write surface (fail-closed).

The label write endpoint carries a shared-secret bearer token. A missing
secret used to mean "no auth" — a silent hole in a fresh clone or any
environment where the env var was never set. Unset is now a hard
misconfiguration: :func:`assert_write_auth_configured` refuses to start
the process while a write route is mounted.

The single documented escape is ``API_ALLOW_UNAUTHENTICATED_WRITE=1``,
which exists so the local dev loop can run the API with no secrets at all.
It is off by default and must stay off on a shared deployment.
"""

from __future__ import annotations

from api.config import (
    API_ALLOW_UNAUTHENTICATED_WRITE,
    LABEL_API_TOKEN,
    LABEL_API_WRITE_ENABLED,
)

#: Env var name operators must set to run the local dev loop unauthenticated.
DEV_ESCAPE_ENV = "API_ALLOW_UNAUTHENTICATED_WRITE"


class WriteAuthConfigError(RuntimeError):
    """Raised when a write route is mounted without an auth secret."""


def write_auth_decision(
    token: str,
    dev_escape: str | bool,
    write_enabled: str | bool,
) -> bool:
    """Pure: return True when startup must be refused.

    Split from :func:`assert_write_auth_configured` so the policy is testable
    without reloading a module, which is what forces ``monkeypatch.setenv``
    into the suite otherwise.

    Args:
        token: ``LABEL_API_TOKEN`` — any non-empty value is a real secret.
        dev_escape: ``API_ALLOW_UNAUTHENTICATED_WRITE``. Accepts the raw env
            string or the already-parsed bool; only the exact string ``"1"``
            (or its parsed ``True``) opens the escape, so ``"true"``/``"yes"``
            keep the guard armed.
        write_enabled: ``LABEL_API_WRITE_ENABLED`` — a kill-switched write
            route is a documented posture, not a misconfiguration.

    Returns:
        True when the caller must refuse to start.
    """
    if token:
        return False
    if dev_escape is True or dev_escape == "1":
        return False
    return bool(write_enabled)


def assert_write_auth_configured() -> None:
    """Refuse to start when a write route is mounted with no token.

    Only the mounted, enabled write route matters: the token is read
    unconditionally by the guard, so a disabled write endpoint is a
    different (documented, kill-switched) posture, not this error.

    Raises:
        WriteAuthConfigError: token unset and no dev escape set.
    """
    if not write_auth_decision(LABEL_API_TOKEN, API_ALLOW_UNAUTHENTICATED_WRITE, LABEL_API_WRITE_ENABLED):
        return
    raise WriteAuthConfigError(
        "write route mounted without an auth secret: set LABEL_API_TOKEN "
        f"(required for POST /api/v1/labels), or set {DEV_ESCAPE_ENV}=1 "
        "for local development only. Unset no longer means open."
    )


__all__ = [
    "DEV_ESCAPE_ENV",
    "WriteAuthConfigError",
    "assert_write_auth_configured",
    "write_auth_decision",
]
