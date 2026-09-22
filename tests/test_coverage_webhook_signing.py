"""Coverage tests for ``lite_indexer.webhooks.delivery.helpers`` signing.

Hermetic — pure HMAC helpers, no network. Complements
``tests/test_webhooks.py`` by pinning the Standard-Webhooks v1 signature
format, multi-secret rotation tokens, and the replay-window verifier
edges (bad timestamp, no v1 token, wrong secret).
"""

from __future__ import annotations

import base64
import hashlib
import hmac

from lite_indexer.webhooks.delivery.helpers import (
    REPLAY_WINDOW_SECONDS,
    _build_signature_header,
    _sign_v1,
    verify_signature,
)

_PAYLOAD = b'{"event":"transfer"}'
_WEBHOOK_ID = "msg_123"
_TS = 1_700_000_000


def test_sign_v1_is_base64_hmac_sha256() -> None:
    expected = base64.b64encode(
        hmac.new(b"secret", f"{_WEBHOOK_ID}.{_TS}.".encode() + _PAYLOAD, hashlib.sha256).digest()
    ).decode()
    assert _sign_v1("secret", _WEBHOOK_ID, _TS, _PAYLOAD) == expected


def test_sign_v1_is_stable_for_same_inputs() -> None:
    assert _sign_v1("s", _WEBHOOK_ID, _TS, _PAYLOAD) == _sign_v1("s", _WEBHOOK_ID, _TS, _PAYLOAD)


def test_signature_header_has_one_v1_token_per_secret() -> None:
    header = _build_signature_header("primary", ["old"], _WEBHOOK_ID, _TS, _PAYLOAD)
    tokens = header.split(" ")
    assert len(tokens) == 2
    assert all(t.startswith("v1,") for t in tokens)
    assert tokens[0] == f"v1,{_sign_v1('primary', _WEBHOOK_ID, _TS, _PAYLOAD)}"


def test_signature_header_primary_only_when_no_additional_secrets() -> None:
    header = _build_signature_header("primary", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    assert header == f"v1,{_sign_v1('primary', _WEBHOOK_ID, _TS, _PAYLOAD)}"


def test_verify_accepts_matching_signature() -> None:
    header = _build_signature_header("secret", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), header, ["secret"], now=_TS) is True


def test_verify_accepts_any_rotation_secret() -> None:
    header = _build_signature_header("new", ["old"], _WEBHOOK_ID, _TS, _PAYLOAD)
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), header, ["old"], now=_TS) is True


def test_verify_rejects_wrong_secret() -> None:
    header = _build_signature_header("secret", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), header, ["other"], now=_TS) is False


def test_verify_rejects_stale_timestamp_outside_replay_window() -> None:
    header = _build_signature_header("secret", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    stale = _TS + REPLAY_WINDOW_SECONDS + 1
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), header, ["secret"], now=stale) is False


def test_verify_accepts_retry_inside_replay_window() -> None:
    header = _build_signature_header("secret", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    later = _TS + REPLAY_WINDOW_SECONDS - 1
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), header, ["secret"], now=later) is True


def test_verify_rejects_non_numeric_timestamp() -> None:
    header = _build_signature_header("secret", [], _WEBHOOK_ID, _TS, _PAYLOAD)
    assert (
        verify_signature(_PAYLOAD, _WEBHOOK_ID, "not-a-number", header, ["secret"], now=_TS)
        is False
    )


def test_verify_rejects_header_without_v1_token() -> None:
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), "v2,abc", ["secret"], now=_TS) is False


def test_verify_rejects_empty_header() -> None:
    assert verify_signature(_PAYLOAD, _WEBHOOK_ID, str(_TS), "", ["secret"], now=_TS) is False
