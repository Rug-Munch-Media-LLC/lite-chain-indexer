"""getLogs capability classification — enums, refusal markers, classifier.

Extracted from ``rpc.capabilities``: pure classification of an
eth_getLogs response into CAPABLE / CANNOT_SERVE_LOGS / TRANSIENT with
no imports from the capability registry (no import cycle).
"""

from __future__ import annotations

import enum
from typing import Any


class _GetLogsProbeStatus(enum.Enum):
    """Outcome of a single eth_getLogs probe request."""

    OK = "ok"
    RANGE_ERROR = "range_error"
    OTHER_ERROR = "other_error"


class GetLogsCapability(enum.Enum):
    """Broad eth_getLogs capability classification for an endpoint.

    Distinct from a transient failure: ``CANNOT_SERVE_LOGS`` is a permanent
    property of the endpoint for the getLogs method (the provider rejects
    the request shape/capability rather than merely a too-large range), so it
    must never be counted as an abuse signal / ban.
    """

    CAPABLE = "capable"
    CANNOT_SERVE_LOGS = "cannot_serve_logs"
    TRANSIENT = "transient"


# Capability-rejection signals: an endpoint that answers eth_blockNumber but
# structurally refuses broad topic-only eth_getLogs. Verified live
# (2026-09-14) the public pool contains such endpoints; without this gate
# they pollute the log path and get mis-counted as bans until the pool burns
# down to the few working endpoints.
#   - HTTP 401/403/405 (auth refusal / Cloudflare challenge / method not
#     allowed) — previously only 405 was treated as a refusal, so a
#     publicnode-style 403 (HTML body) fell through to TRANSIENT and was then
#     hard-banned as ``http_403`` on the fetch path.
#   - JSON-RPC errors: -32701 (address required), -32601 (method not found)
#   - auth/tenant/key refusals and malformed-range refusals
CAPABILITY_HTTP_STATUSES: frozenset[int] = frozenset({401, 403, 405})
CAPABILITY_ERROR_CODES: frozenset[int] = frozenset({-32701, -32601})
CAPABILITY_ERROR_MARKERS: tuple[str, ...] = (
    "must authenticate",
    "unauthorized",
    "address",
    "invalid block range",
    "api key disabled",
    "tenant disabled",
    "please specify an address",
    "method not found",
    "method not allowed",
)
# HTML / bot-challenge bodies are capability refusals, not transient errors.
# Applied to a non-JSON body (and to any error message text).
CAPABILITY_BODY_MARKERS: tuple[str, ...] = (
    "just a moment",
    "attention required",
    "cloudflare",
    "captcha",
    "challenge",
    "access denied",
    "forbidden",
)


def _error_code_and_message(payload: Any) -> tuple[int | None, str]:
    """Extract ``(code, message)`` from a JSON-RPC error payload.

    Handles both the spec object form and the plain-string form some RPCs
    emit (polygon-rpc.com ``"API key disabled, tenant disabled"``).
    """
    if not isinstance(payload, dict):
        return None, ""
    err = payload.get("error")
    if err is None:
        return None, ""
    if isinstance(err, dict):
        code = err.get("code")
        try:
            code_int = int(code) if code is not None else None
        except (TypeError, ValueError):
            code_int = None
        return code_int, str(err.get("message", err))
    return None, str(err)


def classify_logs_capability(http_status: int | None, payload: Any) -> GetLogsCapability:
    """Classify one eth_getLogs response for capability gating.

    Returns :data:`GetLogsCapability.CAPABLE` only when the response carries
    a real ``result`` list; ``CANNOT_SERVE_LOGS`` for structural capability
    refusals (HTTP 401/403/405, JSON-RPC ``-32701``/``-32601``, auth/tenant/
    address markers, or a non-JSON HTML/bot-challenge body);
    ``TRANSIENT`` for everything else (timeouts, 5xx, genuine range limits —
    none of which should permanently exclude the endpoint).

    ``payload`` may be the decoded JSON object OR the raw response text
    (non-JSON bodies are still classified, never silently dropped).
    """
    if http_status in CAPABILITY_HTTP_STATUSES:
        return GetLogsCapability.CANNOT_SERVE_LOGS
    if isinstance(payload, str):
        lowered = payload.lower()
        if any(marker in lowered for marker in CAPABILITY_BODY_MARKERS):
            return GetLogsCapability.CANNOT_SERVE_LOGS
        if any(marker in lowered for marker in CAPABILITY_ERROR_MARKERS):
            return GetLogsCapability.CANNOT_SERVE_LOGS
        return GetLogsCapability.TRANSIENT
    if not isinstance(payload, dict):
        return GetLogsCapability.TRANSIENT
    if isinstance(payload.get("result"), list):
        return GetLogsCapability.CAPABLE
    code, message = _error_code_and_message(payload)
    if code is not None and code in CAPABILITY_ERROR_CODES:
        return GetLogsCapability.CANNOT_SERVE_LOGS
    lowered = message.lower()
    if any(marker in lowered for marker in CAPABILITY_ERROR_MARKERS):
        return GetLogsCapability.CANNOT_SERVE_LOGS
    if any(marker in lowered for marker in CAPABILITY_BODY_MARKERS):
        return GetLogsCapability.CANNOT_SERVE_LOGS
    return GetLogsCapability.TRANSIENT


def is_logs_capability_failure(http_status: int | None, payload: Any) -> bool:
    """Whether a getLogs response is a structural capability refusal."""
    return classify_logs_capability(http_status, payload) is GetLogsCapability.CANNOT_SERVE_LOGS
