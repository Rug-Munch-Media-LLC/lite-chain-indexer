"""Endpoint → provider / key / account id derivation for the quota ledger.

Extracted from ``rpc/ledger.py``: generic, provider-agnostic derivation of
the registrable-domain provider id, the hashed non-secret key id, and the
per-account quota key. Pure URL parsing — no I/O.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import ParseResult, parse_qs, unquote

from lite_indexer.rpc.ledger_budget import KNOWN_ACCOUNT_IDS

#: Query params that carry a provider API key (generic, provider-agnostic).
_KEY_QUERY_PARAMS: tuple[str, ...] = ("dkey", "apikey", "api-key", "api_key", "key", "token")
#: A path segment this long and token-shaped is treated as an embedded key.
_KEY_PATH_RE = re.compile(r"^[A-Za-z0-9_\-]{16,}$")
#: ``name=<token>`` path segments (e.g. ``api=<KEY>``, ``key=<KEY>``).
_KEY_NAME_SEG_RE = re.compile(r"^[A-Za-z0-9_.\-]+=(.+)$")


def _registrable_domain(host: str) -> str:
    """Collapse ``*.example-rpc.com`` subdomains to ``example-rpc.com``.

    Generic account grouping without a baked-in provider list: all chain
    subdomains of one provider collapse to one budget; per-key separation is
    handled by :func:`key_id_for_endpoint`.
    """
    parts = [p for p in host.lower().split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else (host or "unknown")


def provider_id_for_endpoint(endpoint: str) -> str:
    """Derive the account-level provider id from an endpoint URL.

    The id is the registrable domain (``eth-mainnet.g.example.com`` →
    ``example.com``), so all chain subdomains of one provider share a budget
    while different providers stay separate. Key-less hosts group by their
    own domain and never degrade unless the operator seeds a quota.
    """
    from urllib.parse import urlparse

    return _registrable_domain(urlparse(endpoint).hostname or "")


def key_id_for_endpoint(endpoint: str) -> str:
    """Best-effort, NON-SECRET API-key id for a keyed endpoint.

    Keys are detected generically, in this precedence order:

    1. a recognised key query param
       (``dkey``/``apikey``/``api-key``/``api_key``/``key``/``token``);
    2. URL **userinfo** (``user[:pass]@host``) — the username is hashed, or
       ``user:pass`` when a password is present;
    3. the longest token-shaped path segment **anywhere** in the path
       (``/v2/<KEY>``, ``/rpc/<KEY>``, ``/<acct>/<KEY>/...``) or the value of
       a ``name=<token>`` segment (``api=<KEY>``).

    The raw value is HASHED (sha256, first 16 hex chars) so the id is stable
    per key-account but the secret is never persisted in the quota ledger or
    exposed in limiter state.

    Empty for key-less endpoints — they group by provider_id alone.
    """
    from urllib.parse import urlparse

    parsed = urlparse(endpoint)
    raw = _key_raw_value(parsed)

    if not raw:
        return ""
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _key_raw_value(parsed: ParseResult) -> str:
    """Resolve the raw key candidate from a parsed URL (precedence 1-3)."""
    # 1. Explicit query-param key (highest precedence, existing behaviour).
    qs = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    for param in _KEY_QUERY_PARAMS:
        values = qs.get(param)
        if values:
            return values[0]

    # 2. URL userinfo (e.g. https://user:pass@host/...).
    if parsed.username is not None:
        user = unquote(parsed.username)
        password = unquote(parsed.password) if parsed.password is not None else ""
        return f"{user}:{password}" if password else user

    # 3. Path candidates: token-shaped segments or name=value values.
    return _key_from_path(parsed.path)


def _key_from_path(path: str) -> str:
    """Longest token-shaped / ``name=<token>`` path segment, else empty."""
    candidates: list[str] = []
    for segment in path.split("/"):
        if not segment:
            continue
        name_match = _KEY_NAME_SEG_RE.match(segment)
        if name_match:
            candidates.append(name_match.group(1))
        elif _KEY_PATH_RE.match(segment):
            candidates.append(segment)
    return max(candidates, key=len) if candidates else ""


def account_id_for_endpoint(endpoint: str) -> str:
    """Resolve the quota ACCOUNT for an endpoint (never empty).

    Quota is metered per ACCOUNT, not per key: N keys belonging to one
    account share ONE budget, so extra keys add rate but never quota. The
    account is resolved as:

    1. a configured ``key_id`` override (:data:`KNOWN_ACCOUNT_IDS`) — lets an
       operator map one key to a named account;
    2. a configured ``provider_id`` override — groups every key of a provider
       into one named account;
    3. otherwise the ``provider_id`` itself (one account per provider).

    The ``key_id`` is NEVER returned, so quota can never be fanned out across
    keys. Per-key rate limits are tracked separately (``rpc/budgets.py``).
    """
    provider = provider_id_for_endpoint(endpoint)
    key_id = key_id_for_endpoint(endpoint)
    if key_id and key_id in KNOWN_ACCOUNT_IDS:
        return KNOWN_ACCOUNT_IDS[key_id]
    return KNOWN_ACCOUNT_IDS.get(provider, provider)
