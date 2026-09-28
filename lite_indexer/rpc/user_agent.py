"""The single User-Agent every outbound HTTP client in this repo sends.

httpx stamps its own default (``python-httpx/<version>``) on every request
when no ``User-Agent`` is set. Several public RPC gateways sit behind a bot
filter that refuses a bare library default but answers an IDENTIFIED client,
so the honest thing to send is also the functional thing: name the product
and where to read about it.

ONE place on purpose. The RPC call path, the getLogs client, the liveness
probe, the capability probes and the endpoint verifier must all present the
SAME identity — a probe that passes with headers the real call does not send
is exactly how a dead pool used to look healthy (see :mod:`rpc.health`).

Configurable, because provider policy changes without notice: set
``RPC_USER_AGENT`` in the service environment and restart. No code change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

#: Fallback identity used when ``RPC_USER_AGENT`` is unset. Honest product
#: identification — deliberately NOT a browser string. A spoofed
#: ``Mozilla/...`` User-Agent is both dishonest and, on the gateways that
#: actually filter, frequently MORE suspicious than a named client.
DEFAULT_USER_AGENT = "lite-chain-indexer/1.0 (+https://github.com/RugMunchMedia/lite-chain-indexer)"

#: How long an operator-supplied ``RPC_USER_AGENT`` may be.
_MAX_USER_AGENT_CHARS = 200


def sanitize_user_agent(raw: str) -> str:
    """Return a header-safe User-Agent for ``raw``, or the honest default.

    Pure: takes the raw string, reads no environment. :func:`resolve_user_agent`
    is the only place that reads ``os.environ``, so the sanitising rules are
    testable directly with a string argument instead of by mutating the
    process environment.

    A newline in a header value is a request-splitting vector, so anything
    non-printable is dropped; a value that is empty once sanitised falls back
    to :data:`DEFAULT_USER_AGENT` rather than sending a malformed header.

    Returns:
        A non-empty, newline-free User-Agent string of at most
        ``_MAX_USER_AGENT_CHARS`` characters.
    """
    if not raw.strip():
        return DEFAULT_USER_AGENT
    cleaned = "".join(ch for ch in raw if ch.isprintable()).strip()
    if not cleaned:
        return DEFAULT_USER_AGENT
    return cleaned[:_MAX_USER_AGENT_CHARS]


def resolve_user_agent() -> str:
    """Return the configured RPC User-Agent, or the honest default.

    Reads ``RPC_USER_AGENT`` at call time (not import time) so a value set
    after this module was imported — and therefore every test that sets it —
    still takes effect.

    Returns:
        A non-empty, newline-free User-Agent string.
    """
    import os

    return sanitize_user_agent(os.environ.get("RPC_USER_AGENT", ""))


def rpc_headers() -> dict[str, str]:
    """Return the default headers for any client that talks to an RPC.

    ``Content-Type`` is pinned to JSON as well: httpx sets it for
    ``json=`` payloads, but the probes and the batch path spell the body out
    by hand, and an endpoint that answers a browser-shaped request but not a
    correct one is not a pool we want to depend on.

    Returns:
        A fresh dict (callers may mutate it) with the RPC identity headers.
    """
    return {
        "User-Agent": resolve_user_agent(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def client_kwargs() -> dict[str, Any]:
    """Return the ``headers=`` kwargs for an ``httpx`` client constructor.

    Exists so every construction site reads one value instead of re-deriving
    a header dict, and so a test can assert the headers a client was built
    with without reaching into httpx internals.

    Returns:
        ``{"headers": {...}}`` — spread straight into ``AsyncClient(...)``.
    """
    return {"headers": rpc_headers()}


__all__ = [
    "DEFAULT_USER_AGENT",
    "client_kwargs",
    "resolve_user_agent",
    "rpc_headers",
    "sanitize_user_agent",
]
