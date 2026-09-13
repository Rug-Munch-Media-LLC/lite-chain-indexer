"""
Optional ``msgspec`` JSON fast path for hot RPC response parsing.

Every ``eth_getLogs`` / ``eth_call`` / ``eth_blockNumber`` response is JSON
decoded. ``msgspec.json.decode`` is a C-accelerated decoder that is several
times faster than the stdlib on large payloads; when it is importable we run
it on the response BYTES, otherwise this is exactly ``json.loads``.

Parity contract: on ANY msgspec error/edge case we fall back to stdlib
``json.loads``, so success values and failure semantics (error dicts, missing
fields, malformed, non-UTF8, empty) are identical to the previous behaviour.
``msgspec`` is optional (the ``perf`` extra).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

_decoder: Callable[[bytes], Any] | None
try:
    import msgspec

    _decoder = msgspec.json.decode
except ImportError:  # pragma: no cover — optional dependency
    _decoder = None


def msgspec_available() -> bool:
    """Whether the msgspec fast path is active."""
    return _decoder is not None


def decode_bytes(data: bytes) -> Any:
    """Decode JSON ``bytes`` with msgspec when available, else stdlib.

    Falls back to ``json.loads`` on any msgspec failure so the result and
    the raised exception type are identical to the stdlib path.
    """
    if _decoder is not None:
        try:
            return _decoder(data)
        except Exception:  # noqa: BLE001 — fall back for byte-identical parity
            pass
    return json.loads(data)


def decode_response(resp: Any) -> Any:
    """Decode an httpx-like response, preferring the raw-byte fast path.

    Uses ``resp.content`` (bytes) through :func:`decode_bytes` when present
    and non-empty; otherwise defers to ``resp.json()`` so lightweight test
    doubles without a ``content`` attribute keep working unchanged.
    """
    content = getattr(resp, "content", None)
    if isinstance(content, (bytes, bytearray)) and content:
        return decode_bytes(bytes(content))
    return resp.json()


__all__ = ["decode_bytes", "decode_response", "msgspec_available"]
