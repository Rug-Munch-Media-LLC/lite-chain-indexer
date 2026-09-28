"""Pure chunk planning for JSON-RPC batches.

The array POST is bounded by ``RPC_BATCH_MAX`` elements, so a long call list
must be split into consecutive slices before dispatch. That split is a
decision about a sequence and an integer — not about HTTP — so it lives here
and is testable without a client, a socket, or a patched module global.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any


def batch_chunks(
    calls: Sequence[tuple[str, list[Any] | None]],
    batch_max: int,
) -> Iterator[list[tuple[str, list[Any] | None]]]:
    """Split ``calls`` into consecutive slices of at most ``batch_max`` elements.

    Order is preserved: element ``i`` of the input lands in exactly one chunk
    at the same relative position, which is what lets the caller stitch
    per-element results back into input order.

    A ``batch_max`` below 1 is clamped to 1, so a misconfigured value yields
    one call per request (slow, never stuck) rather than an empty loop or a
    ZeroDivision-style crash mid-dispatch.

    Args:
        calls: ``(method, params)`` pairs, in the caller's order.
        batch_max: Maximum elements per array POST.

    Yields:
        ``(start_index, chunk)`` pairs, in order, so a caller can stitch
        per-element results back into input position.
    """
    width = max(1, batch_max)
    for start in range(0, len(calls), width):
        yield start, list(calls[start : start + width])


__all__ = ["batch_chunks"]
