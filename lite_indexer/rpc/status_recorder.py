"""Record the HTTP status of the last completed attempt, per endpoint.

WHY THIS EXISTS. The 2026-09-26 pool walk needs to distinguish three
outcomes that the RPC call path flattens into one: a 200 with a pruned
result, a pool-wide 403, and an endpoint that never answered. The call path
(:mod:`lite_indexer.rpc.client_response`) *has* the status — it passes it
to the throttle/ledger recorders on every attempt — but it did not return
it, so the only place it was visible was inside those recorders.

Rather than change ``call``'s return shape (a dozen call sites, and the
status is only meaningful for exactly one of them), the call path already
hands every status to :func:`note_response_status`. This module reads that
record back per endpoint.

SCOPE, DELIBERATELY NARROW: the record is per endpoint and is cleared by
:meth:`StatusRecorder.begin` before each read, so a read that never reached
the wire reports ``None`` rather than a stale 200 from an earlier call.
The record is best-effort telemetry — a failure to note or read it must
never propagate into the RPC path.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Per-endpoint status of the most recent completed attempt. Bounded by the
#: pool size (single digits per chain) plus whatever endpoints appear in
#: one process; a plain dict is correct here, and an unbounded map keyed on
#: an endpoint URL would be a slow leak on a long crawl.
_last_status: dict[str, int] = {}


class StatusRecorder:
    """Per-endpoint 'what status answered the last attempt' record.

    A tiny object rather than free functions so the call path keeps one
    narrow import and the read side reads naturally.
    """

    @staticmethod
    def note(endpoint: str, status: int) -> None:
        """Record the HTTP status that answered one attempt on ``endpoint``."""
        _last_status[endpoint] = status

    @staticmethod
    def begin(endpoint: str) -> None:
        """Forget any previous status for ``endpoint`` before a new read.

        Called at the start of a read so a transport failure reports "no
        response" (``None``) instead of inheriting the 200 that some
        earlier, unrelated call to the same endpoint produced.
        """
        _last_status.pop(endpoint, None)

    @staticmethod
    def last(endpoint: str) -> int | None:
        """Status of the last completed attempt on ``endpoint``, or ``None``."""
        return _last_status.get(endpoint)


#: Module-level handles so both sides can be used without importing the
#: class — the call path and the reader should not need the same name.
note_response_status = StatusRecorder.note
begin_response_status = StatusRecorder.begin
last_response_status = StatusRecorder.last


__all__ = [
    "StatusRecorder",
    "begin_response_status",
    "last_response_status",
    "note_response_status",
]
