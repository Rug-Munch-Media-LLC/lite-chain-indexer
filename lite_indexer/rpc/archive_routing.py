"""
Archive-class method classification + fail-closed routing guards.

ARCHIVE-class calls touch historical state / logs and can only be answered
by archive-capable nodes: ``eth_getLogs``, ``eth_getProof``, and the
``debug_*`` / ``trace_*`` families. Routing one at a non-archive node fails
SILENTLY (empty logs / missing-trie error), so selection and dispatch are
fail-closed.

Split out of ``rpc/capabilities.py`` to keep that module focused and under
the file-size budget. The capability registry itself lives there; this
module reads it lazily to avoid an import cycle.
"""

from __future__ import annotations

# ARCHIVE class: historical-state / log methods.
ARCHIVE_METHODS: frozenset[str] = frozenset({"eth_getLogs", "eth_getLogsV2", "eth_getProof"})
ARCHIVE_METHOD_PREFIXES: tuple[str, ...] = ("debug_", "trace_")
# eth_getLogs is also answerable by a non-archive node that advertised a
# positive probed range (it serves recent windows); an archive flag OR a
# positive getlogs_max_range admits it.
_ARCHIVE_GETLOGS_METHODS: frozenset[str] = frozenset({"eth_getLogs", "eth_getLogsV2"})


class NoArchiveEndpointError(RuntimeError):
    """Raised when an ARCHIVE-class call has no archive-capable endpoint.

    Fail-closed: an archive method must NEVER fall back to a non-archive
    endpoint (that silently returns wrong/empty data). Callers either wait
    for an archive node to recover or surface this as a hard error.
    """

    def __init__(self, method: str, chain: str | None, endpoints: list[str]) -> None:
        self.method = method
        self.chain = chain
        self.endpoints = list(endpoints)
        super().__init__(
            f"no archive-capable endpoint for {method}{f' on {chain}' if chain else ''} ({len(endpoints)} candidates)"
        )


# Backwards-friendly alias (both names refer to the same fail-closed error).
ArchiveRoutingError = NoArchiveEndpointError


def method_requires_archive(method: str) -> bool:
    """Whether an RPC method is ARCHIVE-class (historical state/logs).

    ARCHIVE class: ``eth_getLogs``, ``eth_getProof``, and the ``debug_*`` /
    ``trace_*`` families. Everything else is TIP-class.
    """
    if method in ARCHIVE_METHODS:
        return True
    return method.startswith(ARCHIVE_METHOD_PREFIXES)


def is_archive_capable(endpoint: str, method: str) -> bool:
    """Whether ``endpoint``'s probed capability admits this ARCHIVE method.

    Fail-closed: an endpoint with no capability record is NOT considered
    archive-capable. For ``eth_getLogs`` a positive ``getlogs_max_range``
    also qualifies (a non-archive node can serve recent windows); other
    archive methods require ``archive is True``.
    """
    from lite_indexer.rpc.capabilities import get_capability

    record = get_capability(endpoint)
    if record is None:
        return False
    if method in _ARCHIVE_GETLOGS_METHODS:
        # A capability-excluded endpoint (broad topic-only getLogs refused
        # live) must never be admitted for getLogs, regardless of an
        # archive flag or a positive range probe.
        if record.logs_capable is False:
            return False
        return bool(record.archive or record.getlogs_max_range > 0)
    return bool(record.archive)


def archive_capable_endpoints(endpoints: list[str], method: str) -> list[str]:
    """Filter ``endpoints`` to those whose capability admits ``method``."""
    return [ep for ep in endpoints if is_archive_capable(ep, method)]


def archive_guard_reason(endpoint: str, method: str) -> str | None:
    """Reason an ARCHIVE-class call must not be dispatched to ``endpoint``.

    Returns ``None`` when the call may proceed: TIP-class methods, or an
    archive-class endpoint whose capability is known-good. An endpoint with
    NO capability record is treated as *unprobed* (allowed) so the pipeline
    still runs before verification has populated the registry; a KNOWN
    non-archive endpoint is fail-closed.
    """
    from lite_indexer.rpc.capabilities import get_capability

    if not method_requires_archive(method):
        return None
    if get_capability(endpoint) is None:
        return None
    if is_archive_capable(endpoint, method):
        return None
    return "endpoint_not_archive_capable"


__all__ = [
    "ARCHIVE_METHODS",
    "ARCHIVE_METHOD_PREFIXES",
    "ArchiveRoutingError",
    "NoArchiveEndpointError",
    "archive_capable_endpoints",
    "archive_guard_reason",
    "is_archive_capable",
    "method_requires_archive",
]
