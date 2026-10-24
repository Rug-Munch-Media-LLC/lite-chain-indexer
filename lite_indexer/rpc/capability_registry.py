"""In-memory + durable registry of endpoint capability records.

Split out of capabilities.py (2026-10-24). Persistence is write-through to the
shared SQLite gating table; behaviour is unchanged.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger
from lite_indexer.rpc.capability_constants import CAPABILITY_PERSIST_TTL
from lite_indexer.rpc.capability_record import (
    CapabilityRecord,
    _record_from_payload,
    _record_payload,
)

logger = get_logger(__name__)


# ── Registry ───────────────────────────────────────────────────────────
# endpoint URL -> CapabilityRecord. The pool admission / routing layer
# reads this via get_capability() to prefer archive endpoints for
# historical ranges and to size getLogs chunks within the probed maximum.
#
# Persisted to the shared SQLite gating table (D2, 2026-09-14): an in-memory
# dict alone meant every daemon restart re-probed the entire world. Records
# are written through on set and loaded lazily/at startup.
_CAPABILITY_RECORDS: dict[str, CapabilityRecord] = {}
_CAPABILITY_KIND = "capability"


def _persist_record(record: CapabilityRecord) -> None:
    """Write-through one capability record to the durable gating store."""
    try:
        from lite_indexer.rpc import cache_db

        cache_db.state_put(_CAPABILITY_KIND, record.endpoint, _record_payload(record), ttl=CAPABILITY_PERSIST_TTL)
    except Exception:  # noqa: BLE001 — persistence must never break probing
        logger.debug("capability_persist_failed", endpoint=record.endpoint[:60])


def load_capabilities() -> int:
    """Load persisted capability verdicts into memory (daemon startup).

    Returns the number of records loaded. Fresh in-memory entries win.
    """
    try:
        from lite_indexer.rpc import cache_db

        stored = cache_db.state_load(_CAPABILITY_KIND)
    except Exception:  # noqa: BLE001
        logger.debug("capability_load_failed")
        return 0
    loaded = 0
    for endpoint, text in stored.items():
        if endpoint in _CAPABILITY_RECORDS:
            continue
        record = _record_from_payload(endpoint, text)
        if record is not None:
            _CAPABILITY_RECORDS[endpoint] = record
            loaded += 1
    if loaded:
        logger.info("capabilities_loaded", records=loaded)
    return loaded


def clear_capabilities() -> None:
    """Drop the in-memory capability registry (tests / shutdown)."""
    _CAPABILITY_RECORDS.clear()


def get_capability(endpoint: str) -> CapabilityRecord | None:
    """Return the capability record for ``endpoint`` (memory, then disk)."""
    record = _CAPABILITY_RECORDS.get(endpoint)
    if record is not None:
        return record
    try:
        from lite_indexer.rpc import cache_db

        text = cache_db.state_get(_CAPABILITY_KIND, endpoint)
    except Exception:  # noqa: BLE001
        return None
    if text is None:
        return None
    record = _record_from_payload(endpoint, text)
    if record is not None:
        _CAPABILITY_RECORDS[endpoint] = record
    return record


def peek_capability(endpoint: str) -> CapabilityRecord | None:
    """Return the IN-MEMORY capability record only (never touches disk).

    Hot-path safe: chunk sizing calls this per endpoint per range, and the
    registry is hydrated once at startup (:func:`load_capabilities`), so the
    disk fallback in :func:`get_capability` would only add latency here.
    """
    return _CAPABILITY_RECORDS.get(endpoint)


def set_capability(record: CapabilityRecord) -> None:
    """Persist a capability record for its endpoint URL (memory + disk)."""
    _CAPABILITY_RECORDS[record.endpoint] = record
    _persist_record(record)


def capability_records() -> dict[str, CapabilityRecord]:
    """Return a copy of the in-memory endpoint capability registry."""
    return dict(_CAPABILITY_RECORDS)
