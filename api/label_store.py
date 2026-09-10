"""Write path for the canonical ``wallet_memory.entity_labels`` label graph.

The read API (``api/db.py``) connects with the SELECT-only ``chain_api``
user. Persisting labels is a different privilege, so this module owns a
separate, lazily-created ClickHouse client built from the write
credentials (``CH_WRITE_*`` falling back to ``CH_*``/``CLICKHOUSE_*``).

Both the label-growth pipeline (which POSTs batches to
``/api/v1/labels``) and the label importer write the same canonical
9-column row shape. ``entity_labels`` is a ``ReplacingMergeTree`` keyed on
``(address, chain_id, source)`` — re-inserting a label is an idempotent
upsert, never a duplicate.

No secrets are hardcoded: every credential comes from the environment.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from api.config import (
    CH_WRITE_DATABASE,
    CH_WRITE_HOST,
    CH_WRITE_PASSWORD,
    CH_WRITE_PORT,
    CH_WRITE_USER,
)

# Canonical 9-column order — must match the INSERT below and the scanner's
# ``EntityLabel.to_ch_row``.
ENTITY_LABELS_DDL = """
CREATE TABLE IF NOT EXISTS entity_labels (
    address     String,
    chain_id    LowCardinality(String),
    entity_type LowCardinality(String) DEFAULT 'unknown',
    label_type  LowCardinality(String) DEFAULT 'unknown',
    name        String DEFAULT '',
    source      LowCardinality(String) DEFAULT '',
    provenance  LowCardinality(String) DEFAULT '',
    first_seen  DateTime DEFAULT now(),
    confidence  Float32 DEFAULT 0.8
) ENGINE = ReplacingMergeTree(first_seen)
ORDER BY (address, chain_id, source);
"""

_INSERT_SQL = (
    "INSERT INTO entity_labels "
    "(address, chain_id, entity_type, label_type, name, "
    " source, provenance, first_seen, confidence) VALUES"
)

_client: Any | None = None


def get_write_client() -> Any | None:
    """Return a lazily-connected ClickHouse write client, or None.

    Returns None (rather than raising) when ClickHouse is unreachable so
    the router can answer 503 without leaking driver internals.
    """
    global _client
    if _client is not None:
        return _client
    try:
        from clickhouse_driver import Client

        client = Client(
            host=CH_WRITE_HOST,
            port=CH_WRITE_PORT,
            user=CH_WRITE_USER,
            password=CH_WRITE_PASSWORD,
            database=CH_WRITE_DATABASE,
            connect_timeout=5,
            send_receive_timeout=30,
        )
        client.execute("SELECT 1")
        _client = client
        return _client
    except Exception:  # noqa: BLE001 — unavailable backend degrades to None
        return None


def ensure_entity_labels_table() -> bool:
    """Idempotently create the ``entity_labels`` table (best-effort).

    Returns True when the table is present after the call. Called by the
    router on first write so a fresh deployment never 500s on a missing
    table; the SQL migration (``016_entity_labels.sql``) is the canonical
    provisioning path.
    """
    client = get_write_client()
    if client is None:
        return False
    try:
        client.execute("CREATE DATABASE IF NOT EXISTS wallet_memory")
        client.execute(ENTITY_LABELS_DDL)
        return True
    except Exception:  # noqa: BLE001 — DDL probe is best-effort
        return False


def insert_entity_labels(labels: list[dict[str, Any]]) -> int:
    """Insert canonical label rows; returns the number accepted for write.

    ``labels`` are dicts with the canonical keys. ``first_seen`` defaults
    to now when absent. Returns 0 when ClickHouse is unavailable or the
    batch is empty — the caller decides whether that is a 503 or a no-op.
    """
    rows = [_to_row(label) for label in labels if label.get("address")]
    if not rows:
        return 0
    client = get_write_client()
    if client is None:
        return 0
    if not ensure_entity_labels_table():
        return 0
    client.execute(_INSERT_SQL, rows)
    return len(rows)


def _to_row(label: dict[str, Any]) -> tuple[Any, ...]:
    """Map a label dict to the canonical 9-column ClickHouse tuple."""
    first_seen = label.get("first_seen")
    if not isinstance(first_seen, datetime):
        first_seen = datetime.now(UTC)
    return (
        str(label["address"]).strip().lower(),
        str(label.get("chain") or "unknown").strip().lower(),
        str(label.get("entity_type") or "unknown"),
        str(label.get("label_type") or "unknown"),
        str(label.get("name") or ""),
        str(label.get("source") or ""),
        str(label.get("provenance") or ""),
        first_seen,
        float(label.get("confidence", 0.8) or 0.8),
    )


__all__ = [
    "ENTITY_LABELS_DDL",
    "ensure_entity_labels_table",
    "get_write_client",
    "insert_entity_labels",
]
