"""Row → model mapping for the label-graph read path.

Split out of ``api/routers/labels.py`` so the router holds only the
endpoint bodies. The mapping is pure: no DB, no HTTP, no config.
"""

from __future__ import annotations

from typing import Any

from api.schemas import EntityLabelOut


def to_label_out(row: dict[str, Any]) -> EntityLabelOut:
    """Map one ClickHouse row to the public Pydantic label model."""
    first_seen = row.get("first_seen")
    return EntityLabelOut(
        address=str(row.get("address", "")),
        chain=str(row.get("chain_id", "")),
        entity_type=str(row.get("entity_type") or "unknown"),
        label_type=str(row.get("label_type") or "unknown"),
        name=str(row.get("name") or ""),
        source=str(row.get("source") or ""),
        provenance=str(row.get("provenance") or ""),
        confidence=float(row.get("confidence") or 0.0),
        first_seen=str(first_seen).replace(" ", "T") + "Z" if first_seen else None,
    )


__all__ = ["to_label_out"]
