"""Label-graph endpoints — the canonical home of wallet labels.

Labels are **data**, so they live with the indexer that owns the raw
on-chain warehouse. Two endpoints:

* ``GET /api/v1/labels/{address}`` — entity resolution: return the
  canonical ``entity_type``/``label_type``/``name`` for an address (plus
  every matching label row, ranked by confidence). Unknown addresses are a
  valid empty answer (``resolved=false``), not a 404.
* ``POST /api/v1/labels`` — persist new labels. This is the write path for
  the label-growth pipeline (and the importer). ``entity_labels`` is a
  ``ReplacingMergeTree`` keyed on ``(address, chain_id, source)``, so a
  re-post is an idempotent upsert.

The scanner queries the GET endpoint for entity resolution and POSTs label
growth here; it no longer owns the ClickHouse label table. Risk scoring /
detection stays in the scanner — only label **storage + resolution** moved.

Writes use a separate ClickHouse write user (``api/label_store.py``); reads
use the SELECT-only ``chain_api`` user via ``api.db``. No raw DB rows ever
leave the API — every response is a Pydantic model.
"""

from __future__ import annotations

import secrets
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status

from api.config import LABEL_API_TOKEN, LABEL_API_WRITE_ENABLED
from api.db import query
from api.label_store import insert_entity_labels
from api.schemas import (
    EntityLabelBatchCreate,
    EntityLabelOut,
    EntityLabelWriteResult,
    EntityResolutionOut,
)

router = APIRouter(prefix="/api/v1/labels", tags=["labels"])

# Max label rows returned for one address. Bounded so a pathological
# address (dozens of upstream sources) cannot fan out a huge response.
_MAX_LABELS = 50
_DEFAULT_LABELS = 20

_LABEL_COLUMNS = "address, chain_id, entity_type, label_type, name, source, provenance, confidence, first_seen"


def _normalize_address(address: str) -> str:
    """Normalize a lookup address.

    ``entity_labels`` stores addresses lowercased (the scanner's
    ``EntityLabel`` normalizes on construction), so EVM and Solana lookups
    both fold to lowercase to hit the same row.
    """
    normalized = address.strip().lower()
    if not normalized:
        raise HTTPException(status_code=422, detail="address must not be empty")
    return normalized


def require_write_auth(authorization: str | None = Header(default=None)) -> None:
    """Guard the label write endpoint with an optional shared-secret bearer token.

    When ``LABEL_API_TOKEN`` is unset the endpoint is open (the API is not
    internet-exposed for writes). When set, a matching
    ``Authorization: Bearer <token>`` header is required. Comparison is
    constant-time.
    """
    if not LABEL_API_TOKEN:
        return
    expected = f"Bearer {LABEL_API_TOKEN}"
    if not authorization or not secrets.compare_digest(authorization, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing label API token",
        )


@router.get(
    "/{address}",
    response_model=EntityResolutionOut,
    summary="Resolve an address's entity label",
)
def get_address_label(
    address: str,
    chain: str | None = Query(
        default=None,
        description="Optional chain filter (e.g. 'ethereum', 'solana'). Omit to search all chains.",
    ),
    limit: int | None = Query(default=None, ge=1, le=_MAX_LABELS),
) -> EntityResolutionOut:
    """Return the entity label(s) for ``address`` (entity resolution).

    Reads the canonical ``wallet_memory.entity_labels`` table with ``FINAL``
    so the ReplacingMergeTree dedups re-imports before ranking. Rows are
    ordered by confidence (highest first); the top row is the *primary*
    label and populates the flat ``entity_type``/``label_type``/``name``
    fields. An address with no label returns ``resolved=false`` and an
    empty list (HTTP 200) — unknown is a valid answer.
    """
    normalized = _normalize_address(address)
    page_limit = min(int(limit or _DEFAULT_LABELS), _MAX_LABELS)

    params: dict[str, object] = {"address": normalized}
    chain_clause = ""
    if chain:
        params["chain"] = chain.strip().lower()
        chain_clause = "AND chain_id = %(chain)s"

    sql = (
        f"SELECT {_LABEL_COLUMNS} "
        "FROM entity_labels FINAL "
        "WHERE address = %(address)s "
        f"{chain_clause} "
        "ORDER BY confidence DESC, first_seen DESC "
        f"LIMIT {page_limit}"
    )
    rows = query(sql, params)

    labels = [_to_label(row) for row in rows]
    if not labels:
        return EntityResolutionOut(address=normalized, chain=chain, resolved=False, labels=[])

    primary = labels[0]
    return EntityResolutionOut(
        address=normalized,
        chain=chain,
        resolved=True,
        entity_type=primary.entity_type,
        label_type=primary.label_type,
        name=primary.name or None,
        primary_label=primary.name or primary.label_type,
        labels=labels,
    )


@router.post(
    "",
    response_model=EntityLabelWriteResult,
    status_code=status.HTTP_201_CREATED,
    summary="Persist entity labels (label-growth write path)",
    dependencies=[Depends(require_write_auth)],
)
def post_labels(payload: EntityLabelBatchCreate) -> EntityLabelWriteResult:
    """Persist a batch of canonical labels to ``entity_labels``.

    Idempotent upsert on ``(address, chain_id, source)``. Returns 503 when
    the write backend is disabled or unreachable — the caller (label-growth
    pipeline) should retry later rather than treat the batch as lost.
    """
    if not LABEL_API_WRITE_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="label write API is disabled",
        )

    rows = [label.model_dump() for label in payload.labels]
    inserted = insert_entity_labels(rows)
    if inserted == 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="label store unavailable",
        )
    addresses = sorted({str(row["address"]).strip().lower() for row in rows if row.get("address")})
    return EntityLabelWriteResult(inserted=inserted, addresses=addresses)


def _to_label(row: dict[str, Any]) -> EntityLabelOut:
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


__all__ = ["router"]
