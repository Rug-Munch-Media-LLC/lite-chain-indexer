"""Raw read-only SQL endpoint.

``POST /api/v1/query`` executes a validated SELECT against the warehouse
and returns columns + rows as JSON. Safety is enforced twice:

1. :mod:`api.security` validates the statement (single statement,
   SELECT/WITH only, no write keywords, table allowlist, LIMIT enforced).
2. The ClickHouse user has SELECT-only grants on ``wallet_memory.*`` —
   the server refuses anything else even if validation is bypassed.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from api.config import CH_QUERY_MAX_RESULT_ROWS, MAX_PAGE_LIMIT
from api.db import query_rows
from api.security import QueryValidationError, validate_readonly

router = APIRouter(prefix="/api/v1/query", tags=["query"])


class QueryRequest(BaseModel):
    """Raw SQL request body."""

    query: str = Field(description="Read-only SELECT statement against wallet_memory.*")
    limit: int | None = Field(
        default=None,
        ge=1,
        le=MAX_PAGE_LIMIT,
        description="Hard cap on returned rows (overrides the statement LIMIT)",
    )


@router.post("", summary="Execute a read-only SQL query")
def post_query(request: Request, req: QueryRequest) -> dict[str, Any]:
    """Execute a validated read-only query and return columns + rows.

    ``request`` is injected by FastAPI and consumed by slowapi for the
    per-IP limit on this endpoint.
    """
    try:
        sql = validate_readonly(req.query)
    except QueryValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Enforce the API row cap on top of any user LIMIT: wrap the validated
    # statement in a subquery with an outer LIMIT. ClickHouse supports
    # SELECT * FROM (user query) LIMIT n.
    cap = min(req.limit or CH_QUERY_MAX_RESULT_ROWS, CH_QUERY_MAX_RESULT_ROWS)
    capped = f"SELECT * FROM ({sql}) LIMIT {cap + 1}"

    try:
        rows, column_types = query_rows(capped)
    except Exception as exc:  # noqa: BLE001 — surface ClickHouse errors cleanly
        raise HTTPException(status_code=400, detail=f"query failed: {exc}") from exc

    columns = [name for name, _ in column_types]
    truncated = len(rows) > cap
    payload_rows = rows[:cap]
    return {
        "columns": columns,
        "rows": [_serialize_row(r) for r in payload_rows],
        "row_count": len(payload_rows),
        "truncated": truncated,
    }


def _serialize_row(row: tuple[Any, ...]) -> list[Any]:
    """Convert a raw ClickHouse row into JSON-safe values."""
    return [_serialize_value(v) for v in row]


def _serialize_value(value: Any) -> Any:
    """Convert a ClickHouse value into a JSON-safe Python value."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):  # datetime / date
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)
