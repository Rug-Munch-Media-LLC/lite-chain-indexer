"""Provider-aware rotation for lake reads (split for size policy).

``read_with_rotation`` is a thin dispatcher: it asks the lake router for the
best provider for ``(chain, table)`` and runs the read through the reader that
matches that provider's kind.

* ``gcp/*``         -> :class:`~lite_indexer.query.bigquery_source.BigQuerySource`
  (the caller's structured filters are handed over verbatim)
* ``s3://`` / other -> the caller's DuckDB/S3 ``_read_raw`` path

Failures and empty reads fall through to the next candidate (bounded).

Extracted 2026-09-21 from biglake_source.py; made provider-aware same day.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Bounded rotation: max providers tried per read before giving up.
_MAX_LAKE_ATTEMPTS = 3

#: Provider kinds routed to the BigQuery warehouse backend.
_GCP_PREFIX = "gcp/"


_READ_IMPL: Callable[..., list[dict[str, Any]]] | None = None


def set_read_with_rotation(fn: Callable[..., list[dict[str, Any]]] | None) -> None:
    """Swap the rotation read loop (None restores the real one).

    Injection seam so a test can serve a fixed page without monkeypatching this
    module. Same pattern as :mod:`lite_indexer.rpc.routing.seams`.
    """
    global _READ_IMPL
    _READ_IMPL = fn


def read_with_rotation(
    source: Any,
    read: tuple[str, list[Any]],
    table: str,
    biglake_table: str,
    filters: dict[str, Any] | None = None,
    *,
    chain: str = "eth",
    bigquery: Any | None = None,
) -> list[dict[str, Any]]:
    """Try providers in rotation; fall through on failure or empty reads.

    Args:
        source: The home lake source (supplies ``_router``/``_read_raw``/``_map_row``).
        read: ``(sql, params)`` for the home source's own DuckDB/S3 reader.
        table: Logical table name used for row mapping.
        biglake_table: Physical lake table used for provider selection.
        filters: Structured filters (``address``/``from_block``/``to_block``/
            ``dates``/``limit``) forwarded to a foreign backend unchanged.
        chain: Selection chain (default ``eth``).
        bigquery: Optional BigQuery backend (tests); defaults to a real source.
    """
    router = source._router()
    if _READ_IMPL is not None:
        return _READ_IMPL(source, read, table, biglake_table, filters)
    filters = filters or {}
    tried: set[str] = set()
    for _ in range(_MAX_LAKE_ATTEMPTS):
        prov = router.select(chain, biglake_table, exclude=tried)
        if prov is None:
            logger.warning("biglake_no_provider", table=table, tried=sorted(tried))
            return []
        tried.add(prov.name)
        try:
            rows = _run_provider(source, prov, read, table=table, chain=chain, filters=filters, bigquery=bigquery)
        except Exception as exc:  # noqa: BLE001 - tier failures must not break routing
            router.report_failure(prov.name, error=str(exc))
            logger.warning("biglake_query_failed", provider=prov.name, error=str(exc)[:120])
            continue
        router.report_success(prov.name)
        if not rows:
            continue  # empty read -> fall through to the next candidate
        return rows
    logger.warning("biglake_all_providers_exhausted", table=table, tried=sorted(tried))
    return []


def _run_provider(
    source: Any,
    prov: Any,
    read: tuple[str, list[Any]],
    *,
    table: str,
    chain: str,
    filters: dict[str, Any],
    bigquery: Any | None,
) -> list[dict[str, Any]]:
    """Execute one provider's read with the reader matching its kind."""
    if str(prov.kind).startswith(_GCP_PREFIX):
        return _read_bigquery(source, table=table, chain=chain, filters=filters, bigquery=bigquery)
    sql, params = read
    cols, raw_rows = source._read_raw(sql, params)
    if not raw_rows:
        return []
    return [source._map_row(table, dict(zip(cols, row, strict=False))) for row in raw_rows]


def _read_bigquery(
    source: Any, *, table: str, chain: str, filters: dict[str, Any], bigquery: Any | None
) -> list[dict[str, Any]]:
    """Delegate a ``gcp/*`` provider read to the BigQuery warehouse backend."""
    from lite_indexer.query.bigquery_source import BigQuerySource, bigquery_table_for

    bq_table = bigquery_table_for(table)
    if bq_table is None:
        raise ValueError(f"no bigquery mapping for lake table {table!r}")
    backend = bigquery
    if backend is None:
        backend = source if isinstance(source, BigQuerySource) else BigQuerySource(router=source._router())
    return backend.query(bq_table, chain, **filters)
