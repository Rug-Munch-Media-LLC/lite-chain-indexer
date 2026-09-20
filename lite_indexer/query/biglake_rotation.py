"""Provider rotation for :class:`BigLakeSource` reads (split for size policy).

Extracted 2026-09-21 from biglake_source.py so the source module stays within
the structure ratchet.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Bounded rotation: max providers tried per read before giving up.
_MAX_LAKE_ATTEMPTS = 3


def read_with_rotation(
    source: Any,
    *,
    sql: str,
    params: list[Any],
    table: str,
    biglake_table: str,
) -> list[dict[str, Any]]:
    """Try providers in rotation; fall through on failure or empty reads."""
    router = source._router()
    tried: set[str] = set()
    for _ in range(_MAX_LAKE_ATTEMPTS):
        prov = router.select("eth", biglake_table, exclude=tried)
        if prov is None:
            logger.warning("biglake_no_provider", table=table, tried=sorted(tried))
            return []
        tried.add(prov.name)
        try:
            cols, raw_rows = source._read_raw(sql, params)
        except Exception as exc:  # noqa: BLE001 - tier failures must not break routing
            router.report_failure(prov.name, error=str(exc))
            logger.warning("biglake_query_failed", provider=prov.name, error=str(exc)[:120])
            continue
        router.report_success(prov.name)
        if not raw_rows:
            continue  # empty read -> fall through to the next candidate
        return [source._map_row(table, dict(zip(cols, row, strict=False))) for row in raw_rows]
    logger.warning("biglake_all_providers_exhausted", table=table, tried=sorted(tried))
    return []
