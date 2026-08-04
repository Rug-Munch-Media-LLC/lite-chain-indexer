"""Wallet intelligence endpoints.

``GET /api/v1/intel/{address}`` returns the full intelligence report
(entity, risk, cluster, similar wallets, activity); the ``/risk``,
``/cluster`` and ``/similar`` sub-resources return single sections; and
``GET /api/v1/intel/alerts`` returns recent high-risk / anomalous
activity.

All data comes from the local ClickHouse warehouse plus best-effort free
external sources (GoPlus, Chainabuse, rekt.news, CryptoscamDB). The
modules are lazy singletons; when the warehouse is unreachable the
endpoints return 503 and the rest of the API keeps working.

NOTE: ``/alerts`` is declared before ``/{address}`` so it is not captured
as an address path parameter.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api import db
from api.config import LANCE_DIR, VECTOR_SEARCH_ENABLED
from lite_indexer.intel import DataSources, IntelReport

router = APIRouter(prefix="/api/v1/intel", tags=["intel"])

_report: IntelReport | None = None
_report_error: str | None = None


def get_report() -> IntelReport:
    """Return the lazy singleton report builder, or raise HTTP 503."""
    global _report, _report_error
    if _report is None and _report_error is None:
        try:
            vector_store = None
            if VECTOR_SEARCH_ENABLED:
                try:
                    from lite_indexer.vectors.lance_store import LanceVectorStore

                    vector_store = LanceVectorStore(path=LANCE_DIR)
                except Exception as exc:  # noqa: BLE001 — vectors are optional for intel
                    _report_error = f"vector store unavailable: {exc}"
            _report = IntelReport(DataSources(query=db.query), vector_store=vector_store)
        except Exception as exc:  # noqa: BLE001 — surface any init failure
            _report_error = str(exc)
            raise HTTPException(status_code=503, detail=f"intel unavailable: {exc}") from exc
    if _report is None:
        raise HTTPException(status_code=503, detail=f"intel unavailable: {_report_error}")
    return _report


def _normalize_address(address: str) -> str:
    """Validate + lowercase an address path parameter."""
    addr = address.strip().lower()
    if not addr or addr in ("null", "undefined"):
        raise HTTPException(status_code=422, detail="address must not be empty")
    return addr


@router.get("/alerts", summary="Recent high-risk / anomalous activity")
async def intel_alerts(
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, bsc)"),
) -> dict[str, Any]:
    """Return recent anomalous wallet activity.

    Two anomaly classes are merged: *tainted deployers* (recent contract
    deployers whose own address or funder is tagged in the scam
    database) and *new-wallet volume* (wallets younger than 48h moving
    unusual transfer volume).
    """
    report = get_report()
    try:
        anomalies = await report.clusterer.detect_anomalies(chain=chain)
    except Exception as exc:  # noqa: BLE001 — surface warehouse errors as 503
        raise HTTPException(status_code=503, detail=f"anomaly scan failed: {exc}") from exc
    return {"chain": chain, "count": len(anomalies), "anomalies": anomalies}


@router.get("/{address}", summary="Full wallet intelligence report")
async def intel_report(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, bsc)"),
) -> dict[str, Any]:
    """Return the complete intelligence report for ``address``."""
    addr = _normalize_address(address)
    report = get_report()
    try:
        doc = await report.full_report(addr, chain=chain)
    except Exception as exc:  # noqa: BLE001 — surface warehouse errors as 503
        raise HTTPException(status_code=503, detail=f"intel report failed: {exc}") from exc
    return doc


@router.get("/{address}/risk", summary="Wallet risk score only")
async def intel_risk(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, bsc)"),
) -> dict[str, Any]:
    """Return the 0-100 risk score with factor breakdown."""
    addr = _normalize_address(address)
    report = get_report()
    try:
        return await report.scorer.score(addr, chain=chain)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"risk scoring failed: {exc}") from exc


@router.get("/{address}/cluster", summary="Related addresses (same entity)")
async def intel_cluster(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, bsc)"),
) -> dict[str, Any]:
    """Return addresses plausibly controlled by the same entity."""
    addr = _normalize_address(address)
    report = get_report()
    try:
        cluster = await report.resolver.cluster_addresses(addr, chain=chain)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"clustering failed: {exc}") from exc
    return {"address": addr, "chain": chain, "count": len(cluster), "addresses": cluster}


@router.get("/{address}/similar", summary="Behaviorally similar wallets")
async def intel_similar(
    address: str,
    k: int = Query(default=10, ge=1, le=100, description="Number of results"),
) -> dict[str, Any]:
    """Return wallets with behavior similar to ``address`` (vector ANN)."""
    addr = _normalize_address(address)
    report = get_report()
    similar = await report.clusterer.find_similar_wallets(addr, k=k)
    if similar and "error" in similar[0]:
        raise HTTPException(status_code=503, detail=similar[0]["error"])
    return {"address": addr, "count": len(similar), "similar": similar}
