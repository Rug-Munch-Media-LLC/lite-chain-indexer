"""BigQuery REST transport — one job per query, free result-page paging.

OFFSET paging re-executes the query per page and re-scans the partition
(2026-09-22 incident: 5 x 49.55 GiB burned a fresh monthly quota). This
transport instead runs ONE job and walks ``getQueryResults`` pageTokens:
result pages are served from the job's cached results — free. It also
exposes the free ``dryRun`` byte estimator so fills can be priced BEFORE
they spend anything.
"""
from __future__ import annotations

import os
import time
from typing import Any, Callable, Iterator

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_common import BigQueryQuotaError, is_quota_error

logger = get_logger(__name__)

_API = "https://bigquery.googleapis.com/bigquery/v2"
_TOKEN_CACHE: dict[str, tuple[Any, float]] = {}
_TRANSPORT: Callable[..., dict[str, Any]] | None = None  # test seam
_TOKEN_PROVIDER: Callable[[str | None], str] | None = None  # test seam


def set_transport(fn: Callable[..., dict[str, Any]] | None) -> None:
    """Inject a fake transport for tests (None restores the real one)."""
    global _TRANSPORT
    _TRANSPORT = fn


def set_token_provider(fn: Callable[[str | None], str] | None) -> None:
    """Inject a token provider for tests (None restores real SA auth)."""
    global _TOKEN_PROVIDER
    _TOKEN_PROVIDER = fn


def _token(creds_path: str | None) -> str:
    """Access token for the SA key file (cached until ~1 min before expiry)."""
    if _TOKEN_PROVIDER is not None:
        return _TOKEN_PROVIDER(creds_path)
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    path = creds_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "/srv/secrets/gcp-biglake-sa.json"
    cached = _TOKEN_CACHE.get(path)
    if cached and cached[1] - 60 > time.time():
        return str(cached[0].token)
    creds = service_account.Credentials.from_service_account_file(
        path, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    creds.refresh(Request())
    expires = creds.expiry.timestamp() if creds.expiry else time.time() + 3000
    _TOKEN_CACHE[path] = (creds, expires)
    return str(creds.token)


def _http(method: str, url: str, *, token: str, params: dict[str, Any] | None = None,
          payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """One REST call; quota rejections become BigQueryQuotaError."""
    if _TRANSPORT is not None:
        return _TRANSPORT(method, url, token=token, params=params, payload=payload)
    with httpx.Client(timeout=180) as client:
        resp = client.request(method, url, params=params, json=payload,
                              headers={"Authorization": f"Bearer {token}"})
    if resp.status_code >= 400:
        detail = resp.text[:300]
        if is_quota_error(detail):
            raise BigQueryQuotaError(detail)
        raise RuntimeError(f"bigquery_rest {resp.status_code}: {detail}")
    return resp.json()


def _param_spec(entry: tuple[str, str, Any]) -> dict[str, Any]:
    """(name, type, value) -> jobs.query queryParameters entry."""
    name, typ, value = entry
    if typ.startswith("ARRAY<") and typ.endswith(">"):
        inner = typ[len("ARRAY<"):-1]
        return {
            "name": name,
            "parameterType": {"type": "ARRAY", "arrayType": {"type": inner}},
            "parameterValue": {"arrayValues": [{"value": str(v)} for v in (value or [])]},
        }
    return {
        "name": name,
        "parameterType": {"type": typ},
        "parameterValue": {"value": None if value is None else str(value)},
    }


def _rows_of(resp: dict[str, Any]) -> list[dict[str, Any]]:
    schema = (resp.get("schema") or {}).get("fields") or []
    names = [f.get("name") for f in schema]
    out: list[dict[str, Any]] = []
    for row in resp.get("rows") or []:
        out.append(dict(zip(names, [c.get("v") for c in row.get("f") or []], strict=False)))
    return out


def estimate_scan_bytes(sql: str, params: list[tuple[str, str, Any]], *, project: str,
                        creds_path: str | None = None) -> int:
    """FREE dryRun: exact bytes this query would scan, without scanning."""
    body = {
        "query": sql, "useLegacySql": False, "dryRun": True,
        "parameterMode": "NAMED", "queryParameters": [_param_spec(p) for p in params],
    }
    resp = _http("POST", f"{_API}/projects/{project}/queries", token=_token(creds_path), payload=body)
    return int(resp.get("totalBytesProcessed") or 0)


def stream_query(sql: str, params: list[tuple[str, str, Any]], *, project: str,
                 creds_path: str | None = None, page_size: int = 50_000) -> Iterator[list[dict[str, Any]]]:
    """Yield pages of ONE query job — result pages never re-scan.

    Pages are walked by ``startIndex`` against the job's ``totalRows``
    (deterministic; the result pages are served from the job's cache, so
    only the first page costs a partition scan).
    """
    body = {
        "query": sql, "useLegacySql": False, "parameterMode": "NAMED",
        "queryParameters": [_param_spec(p) for p in params],
        "maxResults": page_size, "timeoutMs": 120_000,
    }
    token = _token(creds_path)
    resp = _http("POST", f"{_API}/projects/{project}/queries", token=token, payload=body)
    job = resp.get("jobReference") or {}
    job_id = job.get("jobId", "")
    location = job.get("location", "US")
    waited = 0
    while not resp.get("jobComplete", True) and waited < 300:
        time.sleep(2)
        waited += 2
        resp = _http("GET", f"{_API}/projects/{project}/queries/{job_id}", token=token,
                     params={"location": location, "maxResults": page_size})
    total = int(resp.get("totalRows") or 0)
    fetched = 0
    while True:
        rows = _rows_of(resp)
        if not rows:
            break
        yield rows
        fetched += len(rows)
        if fetched >= total:
            break
        resp = _http("GET", f"{_API}/projects/{project}/queries/{job_id}", token=token,
                     params={"startIndex": fetched, "maxResults": page_size, "location": location})
