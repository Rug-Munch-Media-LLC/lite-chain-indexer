"""REST transport tests: one job per query, free pages, dryRun pricing."""
from typing import Any

import pytest

from lite_indexer.query import bigquery_rest as rest
from lite_indexer.query.bigquery_common import BigQueryQuotaError


@pytest.fixture(autouse=True)
def _no_real_token() -> Any:
    rest.set_token_provider(lambda creds_path: "tok")
    yield
    rest.set_token_provider(None)


def test_estimate_uses_dry_run_and_returns_bytes() -> None:
    seen: list[dict[str, Any]] = []

    def fake(method: str, url: str, **kw: Any) -> dict[str, Any]:
        seen.append({"method": method, "url": url, "payload": kw.get("payload")})
        return {"totalBytesProcessed": "123456789"}

    rest.set_transport(fake)
    try:
        n = rest.estimate_scan_bytes("SELECT 1", [("dates", "ARRAY<STRING>", ["2026-09-21"])], project="p")
    finally:
        rest.set_transport(None)
    assert n == 123456789
    assert seen[0]["payload"]["dryRun"] is True
    assert seen[0]["payload"]["queryParameters"][0]["parameterType"]["type"] == "ARRAY"


def test_stream_query_one_job_free_pages() -> None:
    calls: list[tuple[str, str]] = []

    def fake(method: str, url: str, **kw: Any) -> dict[str, Any]:
        calls.append((method, url))
        if method == "POST":
            return {
                "jobReference": {"jobId": "job1", "location": "US"},
                "schema": {"fields": [{"name": "slot"}, {"name": "mint"}]},
                "rows": [{"f": [{"v": "1"}, {"v": "A"}]}],
                "totalRows": "2",
                "jobComplete": True,
            }
        assert kw["params"]["startIndex"] == 1
        return {
            "schema": {"fields": [{"name": "slot"}, {"name": "mint"}]},
            "rows": [{"f": [{"v": "2"}, {"v": "B"}]}],
            "totalRows": "2",
        }

    rest.set_transport(fake)
    try:
        pages = list(rest.stream_query("SELECT x", [], project="p"))
    finally:
        rest.set_transport(None)
    assert pages == [[{"slot": "1", "mint": "A"}], [{"slot": "2", "mint": "B"}]]
    posts = [c for c in calls if c[0] == "POST"]
    assert len(posts) == 1  # exactly ONE job — pages never re-scan
    assert any(c[0] == "GET" for c in calls)


def test_quota_error_maps_to_exception() -> None:
    def fake(method: str, url: str, **kw: Any) -> dict[str, Any]:
        raise BigQueryQuotaError("Quota exceeded: free query bytes scanned")

    rest.set_transport(fake)
    try:
        with pytest.raises(BigQueryQuotaError):
            rest.estimate_scan_bytes("SELECT 1", [], project="p")
    finally:
        rest.set_transport(None)


def test_param_spec_scalar_passthrough() -> None:
    spec = rest._param_spec(("limit", "INT64", 1000))
    assert spec["parameterType"] == {"type": "INT64"}
    assert spec["parameterValue"] == {"value": "1000"}


def test_stream_query_walks_total_rows_by_start_index() -> None:
    """totalRows drives the walk: 3 pages even if pageToken is absent."""
    calls: list[dict[str, Any]] = []
    pages = [
        {"schema": {"fields": [{"name": "n"}]}, "rows": [{"f": [{"v": str(i)}]} for i in range(50_000)], "totalRows": "150000"},
        {"schema": {"fields": [{"name": "n"}]}, "rows": [{"f": [{"v": str(i)}]} for i in range(50_000, 100_000)], "totalRows": "150000"},
        {"schema": {"fields": [{"name": "n"}]}, "rows": [{"f": [{"v": str(i)}]} for i in range(100_000, 150_000)], "totalRows": "150000"},
    ]

    def fake(method: str, url: str, **kw: Any) -> dict[str, Any]:
        calls.append({"method": method, "params": kw.get("params"), "payload": kw.get("payload")})
        if method == "POST":
            return {"jobReference": {"jobId": "j1", "location": "US"}, **pages[0], "jobComplete": True}
        start = int((kw.get("params") or {}).get("startIndex") or 0)
        return pages[start // 50_000]

    rest.set_transport(fake)
    try:
        got = sum(len(p) for p in rest.stream_query("SELECT n", [], project="p", page_size=50_000))
    finally:
        rest.set_transport(None)
    assert got == 150_000
    assert len([c for c in calls if c["method"] == "POST"]) == 1  # one job, free result pages
    starts = [int(c["params"]["startIndex"]) for c in calls if c["method"] == "GET"]
    assert starts == [50_000, 100_000]


def test_stream_query_walks_until_empty_without_total_rows() -> None:
    """Real jobs may omit totalRows: the walk must continue until a page is empty."""
    calls: list[dict[str, Any]] = []
    table_rows = [{"f": [{"v": str(i)}]} for i in range(50_000)]

    def fake(method: str, url: str, **kw: Any) -> dict[str, Any]:
        calls.append({"method": method, "params": kw.get("params")})
        if method == "POST":
            return {"jobReference": {"jobId": "j2", "location": "US"}, "schema": {"fields": [{"name": "n"}]},
                    "rows": table_rows, "jobComplete": True}  # no totalRows!
        start = int((kw.get("params") or {}).get("startIndex") or 0)
        if start >= 150_000:
            return {"schema": {"fields": [{"name": "n"}]}, "rows": []}
        return {"schema": {"fields": [{"name": "n"}]}, "rows": table_rows}

    rest.set_transport(fake)
    try:
        got = sum(len(p) for p in rest.stream_query("SELECT n", [], project="p", page_size=50_000))
    finally:
        rest.set_transport(None)
    assert got == 150_000  # 3 full pages, then the empty page ends the walk
    assert len([c for c in calls if c["method"] == "POST"]) == 1
