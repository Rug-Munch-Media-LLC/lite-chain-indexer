"""Provider-aware lake rotation — s3 vs gcp dispatch, fall-through, no network.

Pure DI: a fake source/router and a fake BigQuery backend. No monkeypatching.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.lakes.registry import LakeProvider
from lite_indexer.lakes.router import LakeProviderRouter
from lite_indexer.query.biglake_rotation import read_with_rotation
from lite_indexer.query.bigquery_spec import BigQuerySpec

SERVES = frozenset({("eth", "token_transfers")})
FILTERS: dict[str, Any] = {
    "address": "0xabc",
    "from_block": 100,
    "to_block": 200,
    "dates": ["2026-09-12"],
    "limit": 5,
}


class FakeRouter(LakeProviderRouter):
    """Records selections and reports without touching disk state."""

    def __init__(self, registry: dict[str, LakeProvider]) -> None:
        super().__init__(registry=registry, usage_path=None)
        self.selected: list[str] = []
        self.successes: list[str] = []
        self.failures: list[str] = []

    def select(self, chain: str, table: str, *, exclude: frozenset[str] | set[str] = frozenset()) -> Any:
        prov = super().select(chain, table, exclude=exclude)
        if prov is not None:
            self.selected.append(prov.name)
        return prov

    def report_success(self, provider: str, *, bytes_read: int = 0) -> None:
        self.successes.append(provider)

    def report_failure(self, provider: str, *, error: str) -> None:
        self.failures.append(provider)


class FakeSource:
    """Home lake source: records the DuckDB/S3 read and maps trivially."""

    def __init__(
        self, router: FakeRouter, rows: list[tuple[Any, ...]] | None = None, raises: Exception | None = None
    ) -> None:
        self.router = router
        self.rows = rows if rows is not None else [("0xHASH",)]
        self.raises = raises
        self.raw_calls: list[tuple[str, list[Any]]] = []

    def _router(self) -> FakeRouter:
        return self.router

    def _read_raw(self, sql: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        self.raw_calls.append((sql, params))
        if self.raises is not None:
            raise self.raises
        return (["tx_hash"], self.rows)

    def _map_row(self, table: str, raw: dict[str, Any]) -> dict[str, Any]:
        return {"table": table, **raw}


class FakeBigQuery:
    """Stand-in for BigQuerySource: records ``query`` args, returns canned rows.

    Mirrors the REAL ``BigQuerySource.query(self, spec: BigQuerySpec)``. The
    old fake took ``(table, chain, **filters)`` and therefore agreed with a
    broken call site, which is how the ``TypeError`` in the gcp leg shipped.
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None, raises: Exception | None = None) -> None:
        self.rows = rows if rows is not None else [{"tx_hash": "0xBQ"}]
        self.raises = raises
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def query(self, spec: BigQuerySpec) -> list[dict[str, Any]]:
        self.calls.append((spec.table, spec.chain, {k: v for k, v in vars(spec).items() if k in FILTERS}))
        if self.raises is not None:
            raise self.raises
        return self.rows


def _router(*kinds: tuple[str, str]) -> FakeRouter:
    return FakeRouter({name: LakeProvider(name=name, kind=kind, serves=SERVES) for name, kind in kinds})


def test_s3_provider_uses_duckdb_reader() -> None:
    router = _router(("s3prov", "s3/anonymous"))
    src = FakeSource(router)
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", FILTERS)
    assert src.raw_calls == [("SELECT 1", [1])]
    assert out == [{"table": "evm_transfers", "tx_hash": "0xHASH"}]
    assert router.successes == ["s3prov"]


def test_bigquery_provider_uses_bigquery_source_with_exact_filters() -> None:
    router = _router(("bigquery", "gcp/bigquery"))
    src = FakeSource(router)
    bq = FakeBigQuery()
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", FILTERS, bigquery=bq)
    assert src.raw_calls == []  # never fell back to DuckDB/S3
    assert bq.calls == [("token_transfers", "eth", FILTERS)]
    assert out == [{"tx_hash": "0xBQ"}]
    assert router.successes == ["bigquery"]


def test_failure_falls_through_to_next_provider() -> None:
    router = _router(("s3prov", "s3/anonymous"), ("bigquery", "gcp/bigquery"))
    src = FakeSource(router, raises=RuntimeError("s3 down"))
    bq = FakeBigQuery()
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", FILTERS, bigquery=bq)
    assert router.selected == ["s3prov", "bigquery"]
    assert router.failures == ["s3prov"]
    assert router.successes == ["bigquery"]
    assert src.raw_calls == [("SELECT 1", [1])]  # tried the DuckDB reader, failed
    assert bq.calls == [("token_transfers", "eth", FILTERS)]  # fell through to BigQuery
    assert out == [{"tx_hash": "0xBQ"}]


def test_no_provider_returns_empty_without_reading() -> None:
    router = _router(("s3prov", "s3/anonymous"))  # serves a different table
    src = FakeSource(router)
    bq = FakeBigQuery()
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "traces", FILTERS, bigquery=bq)
    assert out == []
    assert src.raw_calls == []
    assert bq.calls == []
    assert router.selected == []
