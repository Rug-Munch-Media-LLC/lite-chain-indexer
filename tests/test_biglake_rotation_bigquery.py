"""Regression: the ``gcp/*`` rotation leg must reach ``BigQuerySource.query``.

The rotation used to call ``backend.query(table, chain, **filters)`` while the
real client takes ONE positional ``BigQuerySpec`` — the resulting ``TypeError``
was swallowed by the rotation loop, so every BigQuery lake read returned an
empty page and looked like a healthy miss. The old tests hid it behind a
``FakeBigQuery`` whose signature mirrored the broken call.

Here the stand-in's ``query`` signature matches the REAL
``BigQuerySource.query(self, spec: BigQuerySpec)`` exactly, so a splatted call
cannot pass. No network, no monkeypatching — pure DI.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from lite_indexer.lakes.registry import LakeProvider
from lite_indexer.lakes.router import LakeProviderRouter
from lite_indexer.query.biglake_rotation import read_with_rotation
from lite_indexer.query.bigquery_source import BigQuerySource
from lite_indexer.query.bigquery_spec import BigQuerySpec

SERVES = frozenset({("eth", "token_transfers")})
FILTERS: dict[str, Any] = {
    "address": "0xabc",
    "from_block": 100,
    "to_block": 200,
    "dates": ["2026-09-12"],
    "limit": 5,
}
ROWS: list[dict[str, Any]] = [{"tx_hash": "0xBQ"}]


class RecordingRouter(LakeProviderRouter):
    """Real router with a scripted provider; records what was reported."""

    def __init__(self, name: str = "bigquery", kind: str = "gcp/bigquery", serves: frozenset[Any] = SERVES) -> None:
        super().__init__(registry={name: LakeProvider(name=name, kind=kind, serves=serves)}, usage_path=None)
        self.reported: list[tuple[str, str]] = []

    def report_success(self, provider: str, *, bytes_read: int = 0) -> None:
        self.reported.append(("success", provider))

    def report_failure(self, provider: str, *, error: str) -> None:
        self.reported.append(("failure", provider))


class FakeSource:
    """Home lake source: records the DuckDB/S3 read and maps trivially."""

    def __init__(self, router: RecordingRouter) -> None:
        self._r = router
        self.raw_calls: list[tuple[str, list[Any]]] = []

    def _router(self) -> RecordingRouter:
        return self._r

    def _read_raw(self, sql: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
        self.raw_calls.append((sql, params))
        return (["tx_hash"], [("0xHASH",)])

    def _map_row(self, table: str, raw: dict[str, Any]) -> dict[str, Any]:
        return {"table": table, **raw}


class StrictBigQuery:
    """Stand-in whose ``query`` signature is the REAL ``BigQuerySource.query``.

    Deliberately NOT ``query(table, chain, **filters)`` — that shape is what
    concealed the production defect.
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows if rows is not None else ROWS
        self.specs: list[BigQuerySpec] = []

    def query(self, spec: BigQuerySpec) -> list[dict[str, Any]]:
        self.specs.append(spec)
        return self.rows


def _gcp_read(filters: dict[str, Any], bq: StrictBigQuery) -> list[dict[str, Any]]:
    router = RecordingRouter()
    src = FakeSource(router)
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", filters, bigquery=bq)
    assert src.raw_calls == [], "the gcp leg must not fall back to the DuckDB/S3 reader"
    return out


def test_stand_in_signature_matches_real_client() -> None:
    """The stand-in must be signature-identical to ``BigQuerySource.query``."""
    assert inspect.signature(StrictBigQuery.query) == inspect.signature(BigQuerySource.query)


def test_gcp_leg_forwards_a_typed_spec_and_returns_rows() -> None:
    bq = StrictBigQuery()
    out = _gcp_read(FILTERS, bq)
    assert out == ROWS  # the leg is no longer a silent no-op
    spec = bq.specs[0]
    # physical BigQuery table, not the logical lake table
    assert spec.table == "token_transfers"
    assert spec.chain == "eth"
    assert spec.address == "0xabc"
    assert spec.from_block == 100
    assert spec.to_block == 200
    assert spec.dates == ["2026-09-12"]
    assert spec.limit == 5  # the caller's page, not the spec default


def test_caller_chain_is_forwarded_not_hardcoded() -> None:
    """``chain`` routes the solana branch — it must come from the caller."""
    bq = StrictBigQuery()
    router = RecordingRouter("sol", "gcp/bigquery", frozenset({("solana", "token_transfers")}))
    src = FakeSource(router)
    read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", FILTERS, chain="solana", bigquery=bq)
    assert bq.specs[0].chain == "solana"


def test_unexpected_filter_key_raises_named_error_not_typeerror() -> None:
    """An unknown key must not become a ``TypeError`` out of the rotation path."""
    bq = StrictBigQuery()
    router = RecordingRouter()
    src = FakeSource(router)
    bad = {**FILTERS, "since_blk": 1}
    out = read_with_rotation(src, ("SELECT 1", [1]), "evm_transfers", "token_transfers", bad, bigquery=bq)
    # The rotation loop contains the failure, reports it, and returns no rows
    # for that leg — but the backend was never reached with a broken call.
    assert bq.specs == []
    assert out == []
    assert ("failure", "bigquery") in router.reported


def test_unknown_key_is_reported_by_name_at_the_spec_boundary() -> None:
    from lite_indexer.query.biglake_rotation import _spec_for

    with pytest.raises(ValueError, match="since_blk"):
        _spec_for("token_transfers", "eth", {**FILTERS, "since_blk": 1})
