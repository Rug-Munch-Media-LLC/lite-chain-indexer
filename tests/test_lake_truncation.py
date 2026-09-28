"""Tests for lake row-limit policy and the truncation signal.

A caller asking for 1,000,000 rows used to receive exactly the 1000 newest
ones, as a normal result with no indication it was short. These tests pin
the two halves of the fix: the oversize limit is REJECTED rather than
clamped, and a read that does hit the ceiling reports ``truncated``.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from lite_indexer.query.biglake_source import BigLakeSource
from lite_indexer.query.lake_limits import (
    LAKE_MAX_ROWS,
    SOLANA_MAX_PAGE,
    clamp_limit,
    reject_oversize_limit,
)


def test_oversize_limit_raises_rather_than_clamping() -> None:
    """A limit above the ceiling raises. It must NOT become 1000."""
    with pytest.raises(ValueError, match="exceeds ceiling"):
        reject_oversize_limit(1_000_000)
    with pytest.raises(ValueError):
        reject_oversize_limit(LAKE_MAX_ROWS + 1)
    assert reject_oversize_limit(LAKE_MAX_ROWS) == LAKE_MAX_ROWS
    assert reject_oversize_limit(1) == 1


def test_clamp_limit_is_for_cleared_limits_only() -> None:
    """clamp_limit still bounds, but only for an already-cleared limit."""
    assert clamp_limit(10) == 10
    assert clamp_limit(0) == 1
    assert clamp_limit(99_999, SOLANA_MAX_PAGE) == 50_000


def test_biglake_query_reports_truncation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A page hitting the ceiling returns (rows<=limit, truncated=True).

    The source reads ``limit + 1`` rows as a probe: the extra row's
    existence is the proof that older rows remain, and it is discarded.
    """
    src = BigLakeSource()
    monkeypatch.setattr(src, "available", lambda: True)
    monkeypatch.setattr(src, "_paths", lambda t, d: ["s3://x/date=2026-09-12/"])
    monkeypatch.setattr(src, "_map_row", lambda table, row: row)

    captured: dict[str, list[int]] = {}

    def _fake_rotation(
        source: Any, read: Any, table: str, biglake_table: str, filters: Any = None, **kw: Any
    ) -> list[dict[str, Any]]:
        sql, params = read
        captured["params"] = list(params)
        return [{"block_number": i} for i in range(3)]

    monkeypatch.setattr("lite_indexer.query.biglake_rotation.read_with_rotation", _fake_rotation)

    rows, truncated = cast(tuple[list[dict[str, Any]], bool], src.query("evm_transfers", limit=2))

    assert truncated is True, "2-of-3 rows is a truncated page and must say so"
    assert len(rows) == 2, "the probe row must be discarded, never returned"
    assert captured["params"][-1] == 3, "the query must ask for limit + 1 rows"


def test_biglake_query_untruncated_page(monkeypatch: pytest.MonkeyPatch) -> None:
    """A page that fits entirely reports truncated=False."""
    src = BigLakeSource()
    monkeypatch.setattr(src, "available", lambda: True)
    monkeypatch.setattr(src, "_paths", lambda t, d: ["s3://x/date=2026-09-12/"])
    monkeypatch.setattr(src, "_map_row", lambda table, row: row)
    monkeypatch.setattr(
        "lite_indexer.query.biglake_rotation.read_with_rotation",
        lambda *a, **k: ([{"block_number": 1}],),
    )

    rows, truncated = cast(tuple[list[dict[str, Any]], bool], src.query("evm_transfers", limit=10))
    assert truncated is False
    assert len(rows) == 1


def test_biglake_query_rejects_oversize_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """The reader itself rejects 1,000,000 rows — not just the router."""
    src = BigLakeSource()
    monkeypatch.setattr(src, "available", lambda: True)
    monkeypatch.setattr(src, "_paths", lambda t, d: ["s3://x/"])

    with pytest.raises(ValueError, match="exceeds ceiling"):
        src.query("evm_transfers", limit=1_000_000)


def test_solana_page_ceiling_is_reported_separately() -> None:
    """Solana's 50k page is distinct from the 1000 lake result ceiling."""
    assert SOLANA_MAX_PAGE == 50_000
    assert SOLANA_MAX_PAGE > LAKE_MAX_ROWS
    assert clamp_limit(10**9, SOLANA_MAX_PAGE) == 50_000
