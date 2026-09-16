"""Tests for deep-lake date derivation in QueryRouter (block range → lake partitions)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

from lite_indexer.query.router import QueryRouter

_TS = {
    100: int(datetime(2026, 9, 1, 12, tzinfo=UTC).timestamp()),
    200: int(datetime(2026, 9, 3, 12, tzinfo=UTC).timestamp()),
}


class FakeEnvio:
    def __init__(
        self, ts: dict[int, int] | None = None, available: bool = True, fail: bool = False
    ) -> None:
        self.ts = ts if ts is not None else dict(_TS)
        self._available = available
        self.fail = fail
        self.calls = 0

    def available(self) -> bool:
        return self._available

    def call(
        self,
        chain: str,
        method: str,
        params: list[Any],
        *,
        traces: bool = False,
        timeout: float = 30.0,
    ) -> Any:
        self.calls += 1
        if self.fail:
            raise RuntimeError("boom")
        block = int(params[0], 16)
        value = self.ts.get(block)
        return {"timestamp": hex(value)} if value is not None else None


class FakeBigLake:
    def __init__(self, rows: list[dict[str, Any]] | None = None, fail: bool = False) -> None:
        self.rows = rows or []
        self.fail = fail
        self.seen_dates: Any = "unset"

    def available(self) -> bool:
        return True

    def query(
        self,
        table: str,
        chain: str = "ethereum",
        *,
        from_block: Any = None,
        to_block: Any = None,
        address: Any = None,
        dates: Any = None,
        limit: int = 100,
    ) -> Any:
        if self.fail:
            raise RuntimeError("lake down")
        self.seen_dates = dates
        return self.rows


def _router(envio: Any = None, biglake: Any = None) -> QueryRouter:
    return QueryRouter(
        biglake=cast(Any, biglake or FakeBigLake()),
        envio=cast(Any, envio or FakeEnvio()),
        on_demand_enabled=False,
    )


def test_dates_derived_from_block_timestamps() -> None:
    dates = asyncio.run(_router()._biglake_dates("ethereum", 100, 200))
    assert dates == ["2026-09-01", "2026-09-02", "2026-09-03"]


def test_dates_capped_at_31_days() -> None:
    envio = FakeEnvio(
        {
            100: int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()),
            200: int(datetime(2026, 9, 1, tzinfo=UTC).timestamp()),
        }
    )
    dates = asyncio.run(_router(envio=envio)._biglake_dates("ethereum", 100, 200))
    assert dates is not None
    assert len(dates) == 32
    assert dates[0] == "2026-08-01"
    assert dates[-1] == "2026-09-01"


def test_dates_none_for_non_ethereum_or_missing_range() -> None:
    router = _router()
    assert asyncio.run(router._biglake_dates("base", 100, 200)) is None
    assert asyncio.run(router._biglake_dates("ethereum", None, 200)) is None
    assert asyncio.run(router._biglake_dates("ethereum", 100, None)) is None


def test_dates_none_when_envio_unavailable_or_failing() -> None:
    unavailable = FakeEnvio(available=False)
    assert asyncio.run(_router(envio=unavailable)._biglake_dates("ethereum", 100, 200)) is None
    assert unavailable.calls == 0
    failing = FakeEnvio(fail=True)
    assert asyncio.run(_router(envio=failing)._biglake_dates("ethereum", 100, 200)) is None


def test_biglake_route_passes_derived_dates() -> None:
    lake = FakeBigLake(rows=[])
    router = _router(biglake=lake)
    rows = asyncio.run(
        router._biglake_route(
            "evm_transfers", "ethereum", from_block=100, to_block=200, filters=None, limit=10
        )
    )
    assert rows == []
    assert lake.seen_dates == ["2026-09-01", "2026-09-02", "2026-09-03"]


def test_biglake_route_failure_returns_empty() -> None:
    router = _router(biglake=FakeBigLake(fail=True))
    rows = asyncio.run(
        router._biglake_route(
            "evm_transfers", "ethereum", from_block=100, to_block=200, filters=None, limit=10
        )
    )
    assert rows == []
