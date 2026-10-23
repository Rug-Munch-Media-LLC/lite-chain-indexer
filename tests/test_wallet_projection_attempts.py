"""Regression: the wallet-projection DDL must not retry into a mutation storm.

``ALTER TABLE ... ADD PROJECTION`` is refused with ClickHouse Code 36 while the
merges+mutations pool is busy. The sync loop re-invokes the helper every cycle,
so before the attempt cap a single refused DDL produced one Code 36 error every
few seconds forever (7.8k observed on 2026-10-23, with the projections already
fully materialized). These tests pin the cap.
"""

import asyncio

from lite_indexer.storage import projections


class _Code36Client:
    """ClickHouse stub that refuses every mutation with Code 36."""

    def __init__(self) -> None:
        self.calls = 0

    def execute(self, statement: str) -> None:
        self.calls += 1
        raise RuntimeError(
            "Code: 36. DB::Exception: There are no free entries in pool to execute mutation. (BAD_ARGUMENTS)"
        )


def test_ddl_attempts_are_capped(monkeypatch) -> None:
    monkeypatch.setattr(projections, "_ddl_attempts", 0)
    client = _Code36Client()

    for _ in range(5):
        asyncio.run(projections.ensure_wallet_projection(client))

    # The first ADD PROJECTION raise aborts the statement loop, so each attempt
    # costs exactly one round trip; after the cap the helper is silent.
    assert client.calls == projections._MAX_DDL_ATTEMPTS
    assert projections._ddl_attempts == projections._MAX_DDL_ATTEMPTS


def test_capped_helper_stops_touching_clickhouse(monkeypatch) -> None:
    monkeypatch.setattr(projections, "_ddl_attempts", projections._MAX_DDL_ATTEMPTS)
    client = _Code36Client()

    assert asyncio.run(projections.ensure_wallet_projection(client)) is True
    assert client.calls == 0
