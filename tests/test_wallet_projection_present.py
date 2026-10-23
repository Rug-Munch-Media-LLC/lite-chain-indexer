"""Code 36 removal: ADD PROJECTION only for genuinely absent projections.

ClickHouse validates the merges/mutations-pool sanity check BEFORE it compares
the projection name, so ``ADD PROJECTION IF NOT EXISTS`` raises Code 36 even
when the projection already exists (reproduced 2026-10-23 on 26.7.1 with both
wallet projections fully materialized). The helper must therefore resolve
existence from the table DDL (``SHOW CREATE TABLE`` — the indexer user has no
SELECT grant on ``system.projections``) and issue zero mutation-class DDL
when the projections are present.
"""

import asyncio

from lite_indexer.storage import projections


class _CatalogueClient:
    """Serves ``SHOW CREATE TABLE``; optionally refuses mutation-class DDL."""

    def __init__(self, existing: list[str], refuse_alters: bool = True) -> None:
        self.existing = existing
        self.refuse_alters = refuse_alters
        self.selects = 0
        self.alters: list[str] = []

    def execute(self, statement: str) -> list[tuple[str]]:
        if statement.startswith("SHOW CREATE TABLE"):
            self.selects += 1
            ddl = "CREATE TABLE wallet_memory.evm_transfers (...) ENGINE = MergeTree " + " ".join(
                f"PROJECTION {name} (SELECT 1) " for name in self.existing
            )
            return [(ddl,)]
        self.alters.append(statement)
        if self.refuse_alters:
            raise RuntimeError("Code: 36. DB::Exception: no free entries in pool (BAD_ARGUMENTS)")
        return []


def test_present_projections_issue_no_ddl() -> None:
    projections._ddl_attempts = 0
    client = _CatalogueClient(["wallet_out_proj", "wallet_in_proj"])

    assert asyncio.run(projections.ensure_wallet_projection(client)) is True
    assert client.selects == 1
    assert client.alters == []  # no Code 36 source left


def test_only_the_missing_projection_is_created() -> None:
    projections._ddl_attempts = 0
    client = _CatalogueClient(["wallet_out_proj"], refuse_alters=False)

    assert asyncio.run(projections.ensure_wallet_projection(client)) is True
    assert len(client.alters) == 1
    assert "wallet_in_proj" in client.alters[0]


def test_catalogue_read_failure_falls_back_to_ddl() -> None:
    projections._ddl_attempts = 0

    class _Blind:
        def __init__(self) -> None:
            self.alters = 0

        def execute(self, statement: str) -> list[tuple[str]]:
            if statement.startswith("SHOW CREATE TABLE"):
                raise RuntimeError("SHOW CREATE TABLE unavailable")
            self.alters += 1
            return []

    client = _Blind()
    assert asyncio.run(projections.ensure_wallet_projection(client)) is True
    assert client.alters == 2  # unknown catalogue → attempt both, idempotent
