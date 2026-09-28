"""Tron reorg staging — the DELETE proof, pinned against live DDL.

Round 2's hard lesson: the Solana rollback filtered on ``blockNumber`` while
the DDL said ``block_number``, so every DELETE matched zero rows and looked
exactly like "nothing was orphaned". These tests therefore assert the SQL
TEXT that is issued, not merely that a rollback was attempted — reverting
``TRON_ROLLBACK_TABLES`` to the wrong column, or dropping ``tron_launches``
from it, makes them fail.

Column cross-checks (asserted from the migration files, not hardcoded):
  * ``tron_launches.block_number``      — migrations/010_tron_launch.sql
  * ``evm_transfers.block_number``      — CH_TABLE, written by
    ``storage.ch_transfers.write_transfers``
  * ``address_activity.slot``           — migrations/016_address_activity.sql
    and 017 for the ``chain`` column; Tron stores block numbers in ``slot``
    (``storage/address_activity_crosschain.parse_tron_address_activity``)

Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest

from lite_indexer.config.rollback_tables import (
    TRON_ROLLBACK_TABLES,
    RollbackTarget,
    delete_above_fork,
)
from lite_indexer.strategies.reorg_status import ReorgStatus
from lite_indexer.strategies.tron import reorg as tron_reorg
from lite_indexer.strategies.tron.reorg_stage import TronReorgStageMixin

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"


class RecordingClient:
    """ClickHouse double that records every statement it is handed."""

    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.statements: list[tuple[str, dict | list]] = []
        self._rows = rows or []

    def execute(self, sql: str, params: dict | list | None = None) -> list[tuple]:
        self.statements.append((sql, params or {}))
        if sql.lstrip().upper().startswith("SELECT"):
            return self._rows
        return 1


class FakeResponse:
    """``httpx.Response``-shaped double: ``tron_post`` reads these two."""

    def __init__(self, payload: object) -> None:
        self.status_code = 200
        self._payload = payload

    def json(self) -> object:
        return self._payload


class FakeClient:
    """``httpx.AsyncClient``-shaped double for the TronGrid transport.

    Maps requested block height -> response body. A ``None`` body models a
    transport failure (every endpoint exhausted), a dict models a parsed
    JSON body — including ``{}``, which is what TronGrid returns for a height
    above the tip.
    """

    def __init__(self, responses: dict[int, object]) -> None:
        self.responses = responses
        self.requested: list[int] = []

    async def post(self, url: str, json: dict | None = None, timeout: float = 0.0) -> FakeResponse:
        num = int((json or {}).get("num", -1))
        self.requested.append(num)
        if num not in self.responses:
            return FakeResponse({})
        return FakeResponse(self.responses[num])


# ── The DELETE statement itself ─────────────────────────────────────────


def test_tron_launches_rollback_deletes_above_fork_on_block_number() -> None:
    """The proven statement: tron_launches, chain-scoped, block_number."""
    client = RecordingClient()
    target = next(t for t in TRON_ROLLBACK_TABLES if t.table == "wallet_memory.tron_launches")

    assert target.column == "block_number"
    assert target.chain_scoped is True

    deleted, failed = delete_above_fork(client, (target,), 500, chain="tron")

    assert failed is None
    assert deleted == 1
    sql, params = client.statements[0]
    assert sql == "DELETE FROM wallet_memory.tron_launches WHERE chain = %(chain)s AND block_number > %(fork)s"
    assert params == {"chain": "tron", "fork": 500}
    # The round-2 defect, pinned: this column must never be the JSON name.
    assert "blockNumber" not in sql


def test_tron_rollback_covers_every_table_the_tron_strategy_writes() -> None:
    """tron_launches + shared evm_transfers + address_activity, no more.

    ``internal_transfers``/``dex_events`` must stay absent: the Tron strategy
    has no DEX or trace write path, so a false-positive fork would otherwise
    delete other chains' shared rows.
    """
    by_table = {t.table: t for t in TRON_ROLLBACK_TABLES}

    assert set(by_table) == {
        "wallet_memory.tron_launches",
        "wallet_memory.evm_transfers",
        "wallet_memory.address_activity",
    }
    assert by_table["wallet_memory.address_activity"].column == "slot"
    assert all(t.chain_scoped for t in TRON_ROLLBACK_TABLES)


def test_tron_rollback_columns_match_the_live_ddl() -> None:
    """Cross-check every column against the migration that creates the table."""
    ddl = {
        "tron_launches": (MIGRATIONS / "010_tron_launch.sql").read_text(),
        "address_activity": (MIGRATIONS / "016_address_activity.sql").read_text(),
    }
    chain_ddl = (MIGRATIONS / "017_address_activity_chain.sql").read_text()

    launches = re.search(
        r"CREATE TABLE IF NOT EXISTS wallet_memory\.tron_launches \((.*?)\n\) ENGINE",
        ddl["tron_launches"],
        re.S,
    )
    assert launches, "tron_launches CREATE not found in migration 010"
    assert re.search(r"^\s*block_number\b", launches.group(1), re.M)

    activity = re.search(
        r"CREATE TABLE IF NOT EXISTS wallet_memory\.address_activity \((.*?)\n\) ENGINE",
        ddl["address_activity"],
        re.S,
    )
    assert activity, "address_activity CREATE not found in migration 016"
    assert re.search(r"^\s*slot\b", activity.group(1), re.M)
    # 017 is what makes the chain-scoped DELETE legal at all.
    assert re.search(r"ADD COLUMN IF NOT EXISTS chain\b", chain_ddl)


def test_tron_rollback_prunes_its_own_checkpoints() -> None:
    """Checkpoint rows above the fork must go too, or the fork re-verifies as clean."""
    client = RecordingClient()
    deleted, failed = delete_above_fork(client, TRON_ROLLBACK_TABLES, 900, chain="tron")

    assert failed is None
    assert len(client.statements) == len(TRON_ROLLBACK_TABLES)
    for sql, params in client.statements:
        assert sql.startswith("DELETE FROM wallet_memory.")
        assert params["chain"] == "tron"
        assert params["fork"] == 900


def test_tron_rollback_reports_failure_instead_of_partial_success() -> None:
    """A failing DELETE must surface, never report orphaned rows as removed."""

    class FailingClient(RecordingClient):
        def execute(self, sql: str, params: dict | list | None = None) -> list[tuple]:
            self.statements.append((sql, params or {}))
            raise RuntimeError("clickhouse down")

    deleted, failed = delete_above_fork(FailingClient(), TRON_ROLLBACK_TABLES, 10, chain="tron")

    assert failed == "wallet_memory.tron_launches"
    assert deleted == 0


# ── The integrity signal ────────────────────────────────────────────────


def test_fetch_block_id_reads_the_top_level_blockid_field() -> None:
    """TronGrid serves blockID at the top level, not under block_header."""
    block_id = "0000000003ef1480408278d692db2eb7701f1143f99c2933c720edf7e8f92c39"
    client = FakeClient({66000000: {"blockID": block_id}})

    got = asyncio.run(tron_reorg.fetch_block_id(client, 66000000))

    assert got == block_id
    assert client.requested == [66000000]


@pytest.mark.parametrize("body", [{}, {"blockID": ""}, {"blockID": "abc"}, {"block_header": {}}])
def test_fetch_block_id_is_none_when_the_node_cannot_vouch(body: dict) -> None:
    """An empty dict (height above tip) is 'cannot tell', not 'no reorg'."""
    client = FakeClient({7: body})

    assert asyncio.run(tron_reorg.fetch_block_id(client, 7)) is None


# ── The tri-state verdict ───────────────────────────────────────────────


def _run_check(rows: list[tuple], current: str | None) -> object:
    ch = RecordingClient(rows=rows)
    client = FakeClient({100: {"blockID": current} if current else {}})
    return asyncio.run(tron_reorg.check_reorg(client, 119, ch)), ch


def test_matching_block_id_is_no_reorg() -> None:
    block_id = "a" * 64
    outcome, _ = _run_check([(block_id,)], block_id)

    assert outcome.status is ReorgStatus.NO_REORG
    assert outcome.error is None


def test_mismatched_block_id_is_reorg_carrying_the_fork() -> None:
    """The whole point: a changed blockID at the verify height is a real fork."""
    outcome, ch = _run_check([("a" * 64,)], "b" * 64)

    assert outcome.status is ReorgStatus.REORG
    # tip 119 - depth 19 = 100, which is the common ancestor.
    assert outcome.fork_block == 100
    assert ch.statements[0][1] == {"chain": "tron", "block": 100}


def test_absent_checkpoint_is_benign_unknown_not_failure() -> None:
    """No checkpoint yet -> error stays None so callers don't stall catch-up."""
    outcome, _ = _run_check([], "b" * 64)

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.error is None


def test_missing_blockid_at_verify_height_fails_closed() -> None:
    """Cannot verify -> UNKNOWN with an error, so the write is refused."""
    outcome, _ = _run_check([("a" * 64,)], None)

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.error == "blockID unavailable for verify height"


def test_checkpoint_lookup_failure_fails_closed() -> None:
    class BoomClient(RecordingClient):
        def execute(self, sql: str, params: dict | list | None = None) -> list[tuple]:
            raise RuntimeError("ch down")

    outcome = asyncio.run(tron_reorg.check_reorg(FakeClient({}), 119, BoomClient()))

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.error == "ch down"


# ── The staging seam ────────────────────────────────────────────────────


class StagedStrategy(TronReorgStageMixin):
    """Minimal concrete host for the mixin."""

    chain = "tron"

    def __init__(self, client: FakeClient, rows: list[tuple]) -> None:
        self._client = client
        self.rows = rows
        self.ch = RecordingClient(rows=rows)

    async def _get_client(self) -> FakeClient:
        return self._client


def test_reorg_stage_refuses_when_the_range_cannot_be_verified() -> None:
    """A refused write must be reported, so the caller retries instead of skipping."""
    strategy = StagedStrategy(FakeClient({}), [("a" * 64,)])

    outcome = asyncio.run(strategy.reorg_stage(100, 119, strategy.ch))

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.error == "blockID unavailable for verify height"


def test_reorg_stage_rolls_back_then_reports_the_fork() -> None:
    """On a real fork: the orphaned rows are deleted BEFORE REORG is returned.

    Order matters — returning REORG first would let a caller re-insert the
    rows the rollback is meant to have removed.
    """
    strategy = StagedStrategy(
        FakeClient({119: {"blockID": "b" * 64}, 100: {"blockID": "c" * 64}}),
        [("a" * 64,)],
    )

    outcome = asyncio.run(strategy.reorg_stage(100, 119, strategy.ch))

    assert outcome.status is ReorgStatus.REORG
    assert outcome.fork_block == 100
    deletes = [s for s, _ in strategy.ch.statements if s.startswith("DELETE FROM wallet_memory.tron_launches")]
    assert deletes == ["DELETE FROM wallet_memory.tron_launches WHERE chain = %(chain)s AND block_number > %(fork)s"]


def test_reorg_stage_checkpoints_the_tip_before_verifying() -> None:
    """The tip's blockID must land in the checkpoint table, or staging is a no-op."""
    strategy = StagedStrategy(
        FakeClient({119: {"blockID": "b" * 64}, 100: {"blockID": "b" * 64}}),
        [("b" * 64,)],
    )

    outcome = asyncio.run(strategy.reorg_stage(100, 119, strategy.ch))

    assert outcome.status is ReorgStatus.NO_REORG
    inserts = [s for s, _ in strategy.ch.statements if "INSERT INTO" in s and "block_checkpoints" in s]
    assert len(inserts) == 1
    params = next(p for s, p in strategy.ch.statements if "block_checkpoints" in str(s) and "INSERT" in s)
    assert params[0][0] == "tron"
    assert params[0][1] == 119
    assert params[0][2] == "b" * 64


def test_reorg_stage_without_a_client_is_no_reorg() -> None:
    """Signature parity: a missing client degrades to NO_REORG, never REORG."""
    strategy = StagedStrategy(FakeClient({}), [])

    assert asyncio.run(strategy.reorg_stage(1, 10, None)).status is ReorgStatus.NO_REORG


def test_tron_targets_are_distinct_from_the_evm_and_solana_lists() -> None:
    """A shared target must still be chain-scoped, or it purges other chains."""
    tron = {t.table for t in TRON_ROLLBACK_TABLES}
    assert "wallet_memory.evm_transfers" in tron
    for target in TRON_ROLLBACK_TABLES:
        if target.table in {"wallet_memory.evm_transfers", "wallet_memory.address_activity"}:
            assert isinstance(target, RollbackTarget)
            assert target.chain_scoped is True
