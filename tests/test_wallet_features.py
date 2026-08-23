"""Tests for the competitive wallet-access features.

Covers the four features:

1. Address-first projections (electrs-inspired) — ``projections.py``
2. Token metadata reference table (cryo-inspired) — ``token_meta.py``
3. Per-token incremental balance cache (electrs lazy cache) —
   ``balance_cache.py``
4. Auto-maintained holder-balances materialized view — ``balance_cache.py``

Uses a fake ClickHouse client (same pattern as ``test_parquet_export.py``)
so the suite is hermetic — no CH_HOST required.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.storage.balance_cache import (
    ensure_balance_cache_table,
    ensure_holder_balances_mv,
    get_auto_holders,
    get_token_holders,
    update_holder_balance,
)
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.storage.projections import ensure_wallet_projection
from lite_indexer.storage.token_meta import (
    ensure_token_meta_table,
    resolve_token_meta,
    upsert_token_meta,
)


class FakeClient:
    """Minimal ClickHouse client double for wallet-feature tests.

    Records every query/params pair and simulates the read-modify-write
    behavior of ``holder_balances`` so balance updates are visible to
    later reads (keyed by (chain, token_address, address)).
    """

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params: list[Any] = []
        self.balance_rows: dict[tuple[str, str, str], tuple[int, int]] = {}
        self.holder_rows: list[tuple[Any, ...]] = []
        self.auto_rows: list[tuple[Any, ...]] = []
        self.token_rows: list[tuple[Any, ...]] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        self.params.append(params)

        if query.startswith("SELECT balance, transfer_count FROM"):
            key = (params["chain"], params["token"], params["address"])
            row = self.balance_rows.get(key)
            return [(row[0], row[1])] if row else []
        if query.startswith("SELECT address, balance, transfer_count FROM wallet_memory.holder_balances"):
            return self.holder_rows
        if query.startswith("SELECT address, balance, transfer_count FROM wallet_memory.inner.holder_balances_mv"):
            return self.auto_rows
        if query.startswith("SELECT chain, token_address, symbol, name, decimals, first_seen_block FROM"):
            token = params.get("token") if isinstance(params, dict) else None
            return [r for r in self.token_rows if r[1] == token]
        if query.startswith("INSERT INTO wallet_memory.holder_balances"):
            for row in params or []:
                self.balance_rows[(row[0], row[1], row[2])] = (row[3], row[4])
            return 1
        if query.startswith("INSERT INTO wallet_memory.token_meta_ref"):
            return 1
        return []

    # -- helpers ------------------------------------------------------

    def insert_queries(self) -> list[str]:
        return [q for q in self.queries if q.startswith("INSERT INTO")]


def _transfer(
    *,
    token: str,
    from_addr: str,
    to_addr: str,
    value: str,
    block: int = 100,
) -> dict[str, Any]:
    """Build a parsed transfer event dict matching the pipeline shape."""
    return {
        "type": "transfer",
        "chain": "ethereum",
        "blockNumber": block,
        "blockTimestamp": "2026-08-02T00:00:00Z",
        "txHash": f"0x{block}",
        "logIndex": 0,
        "from": from_addr,
        "to": to_addr,
        "tokenAddress": token,
        "value": value,
        "tokenSymbol": None,
        "tokenName": None,
        "source": "lite_indexer",
    }


# ── Feature 1: Address-first projections ──────────────────────────────


@pytest.mark.asyncio
async def test_ensure_wallet_projection_issues_both_projections() -> None:
    client = FakeClient()
    ok = await ensure_wallet_projection(client)
    assert ok is True
    alters = [q for q in client.queries if q.startswith("ALTER TABLE")]
    assert len(alters) == 2
    assert "wallet_out_proj" in alters[0]
    assert "wallet_in_proj" in alters[1]
    # electrs key-as-record: primary ordering IS the query answer.
    assert "ORDER BY (chain, address, block_number, log_index)" in alters[0]
    assert "'out' AS direction" in alters[0]
    assert "'in' AS direction" in alters[1]
    assert "from_address AS address" in alters[0]
    assert "to_address AS address" in alters[1]


@pytest.mark.asyncio
async def test_ensure_wallet_projection_graceful_on_failure() -> None:
    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("database wallet_memory doesn't exist")

    ok = await ensure_wallet_projection(BrokenClient())
    assert ok is False  # non-fatal: degrades to a log, never raises


@pytest.mark.asyncio
async def test_ensure_wallet_projection_materialize_existing() -> None:
    client = FakeClient()
    await ensure_wallet_projection(client, materialize_existing=True)
    materializes = [q for q in client.queries if "MATERIALIZE PROJECTION" in q]
    assert len(materializes) == 2


# ── Feature 2: Token metadata reference table ─────────────────────────


@pytest.mark.asyncio
async def test_ensure_token_meta_table_ddl() -> None:
    client = FakeClient()
    ok = await ensure_token_meta_table(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE TABLE IF NOT EXISTS wallet_memory.token_meta_ref" in ddl
    assert "ReplacingMergeTree()" in ddl
    assert "ORDER BY (chain, token_address)" in ddl
    assert "decimals Nullable(UInt8)" in ddl


@pytest.mark.asyncio
async def test_upsert_token_meta_inserts_with_defaults() -> None:
    client = FakeClient()
    ok = await upsert_token_meta(client, "ethereum", "0xtoken", None, None, first_seen_block=42)
    assert ok is True
    inserts = client.insert_queries()
    assert len(inserts) == 1
    assert "INSERT INTO wallet_memory.token_meta_ref" in inserts[0]
    row = client.params[-1][0]
    # symbol/name coerce to "" (String non-nullable), decimals stays None.
    assert row[0] == "ethereum"
    assert row[1] == "0xtoken"
    assert row[2] == ""
    assert row[3] == ""
    assert row[4] is None
    assert row[5] == 42


@pytest.mark.asyncio
async def test_resolve_token_meta() -> None:
    client = FakeClient()
    client.token_rows = [("ethereum", "0xtoken", "RUG", "Rug Coin", 18, 42)]
    meta = await resolve_token_meta(client, "ethereum", "0xtoken")
    assert meta == {
        "chain": "ethereum",
        "token_address": "0xtoken",
        "symbol": "RUG",
        "name": "Rug Coin",
        "decimals": 18,
        "first_seen_block": 42,
    }
    assert await resolve_token_meta(client, "ethereum", "0xmissing") is None


# ── Feature 3: Incremental balance cache ──────────────────────────────


@pytest.mark.asyncio
async def test_ensure_balance_cache_table_ddl() -> None:
    client = FakeClient()
    ok = await ensure_balance_cache_table(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE TABLE IF NOT EXISTS wallet_memory.holder_balances" in ddl
    assert "ReplacingMergeTree(version)" in ddl
    assert "balance Int128" in ddl


@pytest.mark.asyncio
async def test_update_holder_balance_read_modify_write() -> None:
    client = FakeClient()
    # Current cached state: balance 100, transfer_count 3.
    client.balance_rows[("ethereum", "0xtoken", "0xholder")] = (100, 3)
    ok = await update_holder_balance(client, "ethereum", "0xtoken", "0xholder", -40)
    assert ok is True
    inserts = client.insert_queries()
    assert len(inserts) == 1
    row = client.params[-1][0]
    assert row[2] == "0xholder"
    assert row[3] == 60  # 100 - 40
    assert row[4] == 4  # transfer_count incremented
    # ReplacingMergeTree(version): a version column is present.
    assert row[6] > 0


@pytest.mark.asyncio
async def test_update_holder_balance_zero_delta_noop() -> None:
    client = FakeClient()
    ok = await update_holder_balance(client, "ethereum", "0xtoken", "0xholder", 0)
    assert ok is False
    assert client.insert_queries() == []


@pytest.mark.asyncio
async def test_get_token_holders_query() -> None:
    client = FakeClient()
    client.holder_rows = [("0xwhale", 1_000_000, 50), ("0xfish", 200, 4)]
    holders = await get_token_holders(client, "ethereum", "0xtoken", min_balance=100, limit=10)
    assert holders == [
        {"address": "0xwhale", "balance": 1_000_000, "transfer_count": 50},
        {"address": "0xfish", "balance": 200, "transfer_count": 4},
    ]
    query = client.queries[0]
    assert "ORDER BY balance DESC LIMIT" in query
    assert query.startswith("SELECT address, balance, transfer_count FROM wallet_memory.holder_balances")


# ── Feature 4: Auto-maintained materialized view ──────────────────────


@pytest.mark.asyncio
async def test_ensure_holder_balances_mv_ddl() -> None:
    client = FakeClient()
    ok = await ensure_holder_balances_mv(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS wallet_memory.holder_balances_mv" in ddl
    assert "ENGINE = SummingMergeTree()" in ddl
    assert "toInt128(value) AS balance" in ddl
    assert "count() AS transfer_count" in ddl
    assert "GROUP BY chain, token_address, address" in ddl


@pytest.mark.asyncio
async def test_get_auto_holders_reads_inner_table() -> None:
    client = FakeClient()
    client.auto_rows = [("0xwhale", 500, 9)]
    holders = await get_auto_holders(client, "ethereum", "0xtoken")
    assert holders == [{"address": "0xwhale", "balance": 500, "transfer_count": 9}]
    assert "wallet_memory.inner.holder_balances_mv" in client.queries[0]


# ── Integration: ClickHouseWriter.write_wallet_features ───────────────


@pytest.mark.asyncio
async def test_write_wallet_features_full(monkeypatch: pytest.MonkeyPatch) -> None:
    import lite_indexer.storage.clickhouse as chmod

    monkeypatch.setattr(chmod, "WALLET_PROJECTION_ENABLED", True)
    monkeypatch.setattr(chmod, "TOKEN_META_ENABLED", True)
    monkeypatch.setattr(chmod, "BALANCE_CACHE_ENABLED", True)
    monkeypatch.setattr(chmod, "HOLDER_BALANCES_MV_ENABLED", True)

    writer = ClickHouseWriter()
    client = FakeClient()
    writer._client = client

    events = [
        # Token A: X -> Y (+100), Y -> Z (+30 net for Y: +100-30 = +70)
        _transfer(token="0xaaa", from_addr="0xX", to_addr="0xY", value="100"),
        _transfer(token="0xaaa", from_addr="0xY", to_addr="0xZ", value="30"),
        # Token B self-transfer: X -> X nets to zero and is skipped.
        _transfer(token="0xbbb", from_addr="0xX", to_addr="0xX", value="50"),
    ]

    stats = await writer.write_wallet_features("ethereum", events)

    assert stats["wallet_projection_ready"] == 1
    assert stats["token_meta_upserted"] == 2  # once per unique token (A, B)
    assert stats["holder_balance_updates"] == 3  # (A,X) -100, (A,Y) +70, (A,Z) +30
    assert stats["holder_balances_mv_ready"] == 1

    # Token meta: one insert per unique token.
    token_meta_inserts = [q for q in client.queries if q.startswith("INSERT INTO wallet_memory.token_meta_ref")]
    assert len(token_meta_inserts) == 2

    # Balance cache: three distinct holders written, X's self-transfer
    # produced no extra (B,X) update.
    balance_inserts = [q for q in client.queries if q.startswith("INSERT INTO wallet_memory.holder_balances")]
    assert len(balance_inserts) == 3

    # DDL guards: second call must not re-run DDL (projections, tables, MV).
    writer._wallet_projection_ready = True
    writer._token_meta_ready = True
    writer._balance_cache_ready = True
    writer._holder_mv_ready = True
    before = len(client.queries)
    await writer.write_wallet_features("ethereum", events)
    new_queries = client.queries[before:]
    assert all(not q.startswith("ALTER TABLE") and not q.startswith("CREATE") for q in new_queries), (
        "DDL must not re-run once ready"
    )


@pytest.mark.asyncio
async def test_write_wallet_features_other_features_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """With the configurable features off, token meta still runs.

    ``TOKEN_META_ENABLED`` is always on (non-configurable), so the token
    metadata reference table is upserted even when every other wallet
    feature is disabled.
    """
    import lite_indexer.storage.clickhouse as chmod

    for flag in (
        "WALLET_PROJECTION_ENABLED",
        "BALANCE_CACHE_ENABLED",
        "HOLDER_BALANCES_MV_ENABLED",
    ):
        monkeypatch.setattr(chmod, flag, False)

    writer = ClickHouseWriter()
    client = FakeClient()
    writer._client = client

    stats = await writer.write_wallet_features(
        "ethereum", [_transfer(token="0xaaa", from_addr="0xX", to_addr="0xY", value="100")]
    )
    assert stats == {"token_meta_upserted": 1}
    queries = client.queries
    assert any("CREATE TABLE IF NOT EXISTS wallet_memory.token_meta_ref" in q for q in queries)
    assert any("INSERT INTO wallet_memory.token_meta_ref" in q for q in queries)
    # The other wallet features must not issue any DDL or inserts.
    assert not any(q.startswith("ALTER TABLE") for q in queries)
    assert not any(q.startswith("INSERT INTO wallet_memory.holder_balances") for q in queries)


@pytest.mark.asyncio
async def test_write_wallet_features_no_client(monkeypatch: pytest.MonkeyPatch) -> None:
    # ISOLATION (found 2026-08-23): this host runs a real ClickHouse and
    # _connect() defaults to it — without pinning an unreachable port the
    # writer CONNECTED to prod-local CH and even ran ensure_wallet_projection
    # DDL against it. Port 1 refuses instantly → _connect() returns None,
    # which is the exact condition this test exists to exercise.
    monkeypatch.setenv("CH_PORT", "1")
    monkeypatch.setenv("CLICKHOUSE_PORT", "1")
    writer = ClickHouseWriter()  # no client, unreachable CH → _connect() returns None
    stats = await writer.write_wallet_features("ethereum", [])
    assert stats == {}


# ── Integration: write_transfers + token meta behavior ────────────────


def test_write_transfers_strips_enrichment_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    """transfer rows never carry token_symbol/token_name/source.

    ``TOKEN_META_ENABLED`` is always on (non-configurable): the INSERT
    names only the 10 hot-path columns and the row tuple matches — the
    enrichment columns stay in the schema (DEFAULT '') but are never
    populated.
    """
    import lite_indexer.storage.clickhouse as chmod

    monkeypatch.setattr(chmod, "CH_TTL_ENABLED", False)

    writer = ClickHouseWriter()
    client = FakeClient()
    writer._client = client

    events = [
        {
            **_transfer(token="0xaaa", from_addr="0xX", to_addr="0xY", value="100"),
            "tokenSymbol": "RUG",
            "tokenName": "Rug Coin",
        }
    ]
    inserted = writer.write_transfers("ethereum", events)
    assert inserted == 1
    insert_q = [q for q in client.queries if q.startswith("INSERT INTO wallet_memory.evm_transfers")]
    assert len(insert_q) == 1
    assert "token_symbol" not in insert_q[0]
    assert "token_name" not in insert_q[0]
    assert "source" not in insert_q[0]

    row = client.params[-1][0]
    assert len(row) == 11  # chain..ingested_at + raw_payload
    assert row[:7] == ("ethereum", 100, "0x100", "0xX", "0xY", "0xaaa", "100")
    assert row[7] is not None  # block_timestamp parsed from the event
    assert row[8] == 0  # log_index
    assert row[9] is not None  # ingested_at timestamp
    assert row[10] == ""  # raw_payload empty (parsed event carries no raw log)
