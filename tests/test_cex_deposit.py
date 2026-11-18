"""Tests for the ``cex_deposit`` wallet-entity heuristic.

The contract under test:

* ``entity_id`` is stable across runs — the same (chain, source) cohort must
  hash to the same id or ``ReplacingMergeTree`` mints a new row every re-run.
* ``confidence_score`` is a bounded float in ``[0, 1)`` — never a boolean, and
  never 1.0, because shared exchange funding is evidence and not proof.
* no entity is emitted for a funding source that is not labelled exchange.

ClickHouse is injected through ``connect``/a fake client (dependency injection,
the same seam style as ``parquet_tooling.set_tool_seams``); nothing here
monkeypatches module internals.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.entities import cex_deposit as cd
from lite_indexer.entities import cex_deposit_sql as sql
from lite_indexer.entities import cex_deposit_writer as writer

EXCHANGE_ADDR = "0xf30ba13e4b04ce5dc4d254ae5fa95477800f0eb0"
UNLABELLED_ADDR = "0x000000000000000000000000000000000000dead"
CHAIN = "ethereum"
WINDOW_START = 1_700_000_000


class FakeClickHouse:
    """Serves canned rows per query template and records the inserts."""

    def __init__(
        self,
        labels: list[tuple[Any, ...]] | None = None,
        native: list[tuple[Any, ...]] | None = None,
        token: list[tuple[Any, ...]] | None = None,
        window: int = WINDOW_START,
        has_table: bool = True,
    ) -> None:
        self.labels = labels or []
        self.native = native or []
        self.token = token or []
        self.window = window
        self.has_table = has_table
        self.inserted: list[Any] = []
        self.queries: list[str] = []

    def execute(self, query: str, params: Any = None) -> Any:
        self.queries.append(query)
        if query is sql.EXCHANGE_LABELS:
            return list(self.labels)
        if query is sql.NATIVE_FIRST_INBOUND:
            return list(self.native)
        if query is sql.TOKEN_FIRST_INBOUND:
            return list(self.token)
        if query is sql.WINDOW_START:
            return [[self.window]]
        if query is sql.TABLE_EXISTS:
            return [[1 if self.has_table else 0]]
        if query is sql.INSERT_ENTITIES:
            self.inserted.append(params)
            return len(params or [])
        return []


def _label(name: str = "Binance 8", subtype: str = "cex_hot_wallet") -> tuple[Any, ...]:
    return (EXCHANGE_ADDR, name, "cex_hot_wallet", "B", subtype)


def _evidence(wallet: str, source: str = EXCHANGE_ADDR, epoch: int = WINDOW_START + 86_400) -> cd.Evidence:
    return cd.Evidence(CHAIN, wallet, source, epoch, cd.TIER_NATIVE)


# ── entity_id stability ─────────────────────────────────────────────


def test_entity_id_is_stable_across_runs() -> None:
    """The same cohort hashes identically on every run and in any case."""
    first = cd.source_entity_id(CHAIN, EXCHANGE_ADDR)
    second = cd.source_entity_id(CHAIN, EXCHANGE_ADDR)
    assert first == second
    assert first == cd.source_entity_id(CHAIN, EXCHANGE_ADDR.upper())
    assert len(first) == 32


def test_entity_id_differs_by_chain_and_source() -> None:
    """Chain and source both participate in the id."""
    base = cd.source_entity_id(CHAIN, EXCHANGE_ADDR)
    assert base != cd.source_entity_id("arbitrum", EXCHANGE_ADDR)
    assert base != cd.source_entity_id(CHAIN, UNLABELLED_ADDR)


def test_build_rows_is_idempotent_over_same_evidence() -> None:
    """Re-running over unchanged evidence yields byte-identical rows."""
    evidence = [_evidence("0xaaa"), _evidence("0xbbb")]
    labels = {EXCHANGE_ADDR: cd.SourceLabel("Binance 8", "cex_hot_wallet")}
    starts = {CHAIN: WINDOW_START}
    first = cd.build_rows(evidence, labels, starts)
    second = cd.build_rows(evidence, labels, starts)
    assert [r.as_tuple() for r in first] == [r.as_tuple() for r in second]
    assert len({r.entity_id for r in first}) == 1


def test_last_updated_at_comes_from_evidence_not_wall_clock() -> None:
    """A re-run must not re-stamp rows, or ReplacingMergeTree never collapses."""
    evidence = [_evidence("0xaaa", epoch=WINDOW_START + 500), _evidence("0xbbb", epoch=WINDOW_START + 900)]
    labels = {EXCHANGE_ADDR: cd.SourceLabel("Binance 8", "cex_hot_wallet")}
    rows = cd.build_rows(evidence, labels, {CHAIN: WINDOW_START})
    assert {r.last_updated_at for r in rows} == {WINDOW_START + 900}


# ── confidence bounds ───────────────────────────────────────────────


def test_confidence_is_a_bounded_float() -> None:
    """Every score is a float in [0, 1), and strictly below the ceiling."""
    evidence = [_evidence(f"0x{i:04x}") for i in range(50)]
    labels = {EXCHANGE_ADDR: cd.SourceLabel("Binance 8", "cex_hot_wallet", "A", "cex_deposit_wallet")}
    rows = cd.build_rows(evidence, labels, {CHAIN: WINDOW_START})
    assert rows
    for row in rows:
        assert isinstance(row.confidence_score, float)
        assert not isinstance(row.confidence_score, bool)
        assert 0.0 <= row.confidence_score < 1.0
        assert row.confidence_score <= cd.MAX_CONFIDENCE


def test_confidence_never_reaches_one_even_at_max_evidence() -> None:
    """The best possible evidence still cannot claim certainty.

    The achievable maximum (native tier + capped support + deposit-wallet
    label) is 0.87; the clamp sits at 0.90. Both are below 1.0 on purpose, so
    this asserts the BOUND rather than pinning the arithmetic — tuning a
    component must not be able to silently claim certainty.
    """
    label = cd.SourceLabel("Binance 8", "cex_hot_wallet", "A", "cex_deposit_wallet")
    score = cd.score_confidence(_evidence("0xaaa"), label, cluster_size=10_000, window_start_epoch=0)
    assert score < 1.0
    assert score <= cd.MAX_CONFIDENCE
    assert cd.MAX_CONFIDENCE < 1.0


def test_token_tier_scores_below_native_tier() -> None:
    """A token inbound is weaker evidence than a native deposit."""
    label = cd.SourceLabel("Binance 8", "exchange")
    native = _evidence("0xaaa")
    token = cd.Evidence(CHAIN, "0xaaa", EXCHANGE_ADDR, native.first_seen_epoch, cd.TIER_TOKEN)
    native_score = cd.score_confidence(native, label, 1, WINDOW_START)
    token_score = cd.score_confidence(token, label, 1, WINDOW_START)
    assert token_score < native_score


def test_window_edge_is_penalised() -> None:
    """Funding at the start of our data is not treated as the true first inbound."""
    label = cd.SourceLabel("Binance 8", "cex_hot_wallet")
    at_edge = cd.score_confidence(_evidence("0xaaa", epoch=WINDOW_START), label, 1, WINDOW_START)
    well_inside = cd.score_confidence(_evidence("0xaaa", epoch=WINDOW_START + 86_400), label, 1, WINDOW_START)
    assert at_edge < well_inside


def test_support_bonus_is_capped() -> None:
    """Cluster size can never dominate the tier."""
    assert cd.support_bonus(0) == 0.0
    assert cd.support_bonus(1) < cd.support_bonus(10)
    assert cd.support_bonus(100) == cd.support_bonus(1_000_000)


# ── the emission gate ───────────────────────────────────────────────


def test_no_entity_for_unlabelled_source() -> None:
    """Evidence from an address that is not a labelled exchange emits nothing."""
    evidence = [_evidence("0xaaa", source=UNLABELLED_ADDR)]
    rows = cd.build_rows(evidence, {EXCHANGE_ADDR: cd.SourceLabel("Binance 8", "cex_hot_wallet")}, {CHAIN: 0})
    assert rows == []


def test_non_exchange_category_is_rejected() -> None:
    """A label in a non-exchange category is not an exchange source."""
    for category in ("defi", "unknown", "mixer", "service"):
        label = cd.SourceLabel("Somewhere", category)
        assert not label.is_exchange
    evidence = [_evidence("0xaaa")]
    rows = cd.build_rows(evidence, {EXCHANGE_ADDR: cd.SourceLabel("Uniswap", "defi")}, {CHAIN: 0})
    assert rows == []


def test_mixed_evidence_emits_only_the_labelled_cluster() -> None:
    """One unlabelled funder does not suppress the labelled cluster."""
    evidence = [_evidence("0xaaa"), _evidence("0xbbb", source=UNLABELLED_ADDR)]
    labels = {EXCHANGE_ADDR: cd.SourceLabel("Binance 8", "cex_hot_wallet")}
    rows = cd.build_rows(evidence, labels, {CHAIN: 0})
    assert [r.wallet_address for r in rows] == ["0xaaa"]
    assert rows[0].heuristic_type == "cex_deposit"
    assert rows[0].entity_category == "exchange_cluster"


# ── writer path (real SQL, fake client) ─────────────────────────────


def test_writer_collects_native_over_token_for_same_wallet() -> None:
    """A wallet funded natively keeps the native tier; the token row is dropped."""
    client = FakeClickHouse(
        labels=[_label()],
        native=[("0xaaa", EXCHANGE_ADDR, WINDOW_START + 100)],
        token=[("0xaaa", EXCHANGE_ADDR, WINDOW_START + 50), ("0xbbb", EXCHANGE_ADDR, WINDOW_START + 60)],
    )
    labels = writer.fetch_exchange_labels(client, CHAIN)
    evidence = writer.collect_evidence(client, CHAIN, labels)
    by_wallet = {e.wallet_address: e for e in evidence}
    assert by_wallet["0xaaa"].tier == cd.TIER_NATIVE
    assert by_wallet["0xbbb"].tier == cd.TIER_TOKEN


def test_writer_run_refuses_to_write_without_target_table() -> None:
    """A missing table is reported, never papered over with a partial write."""
    client = FakeClickHouse(labels=[_label()], has_table=False)
    result = writer.run(lambda: client, chains=(CHAIN,))
    assert result["ok"] is False
    assert result["reason"] == "wallet_entities_missing"
    assert client.inserted == []


def test_writer_dry_run_measures_without_inserting() -> None:
    """A dry run returns the same row count but writes nothing."""
    client = FakeClickHouse(
        labels=[_label()],
        native=[("0xaaa", EXCHANGE_ADDR, WINDOW_START + 100)],
        token=[],
    )
    result = writer.run(lambda: client, chains=(CHAIN,), dry_run=True)
    assert result["ok"] is True
    assert result["rows"] == 1
    assert client.inserted == []


def test_writer_inserts_scored_rows_when_not_dry_run() -> None:
    """The write path batches scored rows in INSERT_COLUMNS order."""
    client = FakeClickHouse(
        labels=[_label()],
        native=[("0xaaa", EXCHANGE_ADDR, WINDOW_START + 100)],
        token=[],
    )
    result = writer.run(lambda: client, chains=(CHAIN,))
    assert result["ok"] is True
    assert result["rows"] == 1
    assert len(client.inserted) == 1
    row = client.inserted[0][0]
    assert len(row) == len(sql.INSERT_COLUMNS)
    assert row[3] == "cex_deposit"
    assert row[6] == "exchange_cluster"
    assert isinstance(row[4], float)


def test_writer_skips_chain_without_exchange_labels() -> None:
    """No labels means no query and no rows — not an empty-result guess."""
    client = FakeClickHouse(labels=[], native=[("0xaaa", EXCHANGE_ADDR, WINDOW_START)], token=[])
    result = writer.run(lambda: client, chains=(CHAIN,), dry_run=True)
    assert result["ok"] is True
    assert result["rows"] == 0
    assert sql.NATIVE_FIRST_INBOUND not in client.queries


def test_writer_reports_unavailable_clickhouse() -> None:
    """An unreachable ClickHouse is a clean failure, not an exception."""
    result = writer.run(lambda: None, chains=(CHAIN,))
    assert result["ok"] is False
    assert result["reason"] == "clickhouse_unavailable"
