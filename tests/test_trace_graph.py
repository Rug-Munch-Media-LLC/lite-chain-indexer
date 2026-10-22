"""Tests for the wallet trace (pure graph/SQL/label logic, no DB)."""

from __future__ import annotations

import pytest

from lite_indexer.query import trace_deployer as dep
from lite_indexer.query.trace_labels import resolve_entities
from lite_indexer.query.trace_model import build_graph
from lite_indexer.query.trace_paths import paths_to_cex
from lite_indexer.query.trace_sql import sql_for

ROOT = "0xroot"
CEX = "0xcex"
MID = "0xmid"


def _row(
    src: str,
    dst: str,
    *,
    asset: str = "0xtoken",
    raw: int = 10**18,
    tx: str = "0x1",
    ts: str = "2026-10-01T00:00:00+00:00",
    tier: str = "clickhouse",
) -> dict:
    return {
        "from": src,
        "to": dst,
        "asset": asset,
        "raw": raw,
        "symbol": "TKN",
        "tx_hash": tx,
        "ts": ts,
        "kind": "erc20",
        "tier": tier,
        "tx_count": 2,
    }


def test_build_graph_scales_raw_with_decimals_and_usd():
    meta = {"0xtoken": {"decimals": 6}}
    prices = {"0xtoken": {"price_usd": 2.0}}
    graph = build_graph(
        ROOT,
        "base",
        [_row(ROOT, MID, raw=5_000_000, tx="0xa")],
        {},
        prices=prices,
        meta=meta,
        hops={ROOT: 0, MID: 1},
    )
    edge = graph["edges"][0]
    assert edge["amount"] == pytest.approx(5.0)
    assert edge["usd"] == pytest.approx(10.0)
    assert edge["hop"] == 1
    root = next(n for n in graph["nodes"] if n["address"] == ROOT)
    assert root["is_root"] is True and root["out_usd"] == pytest.approx(10.0)
    assert graph["nodes"][0]["address"] == ROOT


def test_build_graph_dedupes_same_tx_across_tiers_and_keeps_both_tiers():
    rows = [
        _row(ROOT, MID, tx="0xdup", tier="clickhouse"),
        _row(ROOT, MID, tx="0xdup", tier="biglake"),
    ]
    graph = build_graph(ROOT, "ethereum", rows, {})
    assert len(graph["edges"]) == 1
    assert sorted(graph["edges"][0]["tiers"]) == ["biglake", "clickhouse"]


def test_build_graph_skips_self_loops_and_empty_sides():
    rows = [_row(ROOT, ROOT), _row(ROOT, ""), _row("", MID)]
    assert build_graph(ROOT, "base", rows, {})["edges"] == []


def test_build_graph_marks_stop_addresses_from_labels():
    labels = {CEX: {"kind": "exchange", "label": "Binance 14", "entity": "Binance"}}
    graph = build_graph(ROOT, "base", [_row(ROOT, CEX)], labels)
    assert graph["stop_addresses"] == [CEX]
    node = next(n for n in graph["nodes"] if n["address"] == CEX)
    assert node["kind"] == "exchange" and node["entity"] == "Binance"


def test_paths_to_cex_returns_shortest_with_tx_evidence():
    rows = [_row(ROOT, MID, tx="0xhop1"), _row(MID, CEX, tx="0xhop2")]
    labels = {CEX: {"kind": "exchange", "label": "OKX"}}
    graph = build_graph(ROOT, "base", rows, labels, hops={ROOT: 0, MID: 1, CEX: 2})
    paths = paths_to_cex(graph, ROOT, max_hops=3)
    assert len(paths) == 1
    assert paths[0]["address"] == CEX
    assert paths[0]["hops"] == 2
    assert paths[0]["path"] == [ROOT, MID, CEX]
    assert paths[0]["tx_hashes"] == ["0xhop1", "0xhop2"]


def test_paths_to_cex_respects_max_hops():
    rows = [_row(ROOT, MID, tx="0xhop1"), _row(MID, CEX, tx="0xhop2")]
    graph = build_graph(ROOT, "base", rows, {CEX: {"kind": "exchange"}})
    assert paths_to_cex(graph, ROOT, max_hops=1) == []


def test_sql_for_binds_every_value():
    sql, params = sql_for(
        "evm_edges", chain="base", since="2026-10-01", addrs=[ROOT, CEX], limit=10
    )
    assert "base" not in sql and ROOT not in sql and CEX not in sql
    assert params["chain"] == "base" and params["addrs"] == [ROOT, CEX]


def test_sql_for_rejects_unknown_kind():
    with pytest.raises(KeyError):
        sql_for("drop_everything")


def test_resolve_entities_prefers_taxonomy_kind_and_keeps_risk():
    def run(sql: str, params: dict) -> list[dict]:
        if "wallet_labels" in sql:
            return [
                {
                    "address": CEX,
                    "label_category": "cex_hot_wallet",
                    "label_name": "Binance 14",
                    "label_subtype": "",
                    "is_sanctioned": 1,
                    "source": "import",
                },
            ]
        return [
            {
                "address": CEX,
                "entity_type": "exchange",
                "label_type": "hot_wallet",
                "name": "Binance",
                "source": "scanner",
                "confidence": 0.9,
            }
        ]

    out = resolve_entities(run, [CEX, MID])
    assert out[CEX]["kind"] == "exchange"
    assert out[CEX]["entity"] == "Binance"
    assert out[CEX]["sanctioned"] is True
    assert "sanctioned" in out[CEX]["risk"]
    assert out[CEX]["label"] == "Binance 14"


def test_resolve_entities_survives_label_failure():
    def run(sql: str, params: dict) -> list[dict]:
        raise RuntimeError("clickhouse down")

    assert resolve_entities(run, [CEX]) == {}


def test_deployer_path_flags_shared_funder(monkeypatch):
    calls: list[str] = []

    def fake_trace(deployer, chain, depth, direction, since_days, run):  # noqa: ARG001
        return {
            "nodes": [
                {"address": deployer, "kind": "wallet", "label": "", "entity": ""},
                {"address": "0xfunder", "kind": "wallet", "label": "", "entity": ""},
            ],
            "edges": [
                {
                    "from": "0xfunder",
                    "to": deployer,
                    "asset": "native",
                    "symbol": "",
                    "amount": 1.0,
                    "usd": None,
                    "tx_hash": "0xfund",
                    "ts": "2026-09-01T00:00:00+00:00",
                    "kind": "internal",
                },
            ],
            "paths_to_cex": [],
            "provenance": {"notes": []},
        }

    def fake_run(sql: str, params: dict) -> list[dict]:
        calls.append(params.get("funder") or params.get("address") or "other")
        if "pair_address = %(target)s" in sql:
            return []
        if "uniqExact" in sql:
            return [
                {
                    "rows": 12,
                    "tokens": 3,
                    "first_ts": "2026-09-01",
                    "last_ts": "2026-10-01",
                    "factory": None,
                }
            ]
        if "deployment_sample" in sql or "ORDER BY block_timestamp DESC" in sql:
            return [
                {
                    "deployer": "0xdep",
                    "token_address": "0xtok",
                    "tx_hash": "0xdep",
                    "block_timestamp": "2026-10-01",
                    "type": "pair",
                }
            ]
        return [{"deployer": "0xother"}]

    monkeypatch.setattr(dep, "trace", fake_trace)
    result = dep.deployer_path("0xdep", "base", run=fake_run)
    assert result["found"] is True
    assert result["deployer"] == "0xdep"
    assert result["funder"]["address"] == "0xfunder"
    assert result["shared_funder"]["count"] == 1
    assert result["flags"]["shared_funder"] is True
    assert "0xfunder" in calls
