"""Row builders and cycle cadence — no network, no ClickHouse."""

from __future__ import annotations

import datetime as dt

from lite_indexer.producers.polymarket.rows import holder_rows, market_rows, trade_rows
from lite_indexer.producers.polymarket.run import _due

ASOF = dt.datetime(2026, 10, 25, 9, 0)
TRADE = {
    "proxyWallet": "0xabc",
    "side": "BUY",
    "asset": "111",
    "conditionId": "0xcond",
    "size": 10.0,
    "price": 0.5,
    "timestamp": 1792864734,
    "title": "t",
    "slug": "s",
    "outcome": "Yes",
    "outcomeIndex": 0,
    "transactionHash": "0xtx",
}


def test_trade_rows_dedupes_on_the_order_by_key() -> None:
    rows = trade_rows([TRADE, dict(TRADE)])
    assert len(rows) == 1
    assert rows[0][9] == 5.0  # notional = size * price
    assert rows[0][1].tzinfo is None


def test_trade_rows_drops_rows_without_a_timestamp() -> None:
    broken = {k: v for k, v in TRADE.items() if k != "timestamp"}
    assert trade_rows([broken, TRADE]) == trade_rows([TRADE])


def test_market_rows_keep_one_image_per_condition_and_market() -> None:
    payload = [
        {
            "id": "1",
            "conditionId": "0xa",
            "outcomes": '["Yes","No"]',
            "endDate": "2027-01-01T04:59:00Z",
        },
        {
            "id": "1",
            "conditionId": "0xa",
            "outcomes": '["Yes","No"]',
            "endDate": "2027-01-01T04:59:00Z",
        },
        {"id": "2", "conditionId": "", "outcomes": "[]"},
    ]
    rows = market_rows(payload, ASOF)
    assert len(rows) == 1
    assert rows[0][0] == "1"
    assert rows[0][8] == '["Yes","No"]'
    assert rows[0][4] == dt.datetime(2027, 1, 1, 4, 59)
    assert rows[0][10] == ASOF


def test_holder_rows_flatten_tokens_and_key_on_asof() -> None:
    payload = [
        {"token": "111", "holders": [{"proxyWallet": "0xabc", "amount": 3, "outcomeIndex": 1}]}
    ]
    rows = holder_rows(payload, "0xcond", ASOF)
    assert rows == [(ASOF, "0xcond", "111", "0xabc", 3.0, 1, "", "")]


def test_holders_answer_null_without_crashing() -> None:
    assert holder_rows(None, "0xcond", ASOF) == []


def test_due_respects_the_interval_and_force() -> None:
    now = dt.datetime(2026, 10, 25, 12, 0)
    assert _due("trades", None, now, force=False) is True
    assert _due("trades", now - dt.timedelta(minutes=10), now, force=False) is False
    assert _due("trades", now - dt.timedelta(minutes=10), now, force=True) is True
    assert _due("holders", now - dt.timedelta(minutes=10), now, force=False) is False
    assert _due("markets", now - dt.timedelta(minutes=1), now, force=False) is True
