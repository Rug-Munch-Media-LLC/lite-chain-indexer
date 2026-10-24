"""Lake VALUE columns are nullable DOUBLEs — NULL must survive the mapping.

The sonarx/biglake mappers used to turn a missing ``VALUE`` into the string
``"0"``, which is a fabricated on-chain amount: three of fifty sampled sonarx
rows were silently zero-valued, and a backfill would write millions of fake
zeros into ``evm_transfers``. NULL means "the lake does not know", so it must
reach ClickHouse as NULL (or be dropped when the target column cannot hold it).
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.ingest import lake_sonarx
from lite_indexer.query import lake_row_utils

ADDR_FROM = "0x" + "1" * 40
ADDR_TO = "0x" + "2" * 40
ADDR_TOKEN = "0x" + "3" * 40


def _transfer_row(value: Any) -> dict[str, Any]:
    return {
        "BLOCK_NUMBER": 19_000_000,
        "TRANSACTION_HASH": "0x" + "a" * 64,
        "FROM_ADDRESS": ADDR_FROM,
        "TO_ADDRESS": ADDR_TO,
        "TOKEN_ADDRESS": ADDR_TOKEN,
        "VALUE": value,
        "TIMESTAMP": 1_700_000_000,
        "LOG_INDEX": 7,
    }


def _trace_row(value: Any) -> dict[str, Any]:
    return {
        "BLOCK_NUMBER": 19_000_000,
        "TRANSACTION_HASH": "0x" + "a" * 64,
        "FROM_ADDRESS": ADDR_FROM,
        "TO_ADDRESS": ADDR_TO,
        "VALUE": value,
        "TIMESTAMP": 1_700_000_000,
        "PATH": "0,1",
        "TYPE": "call",
        "GAS_USED": 21_000,
        "INPUT": "0x",
    }


CONVERTERS = [
    pytest.param(lake_row_utils._num_str, id="lake_row_utils"),
    pytest.param(lake_sonarx._num_str, id="lake_sonarx"),
]


@pytest.mark.parametrize("convert", CONVERTERS)
def test_num_str_keeps_null(convert: Any) -> None:
    """NULL in -> NULL out; never the string "0"."""
    assert convert(None) is None


@pytest.mark.parametrize("convert", CONVERTERS)
@pytest.mark.parametrize("value,expected", [(61.0, "61"), (61.5, "61.5"), (0.0, "0"), ("abc", "abc")])
def test_num_str_still_converts_present_values(convert: Any, value: Any, expected: str) -> None:
    """The NULL fix must not change genuine values (including a real 0)."""
    assert convert(value) == expected


@pytest.mark.parametrize("convert", CONVERTERS)
def test_num_str_never_uses_exponent_notation(convert: Any) -> None:
    """18-decimal tokens are integral doubles; "1e+18" is not a decimal amount."""
    out = convert(1e18)
    assert out is not None
    assert "e" not in out.lower()
    assert out == "1000000000000000000"


def test_map_transfer_row_preserves_null_value() -> None:
    """A NULL lake VALUE reaches the writer as None (ClickHouse NULL)."""
    assert lake_sonarx.map_transfer_row(_transfer_row(None))["value"] is None


def test_map_transfer_row_keeps_real_zero() -> None:
    """A genuine zero-value transfer is still written as "0"."""
    assert lake_sonarx.map_transfer_row(_transfer_row(0.0))["value"] == "0"


def test_map_trace_row_does_not_invent_value_wei() -> None:
    """``value_wei`` is UInt256 (not Nullable) — a NULL trace must not become 0."""
    assert lake_sonarx.map_trace_row(_trace_row(None)) is None


def test_map_trace_row_keeps_present_value() -> None:
    mapped = lake_sonarx.map_trace_row(_trace_row(1_000_000.0))
    assert mapped is not None
    assert mapped["value_wei"] == 1_000_000


class _FakeCursor:
    def __init__(self, cols: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.description = [(c,) for c in cols]
        self._rows = rows

    def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
        batch, self._rows = self._rows[:n], self._rows[n:]
        return batch


class _FakeCon:
    def __init__(self, cols: list[str], rows: list[tuple[Any, ...]]) -> None:
        self._cols, self._rows = cols, rows

    def execute(self, _sql: str) -> _FakeCursor:
        return _FakeCursor(self._cols, self._rows)


def test_read_traces_skips_null_value_rows() -> None:
    """NULL-valued traces carry no value transfer and must not be invented as 0."""
    cols = ["BLOCK_NUMBER", "TRANSACTION_HASH", "FROM_ADDRESS", "TO_ADDRESS", "VALUE", "TIMESTAMP", "PATH", "TYPE"]
    rows = [
        (19_000_000, "0x" + "a" * 64, ADDR_FROM, ADDR_TO, None, 1_700_000_000, "0", "call"),
        (19_000_000, "0x" + "b" * 64, ADDR_FROM, ADDR_TO, 5.0, 1_700_000_000, "0", "call"),
    ]
    src = lake_sonarx.SonarxLakeSource(connect=lambda: _FakeCon(cols, rows))
    out = list(src.read_traces("base", "2026-09-12"))
    assert len(out) == 1
    assert out[0]["value_wei"] == 5
