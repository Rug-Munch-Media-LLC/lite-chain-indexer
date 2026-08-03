"""Unit tests for the read-only SQL validator (api.security)."""

from __future__ import annotations

import pytest

from api.security import QueryValidationError, validate_readonly


def test_accepts_simple_select() -> None:
    sql = validate_readonly("SELECT * FROM evm_transfers WHERE chain = 'ethereum' LIMIT 5")
    assert "LIMIT" in sql


def test_appends_limit_when_missing() -> None:
    sql = validate_readonly("SELECT * FROM evm_transfers")
    assert sql.endswith("LIMIT 100")


def test_allows_trailing_semicolon() -> None:
    sql = validate_readonly("SELECT * FROM evm_transfers;")
    assert sql.startswith("SELECT")


def test_rejects_multiple_statements() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("SELECT 1; SELECT 2")


def test_rejects_write_keywords() -> None:
    for bad in ("DROP TABLE evm_transfers", "INSERT INTO x VALUES (1)", "ALTER TABLE x", "OPTIMIZE TABLE x"):
        with pytest.raises(QueryValidationError):
            validate_readonly(bad)


def test_rejects_non_select_leading() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("SHOW TABLES")


def test_rejects_into_outfile() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("SELECT * FROM evm_transfers INTO OUTFILE '/tmp/x'")


def test_rejects_unknown_table() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("SELECT * FROM system.users")


def test_rejects_unknown_db_table() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("SELECT * FROM other_db.secret_table")


def test_accepts_qualified_allowed_table() -> None:
    sql = validate_readonly("SELECT * FROM wallet_memory.evm_transfers")
    assert "wallet_memory.evm_transfers" in sql


def test_accepts_aliases() -> None:
    sql = validate_readonly("SELECT * FROM transfers LIMIT 3")
    assert "transfers" in sql


def test_accepts_with_cte() -> None:
    sql = validate_readonly("WITH t AS (SELECT chain FROM evm_transfers) SELECT chain FROM t LIMIT 5")
    assert sql.startswith("WITH")


def test_empty_query_rejected() -> None:
    with pytest.raises(QueryValidationError):
        validate_readonly("")
    with pytest.raises(QueryValidationError):
        validate_readonly("   ")
