"""Read-only SQL validation for the raw ``POST /api/v1/query`` endpoint.

The real security boundary is the ClickHouse user (SELECT-only grants on
``wallet_memory.*`` — verified at the server). This module is the
application layer that stops accidental damage and runaway scans before
they reach the warehouse:

* single statement only (no multi-statement / ``;`` chaining)
* leading keyword must be SELECT or WITH
* write keywords are rejected anywhere in the statement
* every FROM/JOIN table must resolve to an allowed table or a WITH alias
* a LIMIT is appended when missing (bounded responses)

The validator returns the normalized query (trailing ``;`` stripped,
LIMIT enforced). It raises :class:`QueryValidationError` on violation.
"""

from __future__ import annotations

import re

from api.config import DEFAULT_PAGE_LIMIT
from api.db import ALLOWED_TABLES, TABLE_ALIASES

# Write / destructive keywords — rejected anywhere in the statement.
_WRITE_KEYWORDS: frozenset[str] = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "ALTER",
        "CREATE",
        "DROP",
        "TRUNCATE",
        "RENAME",
        "GRANT",
        "REVOKE",
        "KILL",
        "SYSTEM",
        "OPTIMIZE",
        "ATTACH",
        "DETACH",
        "USE",
        "BACKUP",
        "RESTORE",
        "EXCHANGE",
        "UNDROP",
        "FENCE",
        "EXPLAIN",
        "DESCRIBE",
        "SHOW",
        "EXISTS",
        "CHECK",
    }
)

# SELECT ... INTO (OUTFILE) writes to the server filesystem.
_INTO_PATTERN = re.compile(r"\bINTO\s+(OUTFILE|FILE)\b", re.IGNORECASE)
# FORMAT clause redirects the output format — reject to keep JSON control.
_FORMAT_PATTERN = re.compile(r"\bFORMAT\s+\w+", re.IGNORECASE)
# SETTINGS clause could change behavior — allow but ignore (harmless).
_STATEMENT_SPLIT = re.compile(r";")
_LEADING = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_LIMIT = re.compile(r"\bLIMIT\b", re.IGNORECASE)
_FROM_TABLE = re.compile(
    r"\b(?:FROM|JOIN)\s+([`\w]+)(?:\s*\.\s*([`\w]+))?",
    re.IGNORECASE,
)
_CTE = re.compile(
    r"WITH\s+([`\w]+)\s+AS\s*\(",
    re.IGNORECASE,
)


class QueryValidationError(ValueError):
    """Raised when a raw query violates the read-only policy."""


def _strip_quoted(token: str) -> str:
    """Remove backticks/quotes from a table token and lowercase it."""
    return token.strip("`\"'").lower()


def _resolve_table(db_part: str | None, table_part: str | None) -> str | None:
    """Resolve a ``db.table`` reference to a canonical allowed table.

    Returns the canonical table name when the reference is allowed,
    otherwise None.
    """
    if db_part is not None:
        # Fully-qualified reference: wallet_memory.evm_transfers
        if _strip_quoted(db_part) != "wallet_memory":
            return None
        table = _strip_quoted(table_part or "")
    else:
        table = _strip_quoted(table_part or "")
    if not table:
        return None
    if table in ALLOWED_TABLES:
        return table
    return TABLE_ALIASES.get(table)


def validate_readonly(query: str) -> str:
    """Validate a raw SQL statement and return the normalized query.

    Raises:
        QueryValidationError: when the query violates the read-only policy.
    """
    if not query or not query.strip():
        raise QueryValidationError("query is empty")

    # 1. Single statement only.
    parts = _STATEMENT_SPLIT.split(query)
    # Allow a single trailing semicolon; anything else is multi-statement.
    body = parts[0].strip()
    if len(parts) > 2 or (len(parts) == 2 and parts[1].strip()):
        raise QueryValidationError("multiple statements are not allowed")

    # 2. Leading keyword must be SELECT or WITH.
    if not _LEADING.match(body):
        raise QueryValidationError("only SELECT (read-only) queries are allowed")

    # 3. Write keywords anywhere.
    upper = body.upper()
    for keyword in _WRITE_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper):
            raise QueryValidationError(f"keyword '{keyword}' is not allowed in read-only queries")

    # 4. INTO OUTFILE / FORMAT.
    if _INTO_PATTERN.search(body):
        raise QueryValidationError("INTO OUTFILE is not allowed")
    if _FORMAT_PATTERN.search(body):
        raise QueryValidationError("FORMAT clause is not allowed")

    # 5. Table allowlist (FROM / JOIN), honoring WITH CTE aliases.
    cte_aliases = {_strip_quoted(m) for m in _CTE.findall(body)}
    for match in _FROM_TABLE.finditer(body):
        if match.group(2) is not None:
            # Qualified reference: db.table (group1 = db, group2 = table).
            resolved = _resolve_table(match.group(1), match.group(2))
            token = _strip_quoted(f"{match.group(1)}.{match.group(2)}")
        else:
            # Bare table reference (group1 = table).
            resolved = _resolve_table(None, match.group(1))
            token = _strip_quoted(match.group(1) or "")
        if resolved is None and token not in cte_aliases:
            raise QueryValidationError(f"table '{token}' is not in the read-only allowlist")

    # 6. Enforce LIMIT when missing.
    if not _LIMIT.search(body):
        body = f"{body} LIMIT {DEFAULT_PAGE_LIMIT}"

    return body
