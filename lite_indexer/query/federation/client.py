"""CryptoHouse ClickHouse client: transport, guard enforcement, typed errors.

Endpoint: ``https://crypto-clickhouse.clickhouse.com/?user=crypto``
(empty password). ClickHouse 26.6.1.2191, ``readonly=1``. Verified 2026-10-04.

The hostname uses a HYPHEN; the form in fleet docs
(``crypto.clickhouse.clickhouse.com``) is NXDOMAIN.

Uses stdlib ``urllib``: the repo's default ``.venv`` has no ``httpx`` and this
layer must import fleet-wide. Credentials are the anonymous public ones — no
key to leak, none read from env.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from lite_indexer.query.federation.errors import (
    MAX_EXECUTION_TIME_S,
    CryptoHouseError,
    QueryTimeoutError,
    classify_error,
)
from lite_indexer.query.federation.guard import assert_bounded

DEFAULT_URL: Final[str] = "https://crypto-clickhouse.clickhouse.com/?user=crypto"
DEFAULT_USER: Final[str] = "crypto"
_READ_PREFIX: Final[tuple[str, ...]] = (
    "select",
    "with",
    "show",
    "describe",
    "desc",
    "explain",
)
_PLACEHOLDER: Final[re.Pattern[str]] = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*):[A-Za-z0-9_]+\}")


@dataclass(frozen=True, slots=True)
class QueryResult:
    """A completed read plus timing and guard metadata."""

    rows: list[dict[str, Any]]
    elapsed_s: float
    sql: str
    guarded_tables: tuple[str, ...]
    fmt: str

    @property
    def count(self) -> int:
        return len(self.rows)


class CryptoHouseClient:
    """Read-only HTTP client for one ClickHouse query at a time.

    Budget is 60 queries/hour, so connection reuse buys nothing.
    ``verify_guard=False`` is for introspection over ``system.*`` only.
    """

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        user: str = DEFAULT_USER,
        password: str = "",
        timeout_s: int = MAX_EXECUTION_TIME_S + 15,
        read_only: bool = True,
        verify_guard: bool = True,
    ) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.timeout_s = timeout_s
        self.read_only = read_only
        self.verify_guard = verify_guard

    def _endpoint(self, database: str | None) -> str:
        params = {"user": self.user}
        if self.password:
            params["password"] = self.password
        if database:
            params["database"] = database
        sep = "&" if "?" in self.url else "?"
        return f"{self.url}{sep}{urllib.parse.urlencode(params)}"

    def _send(self, sql: str, database: str | None) -> tuple[str, float]:
        req = urllib.request.Request(self._endpoint(database), data=sql.encode("utf-8"), method="POST")
        req.add_header("Content-Type", "text/plain; charset=utf-8")
        started = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:  # noqa: S310
                return resp.read().decode("utf-8", errors="replace"), time.monotonic() - started
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise classify_error(detail, sql, time.monotonic() - started) from exc
        except TimeoutError as exc:
            raise QueryTimeoutError("client-side socket timeout", sql, time.monotonic() - started) from exc
        except urllib.error.URLError as exc:
            elapsed = time.monotonic() - started
            if isinstance(exc.reason, TimeoutError):
                raise QueryTimeoutError("client-side socket timeout", sql, elapsed) from exc
            raise CryptoHouseError(f"transport failure: {exc}") from exc

    def execute(
        self,
        sql: str,
        *,
        database: str | None = None,
        params: Mapping[str, Any] | None = None,
        fmt: str = "TabSeparated",
        extra_large: Sequence[str] = (),
    ) -> QueryResult:
        """Run one guarded read; returns rows plus timing metadata."""
        rendered = render_params(sql, params)
        if self.read_only and not _is_readonly(rendered):
            raise CryptoHouseError("refusing non-SELECT on a read-only client")
        guarded = assert_bounded(rendered, extra_large=extra_large) if self.verify_guard else ()
        payload, elapsed = self._send(rendered, database)
        return QueryResult(
            rows=_decode(payload, fmt),
            elapsed_s=elapsed,
            sql=rendered,
            guarded_tables=guarded,
            fmt=fmt,
        )

    def scalar(self, sql: str, **kwargs: Any) -> Any:
        """First column of the first row, or ``None``."""
        result = self.execute(sql, **kwargs)
        if not result.rows:
            return None
        return next(iter(result.rows[0].values()), None)

    def count(self, table: str, where: str = "", **kwargs: Any) -> int:
        """``count()`` over ``table`` with an optional caller WHERE clause."""
        clause = f" WHERE {where}" if where else ""
        return int(self.scalar(f"SELECT count() AS n FROM {table}{clause}", **kwargs) or 0)


def _is_readonly(sql: str) -> bool:
    cleaned = re.sub(r"--[^\n]*", " ", sql)
    cleaned = re.sub(r"[\s(]+", " ", cleaned).strip().lower()
    return cleaned.startswith(_READ_PREFIX)


def render_params(sql: str, params: Mapping[str, Any] | None) -> str:
    """Substitute ``{name:Type}`` placeholders with quoted literals.

    Values never reach SQL unescaped; identifiers must still pass
    ``schemas.validate_ident`` (ClickHouse cannot bind those).
    """
    if not params:
        return sql

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            raise KeyError(f"placeholder {{{name}:Type}} has no supplied value")
        return quote_literal(params[name])

    return _PLACEHOLDER.sub(repl, sql)


def quote_literal(value: Any) -> str:
    """Render a Python value as a quoted ClickHouse literal."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _decode(payload: str, fmt: str) -> list[dict[str, Any]]:
    if fmt == "json":
        return [json.loads(line) for line in payload.splitlines() if line.strip()]
    lines = [ln for ln in payload.split("\n") if ln.strip()]
    if not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, ln.split("\t"), strict=False)) for ln in lines[1:]]


_default_client: CryptoHouseClient | None = None


def default_client() -> CryptoHouseClient:
    """Process-wide client against the anonymous public endpoint."""
    global _default_client
    if _default_client is None:
        _default_client = CryptoHouseClient()
    return _default_client


def query_rows(sql: str, params: Mapping[str, Any] | None = None, **kwargs: Any) -> list[dict[str, Any]]:
    """One-shot read via the default client."""
    return default_client().execute(sql, params=params, **kwargs).rows
