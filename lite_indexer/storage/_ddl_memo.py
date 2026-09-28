"""Process-level memo for DDL-issuing schema helpers.

Extracted from ``storage/_schema.py`` (shrink-only size ratchet). Both
schema halves decorate their ``_ensure_*`` methods with ``_ddl_memo``;
the memo table must be a single shared object, so it lives here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])

# Process-level memo: ensure_ttl_policies() runs ~20 DDL statements
# (column-TTL ALTERs + projections + table TTLs). On the write path this
# re-ran on EVERY dex batch — measured 2026-09-23 at 218 ALTERs/table/30min,
# which saturated the ext4 journal. Re-ensure at most every 6 h.
_TTL_ENSURED_AT: dict[str, float] = {}
_TTL_REENSURE_SECONDS = 6 * 3600


def _ddl_memo(fn: F) -> F:
    """Run a DDL-issuing routine at most once per ``_TTL_REENSURE_SECONDS``.

    Every ``_ensure_*`` method below issues one or more ALTERs and is called
    from the per-batch write path. Before this guard each dex/transfer batch
    re-issued ~20 DDL statements (measured 2026-09-23: 218 ALTERs / table /
    30 min, incl. MODIFY ORDER BY on a 29 GiB table) which saturated the
    ext4 journal and wedged dockerd.
    """
    import functools
    import time

    @functools.wraps(fn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        key = fn.__name__ + ":" + "|".join(str(a)[:40] for a in args)
        now = time.monotonic()
        if now - _TTL_ENSURED_AT.get(key, 0.0) < _TTL_REENSURE_SECONDS:
            return None
        try:
            return fn(self, *args, **kwargs)
        finally:
            _TTL_ENSURED_AT[key] = time.monotonic()

    return cast(F, wrapper)
