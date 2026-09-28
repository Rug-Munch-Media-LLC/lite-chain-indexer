"""Shared SQL fragment helpers for the wallet-graph queries.

Mirrors the ``(sql_fragment, params)`` idiom used by ``api/params.py`` — the
fragment is a module-constant string with a named placeholder, the value
lives only in ``params``. A caller-controlled chain can therefore never
appear in the SQL text; it is bound at execution time.

This lives in ``lite_indexer.query`` rather than importing ``api.params``
because dependencies flow DOWN (``api/ -> query/ -> config/``): the query
layer must never reach up into the API layer.
"""

from __future__ import annotations

from typing import Any

#: Chain predicate fragment. The value is supplied out-of-band as the
#: ``chain`` parameter, so the string is constant.
CHAIN_FRAGMENT = " AND chain = %(chain)s"


def chain_filter(chain: str | None, column: str = "chain") -> tuple[str, dict[str, Any]]:
    """Return ``(sql_fragment, params)`` for an optional chain filter.

    An empty/blank chain yields an empty fragment (all chains) and no
    params, so the caller can append it unconditionally.
    """
    if not chain or not chain.strip():
        return "", {}
    return f" AND {column} = %(chain)s", {"chain": chain.strip().lower()}
