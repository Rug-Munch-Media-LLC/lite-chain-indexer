"""Parameterized SQL for the wallet trace — one dispatcher, no bound values.

Every statement the trace issues is built here so the orchestrator never
concatenates a caller value into SQL. Statement selection is by ``kind``;
table names are module constants (one canonical truth for the trace).

Rows are aggregated per ``(from, to, asset)`` so one hop costs one scan
instead of one query per transaction. ``argMin``/``argMax`` keep a real
transaction hash as evidence for the earliest and latest movement.
"""

from __future__ import annotations

from typing import Any

EVM_TABLE = "wallet_memory.evm_transfers"
INTERNAL_TABLE = "wallet_memory.internal_transfers"
LABEL_TABLE = "wallet_memory.wallet_labels"
ENTITY_TABLE = "wallet_memory.entity_labels"
DEPLOYER_TABLE = "wallet_memory.deployer_events"
PRICE_TABLE = "wallet_memory.token_prices"
META_TABLE = "wallet_memory.token_meta_ref"

#: ``deployer_events`` stores the resolved EOA separately and leaves it empty
#: on rows the RPC leg never resolved — one expression, used everywhere.
_DEPLOYER_EXPR = "if(deployer_eoa = '', deployer, deployer_eoa)"

__all__ = ["sql_for"]


def _evm_edges(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT from_address, to_address, token_address,
       any(token_symbol) AS symbol,
       count() AS tx_count,
       sum(toUInt256OrZero(value)) AS raw_amount,
       min(block_timestamp) AS first_ts,
       max(block_timestamp) AS last_ts,
       argMin(tx_hash, block_timestamp) AS first_tx,
       argMax(tx_hash, block_timestamp) AS last_tx
FROM {EVM_TABLE}
WHERE chain = %(chain)s
  AND block_timestamp >= %(since)s
  AND (from_address IN %(addrs)s OR to_address IN %(addrs)s)
GROUP BY from_address, to_address, token_address
ORDER BY raw_amount DESC
LIMIT %(limit)s
"""
    return sql, {
        "chain": kw["chain"],
        "since": kw["since"],
        "addrs": list(kw["addrs"]),
        "limit": int(kw["limit"]),
    }


def _internal_edges(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT from_address, to_address,
       count() AS tx_count,
       sum(value_wei) AS raw_amount,
       min(block_timestamp) AS first_ts,
       max(block_timestamp) AS last_ts,
       argMin(tx_hash, block_timestamp) AS first_tx,
       argMax(tx_hash, block_timestamp) AS last_tx
FROM {INTERNAL_TABLE}
WHERE chain = %(chain)s
  AND block_timestamp >= %(since)s
  AND value_wei > 0
  AND error = ''
  AND (from_address IN %(addrs)s OR to_address IN %(addrs)s)
GROUP BY from_address, to_address
ORDER BY raw_amount DESC
LIMIT %(limit)s
"""
    return sql, {
        "chain": kw["chain"],
        "since": kw["since"],
        "addrs": list(kw["addrs"]),
        "limit": int(kw["limit"]),
    }


def _labels(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT address, label_category, label_name, label_subtype,
       is_sanctioned, source
FROM {LABEL_TABLE} FINAL
WHERE address IN %(addrs)s
"""
    return sql, {"addrs": list(kw["addrs"])}


def _entities(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT address, entity_type, label_type, name, source, confidence
FROM {ENTITY_TABLE} FINAL
WHERE address IN %(addrs)s
"""
    return sql, {"addrs": list(kw["addrs"])}


def _deployments(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT count() AS rows,
       uniqExact(token_address) AS tokens,
       min(block_timestamp) AS first_ts,
       max(block_timestamp) AS last_ts,
       any(factory) AS factory
FROM {DEPLOYER_TABLE}
WHERE chain = %(chain)s AND {_DEPLOYER_EXPR} = %(address)s
"""
    return sql, {"chain": kw["chain"], "address": kw["address"]}


def _deployment_sample(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT {_DEPLOYER_EXPR} AS deployer, token_address, tx_hash, block_timestamp, type
FROM {DEPLOYER_TABLE}
WHERE chain = %(chain)s AND {_DEPLOYER_EXPR} = %(address)s
ORDER BY block_timestamp DESC
LIMIT %(limit)s
"""
    return sql, {"chain": kw["chain"], "address": kw["address"], "limit": int(kw["limit"])}


def _deployer_of_token(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT {_DEPLOYER_EXPR} AS deployer, token_address, tx_hash, block_timestamp, type
FROM {DEPLOYER_TABLE}
WHERE chain = %(chain)s
  AND (token_address = %(target)s OR pair_address = %(target)s)
ORDER BY block_timestamp ASC
LIMIT %(limit)s
"""
    return sql, {"chain": kw["chain"], "target": kw["target"], "limit": int(kw["limit"])}


def _prices(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT token_address, any(price_usd) AS price_usd, any(symbol) AS symbol
FROM {PRICE_TABLE}
WHERE chain = %(chain)s AND token_address IN %(tokens)s
GROUP BY token_address
"""
    return sql, {"chain": kw["chain"], "tokens": list(kw["tokens"])}


def _meta(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT token_address, any(decimals) AS decimals, any(symbol) AS symbol
FROM {META_TABLE}
WHERE chain = %(chain)s AND token_address IN %(tokens)s
GROUP BY token_address
"""
    return sql, {"chain": kw["chain"], "tokens": list(kw["tokens"])}


def _shared_funder(kw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sql = f"""
SELECT DISTINCT to_address AS deployer
FROM {EVM_TABLE}
WHERE chain = %(chain)s
  AND from_address = %(funder)s
  AND to_address IN (
      SELECT DISTINCT {_DEPLOYER_EXPR} FROM {DEPLOYER_TABLE} WHERE chain = %(chain)s
  )
  AND to_address != %(exclude)s
LIMIT %(limit)s
"""
    return sql, {
        "chain": kw["chain"],
        "funder": kw["funder"],
        "exclude": kw["exclude"],
        "limit": int(kw["limit"]),
    }


_BUILDERS = {
    "evm_edges": _evm_edges,
    "internal_edges": _internal_edges,
    "labels": _labels,
    "entities": _entities,
    "deployments": _deployments,
    "deployment_sample": _deployment_sample,
    "deployer_of_token": _deployer_of_token,
    "prices": _prices,
    "meta": _meta,
    "shared_funder": _shared_funder,
}


def sql_for(kind: str, **kw: Any) -> tuple[str, dict[str, Any]]:
    """Return ``(sql, params)`` for one trace statement.

    Args:
        kind: One of ``evm_edges``, ``internal_edges``, ``labels``,
            ``entities``, ``deployments``, ``deployment_sample``,
            ``deployer_of_token``, ``prices``, ``meta``, ``shared_funder``.
        **kw: Statement arguments (``chain``, ``addrs``, ``since``, ...).

    Raises:
        KeyError: ``kind`` is not a trace statement.
    """
    builder = _BUILDERS.get(kind)
    if builder is None:
        raise KeyError(f"unknown trace statement: {kind}")
    return builder(kw)
