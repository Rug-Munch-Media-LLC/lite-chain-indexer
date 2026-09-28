"""Wallet graph primitives — who funded this wallet, who are its counterparties.

Two read-only questions, both answerable from the transfers the indexer
ALREADY writes to ``wallet_memory.evm_transfers``:

* :func:`earliest_inbound_funder` — the address that sent this wallet its
  FIRST inbound transfer.
* :func:`counterparties` — every distinct address this wallet has
  transacted with, in either direction.

THE FUNDER HEURISTIC — read before trusting the result
------------------------------------------------------
"Earliest inbound transfer" is a HEURISTIC, not a proof of provenance. It
is correct for the common case: a fresh EOA that was dusted/airdropped
into, then swept or bridged out. It is WRONG for a real class of wallets:

* **Contract deployers** — the first inbound is usually the deployer's
  first token interaction, not the funding of the deployer itself.
* **Bridge recipients** — a bridge contract is the counterparty, and the
  EOA that actually sent the ETH on the source chain is invisible here.
  Cross-chain provenance needs ``wallet_memory.bridge_mappings``.
* **Multisigs / contract wallets** — a Safe is funded by an owner exec-
  uting its own deposit; the "funder" is a signer, not a source of funds.
* **Internal/CPI funding** — a WETH unwrap, a router settlement or a
  plain value transfer never emits an ERC-20 ``Transfer`` log, so it
  never lands in ``evm_transfers`` at all. Those flows live in
  ``wallet_memory.internal_transfers``
  (``CH_INTERNAL_TRANSFERS_TABLE``), which this module deliberately does
  NOT read: it carries only a 90-day TTL and is collected per-chain on a
  bounded block window, so merging it would make the answer
  *inconsistently* better rather than uniformly better. An earliest
  inbound that predates the internal-transfer window, or that the
  internal table shows was an internal hop, is exactly the class where
  this heuristic is weakest.

FOLLOW-UP (not built here): join ``internal_transfers`` (and
``bridge_mappings``) to the earliest-inbound row, expose it as a
``source`` field on the funder record, and let the caller see both the
ERC-20 first-inbound and the trace-level first-inbound side by side.

Both functions are PURE read paths: they take an executor, emit
parameterized SQL, and return plain dicts. No ClickHouse client is
constructed here, so the API layer owns the connection and the tests own
the fake.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lite_indexer.query.wallet_graph_sql import chain_filter

#: Read executor: ``(sql, params) -> list[dict]``. ``api.db.query`` matches
#: this signature; tests pass a fake with the same shape.
Executor = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

#: Columns read from ``wallet_memory.evm_transfers`` by both primitives.
_TRANSFER_COLUMNS = (
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "from_address",
    "to_address",
    "token_address",
    "value",
)


def earliest_inbound_funder(
    run: Executor,
    address: str,
    chain: str | None = None,
) -> dict[str, Any] | None:
    """Return the first inbound transfer's sender — the funder heuristic.

    The earliest inbound transfer IS the funder under the assumption that
    a wallet's first receipt is how it came into existence. Sorts by
    ``(block_number, log_index)`` — NOT by ``value`` and NOT by recency —
    so a large later transfer can never displace the true first one.
    Ties inside one block are broken by log index, which is the ordering
    the chain itself executed them in.

    Internal (CPI) transfers are NOT consulted; see the module docstring
    for why, and for the wallet classes where this is wrong.

    Args:
        run: Read executor — ``(sql, params) -> list[dict]``.
        address: Wallet address (any case; normalized to lowercase).
        chain: Optional chain filter.

    Returns:
        The funder row (``from_address``, ``block_number``, ``block_timestamp``,
        ``tx_hash``, ``log_index``, ``token_address``, ``value``), or
        ``None`` when the wallet only ever sent (an outbound-only wallet
        has no funder) or has no indexed transfers.
    """
    columns = ", ".join(_TRANSFER_COLUMNS)
    chain_sql, chain_params = chain_filter(chain)
    sql = (
        f"SELECT {columns} FROM wallet_memory.evm_transfers "
        "WHERE to_address = %(address)s AND from_address != %(address)s"
        f"{chain_sql} "
        "ORDER BY block_number ASC, log_index ASC "
        "LIMIT 1"
    )
    params: dict[str, Any] = {"address": address.strip().lower(), **chain_params}
    rows = run(sql, params)
    return rows[0] if rows else None
