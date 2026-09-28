"""Counterparty aggregation for :mod:`lite_indexer.query.wallet_graph`.

A wallet's counterparty set is every distinct address it has transacted
with, in BOTH directions. The relationship is symmetric by construction:
if ``A`` sent to ``B``, then ``B`` is an inbound counterparty of ``A``'s
view, and the same transfer is an outbound edge when the query runs
against ``B`` — same edge, roles swapped.

Direction is derived per row (``from_address`` vs ``to_address`` relative
to the queried wallet) rather than baked into a UNION of two scans, which
keeps this to one pass over the two address projections. Self-transfers
are excluded: an address is not its own counterparty.

Same parametrisation contract as the funder query — the address and the
chain are placeholders; neither is ever interpolated.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lite_indexer.query.wallet_graph_sql import chain_filter

#: Read executor: ``(sql, params) -> list[dict]``. ``api.db.query`` matches
#: this signature; tests pass a fake with the same shape.
Executor = Callable[[str, dict[str, Any]], list[dict[str, Any]]]


def counterparties(
    run: Executor,
    address: str,
    chain: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return the distinct addresses ``address`` has transacted with.

    Both directions are covered by ``(from_address = w OR to_address = w)``
    and ``counterparty`` resolves to whichever side is not the queried
    wallet. Rows are aggregated per counterparty, so a wallet that traded
    500 times with one venue is one row rather than 500.

    Ordered by ``last_seen_block DESC`` — most recently active
    counterparties first, the order a tracer reads them in. ``limit`` is
    coerced with ``int()`` (never a raw string), so it cannot carry SQL;
    the API layer additionally clamps it against ``MAX_PAGE_LIMIT``.

    ``out_volume``/``in_volume`` are raw token amounts (UInt256) summed in
    ClickHouse and returned verbatim. They are NOT USD, are not
    price-adjusted, and mix tokens when a wallet trades several — a
    consumer that needs a comparable magnitude must group by token first.

    Args:
        run: Read executor — ``(sql, params) -> list[dict]``.
        address: Wallet address (any case; normalized to lowercase).
        chain: Optional chain filter.
        limit: Maximum counterparties to return.

    Returns:
        One row per counterparty (``counterparty``, ``chain``,
        ``first_seen_block``, ``last_seen_block``, ``out_volume``,
        ``in_volume``, ``transfer_count``), most recently active first.
    """
    chain_sql, chain_params = chain_filter(chain)
    wallet = address.strip().lower()
    sql = (
        "SELECT counterparty, any(chain) AS chain, "
        "min(block_number) AS first_seen_block, "
        "max(block_number) AS last_seen_block, "
        "sumIf(toUInt256(value), from_address = %(address)s) AS out_volume, "
        "sumIf(toUInt256(value), to_address = %(address)s) AS in_volume, "
        "count() AS transfer_count "
        "FROM ("
        "SELECT chain, block_number, from_address, to_address, value, "
        "if(from_address = %(address)s, to_address, from_address) AS counterparty "
        "FROM wallet_memory.evm_transfers "
        "WHERE (from_address = %(address)s OR to_address = %(address)s) "
        f"{chain_sql} "
        ") "
        "GROUP BY counterparty "
        f"ORDER BY last_seen_block DESC LIMIT {int(limit)}"
    )
    params: dict[str, Any] = {"address": wallet, **chain_params}
    return run(sql, params)
