"""Response models for the wallet-graph endpoints.

Kept out of ``api/schemas.py`` on purpose: that module is grandfathered at
302 lines and may not grow (shrink-only size ratchet). New surfaces get
their own model module.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class FunderLink(BaseModel):
    """One hop in a wallet's funding chain.

    ``funder`` is the address that sent the wallet its earliest inbound
    indexed transfer. This is a HEURISTIC — see
    ``lite_indexer.query.wallet_graph`` for the classes of wallet where the
    earliest ERC-20 inbound is not the true source of funds (contract
    deployers, bridge recipients, multisigs, and any wallet whose first
    funding was an internal/CPI transfer invisible to ``eth_getLogs``).
    """

    address: str = Field(description="The wallet being traced")
    chain: str
    funder: str = Field(description="Sender of the earliest inbound transfer (empty when unfunded)")
    funder_tx_hash: str = ""
    funder_block_number: int = 0
    funder_block_timestamp: str = ""
    funder_token_address: str = ""
    funder_value: str = Field(default="0", description="Raw token amount of that transfer, decimal string")
    depth: int = Field(default=1, description="Hop distance from the queried address (0 = itself)")
    found: bool = Field(default=False, description="False when the wallet has no inbound transfer")
    heuristic: str = Field(
        default="earliest_inbound_evm_transfer",
        description="Which rule produced this result — always a heuristic, never a proof",
    )


class Counterparty(BaseModel):
    """A distinct address a wallet has transacted with, in either direction.

    ``out_volume``/``in_volume`` are RAW token amounts summed per direction.
    They are not USD, not price-adjusted, and mix tokens when the wallet
    trades several — compare magnitude only within one token.
    """

    counterparty: str
    chain: str
    first_seen_block: int
    last_seen_block: int
    out_volume: str = Field(default="0", description="Raw tokens sent wallet -> counterparty")
    in_volume: str = Field(default="0", description="Raw tokens sent counterparty -> wallet")
    transfer_count: int = 0
