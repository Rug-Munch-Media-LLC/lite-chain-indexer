"""
NFT (ERC-721) Transfer tracker plugin — example.

Tracks Transfer events for a specific ERC-721 NFT contract.
Each transfer is written to ClickHouse via ``ctx.ch_writer``.

The manifest declares the contract address (Bored Ape Yacht Club on
Ethereum), the Transfer event signature (all 3 params indexed, common
for ERC-721), and a ClickHouse output schema with an explicit
``order_by`` key. On plugin load the indexer auto-creates the table.

Install::

    python -m lite_indexer.plugin.manager install --from examples/plugins/nft-transfers nft-transfers
"""

from __future__ import annotations

import contextlib
from typing import Any

from lite_indexer.plugin import BasePlugin, DecodedEvent, PluginContext


class NFTTransfersPlugin(BasePlugin):
    """Writes ERC-721 Transfer events to ClickHouse."""

    async def on_transfer(self, event: DecodedEvent, ctx: PluginContext) -> None:
        """Handle Transfer(address,address,uint256) events."""
        row: dict[str, Any] = {
            "sender": event.args.get("from", ""),
            "receiver": event.args.get("to", ""),
            "token_id": event.args.get("tokenId", 0),
            "_block_number": event.block_number,
            "_tx_hash": event.tx_hash,
            "_timestamp": "now()",
        }

        if ctx.logger is not None:
            ctx.logger.info(
                "nft_transfer",
                plugin=self.name,
                block=event.block_number,
                token_id=row["token_id"],
                receiver=str(row["receiver"])[:10],
            )

        # Write to ClickHouse via the provided writer handle.
        ch = ctx.ch_writer
        if ch is None:
            return

        client = ch.client()
        if client is None:
            return

        # Best-effort — never break the sync loop.
        with contextlib.suppress(Exception):
            client.execute(
                "INSERT INTO nft_transfers (sender, receiver, token_id, _block_number, _tx_hash, _timestamp) VALUES",
                [row],
            )
