"""
ERC-20 Transfer tracker plugin — example.

Tracks Transfer events for a specific ERC-20 token contract.
Each transfer is written to ClickHouse via ``ctx.ch_writer``.

The manifest declares the contract address (USDC on Ethereum), the
Transfer event signature, and a ClickHouse output schema. On plugin
load the indexer auto-creates the table from the schema.

Install::

    python -m lite_indexer.plugin.manager install --from examples/plugins/erc20-tracker erc20-tracker
"""

from __future__ import annotations

from typing import Any

from lite_indexer.plugin import BasePlugin, DecodedEvent, PluginContext


class ERC20TrackerPlugin(BasePlugin):
    """Writes ERC-20 Transfer events to ClickHouse."""

    async def on_transfer(self, event: DecodedEvent, ctx: PluginContext) -> None:
        """Handle Transfer(address,address,uint256) events."""
        row: dict[str, Any] = {
            "sender": event.args.get("from", ""),
            "receiver": event.args.get("to", ""),
            "value": event.args.get("value", 0),
            "_block_number": event.block_number,
            "_tx_hash": event.tx_hash,
            "_timestamp": "now()",
        }

        if ctx.logger is not None:
            ctx.logger.info(
                "erc20_transfer",
                plugin=self.name,
                block=event.block_number,
                sender=str(row["sender"])[:10],
                value=str(row["value"]),
            )

        # Write to ClickHouse via the provided writer handle.
        ch = ctx.ch_writer
        if ch is None:
            return

        client = ch.client()
        if client is None:
            return

        try:
            client.execute(
                "INSERT INTO erc20_transfers (sender, receiver, value, _block_number, _tx_hash, _timestamp) VALUES",
                [row],
            )
        except Exception:
            # Best-effort — never break the sync loop.
            pass
