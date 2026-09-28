"""
Uniswap V3 liquidity position plugin — example.

Demonstrates the plugin contract: subclass :class:`BasePlugin`, implement
the handler methods named in ``manifest.yaml``, and the indexer loop
dispatches decoded contract events to them.

Handlers receive a :class:`DecodedEvent` (with ``args`` mapped by the
manifest ``params``) and a :class:`PluginContext`. This example appends
each position change to ``<data_dir>/plugin-data/uniswap-v3-positions/
events.jsonl`` so the effect is visible without a database.

Install with::

    python -m lite_indexer.plugin.manager install --from examples/plugins/uniswap-v3-positions uniswap-v3-positions
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.plugin import BasePlugin, DecodedEvent, PluginContext

logger = get_logger(__name__)


class UniswapV3PositionsPlugin(BasePlugin):
    """Records Uniswap V3 liquidity position changes as JSONL rows."""

    async def on_increase_liquidity(self, event: DecodedEvent, ctx: PluginContext) -> None:
        """Handle ``IncreaseLiquidity`` events (manifest handler)."""
        await self._record(event, "increase", ctx)

    async def on_decrease_liquidity(self, event: DecodedEvent, ctx: PluginContext) -> None:
        """Handle ``DecreaseLiquidity`` events (manifest handler)."""
        await self._record(event, "decrease", ctx)

    async def _record(self, event: DecodedEvent, action: str, ctx: PluginContext) -> None:
        """Append one decoded position event to the plugin JSONL log."""
        row: dict[str, Any] = {
            "plugin": self.name,
            "action": action,
            "chain": event.chain or ctx.chain,
            "block_number": event.block_number,
            "tx_hash": event.tx_hash,
            "log_index": event.log_index,
            "contract": event.contract_address,
            "token_id": event.args.get("tokenId"),
            "owner": event.args.get("owner"),
            "amount0": event.args.get("amount0"),
            "amount1": event.args.get("amount1"),
        }

        out_dir = self._output_dir(ctx)
        out_dir.mkdir(parents=True, exist_ok=True)
        with (out_dir / "events.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, default=str) + "\n")

        logger.info(
            "plugin_event_recorded",
            plugin=self.name,
            action=action,
            block=event.block_number,
            tx=event.tx_hash[:18],
        )

    def _output_dir(self, ctx: PluginContext) -> Path:
        """Return the per-plugin data directory."""
        # bandit B108: example plugin fallback
        base = Path(ctx.data_dir) if ctx.data_dir else Path("/tmp")  # nosec B108
        return base / "plugin-data" / self.name
