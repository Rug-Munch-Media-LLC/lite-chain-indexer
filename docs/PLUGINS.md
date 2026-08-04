# Plugins — extending LiteChainIndexer without forking

LiteChainIndexer's **plugin system** lets you index custom contracts and
events without modifying core code. A plugin is a small folder declaring
which contracts/events it cares about (a `manifest.yaml`) and a Python
module extending [`BasePlugin`](../lite_indexer/plugin/base.py). The
indexer loop decodes matching events and dispatches them to your handler
methods.

This is the extensibility story behind the "plugin system" row in the
[competitive comparison](COMPARISON.md) — the same capability competitors
sell as "custom handlers" (Ponder/Subsquid indexer files, The Graph
subgraph mappings), without a TypeScript toolchain or a redeploy pipeline.

---

## What a plugin can do

- **Decode events** for any contract address on a supported chain
  (ERC-20-style `Transfer`, Uniswap V2/V3 pair events, custom ABI events).
- **React per event** with an async handler that receives the decoded
  arguments (`DecodedEvent.args`) and a `PluginContext` (chain, block,
  data dir, ClickHouse writer, logger).
- **Persist data** to JSONL, ClickHouse, or anywhere else the handler
  chooses — the plugin owns its output.
- **Replay history** — the indexer dispatches historical events during
  backfill, so a plugin sees past activity for its declared contracts.

## What a plugin cannot do

- It cannot change the core indexing loop, RPC pool, or storage layer.
  That keeps plugins safe: a broken plugin fails one contract, not the
  whole indexer.
- It cannot access secrets or the RPC endpoints directly — the plugin
  contract deliberately exposes a narrow, typed surface.

---

## Anatomy of a plugin

```
my-plugin/
├── manifest.yaml        # what to index (contracts + events)  [required]
└── plugin.py            # handler implementations             [required]
```

### manifest.yaml

```yaml
name: my-plugin            # unique plugin id (used for install/enable)
version: 1.0.0
author: You
description: What this plugin does

chain: ethereum            # chain this plugin targets
contracts:
  - address: "0x..."
    start_block: 12345678  # replay from this block (backfill)

events:
  - name: Transfer         # event name (for docs/logging)
    signature: "Transfer(address,address,uint256)"   # canonical ABI signature
    params: [from, to, value]                        # decoded arg names
    indexed: [0, 1]                                  # which params are indexed
    handler: on_transfer   # method name on your plugin class
```

### plugin.py

```python
from lite_indexer.plugin import BasePlugin, DecodedEvent, PluginContext


class MyPlugin(BasePlugin):
    """One async method per event handler declared in the manifest."""

    async def on_transfer(self, event: DecodedEvent, ctx: PluginContext) -> None:
        # event.args holds decoded params keyed by the manifest `params`.
        sender = event.args["from"]
        receiver = event.args["to"]
        value = event.args["value"]
        # Do something with them — log, write JSONL, insert into ClickHouse.
        self.logger.info("transfer_seen", sender=sender, value=value)
```

Handlers are `async def` methods whose name matches the manifest
`handler:` field. They receive:

| Argument | Type | Contents |
|----------|------|----------|
| `event` | `DecodedEvent` | `chain`, `block_number`, `tx_hash`, `log_index`, `contract_address`, `args` (decoded params) |
| `ctx` | `PluginContext` | `chain`, `block_number`, `data_dir`, ClickHouse writer handle, logger |

The working example in this repo is
[`examples/plugins/uniswap-v3-positions/`](../examples/plugins/uniswap-v3-positions/)
— a complete Uniswap V3 liquidity-position tracker that writes JSONL.

---

## Install, list, enable, disable

Plugins are managed with the `lite_indexer.plugin.manager` CLI:

```bash
# Install from a local folder (copies into the plugins dir)
python -m lite_indexer.plugin.manager install --from examples/plugins/uniswap-v3-positions uniswap-v3-positions

# Install from the community registry (if published)
python -m lite_indexer.plugin.manager install uniswap-v3-positions

# List installed plugins and their status
python -m lite_indexer.plugin.manager list

# Enable / disable without uninstalling
python -m lite_indexer.plugin.manager enable uniswap-v3-positions
python -m lite_indexer.plugin.manager disable uniswap-v3-positions
```

A plugin is picked up on the next indexer start — plugins are loaded
once per chain when the sync loop begins, so after installing/enabling a
plugin restart the service to activate it:

```bash
systemctl restart lite-indexer
journalctl -u lite-indexer | grep plugin_loaded   # confirm dispatch
```

Plugin loading is gated by two environment variables (see
[`lite_indexer/config.py`](../lite_indexer/config.py)):

| Env | Default | Meaning |
|-----|---------|---------|
| `LITE_INDEXER_PLUGIN_DIR` | `<data_dir>/plugins` | Where plugins live |
| `PLUGIN_ENABLED` | `1` | Set `0` to disable the plugin layer entirely |

Load failures never break indexing: the loader skips invalid plugins
with a warning and the sync loop continues unchanged.

## Plugin discovery & community plugins

Plugins installed from a folder are local to your deployment. The plugin
registry module (`lite_indexer.plugin.registry`) supports a community
registry: `community_plugins()` lists community entries and
`find_community(name)` resolves one. Publishing a community plugin is a
manifest + code + metadata JSON in the registry store — see
`lite_indexer/plugin/registry.py` for the schema.

---

## Writing a plugin that survives upgrades

1. **Use only the public plugin API** (`from lite_indexer.plugin import ...`).
   Everything else in `lite_indexer` is internal and may change.
2. **Keep handlers idempotent** — the indexer may replay ranges after a
   reorg, so your handler should tolerate seeing the same event twice.
3. **Fail closed, not loud** — wrap persistence in try/except and log;
   an exception in one handler is isolated to that plugin.
4. **Version your manifest** — bump `version:` when the schema changes so
   operators can track which plugins are active on which indexer.
