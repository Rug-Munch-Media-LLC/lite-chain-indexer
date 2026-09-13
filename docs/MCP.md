# MCP Server (read-only)

`lite_indexer.mcp_server` is a minimal [FastMCP](https://github.com/jlowin/fastmcp)
stdio server that lets any MCP client (Claude Desktop, opencode, ...) inspect a
running Lite Chain Indexer without shell access. It is **read-only**: every tool
reads the state directory and the indexer's ClickHouse tables; nothing is written.

## Install

The server is an optional extra — it is not a core dependency:

```bash
pip install "litechainindexer[mcp]"
```

## Run

```bash
lci-mcp                       # console script
python -m lite_indexer.mcp_server   # equivalent
```

The server speaks MCP over stdio and reads the same environment as the daemon
(`CH_*` / `CLICKHOUSE_*`, the shared env file, `DATA_DIR`, `RPC_THROTTLE_FILE`,
`QUOTA_LEDGER_FILE`, `ATTESTATION_ENABLED`).

## Client configuration

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "lite-chain-indexer": {
      "command": "lci-mcp",
      "env": { "DATA_DIR": "/var/data/chain-indexer" }
    }
  }
}
```

opencode (`opencode.jsonc`):

```jsonc
{
  "mcp": {
    "lite-chain-indexer": { "type": "local", "command": ["lci-mcp"], "enabled": true }
  }
}
```

## Tools

| Tool | Returns |
|------|---------|
| `lci_status()` | Configured chains with last block/slot, seconds since advance, aggregate quota usage, recent endpoint failures. |
| `lci_chain_head(chain)` | Latest indexed block/slot plus seconds since the last advance for one chain. |
| `lci_recent_transfers(chain, address?, limit=20)` | Recent indexed transfer rows (EVM/Tron: `evm_transfers`; Solana: `solana_transfers`), newest first. `limit` is clamped to 1–100 and `address` is validated. |
| `lci_rpc_health()` | Endpoint health summary (healthy / backing off / rate-limited) and recent failures from the persisted throttle state. |
| `lci_attest_verify(manifest_path)` | Verifies an attestation manifest's Merkle inclusion proof when attestation is enabled. |

A chain is considered "configured" when it owns a `lite_<chain>.json` or
`highwater_<chain>.json` file in the state directory, so the allow-list always
matches what the daemon actually writes.

## Safety

- **Read-only.** No tool writes state, inserts rows, or triggers RPC calls.
- **No secrets.** Endpoint URLs pass through the indexer's shared redaction
  processor before leaving the process; API keys in query params, userinfo, and
  key-like path segments are masked. Quota output reports counts only — never
  account or key identifiers.
- **Bounded inputs.** `limit` is clamped, `chain` is checked against the
  configured allow-list, `address` must match an EVM or base58 shape, and
  `manifest_path` must resolve to a file under the attestation directory.
- **Graceful degradation.** An unreachable ClickHouse or missing throttle file
  returns a `status` object instead of raising.

## Environment

| Variable | Purpose |
|----------|---------|
| `CH_HOST` / `CLICKHOUSE_HOST` | ClickHouse host (default `127.0.0.1`). |
| `CH_PORT` / `CLICKHOUSE_PORT` | Native protocol port (default `9000`). |
| `CH_USER` / `CH_PASSWORD` | ClickHouse credentials. |
| `CH_DATABASE` / `CLICKHOUSE_DATABASE` | Database (default `wallet_memory`). |
| `DATA_DIR` | Root of the indexer's state + ledger files. |
| `RPC_THROTTLE_FILE` | Persisted throttle/health state. |
| `QUOTA_LEDGER_FILE` | Persisted quota ledger. |
| `ATTESTATION_ENABLED` | Enables `lci_attest_verify` when not `0`. |
| `LITE_INDEXER_MCP_MANIFEST_ROOT` | Optional override for the allowed manifest root. |
| `LITE_INDEXER_MCP_EXPECTED_ROOT` | Optional trusted Merkle root for verification. |

## Tests

```bash
python3 -m pytest tests/ -q -k "mcp or status or health or transfer" -o addopts=""
```
