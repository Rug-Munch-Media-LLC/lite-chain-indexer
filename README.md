# lite-chain-indexer

A multi-chain indexer that runs on free public RPCs: it backfills blocks and logs,
normalises transfers, and exports Parquet you can query with DuckDB — without a
paid node provider.

**Status:** beta · **Licence:** MIT, see [LICENSE](LICENSE).

Canonical: https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer

## Run it yourself

**Needs:** Python 3.11+, Postgres for indexed rows, and RPC endpoints you supply
(free tiers are the point of the project; no paid provider is required).
`bootstrap` additionally needs ClickHouse, `rclone` and reachability of a public
snapshot bucket.
**Minimal env:** `LCI_DATABASE_URL`; `CH_DATABASE` if you bootstrap or export.

```bash
git clone https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git
cd lite-chain-indexer
python3 -m venv .venv && . .venv/bin/activate
pip install -e .
export LCI_DATABASE_URL=postgresql://localhost/lci
lite-indexer --chain ethereum
```

Both console scripts are installed: `lite-indexer` (operator CLI) and `lci-mcp`
(the same data over MCP for agents).

To start from a published snapshot rather than genesis over free RPCs:

```bash
lite-indexer bootstrap --dry-run --all   # plan only, writes nothing
lite-indexer bootstrap --all
```

Check it works:

```bash
lite-indexer --help
```

That is the install check: it resolves the CLI and lists every command family
(`bootstrap`, `backfill`, `export-parquet`, `integrity-check`, `reconcile`,
`attest`, `query`, `lake-*`). The check that matters after a run needs ClickHouse:

```bash
lite-indexer integrity-check
```

It compares stored row counts against the ranges the indexer claims to have
fetched, so a silent gap shows up as a number instead of a missing feature later.

## What it needs

| Component | Role | Where it comes from |
|---|---|---|
| Postgres | normalised blocks, transfers, logs | your own; no container for it ships here |
| ClickHouse | bootstrap target, Parquet export, integrity counts | your own |
| RPC endpoints | every chain read | your own; free or paid, `config/production.example.toml` documents the budget shape |
| NATS | optional, only on the `compose.nats.yml` path | this repo |
| rmi-backend | not required | this repo runs standalone and calls nothing upstream |

`config/chain-states.yml` is resume state, not configuration — it is written at
runtime and should not be edited by hand.

## What it does

- **Backfill and follow.** `lite-indexer --chain <name> --from-block N` walks
  history; the daemon keeps following the head with slot rotation across chains
  across the endpoints you configure.
- **Normalised storage.** Transfers, blocks and logs land in Postgres in a schema
  shared across chains; `chain-states.yml` records per-chain progress.
- **Parquet export.** `lite-indexer export` writes partitioned Parquet, which is
  the intended read path for analytics — DuckDB over Parquet, not queries against
  the hot tables.
- **Integrity tooling.** `integrity check`, `reconcile`, `recover` and `attest`
  exist because free RPCs reorg and time out; a run that claims a block it never
  fetched is detectable.
- **MCP surface.** `lci-mcp` exposes query and state tools for agent use.

## How it is put together

```
lite_indexer/
  cli/            argparse surface, one _*_parsers module per command family
  adapters/       per-chain RPC adapters
  chains/         chain-specific quirks
  config/         TOML + chain state
  attestation.py  block-range manifests
  chunks.py       range planning
```

Command families: `bootstrap`, `export`, `integrity`, `backfill`, `query`,
`attest`, `lake`.

## Configuration

Configuration is environment variables read in `lite_indexer/config/`, with CLI
flags overriding them. `config/chain-states.yml` holds per-chain resume state and
is data, not config.

`config/production.example.toml` is a *documented example* of the per-provider RPC
budget shape — nothing reads it at runtime. Copy the relevant block into the
matching environment variable.

| Variable | Purpose |
|---|---|
| `RPC_PROVIDER_RPS`, `RPC_PROVIDER_MAX_CONCURRENCY` | per-provider ceilings |
| `RPC_PROVIDER_DAILY_BUDGET` | requests per UTC day (a count, not USD); 0 = unlimited |
| `CH_DATABASE`, `CH_*_TABLE` | ClickHouse analytics target and table names |
| `BACKFILL_MAX_CONCURRENCY`, `BACKFILL_MAX_ATTEMPTS` | backfill behaviour |
| `ATTESTATION_ENABLED`, `ATTESTATION_DIR` | block-range manifests |
| `TRACE_ENABLED`, `TRACE_CONCURRENCY` | trace fetching, off by default on most RPCs |

## Known limits

- Free RPCs rate-limit and drop ranges. Reorgs and gaps are expected; that is why
  the integrity commands exist.
- No trace/debug namespaces. This indexes logs and transfers, not internal calls.
- Postgres grows fast on high-volume chains. Export and prune, or run one chain
  per database.
- `chain-indexer` in the same organisation is a different, larger project — do not
  confuse the two.

## Licence

MIT.
