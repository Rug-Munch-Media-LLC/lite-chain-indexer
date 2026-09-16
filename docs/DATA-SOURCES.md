# Data Sources — bring your own (a few examples)

LiteChainIndexer ships **no curated source matrix and no data-source
secrets** — that's deliberate. The engine is the gift; *which* data you
hook up is your choice. Extend it three ways:

- **Endpoint overlay** — add your own JSON-RPC endpoints; keys stay in a
  file outside the repo. See [PROVIDER_OVERLAY.md](PROVIDER_OVERLAY.md).
- **Plugins** — decode/index any contract or event without forking core.
  See [PLUGINS.md](PLUGINS.md).
- **Query-time sources** — add on-demand fetchers under
  `lite_indexer/query/`; the router falls through to them when local data
  can't answer a query.

## Examples people commonly hook up (public, keyless to start)

| Source | What it gives you | Where to look |
|---|---|---|
| Public RPC pools | live blocks, logs, receipts | publicnode.com, official chain RPCs |
| AWS Public Blockchain | bulk Ethereum + Bitcoin history (incl. traces) | `s3://aws-public-blockchain/v1.0` |
| A public portal | wide historical datasets, many chains | your portal of choice (SQD, Envio, …) |
| Your own node | anything you already run | your RPC URL via overlay |

A few examples only — the full playbook is not shipped. Pick what fits
your coverage and budget, mix freely.
