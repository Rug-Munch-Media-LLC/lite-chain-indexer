# Provider Overlay

`lite-chain-indexer` ships with a **free, keyless** endpoint pool: official
chain RPCs, [PublicNode](https://publicnode.com), and similar public
gateways. No API keys are required and nothing needs to be configured.

If you want to extend the pool with your own JSON-RPC providers (keyed
free tiers, dedicated nodes, private gateways), supply an **endpoint
overlay** at runtime. The overlay lives **outside this repository** and is
never committed — it is only URL templates, never secret values.

## How it works

Set `LITE_INDEXER_ENDPOINT_OVERLAY` to a JSON file path. Default:
`/srv/secrets/lite-indexer-endpoints.json`.

At import time the indexer:

1. loads the builtin free/keyless endpoints (free-first order), then
2. appends the overlay endpoints for each chain, in the order you list
   them, skipping duplicates, then
3. resolves `${ENV_VAR}` placeholders from the environment, dropping any
   endpoint whose variable is unset.

A missing or unreadable overlay is fine — the indexer silently falls back
to the free/keyless defaults. It never crashes.

## Format

```json
{
  "ethereum": {
    "endpoints": [
      "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"
    ],
    "notes": {
      "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}": "T4 — my provider"
    }
  },
  "solana": [
    "https://solana.example-rpc.com/?api-key=${YOUR_PROVIDER_KEY}"
  ]
}
```

- Keys are chain names (`ethereum`, `base`, `bsc`, `solana`, `tron`, …).
- `endpoints` is an **ordered** list — highest tier first. The indexer
  appends them after the builtin endpoints, so a curated archive provider
  you trust can be placed wherever your scoring expects it.
- `notes` is optional free-form metadata (tier annotations). It is not used
  for routing today; it documents the list.
- A bare list (`"solana": [ ... ]`) is shorthand for `{"endpoints": [...]}`.
- Put **placeholder names only** in the file (`${YOUR_PROVIDER_KEY}`). The
  actual key value belongs in your environment / secret store.

## Example: add any EIP-155 provider

```bash
# 1. Export the key (never commit it)
export YOUR_PROVIDER_KEY=...

# 2. Write the overlay outside the repo
cat > /srv/secrets/lite-indexer-endpoints.json <<'JSON'
{
  "ethereum": {
    "endpoints": [
      "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"
    ]
  }
}
JSON
chmod 600 /srv/secrets/lite-indexer-endpoints.json

# 3. Point the indexer at it and restart
export LITE_INDEXER_ENDPOINT_OVERLAY=/srv/secrets/lite-indexer-endpoints.json
```

That's it. Your provider is appended after the builtin free endpoints for
Ethereum. Remove the file (or unset the variable) and you are back to the
zero-key default with no code changes.

## Guarantees

- The overlay is read-only input; it cannot remove builtin endpoints.
- Duplicate URLs are de-duplicated, keeping the first occurrence.
- Unresolved `${VAR}` endpoints are dropped, never dialed as literals.
- The overlay file is never read from inside the repository; keep it in a
  secrets directory with mode `600`.

## Optional: provider quota seeds

Per-account quota degrade is opt-in. Seed provider budgets with a JSON map in
`LITE_INDEXER_QUOTA_SEEDS`, keyed by the endpoint's registrable domain:

```bash
export LITE_INDEXER_QUOTA_SEEDS='{"example-rpc.com":{"budget_units":30000000,"unit_type":"CU","period":"month"}}'
```

Unseeded providers are still tracked (and their per-key concurrency/rate caps
still apply) but never enter the degrade ladder.
