# RPC Providers

> This document moved. The public repository ships a **free, keyless**
> endpoint pool plus a generic extension point; curated/keyed provider
> lists are private and supplied at runtime.

Lite Chain Indexer runs on **keyless public endpoints out of the box**:
official chain RPCs, [PublicNode](https://publicnode.com), and similar
public gateways. No API keys are required and no provider list is baked
into the tree.

## Extending the pool

Bring your own JSON-RPC providers (free tiers, dedicated nodes, private
gateways) via a runtime **endpoint overlay** kept outside the repository.
See [`PROVIDER_OVERLAY.md`](PROVIDER_OVERLAY.md) for the format and a
generic example.

## How placeholders work

Any keyed endpoint you add references its key as a `${ENV_VAR}`
placeholder, resolved via `os.path.expandvars` at import time. Endpoints
whose variable is unset are excluded from the active pool — a fresh clone
with zero env vars still indexes fine on the public pool. Secret values
never enter the repository or the overlay file.

## Capacity accounting: quota is per ACCOUNT, not per key

Most keyed free tiers meter their free **quota** (compute units / credits /
requests) per **account**; extra keys on the same account add *rate*
(rps/concurrency), never quota. The quota ledger therefore groups usage by
an `account_id` that defaults to the provider id — all keys of one provider
draw **one** budget, so capacity is never over-counted N×. If you genuinely
hold several accounts on one provider, name them with a JSON map in
`LITE_INDEXER_ACCOUNT_IDS` (`{"<provider>": "<account>"}` or
`{"<key-id>": "<account>"}`); per-key rate limits stay separate.

## Proxy routing: key-less only

Key-less public endpoints are limited **per egress IP**, so a residential
proxy pool is the right lever for them. Keyed endpoints meter per
account/key, so proxying them adds latency and burns GB for no quota gain.
`RPC_PROXY_KEYLESS_ONLY=1` (with `RPC_PROXY_ENABLED=1`) routes key-less
endpoints through the pool while keyed endpoints stay direct-first, using a
proxy only after a transport failure. Default is off — behaviour is
unchanged until explicitly enabled.

