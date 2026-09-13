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
