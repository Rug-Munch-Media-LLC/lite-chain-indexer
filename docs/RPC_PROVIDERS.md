# RPC Providers — Free-First Fallbacks & Freemium Recommendations

> Lite Chain Indexer runs on **keyless public endpoints out of the box**.
> Keyed freemium providers are the recommended upgrade for heavier indexing —
> better rate limits and getLogs depth, still free at moderate volume.

## How keys work (and why ours are safe)

Every keyed endpoint in this repo references its key as a `${ENV_VAR}`
placeholder (e.g. `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`),
resolved via `os.path.expandvars` at dial time. Unresolved placeholders are
**skipped by the endpoint verifier** — a fresh clone with zero env vars still
indexes fine on the public pool. The maintainers' production keys live only
in the deployment environment; they never enter the tree (enforced by test).

## Recommended freemium fallbacks

Set any of these to upgrade a chain's pool automatically:

| Provider | Env var | Free tier | Chains | Notes |
|---|---|---|---|---|
| Alchemy | `ALCHEMY_KEY` | 30M CU/mo | 90+ networks | Archive + debug traces; best EVM dev depth |
| dRPC | `DRPC_KEY` | 210M CU/30d | 115 chains / 203 networks | Load-balanced across 60+ providers |
| Ankr | `ANKR_KEY` | 200M credits/mo | 80+ networks | Broadest multi-chain; cheapest PAYG |
| BlockPI | `BLOCKPI_KEY` | 50M RU/mo | 40+ chains | Deepest free multi-chain tier after Ankr/dRPC |
| QuickNode | `QUICKNODE_KEY` | 10M credits/mo | 83+ chains | Add-on marketplace; Solana + Bitcoin |
| Helius | `HELIUS_KEY` | 1M credits/mo | Solana | LaserStream gRPC, DAS API, stake-weighted QoS |
| Chainstack | Per-node URL env (`CHAINSTACK_SOLANA_URL`, `CHAINSTACK_TRON_URL`) | 3M req/mo | Major EVMs + Solana + Tron | Per-node full URLs (may use HTTP Basic auth), not a single key |
| Tatum | `TATUM_KEY` | 500k credits/mo | 130+ chains | Unified multi-chain API |
| NOWNodes | `NOWNODES_KEY` | 20k req/mo | 100+ networks | Explorers included |
| GetBlock | `GETBLOCK_KEY` | 40k trial req | 50+ chains | Dedicated-node options |

## Support the project — referral signups

The providers below pay **revenue share** when you sign up through our links
at no extra cost to you. Links fund continued OSS development.

| Provider | Published terms | Signup |
|---|---|---|
| QuickNode | 50% of first month (credits); 15% recurring on dev subs | `SET_ME` |
| Chainstack | 10% of referred purchases for 1 year (credits) | `SET_ME` |
| GetBlock | 1–15% cash (deal-size dependent) | `SET_ME` |
| NOWNodes | 5% cash or 10% credits — your choice | `SET_ME` |
| Tatum | up to 15% cash | `SET_ME` |
| Alchemy | developer referral program (credits) | `SET_ME` |

Ankr, dRPC, BlockPI, and Helius have no public self-serve affiliate program
at verification time (2026-08) — we recommend them on merit alone.

**Maintainers:** drop your referral URLs into
`lite_indexer/chains/rpc_registry.py` (`signup_url="SET_ME"` slots) — the
registry is the single source of truth and this table mirrors it. Program
terms verified 2026-08 against public program pages; percentages may change.

## Tiered pool design

```
T1-T3  keyless public endpoints   ← works with zero configuration
T4     keyed freemium (${ENV})    ← auto-joins the pool when env var set
       resolution: os.path.expandvars → unresolved = skipped by verifier
```

See `chains/endpoints.py` for the per-chain pools and
`chains/rpc_registry.py` for the provider registry (programmatic access:
`signup_table()`, `providers_with_referrals()`).
