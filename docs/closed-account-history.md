# Closed-Account History Recovery — Platform Capability Matrix

> **Purpose:** Recover transaction history for Solana accounts that have been
> **closed/deleted** (rent-reclaimed). Standard `getSignaturesForAddress`
> returns `[]` for these because the live signature index drops them.
> **Verified 2026-09-07** against the SOSANA/CRM investigation wallets.

## The Core Problem

When a Solana account is closed (owner executes close + reclaims rent),
`getSignaturesForAddress` returns **0 signatures** on every provider. The
history still exists in the ledger, but the *signature index* is purged.
You need an **archival / token-account-aware** method instead.

## The Verified Solution: Helius `getTransactionsForAddress`

Helius's enhanced method indexes **token-account activity**, so it retains
closed accounts. This is the ONLY method that recovered deleted-account
history in our live test.

### Endpoint
```
POST https://mainnet.helius-rpc.com/?api-key=<KEY>
Content-Type: application/json
```

### Request
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getTransactionsForAddress",
  "params": [
    "<ADDRESS>",
    {
      "transactionDetails": "full",
      "sortOrder": "asc",
      "limit": 1000,
      "maxSupportedTransactionVersion": 0,
      "filters": {
        "tokenAccounts": "all",
        "blockTime": { "gte": 1756000000 }
      }
    }
  ]
}
```

### Key parameter: `filters.tokenAccounts`
| Value | Behavior |
|-------|----------|
| `none` (default) | Only txs directly referencing the wallet — **misses closed accounts** |
| `balanceChanged` | Wallet OR token accounts it owns with balance changes (recommended) |
| `all` | Wallet OR **any token account it owns** — **recovers closed accounts** |

### Verified results (2026-09-07)
- `BKLBtcJQ...` (deleted, birth-suppression dumper): `getSignaturesForAddress`=0 → `getTransactionsForAddress tokenAccounts:all`=**10 txs** (back to 2025-08-28)
- `HLnpSz9h2S4hiLQ4...DaiTLcC` (whale, live EOA): `getTransactionsForAddress tokenAccounts:all`=**100 txs** (back to **2025-04-10**, the SHIFT AI launch window)

### Critical insight — the "deleted feeder" is a token account, not a wallet
The CRM feeder `HLnpSz9h2S4hiLQ4uN3vGYAHJqDmbFCwkx9prSXkdPMc` returns **0 sigs
even via `getTransactionsForAddress`** because it's a **token account (ATA)**
whose history is attributed to its **owner**, not the token account address.
Its owner is the whale `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC` (shares
the 16-char vanity prefix `HLnpSz9h2S4hiLQ4`). **To recover a deleted token
account's history, query the OWNER wallet with `tokenAccounts: all`.**

### Metering (Helius free tier)
- `transactionDetails: signatures` = 10 credits flat
- `transactionDetails: full` = 10 credits per 100 returned
- Failed/empty responses = free
- Mainnet = unlimited retention; Devnet = 2 weeks

### Limitations
- `tokenAccounts` filter does **not** cover pre-slot `111,491,819` (Dec 2022)
  — the `owner` field in token balance metadata didn't exist yet.
- One address per request (no multi-address batching).

## Other Platforms (status)

| Platform | Method | Closed-account? | Status |
|----------|--------|-----------------|--------|
| **Helius** | `getTransactionsForAddress` (tokenAccounts:all) | ✅ YES | **Verified working** |
| **Alchemy** | `getTokenAccountsByOwnerAtSlot` / account archive | ✅ YES | archived account state available on paid tiers |
| **Shyft** | `GET /sol/v1/transaction/history` | ❌ empty for closed | Key works, returns `[]` for deleted |
| **Dune** | `solana.transactions` (account_keys filter) | ✅ YES | `dune_api_key` valid; needs a saved query ID wired |
| **Bitquery** | `solana.transfers` GraphQL | ✅ YES (archival) | 402 — paid credits required |
| **getblock** | `getSignaturesForAddress` | ❌ | returns 0 for closed |
| **chainstack** | `getSignaturesForAddress` | ❌ | returns 0 for closed |

## RPC Pool Endpoints (free tier, in lite-chain-indexer)

The public pool ships keyless Solana endpoints only (official + public
gateways). Keyed/curated providers are supplied by the private runtime
overlay — see `docs/PROVIDER_OVERLAY.md`.

## Indexer Integration

`getTransactionsForAddress` should be added to
`lite_indexer/strategies/solana/rpc.py` as a dedicated method, alongside the
existing `getSignaturesForAddress`. The token-account-aware filter is what
makes closed-account backfill possible — the existing
`getSignaturesForAddress`-only path silently drops every closed account.

## Keys (secret store)

All provider keys live in the deployment secret store and are injected as
environment variables at runtime. No key value is stored in this repo or in
the endpoint overlay; the overlay references `${ENV_VAR}` placeholders only.
