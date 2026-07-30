# Task 1: Add new free public RPCs + API key placeholders

## Scope
Files: lite_indexer/chains.py, lite_indexer/rpc/tiers.py

## What to do

### A. chains.py — Add new endpoints with API key placeholders

Add these new free-tier RPC providers to GET_LOGS_ENDPOINTS. Use `${ENV_VAR}` placeholders for API keys:

**Ethereum** (currently 12 endpoints):
- Add Alchemy free: `"https://eth-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"` — insert as T2 after `eth.drpc.org`
- Add GetBlock free: `"https://eth.getblock.io/mainnet/?api_key=\${GETBLOCK_KEY}"` — insert as T3
- Add Chainstack: `"https://ethereum-mainnet.core.chainstack.com/\${CHAINSTACK_KEY}"` — insert as T3
- Add QuickNode: `"https://\${QUICKNODE_ENDPOINT}"` — insert as T3
- Add Moralis: `"https://ethereum-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"` — (Moralis uses Alchemy under the hood, so skip duplicate)

For chains where these providers also offer endpoints, add similar entries:
- **Base**: Add `"https://base-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"` 
- **BSC**: Add `"https://bsc-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"`
- **Polygon**: Add `"https://polygon-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"`
- **Arbitrum**: Add `"https://arb-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"`
- **Optimism**: Add `"https://opt-mainnet.g.alchemy.com/v2/\${ALCHEMY_KEY}"`

NOTE: These Alchemy endpoints already exist in FREEMIUM_ENDPOINTS in tiers.py, but we want them in the main GET_LOGS_ENDPOINTS too so they're always available (not gated by ALLOW_PAID_RPC flag). They just need the correct env var set.

### B. tiers.py — Expand FREEMIUM endpoints

Add Chainstack, GetBlock, QuickNode to FREEMIUM_ENDPOINTS for Ethereum:
```python
"ethereum": [
    "...existing...",
    "https://eth.getblock.io/mainnet/?api_key=${GETBLOCK_KEY}",
    "https://ethereum-mainnet.core.chainstack.com/${CHAINSTACK_KEY}",
    "https://${QUICKNODE_ENDPOINT}",
],
```

Add the same providers to PAID_ENDPOINTS if appropriate, copying from existing format.

### C. chains.py — PROVIDER_RATE_LIMITS + MAX_BLOCK_RANGE

Add rate limits for new providers:
```python
"alchemy.com": {"req_per_sec": 10, "burst": 20},
"getblock.io": {"req_per_sec": 10, "burst": 20},
"chainstack.com": {"req_per_sec": 10, "burst": 20},
```

Add MAX_BLOCK_RANGE for new providers:
```python
"alchemy.com": 10000,
"getblock.io": 5000,
"chainstack.com": 10000,
```

### D. Update summary counts in chains.py

Update the comment at the bottom:
- Update total endpoint counts
- Change "104 endpoints across 14 chains" to reflect new additions
- Change "Grand total: 110 endpoints across 16 chains" to reflect new total

### E. Update chains.py docstring header

Change "Total: 111 endpoints across 14 chains (69 new)" to reflect new counts.

## Constraints
- Use `${ENV_VAR}` placeholder format consistently
- Keep existing endpoints in their current order, insert new ones appropriately
- Comment tiers properly
- Do NOT delete any existing endpoints
- Verify the file still passes `python3 -c "from lite_indexer.chains import *"` after changes
