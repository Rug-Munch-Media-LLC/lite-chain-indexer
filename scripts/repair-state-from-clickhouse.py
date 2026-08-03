#!/usr/bin/env python3
"""Repair LiteIndexer state files so every chain resumes from the last
block/slot actually present in ClickHouse.

Why: the 08/03 OOM crash loop corrupted state files (ethereum was reset
to block ~100 while ClickHouse had 25.6M, triggering a genesis backfill)
and silent ClickHouse write failures advanced bsc/solana/tron states past
the ClickHouse max (data gaps).

This sets each chain's state.last_block to the ClickHouse max so the
indexer resumes forward from durable data — no genesis backfill, and
re-fetches the missing tail for chains whose state ran ahead.

Usage:
    python3 scripts/repair-state-from-clickhouse.py [--dry-run]
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/data/chain-indexer"))
STATE_DIR = DATA_DIR / "state"
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")
CH_DB = os.environ.get("CH_DATABASE", "wallet_memory")

# Chains and the ClickHouse column/table that stores their max progress.
# evm_transfers uses block_number; solana uses slot on its own table.
CHAIN_QUERIES = {
    "evm": "SELECT max(block_number) FROM {db}.evm_transfers WHERE chain = %(chain)s",
    "solana": "SELECT max(slot) FROM {db}.solana_spl_balance_changes",
}

EVM_CHAINS = [
    "ethereum",
    "base",
    "bsc",
    "polygon",
    "arbitrum",
    "optimism",
    "avalanche",
    "linea",
    "scroll",
    "zksync",
    "mantle",
    "gnosis",
    "tron",
]


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    from clickhouse_driver import Client as CHClient

    client = CHClient(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database=CH_DB)

    repairs = 0
    for chain in EVM_CHAINS:
        ch_max = client.execute(CHAIN_QUERIES["evm"].format(db=CH_DB), {"chain": chain})[0][0]
        ch_max = ch_max or 0
        state_path = STATE_DIR / f"lite_{chain}.json"
        if not state_path.exists():
            print(f"{chain}: no state file, skip")
            continue
        data = json.loads(state_path.read_text())
        state_block = int(data.get("last_block", 0))
        if state_block == ch_max:
            print(f"{chain}: state={state_block} ch_max={ch_max} — ok")
            continue
        # Set state to CH max so the next cycle resumes from durable data.
        data["last_block"] = ch_max
        data["_repaired"] = True
        print(
            f"{chain}: state={state_block} -> {ch_max} "
            f"({'rewind to fill gap' if state_block > ch_max else 'advance to durable data'})"
        )
        if not dry_run:
            tmp = state_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2))
            os.replace(tmp, state_path)
        repairs += 1

    # Solana uses slot on its own table.
    solana_state = STATE_DIR / "lite_solana.json"
    ch_solana_max = client.execute(CHAIN_QUERIES["solana"].format(db=CH_DB))[0][0] or 0
    if solana_state.exists():
        data = json.loads(solana_state.read_text())
        state_block = int(data.get("last_block", 0))
        if state_block != ch_solana_max:
            print(
                f"solana: state={state_block} -> {ch_solana_max} "
                f"({'rewind to fill gap' if state_block > ch_solana_max else 'advance to durable data'})"
            )
            data["last_block"] = ch_solana_max
            data["_repaired"] = True
            if not dry_run:
                tmp = solana_state.with_suffix(".json.tmp")
                tmp.write_text(json.dumps(data, indent=2))
                os.replace(tmp, solana_state)
            repairs += 1
        else:
            print(f"solana: state={state_block} ch_max={ch_solana_max} — ok")

    print(f"\n{repairs} chain(s) repaired ({'DRY RUN' if dry_run else 'written'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
