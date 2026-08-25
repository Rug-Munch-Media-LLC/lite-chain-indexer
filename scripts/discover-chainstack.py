#!/usr/bin/env python3
"""Chainstack node auto-discovery — turns deployed nodes into RPC endpoints.

Queries the Chainstack platform API for every running node in the org and
emits ready-to-use endpoint URLs ({network}.core.chainstack.com/{auth_key}
or {node-id}.p2pify.com/{auth_key}), grouped by chain, with paste-ready
snippets for:

  * lite-chain-indexer  lite_indexer/chains/endpoints.py
  * rugmunchbot         rpc_router (via rmi-backend canonical)

Usage:
    CHAINSTACK_API_KEY=... python3 scripts/discover-chainstack.py
    # or after storing in gopass:
    secrets-manager show chainstack_api_key | \
        python3 scripts/discover-chainstack.py --stdin
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

API = "https://api.chainstack.com/v1/nodes/"

# Chainstack network slug -> our registry chain name.
NETWORK_MAP = {
    "ethereum-mainnet": "ethereum",
    "base-mainnet": "base",
    "bsc-mainnet": "bsc",
    "polygon-mainnet": "polygon",
    "arbitrum-mainnet": "arbitrum",
    "optimism-mainnet": "optimism",
    "avalanche-mainnet": "avalanche",
    "fantom-mainnet": "fantom",
    "linea-mainnet": "linea",
    "scroll-mainnet": "scroll",
    "zksync-mainnet": "zksync",
    "mantle-mainnet": "mantle",
    "gnosis-mainnet": "gnosis",
    "celo-mainnet": "celo",
    "solana-mainnet": "solana",
}


def fetch_nodes(key: str) -> list[dict]:
    req = urllib.request.Request(API, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.load(resp)
    return list(data.get("results", []))


def endpoint_for(node: dict) -> str | None:
    details = node.get("details") or {}
    https = details.get("https_endpoint")
    auth_key = details.get("auth_key")
    if not https or not auth_key:
        return None
    return f"{https.rstrip('/')}/{auth_key}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stdin", action="store_true", help="read API key from stdin")
    args = ap.parse_args()

    key = sys.stdin.readline().strip() if args.stdin else os.getenv("CHAINSTACK_API_KEY", "")
    if not key:
        print("no API key (set CHAINSTACK_API_KEY or use --stdin)", file=sys.stderr)
        return 1

    try:
        nodes = fetch_nodes(key)
    except Exception as exc:  # noqa: BLE001
        print(f"platform API failed: {exc}", file=sys.stderr)
        return 1

    running = [n for n in nodes if n.get("status") == "running"]
    print(f"org nodes: {len(nodes)} total, {len(running)} running\n")

    by_chain: dict[str, list[str]] = {}
    for node in running:
        network = (
            (node.get("network") or {}).get("name", "")
            if isinstance(node.get("network"), dict)
            else str(node.get("network") or "")
        )
        # The API returns network as id (NW-...); use the node name/endpoint
        # host to infer the chain instead.
        ep = endpoint_for(node)
        if not ep:
            continue
        host = ep.split("//", 1)[-1].split("/", 1)[0]
        slug = host.split(".")[0]
        chain = NETWORK_MAP.get(slug)
        if chain is None:
            print(f"  ? unmapped network host: {host} ({node.get('name')})")
            continue
        by_chain.setdefault(chain, []).append(ep)

    if not by_chain:
        print(
            "No running nodes yet.\n"
            "Deploy shared nodes in console.chainstack.com:\n"
            "  Projects -> + Create project -> Deploy node ->\n"
            "  pick Solana Mainnet / Ethereum / Base / ... (Shared, Developer plan)\n"
            "Then re-run this script."
        )
        return 0

    print("# ── Paste-ready endpoints ──")
    for chain in sorted(by_chain):
        print(f"\n# ── {chain} ──")
        for ep in by_chain[chain]:
            print(f'    "{ep}",')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
