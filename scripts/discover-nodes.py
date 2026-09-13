#!/usr/bin/env python3
"""Node auto-discovery — turns deployed nodes into RPC endpoints.

Queries a node-platform API for every running node in the account and emits
ready-to-use endpoint URLs (``{network}.<platform-host>/{auth_key}`` or
``{node-id}.<platform-host>/{auth_key}``), grouped by chain, with paste-ready
snippets for:

  * lite-chain-indexer  lite_indexer/chains/endpoints.py
  * rugmunchbot         rpc_router (via rmi-backend canonical)

The platform is provider-neutral: point ``NODE_PLATFORM_API`` at your node
platform's ``/nodes/`` collection URL and supply its key via
``NODE_PLATFORM_API_KEY``. Provider hosts, keys and tiers stay out of the
public tree.

Usage:
    NODE_PLATFORM_API=https://api.example-rpc-platform.com/v1/nodes/ \\
    NODE_PLATFORM_API_KEY=... python3 scripts/discover-nodes.py
    # or after storing in the secrets manager:
    secrets-manager show node_platform_api_key | \\
        python3 scripts/discover-nodes.py --stdin
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

# Default placeholder; override with NODE_PLATFORM_API for your provider.
DEFAULT_API = "https://api.example-rpc-platform.com/v1/nodes/"

# Platform network slug -> our registry chain name.
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


def _api_url() -> str:
    return os.environ.get("NODE_PLATFORM_API", DEFAULT_API)


def fetch_nodes(api: str, key: str) -> list[dict]:
    req = urllib.request.Request(api, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"})
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

    key = sys.stdin.readline().strip() if args.stdin else os.getenv("NODE_PLATFORM_API_KEY", "")
    if not key:
        print("no API key (set NODE_PLATFORM_API_KEY or use --stdin)", file=sys.stderr)
        return 1

    try:
        nodes = fetch_nodes(_api_url(), key)
    except Exception as exc:  # noqa: BLE001
        print(f"platform API failed: {exc}", file=sys.stderr)
        return 1

    running = [n for n in nodes if n.get("status") == "running"]
    print(f"org nodes: {len(nodes)} total, {len(running)} running\n")

    by_chain: dict[str, list[str]] = {}
    for node in running:
        # The API returns network as id (NW-...); use the node endpoint host
        # to infer the chain instead.
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
        print("No running nodes yet.\nDeploy shared nodes in your node-platform console, then re-run this script.")
        return 0

    print("# ── Paste-ready endpoints ──")
    for chain in sorted(by_chain):
        print(f"\n# ── {chain} ──")
        for ep in by_chain[chain]:
            print(f'    "{ep}",')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
