#!/usr/bin/env python3
"""RPC endpoint probe & ingest pipeline.

Live-verifies RPC endpoints and emits ready-to-paste registry snippets for
BOTH consumers of the fleet's RPC data:

  1. lite-chain-indexer  lite_indexer/chains/endpoints.py  (GET_LOGS_ENDPOINTS)
  2. rugmunchbot         src/rugmunchbot/_vendor/scanner_engines/rpc_router.py
     (canonical: rmi-backend app/domains/scanner/engines/rpc_router.py)

Pipeline: CANDIDATE_ENDPOINTS (chains/endpoints.py) -> probe -> report ->
promote the survivors. Nothing is auto-promoted into the active registries
without a human-reviewed snippet paste — the indexer's history shows blind
additions get culled; this tool makes verification cheap instead.

Usage:
  # Probe all candidates (default) with getLogs compatibility check:
  python3 scripts/probe-rpc-endpoints.py

  # Probe specific chains only, skip the slower getLogs check:
  python3 scripts/probe-rpc-endpoints.py --chains ethereum,base --no-getlogs

  # Re-verify the ACTIVE pools (drift detection — providers die):
  python3 scripts/probe-rpc-endpoints.py --active

  # Emit promotion snippets from the last report:
  python3 scripts/probe-rpc-endpoints.py --emit-only

Output: /var/data/chain-indexer/rpc_probe_report.json (+ stdout summary).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx

# Make the package importable when run from a checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lite_indexer.chains.endpoints import (  # noqa: E402
    CANDIDATE_ENDPOINTS,
    EXPECTED_CHAIN_IDS,
    GET_LOGS_ENDPOINTS,
)
from lite_indexer.env import load_env_file  # noqa: E402

load_env_file()

REPORT_PATH = Path(os.getenv("RPC_PROBE_REPORT", "/var/data/chain-indexer/rpc_probe_report.json"))
PROBE_TIMEOUT = float(os.getenv("RPC_PROBE_TIMEOUT", "8.0"))
LATENCY_PROMOTE_MS = float(os.getenv("RPC_PROBE_MAX_LATENCY_MS", "1500"))
GETLOGS_RANGE = 10  # blocks back from tip for the topic-only getLogs check

async def _rpc(client: httpx.AsyncClient, url: str, method: str, params: list) -> tuple[dict | None, float, str]:
    """Single JSON-RPC POST. Returns (response_dict, latency_ms, error_class)."""
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    t0 = time.monotonic()
    try:
        resp = await client.post(url, json=body)
        latency = (time.monotonic() - t0) * 1000
        if resp.status_code == 429:
            return None, latency, "rate_limited"
        if resp.status_code != 200:
            return None, latency, f"http_{resp.status_code}"
        data = resp.json()
        if not isinstance(data, dict):
            return None, latency, "bad_payload"
        if "error" in data:
            return None, latency, f"rpc_error_{data['error'].get('code', '?')}"
        return data, latency, ""
    except httpx.TimeoutException:
        return None, (time.monotonic() - t0) * 1000, "timeout"
    except (httpx.HTTPError, ValueError, OSError) as exc:
        return None, (time.monotonic() - t0) * 1000, type(exc).__name__[:40]


async def probe_endpoint(client: httpx.AsyncClient, chain: str, url: str, do_getlogs: bool) -> dict:
    """Full probe: chainId identity + blockNumber liveness (+ getLogs shape)."""
    result: dict = {
        "chain": chain,
        "url": url,
        "chain_id_ok": False,
        "block_number_ok": False,
        "getlogs_ok": None,
        "latency_ms": None,
        "errors": [],
        "verdict": "reject",
    }

    cid, lat, err = await _rpc(client, url, "eth_chainId", [])
    if err or not cid:
        result["errors"].append(f"chainId:{err or 'empty'}")
        return result
    got_cid = cid.get("result")
    expected = EXPECTED_CHAIN_IDS.get(chain)
    result["chain_id_ok"] = bool(got_cid and expected and str(got_cid).lower() == expected)
    if not result["chain_id_ok"]:
        result["errors"].append(f"chainId_mismatch:got={got_cid}want={expected}")
        return result

    bn, lat, err = await _rpc(client, url, "eth_blockNumber", [])
    if err or not bn or bn.get("result") in (None, "0x"):
        result["errors"].append(f"blockNumber:{err or 'empty'}")
        return result
    result["block_number_ok"] = True
    result["latency_ms"] = round(lat, 1)
    tip = int(bn["result"], 16)

    if do_getlogs:
        gl, _lat, err = await _rpc(
            client,
            url,
            "eth_getLogs",
            [{"fromBlock": hex(max(0, tip - GETLOGS_RANGE)), "toBlock": "latest"}],
        )
        if gl is not None:
            result["getlogs_ok"] = True
        else:
            result["getlogs_ok"] = False
            result["errors"].append(f"getLogs:{err}")

    # Verdict: promote only fast, fully-compatible endpoints.
    if result["latency_ms"] > LATENCY_PROMOTE_MS:
        result["verdict"] = "slow"
    elif do_getlogs and result["getlogs_ok"] is False:
        result["verdict"] = "fallback_only"  # blockNumber/timestamp use only
    else:
        result["verdict"] = "promote"
    return result


async def run_probe(targets: dict[str, list[str]], do_getlogs: bool, concurrency: int = 12) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)

    async def guarded(chain: str, url: str) -> dict:
        async with sem:
            async with httpx.AsyncClient(timeout=PROBE_TIMEOUT) as client:
                return await probe_endpoint(client, chain, url, do_getlogs)

    tasks = [guarded(c, u) for c, urls in targets.items() for u in urls]
    return list(await asyncio.gather(*tasks))


def emit_snippets(results: list[dict]) -> str:
    """Render promotion snippets for both registry formats."""
    promoted = [r for r in results if r["verdict"] == "promote"]
    fallbacks = [r for r in results if r["verdict"] == "fallback_only"]
    lines: list[str] = [
        f"# ── Promoted by scripts/probe-rpc-endpoints.py {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} ──"
    ]

    by_chain: dict[str, list[dict]] = {}
    for r in promoted + fallbacks:
        by_chain.setdefault(r["chain"], []).append(r)

    for chain in sorted(by_chain):
        lines.append(f"\n# ── {chain} ──")
        for r in sorted(by_chain[chain], key=lambda x: x["latency_ms"] or 9e9):
            tag = "" if r["verdict"] == "promote" else "  # FALLBACK-ONLY (no topic-only getLogs)"
            lines.append(f"#   verified: {r['latency_ms']}ms{tag}")
            lines.append(f'#     indexer : "{r["url"]}",')
            lines.append(f'#     bot     : "{r["url"]}",')

    if not promoted and not fallbacks:
        lines.append("# (nothing qualified for promotion this run)")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chains", default="", help="comma-separated chain filter")
    ap.add_argument("--no-getlogs", action="store_true", help="skip eth_getLogs shape check")
    ap.add_argument("--active", action="store_true", help="re-verify ACTIVE pools (drift check)")
    ap.add_argument("--emit-only", action="store_true", help="re-render snippets from last report")
    args = ap.parse_args()

    if args.emit_only:
        if not REPORT_PATH.exists():
            print(f"no report at {REPORT_PATH}", file=sys.stderr)
            return 1
        results = json.loads(REPORT_PATH.read_text())["results"]
        print(emit_snippets(results))
        return 0
    chains = {c.strip() for c in args.chains.split(",") if c.strip()}
    if args.active:
        targets = {
            c: [u for u in urls if "${" not in u] for c, urls in GET_LOGS_ENDPOINTS.items() if not chains or c in chains
        }
    else:
        targets = {c: urls for c, urls in CANDIDATE_ENDPOINTS.items() if not chains or c in chains}
        # Dedup guard: never re-probe (or promote) something already active.
        for c in list(targets):
            targets[c] = [u for u in targets[c] if u not in set(GET_LOGS_ENDPOINTS.get(c, []))]
    total = sum(len(v) for v in targets.values())
    print(f"probing {total} endpoints across {len(targets)} chains (getlogs={'off' if args.no_getlogs else 'on'}) ...")

    results = asyncio.run(run_probe(targets, do_getlogs=not args.no_getlogs))

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "mode": "active" if args.active else "candidates",
                "results": results,
            },
            indent=2,
        )
    )

    counts: dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("verdicts:", counts)
    print(f"report: {REPORT_PATH}")
    print()
    print(emit_snippets(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
