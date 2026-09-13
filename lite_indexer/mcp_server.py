"""Read-only MCP server over the Lite Chain Indexer's own data.

Strictly read-only; install via ``pip install "litechainindexer[mcp]"`` and run ``lci-mcp``.
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - exercised by the subprocess test
    raise SystemExit("fastmcp is required; install with: pip install 'litechainindexer[mcp]'") from exc

from lite_indexer import config as cfg
from lite_indexer.logging import redact_secrets

_ADDRESS_RE = re.compile(r"(?:0x[a-fA-F0-9]{40})|(?:[1-9A-HJ-NP-Za-km-z]{32,44})")


def _scrub(value: str) -> str:
    return str(redact_secrets(None, "mcp", {"value": value})["value"])


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _chains() -> list[str]:
    names: set[str] = set()
    for pattern in ("lite_*.json", "highwater_*.json"):
        prefix = pattern.split("*")[0]
        names.update(p.stem.removeprefix(prefix) for p in cfg.STATE_DIR.glob(pattern))
    return sorted(names)


def lci_chain_head(chain: str) -> dict[str, Any]:
    """Latest indexed block/slot for one chain plus seconds since the last advance."""
    name = chain.strip().lower()
    allowed = _chains()
    if name not in allowed:
        return {"status": "error", "error": f"unknown chain; configured: {', '.join(allowed) or 'none'}"}
    path = cfg.STATE_DIR / f"lite_{name}.json"
    state, high = _read_json(path), _read_json(cfg.STATE_DIR / f"highwater_{name}.json")
    stamp = state.get("last_timestamp")
    try:
        age: float | None = round(time.time() - datetime.fromisoformat(str(stamp)).timestamp(), 1)
    except (TypeError, ValueError):
        age = round(time.time() - path.stat().st_mtime, 1) if path.exists() else None
    return {
        "status": "ok",
        "chain": name,
        "last_block": state.get("last_block"),
        "highwater_block": high.get("last_block"),
        "last_timestamp": stamp,
        "seconds_since_advance": age,
    }


def lci_rpc_health() -> dict[str, Any]:
    """RPC endpoint health summary plus recent failures from the persisted throttle state (keys masked)."""
    throttle = _read_json(Path(cfg.RPC_THROTTLE_FILE))
    states = [s for s in throttle.values() if isinstance(s, dict)]
    errored: list[dict[str, Any]] = []
    for endpoint, state in throttle.items():
        if not isinstance(state, dict):
            continue
        status, hits = int(state.get("last_status") or 200), int(state.get("rate_limit_hits") or 0)
        if status < 400 and not state.get("in_backoff") and hits == 0:
            continue
        errored.append(
            {
                "endpoint": _scrub(str(endpoint)),
                "last_status": status,
                "rate_limit_hits": hits,
                "in_backoff": bool(state.get("in_backoff")),
            }
        )
    errored.sort(key=lambda e: (0 if e["in_backoff"] else 1, -int(e["rate_limit_hits"])))
    backoff = sum(1 for s in states if s.get("in_backoff"))
    limited = sum(1 for s in states if int(s.get("last_status") or 200) == 429)
    healthy = sum(1 for s in states if not s.get("in_backoff") and int(s.get("last_status") or 200) < 400)
    return {
        "status": "ok" if states else "unavailable",
        "endpoints": len(states),
        "healthy": healthy,
        "in_backoff": backoff,
        "rate_limited": limited,
        "errors": errored[:5],
    }


def lci_status() -> dict[str, Any]:
    """Chains configured, per-chain progress, quota summary and recent RPC errors."""
    chains = [lci_chain_head(n) for n in _chains()]
    path = Path(os.environ.get("QUOTA_LEDGER_FILE") or str(cfg.DATA_DIR / "quota_ledger.json"))
    entries = [e for e in _read_json(path).values() if isinstance(e, dict)]
    pcts = sorted((float(e.get("consumed_pct") or 0.0) for e in entries), reverse=True)
    quota = {
        "accounts": len(entries),
        "parked": sum(1 for e in entries if e.get("parked")),
        "max_degrade_level": max((int(e.get("degrade_level") or 0) for e in entries), default=0),
        "max_consumed_pct": pcts[0] if pcts else 0.0,
        "over_80pct": sum(1 for p in pcts if p > 80.0),
    }
    return {"status": "ok", "chains": chains, "quota": quota, "recent_errors": lci_rpc_health()["errors"]}


def _client() -> Any:
    from clickhouse_driver import Client

    env = os.getenv
    return Client(
        host=env("CH_HOST") or env("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(env("CH_PORT") or env("CLICKHOUSE_PORT") or "9000"),
        user=env("CH_USER") or env("CLICKHOUSE_USER", "default"),
        password=env("CH_PASSWORD") or env("CLICKHOUSE_PASSWORD") or "",
        database=env("CH_DATABASE") or env("CLICKHOUSE_DATABASE") or cfg.CH_DATABASE,
        connect_timeout=5,
        send_receive_timeout=15,
    )


def lci_recent_transfers(chain: str, address: str | None = None, limit: int = 20) -> dict[str, Any]:
    """Recent indexed transfer rows for a chain, optionally filtered by address (limit clamped 1-100)."""
    name = chain.strip().lower()
    allowed = _chains()
    if name not in allowed:
        return {"status": "error", "error": f"unknown chain; configured: {', '.join(allowed) or 'none'}"}
    if address is not None and not _ADDRESS_RE.fullmatch(address.strip()):
        return {"status": "error", "error": "invalid address"}
    count = max(1, min(int(limit), 100))
    params: dict[str, Any] = {"lim": count}
    if name == "solana":
        cols = "slot, signature, from_address, to_address, amount, mint, transfer_type, block_timestamp"
        table = os.environ.get("CH_SOLANA_TRANSFERS_TABLE", "wallet_memory.solana_transfers")
        sql, order = f"SELECT {cols} FROM {table}", "slot DESC"
    else:
        cols = (
            "chain, block_number, tx_hash, from_address, to_address, token_address, value, block_timestamp, log_index"
        )
        sql, order = f"SELECT {cols} FROM {cfg.CH_TABLE} WHERE chain = %(chain)s", "block_number DESC, log_index DESC"
        params["chain"] = name
    clause = ""
    if address is not None:
        clause = (" WHERE " if name == "solana" else " AND ") + "(from_address = %(addr)s OR to_address = %(addr)s)"
        params["addr"] = address.strip()
    query = f"{sql}{clause} ORDER BY {order} LIMIT %(lim)s"
    try:
        rows, columns = _client().execute(query, params, with_column_types=True)
    except Exception as exc:  # noqa: BLE001 - an unreachable DB must not crash the server
        return {"status": "unavailable", "error": _scrub(str(exc))[:160], "rows": []}
    names = [str(column[0]) for column in columns]
    return {"status": "ok", "limit": count, "rows": [dict(zip(names, row, strict=True)) for row in rows]}


def lci_attest_verify(manifest_path: str) -> dict[str, Any]:
    """Verify an attestation manifest's Merkle inclusion proof (when attestation is enabled)."""
    if not cfg.ATTESTATION_ENABLED:
        return {"status": "disabled", "error": "attestation is disabled (ATTESTATION_ENABLED=0)"}
    root = Path(os.environ.get("LITE_INDEXER_MCP_MANIFEST_ROOT") or str(cfg.ATTESTATION_DIR)).expanduser().resolve()
    path = Path(manifest_path).expanduser().resolve()
    if not path.is_file() or root not in path.parents:
        return {"status": "error", "error": "manifest_path must be a file under the attestation directory"}
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "error": str(exc)[:160]}
    from lite_indexer.attestation import verify_manifest

    valid = verify_manifest(manifest, expected_root=os.environ.get("LITE_INDEXER_MCP_EXPECTED_ROOT") or None)
    return {
        "status": "ok" if valid else "invalid",
        "chain": manifest.get("chain"),
        "from_block": manifest.get("from_block"),
        "to_block": manifest.get("to_block"),
        "root": manifest.get("root"),
    }


mcp = FastMCP("lite-chain-indexer")
for _tool in (lci_status, lci_chain_head, lci_recent_transfers, lci_rpc_health, lci_attest_verify):
    mcp.add_tool(_tool)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
