"""The persisted capability record for one RPC endpoint.

Split out of capabilities.py (2026-10-24). The dataclass and its JSON payload
codec are unchanged; the registry that stores them lives in
capability_registry.py.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class CapabilityRecord:
    """Persisted capability probe results for one RPC endpoint.

    Attributes:
        endpoint: RPC endpoint URL.
        chain: Chain name the endpoint serves.
        archive: True when the node serves genesis-era state.
        getlogs_max_range: Largest eth_getLogs block range the endpoint
            accepted during probing (0 when nothing worked).
        block_receipts: True when eth_getBlockReceipts is supported.
        cu_tariff: Per-method compute-unit weights (seeded).
        probed_at: Epoch seconds of the last probe.
    """

    endpoint: str
    chain: str
    archive: bool = False
    getlogs_max_range: int = 0
    block_receipts: bool = False
    cu_tariff: dict[str, float] = field(default_factory=dict)
    probed_at: float = 0.0
    # Broad tip-range eth_getLogs capability: True = serves topic-only
    # getLogs (the indexer's core workload), False = CANNOT_SERVE_LOGS
    # (permanent capability refusal), None = never probed / transient.
    # A False endpoint may still serve eth_blockNumber (tip/head-only) but
    # must NEVER be selected for eth_getLogs.
    logs_capable: bool | None = None


def _record_payload(record: CapabilityRecord) -> str:
    """JSON payload for one capability record (secret-free: URL + verdicts)."""
    return json.dumps(asdict(record), separators=(",", ":"), sort_keys=True)


def _record_from_payload(endpoint: str, text: str) -> CapabilityRecord | None:
    """Decode a persisted capability payload; None when malformed."""
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return CapabilityRecord(
            endpoint=endpoint,
            chain=str(data.get("chain", "")),
            archive=bool(data.get("archive", False)),
            getlogs_max_range=int(data.get("getlogs_max_range", 0) or 0),
            block_receipts=bool(data.get("block_receipts", False)),
            cu_tariff={str(k): float(v) for k, v in (data.get("cu_tariff") or {}).items()},
            probed_at=float(data.get("probed_at", 0.0) or 0.0),
            logs_capable=data.get("logs_capable") if data.get("logs_capable") is None else bool(data["logs_capable"]),
        )
    except (TypeError, ValueError):
        return None
