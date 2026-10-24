"""Entity resolution for trace nodes — who is this address?

Two label stores exist and they are not equivalent:

* ``wallet_labels`` — 40M-row curated taxonomy imported
  from public label sets (``exchange``, ``cex_hot_wallet``, ``mixer``,
  ``sanctioned``, ...). Wide but noisy: one category ("Wilder BTC:
  Centralized exchange") accounts for 15M rows.
* ``entity_labels`` — the 9-column canonical store written by the scanner
  and the label-growth pipeline (204K rows). Narrow, higher confidence.

The curated store wins for ``kind`` when it has a taxonomy answer; the
canonical store wins for display name/entity type. Both are address-keyed,
so one ``IN`` query each covers a whole frontier.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lite_indexer.query.trace_sql import sql_for

Run = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

_KIND_BY_CATEGORY = {
    "exchange": "exchange",
    "cex_hot_wallet": "exchange",
    "p2p_service": "exchange",
    "bridge": "bridge",
    "mixer": "mixer",
    "defi": "protocol",
    "lending": "protocol",
    "mining_pool": "mining_pool",
}

_KIND_BY_ENTITY_TYPE = {
    "exchange": "exchange",
    "bridge": "bridge",
    "mixer": "mixer",
    "dex": "protocol",
    "lending": "protocol",
    "oracle": "protocol",
    "liquid_staking": "protocol",
    "multisig": "protocol",
    "gaming": "protocol",
    "contract": "contract",
}

_RISK_CATEGORIES = frozenset({"sanctioned", "darknet_market", "ransomware", "money_laundering", "scam", "malicious"})


def _merge(target: dict[str, Any], address: str) -> dict[str, Any]:
    return target.setdefault(
        address,
        {
            "kind": "wallet",
            "label": "",
            "entity": "",
            "entity_type": "",
            "sanctioned": False,
            "risk": set(),
        },
    )


def resolve_entities(run: Run, addrs: list[str]) -> dict[str, dict[str, Any]]:
    """Resolve labels for every address in ``addrs`` (one query per store).

    Args:
        run: Executor — ``(sql, params) -> rows``.
        addrs: Addresses to resolve. EVM addresses must already be lowercase;
            base58 Solana addresses are matched case-sensitively.

    Returns:
        ``{address: {kind, label, entity, entity_type, sanctioned, risk}}``.
        Missing addresses are simply absent — callers treat them as wallets.
    """
    out: dict[str, dict[str, Any]] = {}
    if not addrs:
        return out
    unique = sorted(set(addrs))

    try:
        sql, params = sql_for("labels", addrs=unique)
        label_rows = run(sql, params)
    except Exception:  # noqa: BLE001 — labels are enrichment; a trace must still run
        label_rows = []
    for row in label_rows:
        address = str(row.get("address") or "")
        if not address:
            continue
        category = str(row.get("label_category") or "")
        name = str(row.get("label_name") or "")
        entry = _merge(out, address)
        if category in _KIND_BY_CATEGORY and entry["kind"] == "wallet":
            entry["kind"] = _KIND_BY_CATEGORY[category]
        if category in _RISK_CATEGORIES:
            entry["risk"].add(category)
        if row.get("is_sanctioned"):
            entry["sanctioned"] = True
            entry["risk"].add("sanctioned")
        if name and not entry["label"]:
            entry["label"] = name

    try:
        sql, params = sql_for("entities", addrs=unique)
        entity_rows = run(sql, params)
    except Exception:  # noqa: BLE001 — same rule: enrichment never breaks the trace
        entity_rows = []
    for row in entity_rows:
        address = str(row.get("address") or "")
        if not address:
            continue
        entity_type = str(row.get("entity_type") or "")
        entry = _merge(out, address)
        mapped = _KIND_BY_ENTITY_TYPE.get(entity_type)
        if mapped and entry["kind"] in ("wallet", "contract"):
            entry["kind"] = mapped
        if entity_type == "malicious":
            entry["risk"].add("malicious")
        entry["entity_type"] = entity_type or entry["entity_type"]
        name = str(row.get("name") or "")
        if name:
            entry["entity"] = name
            if not entry["label"]:
                entry["label"] = name

    for entry in out.values():
        entry["risk"] = sorted(entry["risk"])
    return out
