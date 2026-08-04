"""
Cross-chain fund tracing — follow the money across chains, bridges, DEXes
and CEX deposits (Arkham-style, but free + OSS).

:class:`FundTracer` runs directly on the lite-indexer ClickHouse warehouse
(``wallet_memory.evm_transfers``, ``internal_transfers``,
``solana_spl_balance_changes``, ``dex_events``, plus ``wallet_labels`` /
``scam_addresses`` for entity tagging and risk scoring). It needs no extra
indexers or third-party APIs — everything is derived from data we already
collect.

Core capabilities:

* ``trace_forward``  — directed graph of downstream movement from a tx.
* ``trace_backward`` — upstream funding tree for an address.
* ``find_cross_chain_path`` — BFS for a path between two wallets.
* ``detect_bridge_usage`` — bridge interactions for a wallet.
* ``detect_cex_deposits`` — deposits to known CEX hot wallets.

Bridge matching uses two complementary signals:

1. **Same-EOA heuristic** — the depositor on chain A appears on chain B
   receiving the bridged asset within a configurable window (default 30
   minutes). This is the classic "deposit on chain A, matching withdraw on
   chain B" pattern.
2. **Counterpart-contract matching** — the destination-side bridge contract
   (from :data:`address_registry.BRIDGE_COUNTERPARTS`) emits the release /
   mint. Exact addresses are only trusted when they appear in the registry.

All queries are read-only. Bridge matches are *optionally* persisted to
``wallet_memory.bridge_mappings`` (best-effort) so later traces reuse them.
"""

from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from lite_indexer.config import (
    CH_DATABASE,
    CH_DEX_TABLE,
    CH_EVM_TRANSFERS_TABLE,
    CH_INTERNAL_TRANSFERS_TABLE,
    CH_LABELS_TABLE,
    CH_SCAM_TABLE,
    CH_SOLANA_TABLE,
)
from lite_indexer.forensics import bridge_store
from lite_indexer.forensics.address_registry import DEFAULT_REGISTRY, AddressRegistry
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Data model ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TraceNode:
    """One address on one chain in a trace graph."""

    address: str
    chain: str
    label: str = ""
    entity_type: str = ""  # bridge | cex_hot_wallet | dex | mixer | scam | eoa | contract | unknown
    risk_score: float = 0.0
    is_scam: bool = False
    first_seen: str = ""


@dataclass(frozen=True)
class TraceEdge:
    """One money movement between two nodes."""

    from_address: str
    to_address: str
    tx_hash: str
    chain: str
    amount: str
    token: str = ""
    edge_type: str = "transfer"  # transfer | internal | swap | bridge_deposit | bridge_withdraw | cex_deposit
    timestamp: str = ""
    protocol: str = ""
    confidence: float = 1.0


@dataclass
class TraceGraph:
    """Directed graph of fund movement."""

    nodes: dict[str, TraceNode] = field(default_factory=dict)
    edges: list[TraceEdge] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def add_node(self, node: TraceNode) -> None:
        key = _node_key(node.chain, node.address)
        existing = self.nodes.get(key)
        if existing is None:
            self.nodes[key] = node
            return
        # Merge: prefer the richer label / entity type, max risk.
        merged = TraceNode(
            address=existing.address,
            chain=existing.chain,
            label=existing.label or node.label,
            entity_type=existing.entity_type or node.entity_type,
            risk_score=max(existing.risk_score, node.risk_score),
            is_scam=existing.is_scam or node.is_scam,
            first_seen=min(existing.first_seen or node.first_seen, node.first_seen or existing.first_seen),
        )
        self.nodes[key] = merged

    def add_edge(self, edge: TraceEdge) -> bool:
        """Add an edge if it is not already present. Returns True when new."""
        key = (edge.tx_hash, edge.chain, edge.from_address, edge.to_address, edge.token, edge.edge_type)
        if any((e.tx_hash, e.chain, e.from_address, e.to_address, e.token, e.edge_type) == key for e in self.edges):
            return False
        self.edges.append(edge)
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [asdict(n) for n in self.nodes.values()],
            "edges": [asdict(e) for e in self.edges],
            "stats": self.stats,
        }


@dataclass(frozen=True)
class BridgeInteraction:
    """A wallet's interaction with a bridge contract."""

    address: str
    bridge_name: str
    direction: str  # deposit | withdraw
    chain: str
    tx_hash: str
    token: str
    amount: str
    timestamp: str
    counterpart_chain: str = ""
    counterpart_tx: str = ""
    matched: bool = False
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CexDeposit:
    """A deposit to a known CEX hot wallet."""

    address: str
    cex_name: str
    chain: str
    to_address: str
    tx_hash: str
    token: str
    amount: str
    timestamp: str
    confidence: float = 1.0
    pattern: str = "direct"  # direct | sweep

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _node_key(chain: str, address: str) -> str:
    return f"{chain}:{address.strip().lower()}"


# ── Risk scoring tables ─────────────────────────────────────────────────

_SCAM_RISK: dict[str, float] = {
    "money_laundering": 1.0,
    "exploiter": 1.0,
    "malicious": 1.0,
    "phish-hack": 1.0,
    "ransomware": 0.95,
    "scam": 0.95,
    "ponzi": 0.9,
    "exploit": 0.9,
    "mixer": 0.9,
}

_LABEL_RISK: dict[str, float] = {
    "sanctioned": 0.95,
    "ransomware": 0.95,
    "mixer": 0.9,
    "darknet_market": 0.9,
    "money_laundering": 0.9,
    "scam": 0.8,
    "gambling": 0.6,
    "p2p_service": 0.4,
    "defi": 0.2,
    "dex": 0.2,
    "service": 0.2,
    "mining_pool": 0.2,
    "exchange": 0.1,
    "individual": 0.15,
}

_REGISTRY_RISK: dict[str, float] = {
    "bridge": 0.3,
    "canonical_l1_bridge": 0.3,
    "canonical_l2_bridge": 0.3,
    "bridge_router": 0.3,
    "cex_hot_wallet": 0.1,
}

_DEFAULT_RISK = 0.05


def _risk_for(entity_type: str, label_category: str, threat_type: str, sanctioned: bool) -> float:
    """Combine registry type + wallet label + scam threat into a 0..1 score."""
    risk = _DEFAULT_RISK
    if entity_type in _REGISTRY_RISK:
        risk = max(risk, _REGISTRY_RISK[entity_type])
    if label_category in _LABEL_RISK:
        risk = max(risk, _LABEL_RISK[label_category])
    if threat_type in _SCAM_RISK:
        risk = max(risk, _SCAM_RISK[threat_type])
    if sanctioned:
        risk = max(risk, 0.95)
    return round(risk, 3)


# ── Query helper ────────────────────────────────────────────────────────


def _default_client() -> Any | None:
    """Lazily build a ClickHouse client from env (mirrors api/db.get_client).

    Read-only usage: the same SELECT-only ``chain_api`` / ``rmi`` users the
    rest of the project connects with.
    """
    import clickhouse_driver  # deferred: optional dependency

    host = os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST") or "127.0.0.1"
    port = int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000")
    user = os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER") or "chain_api"
    password = os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
    db = os.getenv("CH_DATABASE") or CH_DATABASE or "wallet_memory"
    client = clickhouse_driver.Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        connect_timeout=5,
        send_receive_timeout=30,
    )
    return client


class _ThreadLocalClient:
    """Thread-local ClickHouse client adapter returning dict rows."""

    def __init__(self) -> None:
        self._local = threading.local()

    def __call__(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        client = getattr(self._local, "client", None)
        if client is None:
            client = _default_client()
            if client is None:
                raise RuntimeError("clickhouse unavailable")
            self._local.client = client
        rows, column_types = client.execute(sql, params or {}, with_column_types=True)
        columns = [name for name, _ in column_types]
        return [dict(zip(columns, row, strict=True)) for row in rows]


# ── The tracer ──────────────────────────────────────────────────────────


class FundTracer:
    """Cross-chain fund tracing over the lite-indexer ClickHouse warehouse.

    Args:
        query: Callable ``(sql, params) -> list[dict]``. Defaults to a
            thread-local ClickHouse client built from the environment.
        registry: :class:`AddressRegistry` instance (defaults to the
            curated bridge + CEX registry).
        bridge_window_minutes: Time window for matching a bridge deposit on
            chain A with the release on chain B.
        per_hop_limit: Max outbound events expanded per address per hop.
        max_nodes: Hard cap on graph size (loop protection).
        persist_matches: Write confirmed bridge matches to
            ``wallet_memory.bridge_mappings`` (best-effort).
    """

    def __init__(
        self,
        query: Callable[[str, dict[str, Any] | None], list[dict[str, Any]]] | None = None,
        registry: AddressRegistry | None = None,
        bridge_window_minutes: int = 30,
        per_hop_limit: int = 50,
        max_nodes: int = 500,
        persist_matches: bool = True,
    ) -> None:
        self._q = query if query is not None else _ThreadLocalClient()
        self.registry = registry or DEFAULT_REGISTRY
        self.bridge_window = timedelta(minutes=bridge_window_minutes)
        self.per_hop_limit = per_hop_limit
        self.max_nodes = max_nodes
        self.persist_matches = persist_matches

    # ── Public async API ────────────────────────────────────────────

    async def trace_forward(
        self,
        tx_hash: str,
        chain: str,
        max_depth: int = 10,
        max_hops: int = 100,
    ) -> TraceGraph:
        """Trace funds forward from a transaction (directed downstream graph).

        Args:
            tx_hash: Source transaction hash.
            chain: Chain the transaction is on (e.g. 'ethereum').
            max_depth: Max hops from the root before stopping.
            max_hops: Max total graph edges before stopping.

        Returns:
            :class:`TraceGraph` of downstream movement.
        """
        return await asyncio.to_thread(self._trace_forward_sync, tx_hash, chain, max_depth, max_hops)

    async def trace_backward(
        self,
        address: str,
        start_time: datetime,
        end_time: datetime,
        max_depth: int = 5,
        funding_window_hours: int = 48,
    ) -> TraceGraph:
        """Trace funds backward to source: where did this address get its money?

        Args:
            address: Wallet address (EVM or Solana).
            start_time: Inclusive start of the investigation window.
            end_time: Inclusive end of the investigation window.
            max_depth: How many funding generations to walk back.
            funding_window_hours: Look-back window for each upstream sender.

        Returns:
            :class:`TraceGraph` — the upstream funding tree.
        """
        return await asyncio.to_thread(
            self._trace_backward_sync,
            address,
            start_time,
            end_time,
            max_depth,
            funding_window_hours,
        )

    async def find_cross_chain_path(
        self,
        from_address: str,
        to_address: str,
        max_depth: int = 5,
    ) -> TraceGraph | None:
        """BFS to find a path between two wallets across chains.

        Expands outbound transfers (same chain) and bridge matches
        (cross-chain via same-EOA + counterpart contracts).

        Args:
            from_address: Starting wallet.
            to_address: Target wallet (any chain).
            max_depth: Max BFS expansion depth.

        Returns:
            A :class:`TraceGraph` with the path (nodes + edges), or None.
        """
        return await asyncio.to_thread(self._find_cross_chain_path_sync, from_address, to_address, max_depth)

    async def detect_bridge_usage(self, address: str) -> list[BridgeInteraction]:
        """Identify all bridge interactions for a wallet.

        Scans every registry chain for transfers between the wallet and
        registered bridge contracts, then attempts to match each deposit /
        withdraw to its cross-chain counterpart.
        """
        return await asyncio.to_thread(self._detect_bridge_usage_sync, address)

    async def detect_cex_deposits(self, address: str) -> list[CexDeposit]:
        """Detect deposits from a wallet to known CEX hot wallets."""
        return await asyncio.to_thread(self._detect_cex_deposits_sync, address)

    # ── Entity classification / risk ────────────────────────────────

    def _node_info(self, chain: str, address: str) -> tuple[str, str, float, bool]:
        """Return (label, entity_type, risk_score, is_scam) for an address."""
        addr = address.strip().lower()
        entry = self.registry.lookup(chain, addr)
        label = entry["name"] if entry else ""
        entity_type = entry["type"] if entry else ""
        risk = _risk_for(entity_type or "", "", "", False)

        try:
            labels = self._q(
                f"SELECT label_name, label_category, is_sanctioned FROM {CH_LABELS_TABLE} "
                "WHERE address = %(a)s LIMIT 5",
                {"a": addr},
            )
        except Exception:  # noqa: BLE001 — labeling is best-effort
            labels = []
        for row in labels:
            if not label and row.get("label_name"):
                label = str(row.get("label_name"))
            category = str(row.get("label_category") or "")
            sanctioned = bool(row.get("is_sanctioned"))
            risk = max(risk, _risk_for("", category, "", sanctioned))
            if not entity_type and category == "exchange":
                entity_type = "cex_hot_wallet"
            if category == "mixer":
                entity_type = "mixer"
            if category in ("lending", "defi", "dex", "service"):
                entity_type = entity_type or "defi"

        try:
            scams = self._q(
                f"SELECT threat_type, confidence FROM {CH_SCAM_TABLE} WHERE address = %(a)s LIMIT 5",
                {"a": addr},
            )
        except Exception:  # noqa: BLE001
            scams = []
        is_scam = False
        for row in scams:
            threat = str(row.get("threat_type") or "")
            risk = max(risk, _risk_for("", "", threat, False))
            if threat in _SCAM_RISK:
                is_scam = True
                entity_type = entity_type or "scam"
        if not entity_type:
            entity_type = "eoa"
        return label, entity_type, risk, is_scam

    def _node(self, chain: str, address: str, first_seen: str = "") -> TraceNode:
        label, entity_type, risk, is_scam = self._node_info(chain, address)
        return TraceNode(
            address=address.strip().lower(),
            chain=chain,
            label=label,
            entity_type=entity_type,
            risk_score=risk,
            is_scam=is_scam,
            first_seen=first_seen,
        )

    # ── Low-level queries ───────────────────────────────────────────

    def _fmt_ts(self, value: Any) -> str:
        """Format a ClickHouse DateTime value as ISO-8601."""
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC).isoformat()
        return str(value).replace(" ", "T") + "Z"

    def tx_window(self, tx_hash: str, chain: str) -> tuple[str, datetime, datetime] | None:
        """Return (root_address, window_start, window_end) for a transaction.

        Used by backward traces that start from a tx hash: the root is the
        tx sender, and the window covers the tx's activity. Returns None
        when the tx has no transfer events in the warehouse.
        """
        events = self._tx_events(tx_hash, chain)
        if not events:
            return None
        first = events[0]["block_timestamp"]
        last = events[-1]["block_timestamp"]
        return (
            str(events[0]["from_address"]),
            first - timedelta(days=2),
            last + timedelta(hours=6),
        )

    def _tx_events(self, tx_hash: str, chain: str) -> list[dict[str, Any]]:
        """All ERC-20 transfer events in a transaction."""
        return self._q(
            f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
            "from_address, to_address, token_address, value "
            f"FROM {CH_EVM_TRANSFERS_TABLE} WHERE chain = %(c)s AND tx_hash = %(tx)s "
            "ORDER BY log_index LIMIT 100",
            {"c": chain, "tx": tx_hash.strip().lower()},
        )

    def _internal_tx_events(self, tx_hash: str, chain: str) -> list[dict[str, Any]]:
        """Value-moving internal calls inside a transaction."""
        return self._q(
            f"SELECT chain, block_number, block_timestamp, tx_hash, from_address, "
            "to_address, value_wei, call_type FROM "
            f"{CH_INTERNAL_TRANSFERS_TABLE} WHERE chain = %(c)s AND tx_hash = %(tx)s "
            "ORDER BY trace_address LIMIT 100",
            {"c": chain, "tx": tx_hash.strip().lower()},
        )

    def _outbound(
        self,
        address: str,
        chain: str,
        after: datetime | None = None,
        before: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """ERC-20 transfers FROM an address (projection-friendly shape)."""
        clauses = ["chain = %(c)s", "from_address = %(a)s"]
        params: dict[str, Any] = {"c": chain, "a": address.strip().lower()}
        if after is not None:
            clauses.append("block_timestamp >= %(after)s")
            params["after"] = after
        if before is not None:
            clauses.append("block_timestamp <= %(before)s")
            params["before"] = before
        sql = (
            f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
            "from_address, to_address, token_address, value "
            f"FROM {CH_EVM_TRANSFERS_TABLE} WHERE "
            + " AND ".join(clauses)
            + " ORDER BY block_timestamp, block_number LIMIT "
            + str(limit or self.per_hop_limit)
        )
        return self._q(sql, params)

    def _inbound(
        self,
        address: str,
        chain: str,
        after: datetime | None = None,
        before: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """ERC-20 transfers TO an address (projection-friendly shape)."""
        clauses = ["chain = %(c)s", "to_address = %(a)s"]
        params: dict[str, Any] = {"c": chain, "a": address.strip().lower()}
        if after is not None:
            clauses.append("block_timestamp >= %(after)s")
            params["after"] = after
        if before is not None:
            clauses.append("block_timestamp <= %(before)s")
            params["before"] = before
        sql = (
            f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
            "from_address, to_address, token_address, value "
            f"FROM {CH_EVM_TRANSFERS_TABLE} WHERE "
            + " AND ".join(clauses)
            + " ORDER BY block_timestamp, block_number LIMIT "
            + str(limit or self.per_hop_limit)
        )
        return self._q(sql, params)

    def _internal_outbound(
        self,
        address: str,
        chain: str,
        after: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Value-moving internal calls FROM an address."""
        clauses = ["chain = %(c)s", "from_address = %(a)s", "value_wei > 0"]
        params: dict[str, Any] = {"c": chain, "a": address.strip().lower()}
        if after is not None:
            clauses.append("block_timestamp >= %(after)s")
            params["after"] = after
        sql = (
            "SELECT chain, block_number, block_timestamp, tx_hash, from_address, "
            "to_address, value_wei, call_type FROM "
            + CH_INTERNAL_TRANSFERS_TABLE
            + " WHERE "
            + " AND ".join(clauses)
            + " ORDER BY block_timestamp, block_number LIMIT "
            + str(limit or self.per_hop_limit)
        )
        return self._q(sql, params)

    def _solana_changes(
        self,
        address: str,
        after: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Solana SPL balance changes for a wallet (owner-based)."""
        clauses = ["owner = %(a)s"]
        params: dict[str, Any] = {"a": address.strip().lower()}
        if after is not None:
            clauses.append("block_timestamp >= %(after)s")
            params["after"] = after
        sql = (
            "SELECT chain, block_timestamp, tx_hash, mint, owner, delta FROM "
            + CH_SOLANA_TABLE
            + " WHERE "
            + " AND ".join(clauses)
            + " ORDER BY block_timestamp LIMIT "
            + str(limit or self.per_hop_limit)
        )
        return self._q(sql, params)

    def _dex_swaps(
        self,
        address: str,
        chain: str,
        after: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """DEX swaps where the wallet is sender or recipient."""
        clauses = ["chain = %(c)s", "(sender = %(a)s OR recipient = %(a)s)"]
        params: dict[str, Any] = {"c": chain, "a": address.strip().lower()}
        if after is not None:
            clauses.append("block_timestamp >= %(after)s")
            params["after"] = after
        sql = (
            f"SELECT chain, block_number, block_timestamp, tx_hash, dex, token0, "
            "token1, sender, recipient, amount0, amount1, price_usd, volume_usd "
            f"FROM {CH_DEX_TABLE} WHERE "
            + " AND ".join(clauses)
            + " ORDER BY block_timestamp, block_number LIMIT "
            + str(limit or self.per_hop_limit)
        )
        return self._q(sql, params)

    def _chain_activity(self, address: str, limit: int = 6) -> list[str]:
        """Chains where an address has activity (for cross-chain seeding)."""
        try:
            rows = self._q(
                f"SELECT chain, count() AS n FROM {CH_EVM_TRANSFERS_TABLE} "
                "WHERE from_address = %(a)s GROUP BY chain ORDER BY n DESC LIMIT " + str(limit),
                {"a": address.strip().lower()},
            )
            return [str(r["chain"]) for r in rows]
        except Exception:  # noqa: BLE001 — seeding is best-effort
            return []

    def _transfers_between(
        self,
        from_set: list[str],
        to_set: list[str],
        chain: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Transfers between two address sets on one chain."""
        if not from_set or not to_set:
            return []
        params: dict[str, Any] = {
            "c": chain,
            "f": [a.strip().lower() for a in from_set],
            "t": [a.strip().lower() for a in to_set],
        }
        return self._q(
            "SELECT chain, block_timestamp, tx_hash, log_index, from_address, "
            "to_address, token_address, value FROM "
            + CH_EVM_TRANSFERS_TABLE
            + " WHERE chain = %(c)s AND from_address IN %(f)s AND to_address IN %(t)s "
            "ORDER BY block_timestamp DESC LIMIT " + str(limit),
            params,
        )

    # ── Bridge matching ─────────────────────────────────────────────

    def _match_bridge_release(
        self,
        depositor: str,
        token: str,
        amount: Decimal,
        deposit_ts: datetime,
        bridge_chain: str,
        bridge_addr: str,
        bridge_name: str,
        counterpart_chains: list[str],
    ) -> tuple[dict[str, Any] | None, str | None, float]:
        """Find the release of a bridge deposit on a destination chain.

        Strategy 1 (same-EOA): the depositor receives the bridged asset on
        the destination chain within the window.

        Strategy 2 (counterpart contract): a transfer FROM the destination
        bridge contract within the window (covers relayed/minted flows).

        Returns ``(matched_row, dest_chain, confidence)``.
        """
        window = self.bridge_window
        t0 = deposit_ts
        t1 = deposit_ts + window
        best: tuple[dict[str, Any] | None, str | None, float] = (None, None, 0.0)

        for dest_chain in counterpart_chains:
            # Strategy 1 — same-EOA inflow on destination chain.
            try:
                inbound = self._inbound(depositor, dest_chain, after=t0, before=t1, limit=20)
            except Exception:  # noqa: BLE001 — matching is best-effort
                inbound = []
            for row in inbound:
                row_amt = _dec(row.get("value"))
                # Amount proximity: canonical bridges move the same amount
                # (minus fees); wrapped/Hop flows differ by design. Score
                # time proximity + amount proximity.
                time_gap = abs((row["block_timestamp"] - deposit_ts).total_seconds())
                if row_amt and amount:
                    ratio = float(min(row_amt, amount) / max(row_amt, amount))
                    amt_score = ratio
                else:
                    amt_score = 0.5
                time_score = max(0.0, 1.0 - time_gap / (window.total_seconds() * 2))
                score = 0.6 * amt_score + 0.4 * time_score
                if score > best[2]:
                    best = (row, dest_chain, round(score, 3))

            # Strategy 2 — counterpart bridge contract releases funds.
            for c_chain, c_addr in self.registry.counterpart_bridges(bridge_chain, bridge_addr):
                if c_chain != dest_chain:
                    continue
                try:
                    releases = self._outbound(c_addr, c_chain, after=t0, before=t1, limit=20)
                except Exception:  # noqa: BLE001
                    releases = []
                for row in releases:
                    time_gap = abs((row["block_timestamp"] - deposit_ts).total_seconds())
                    time_score = max(0.0, 1.0 - time_gap / (window.total_seconds() * 2))
                    score = 0.5 + 0.5 * time_score
                    if score > best[2]:
                        best = (row, c_chain, round(score, 3))
        return best

    # ── Forward tracing ─────────────────────────────────────────────

    def _trace_forward_sync(
        self,
        tx_hash: str,
        chain: str,
        max_depth: int,
        max_hops: int,
    ) -> TraceGraph:
        graph = TraceGraph(stats={"root_tx": tx_hash, "root_chain": chain, "direction": "forward"})
        events = self._tx_events(tx_hash, chain)
        if not events:
            graph.stats["error"] = "no transfer events found for tx"
            return graph

        visited_nodes: set[str] = set()
        queue: list[tuple[str, str, int, datetime]] = []  # (chain, addr, depth, since_ts)

        for evt in events:
            ts = evt["block_timestamp"]
            frm = str(evt["from_address"])
            to = str(evt["to_address"])
            graph.add_node(self._node(chain, frm, first_seen=self._fmt_ts(ts)))
            graph.add_node(self._node(chain, to, first_seen=self._fmt_ts(ts)))
            graph.add_edge(
                TraceEdge(
                    from_address=frm,
                    to_address=to,
                    tx_hash=tx_hash,
                    chain=chain,
                    amount=str(evt.get("value") or "0"),
                    token=str(evt.get("token_address") or ""),
                    edge_type="transfer",
                    timestamp=self._fmt_ts(ts),
                )
            )
            # Classify the root recipient: bridge → cross-chain follow,
            # CEX → tag as deposit, else expand forward.
            entity = self.registry.lookup(chain, to)
            etype = entity["type"] if entity else ""
            if etype in ("bridge", "canonical_l1_bridge", "canonical_l2_bridge", "bridge_router"):
                self._follow_bridge(graph, chain, frm, to, evt, visited_nodes, queue, max_depth)
                continue
            if etype == "cex_hot_wallet":
                graph.edges[-1] = TraceEdge(
                    from_address=frm,
                    to_address=to,
                    tx_hash=tx_hash,
                    chain=chain,
                    amount=str(evt.get("value") or "0"),
                    token=str(evt.get("token_address") or ""),
                    edge_type="cex_deposit",
                    timestamp=self._fmt_ts(ts),
                    protocol=entity["name"] if entity else "",
                )
                continue
            # Follow the recipient forward.
            queue.append((chain, to, 1, ts))

        self._expand_queue(graph, queue, visited_nodes, max_depth, max_hops)
        graph.stats["node_count"] = len(graph.nodes)
        graph.stats["edge_count"] = len(graph.edges)
        return graph

    def _expand_queue(
        self,
        graph: TraceGraph,
        queue: list[tuple[str, str, int, datetime]],
        visited_nodes: set[str],
        max_depth: int,
        max_hops: int,
    ) -> None:
        while queue:
            if len(graph.edges) >= max_hops or len(graph.nodes) >= self.max_nodes:
                graph.stats["truncated"] = True
                break
            chain, addr, depth, since = queue.pop(0)
            key = _node_key(chain, addr)
            if key in visited_nodes:
                continue
            visited_nodes.add(key)
            if depth > max_depth:
                continue

            try:
                outbound = self._outbound(addr, chain, after=since, limit=self.per_hop_limit)
            except Exception:  # noqa: BLE001 — expansion is best-effort
                outbound = []
            for evt in outbound:
                if len(graph.edges) >= max_hops or len(graph.nodes) >= self.max_nodes:
                    graph.stats["truncated"] = True
                    return
                ts = evt["block_timestamp"]
                to = str(evt["to_address"])
                to_key = _node_key(chain, to)
                graph.add_node(self._node(chain, to, first_seen=self._fmt_ts(ts)))
                is_new = graph.add_edge(
                    TraceEdge(
                        from_address=addr,
                        to_address=to,
                        tx_hash=str(evt["tx_hash"]),
                        chain=chain,
                        amount=str(evt.get("value") or "0"),
                        token=str(evt.get("token_address") or ""),
                        edge_type="transfer",
                        timestamp=self._fmt_ts(ts),
                    )
                )
                if not is_new:
                    continue
                entity = self.registry.lookup(chain, to)
                etype = entity["type"] if entity else ""
                if etype in ("bridge", "canonical_l1_bridge", "canonical_l2_bridge", "bridge_router"):
                    self._follow_bridge(graph, chain, addr, to, evt, visited_nodes, queue, max_depth)
                    continue
                if etype == "cex_hot_wallet":
                    # Terminal: tag the edge as a CEX deposit.
                    graph.edges[-1] = TraceEdge(
                        from_address=addr,
                        to_address=to,
                        tx_hash=str(evt["tx_hash"]),
                        chain=chain,
                        amount=str(evt.get("value") or "0"),
                        token=str(evt.get("token_address") or ""),
                        edge_type="cex_deposit",
                        timestamp=self._fmt_ts(ts),
                        protocol=entity["name"] if entity else "",
                    )
                    continue
                if depth + 1 <= max_depth and to_key not in visited_nodes:
                    queue.append((chain, to, depth + 1, ts))

            # Internal ETH flows from this address.
            try:
                internals = self._internal_outbound(addr, chain, after=since, limit=20)
            except Exception:  # noqa: BLE001
                internals = []
            for evt in internals:
                if len(graph.edges) >= max_hops:
                    graph.stats["truncated"] = True
                    return
                ts = evt["block_timestamp"]
                to = str(evt["to_address"])
                graph.add_node(self._node(chain, to, first_seen=self._fmt_ts(ts)))
                graph.add_edge(
                    TraceEdge(
                        from_address=addr,
                        to_address=to,
                        tx_hash=str(evt["tx_hash"]),
                        chain=chain,
                        amount=str(evt.get("value_wei") or "0"),
                        token="native",
                        edge_type="internal",
                        timestamp=self._fmt_ts(ts),
                    )
                )

            # DEX swaps → token conversion edges.
            try:
                swaps = self._dex_swaps(addr, chain, after=since, limit=20)
            except Exception:  # noqa: BLE001
                swaps = []
            for sw in swaps:
                if len(graph.edges) >= max_hops:
                    graph.stats["truncated"] = True
                    return
                ts = sw["block_timestamp"]
                counterparty = str(sw["recipient"] if sw.get("sender") == addr else sw.get("sender") or addr)
                graph.add_node(self._node(chain, counterparty, first_seen=self._fmt_ts(ts)))
                graph.add_edge(
                    TraceEdge(
                        from_address=addr,
                        to_address=counterparty,
                        tx_hash=str(sw["tx_hash"]),
                        chain=chain,
                        amount=str(sw.get("amount0") or sw.get("amount1") or "0"),
                        token=str(sw.get("token0") or ""),
                        edge_type="swap",
                        timestamp=self._fmt_ts(ts),
                        protocol=str(sw.get("dex") or "dex"),
                    )
                )

    def _follow_bridge(
        self,
        graph: TraceGraph,
        chain: str,
        depositor: str,
        bridge_addr: str,
        deposit_evt: dict[str, Any],
        visited_nodes: set[str],
        queue: list[tuple[str, str, int, datetime]],
        max_depth: int,
    ) -> None:
        """Detect the bridge release for a deposit and continue tracing there."""
        entry = self.registry.lookup(chain, bridge_addr)
        bridge_name = entry["name"] if entry else bridge_addr[:10]
        deposit_ts = deposit_evt["block_timestamp"]
        amount = _dec(deposit_evt.get("value"))
        token = str(deposit_evt.get("token_address") or "")

        # Destination chains: registry counterparts, else every registry chain
        # (the same-EOA heuristic is chain-agnostic).
        counterparts = self.registry.counterpart_bridges(chain, bridge_addr)
        dest_chains = [c for c, _ in counterparts] or self.registry.chains()
        dest_chains = [c for c in dest_chains if c != chain]

        row, dest_chain, confidence = self._match_bridge_release(
            depositor, token, amount, deposit_ts, chain, bridge_addr, bridge_name, dest_chains
        )
        if row is None or dest_chain is None:
            graph.add_edge(
                TraceEdge(
                    from_address=depositor,
                    to_address=bridge_addr,
                    tx_hash=str(deposit_evt["tx_hash"]),
                    chain=chain,
                    amount=str(deposit_evt.get("value") or "0"),
                    token=token,
                    edge_type="bridge_deposit",
                    timestamp=self._fmt_ts(deposit_ts),
                    protocol=bridge_name,
                    confidence=0.0,
                )
            )
            return

        matched_ts = row["block_timestamp"]
        recipient = str(row.get("to_address") or depositor)
        # Also add the bridge deposit edge on the source chain.
        graph.add_edge(
            TraceEdge(
                from_address=depositor,
                to_address=bridge_addr,
                tx_hash=str(deposit_evt["tx_hash"]),
                chain=chain,
                amount=str(deposit_evt.get("value") or "0"),
                token=token,
                edge_type="bridge_deposit",
                timestamp=self._fmt_ts(deposit_ts),
                protocol=bridge_name,
                confidence=confidence,
            )
        )
        graph.add_node(self._node(dest_chain, recipient, first_seen=self._fmt_ts(matched_ts)))
        graph.add_edge(
            TraceEdge(
                from_address=str(row.get("from_address") or bridge_addr),
                to_address=recipient,
                tx_hash=str(row.get("tx_hash") or ""),
                chain=dest_chain,
                amount=str(row.get("value") or "0"),
                token=str(row.get("token_address") or token),
                edge_type="bridge_withdraw",
                timestamp=self._fmt_ts(matched_ts),
                protocol=bridge_name,
                confidence=confidence,
            )
        )

        if self.persist_matches:
            self._persist_match(
                source_address=depositor,
                source_chain=chain,
                dest_address=recipient,
                dest_chain=dest_chain,
                bridge_name=bridge_name,
                tx_hash=str(deposit_evt["tx_hash"]),
                bridged_at=deposit_ts,
                confidence=confidence,
            )

        key = _node_key(dest_chain, recipient)
        if key not in visited_nodes:
            queue.append((dest_chain, recipient, 1, matched_ts))

    def _persist_match(
        self,
        source_address: str,
        source_chain: str,
        dest_address: str,
        dest_chain: str,
        bridge_name: str,
        tx_hash: str,
        bridged_at: datetime,
        confidence: float,
    ) -> None:
        """Best-effort write of a confirmed bridge match to bridge_mappings."""
        try:
            client = _default_client()
            if client is None:
                return
            bridge_store.ensure_bridge_mappings_table(client)
            bridge_store.write_bridge_mappings(
                client,
                [
                    {
                        "source_address": source_address,
                        "source_chain": source_chain,
                        "dest_address": dest_address,
                        "dest_chain": dest_chain,
                        "bridge_name": bridge_name,
                        "tx_hash": tx_hash,
                        "bridged_at": bridged_at,
                        "confidence": confidence,
                    }
                ],
            )
        except Exception:  # noqa: BLE001 — persistence is best-effort
            logger.debug("bridge_mapping_persist_skipped")

    # ── Backward tracing ────────────────────────────────────────────

    def _trace_backward_sync(
        self,
        address: str,
        start_time: datetime,
        end_time: datetime,
        max_depth: int,
        funding_window_hours: int,
    ) -> TraceGraph:
        graph = TraceGraph(
            stats={
                "root_address": address.strip().lower(),
                "window_start": start_time.isoformat(),
                "window_end": end_time.isoformat(),
                "direction": "backward",
            }
        )
        addr = address.strip().lower()
        chains = self._chain_activity(addr) or ["ethereum"]
        # Solana fallback when no EVM activity.
        if not chains:
            chains = ["solana"]

        visited: set[str] = set()
        root_ts = end_time
        graph.add_node(self._node(chains[0], addr, first_seen=self._fmt_ts(root_ts)))

        def expand(current: str, chain: str, cutoff: datetime, depth: int) -> None:
            if depth > max_depth or len(graph.nodes) >= self.max_nodes:
                return
            key = _node_key(chain, current)
            if key in visited:
                return
            visited.add(key)
            try:
                inbound = self._inbound(
                    current,
                    chain,
                    after=cutoff - timedelta(hours=funding_window_hours),
                    before=cutoff,
                    limit=self.per_hop_limit,
                )
            except Exception:  # noqa: BLE001
                inbound = []
            for evt in inbound:
                if len(graph.nodes) >= self.max_nodes:
                    graph.stats["truncated"] = True
                    return
                ts = evt["block_timestamp"]
                sender = str(evt["from_address"])
                sender_key = _node_key(chain, sender)
                graph.add_node(self._node(chain, sender, first_seen=self._fmt_ts(ts)))
                graph.add_edge(
                    TraceEdge(
                        from_address=sender,
                        to_address=current,
                        tx_hash=str(evt["tx_hash"]),
                        chain=chain,
                        amount=str(evt.get("value") or "0"),
                        token=str(evt.get("token_address") or ""),
                        edge_type="transfer",
                        timestamp=self._fmt_ts(ts),
                    )
                )
                if sender_key not in visited:
                    expand(sender, chain, ts, depth + 1)

        for chain in chains:
            expand(addr, chain, root_ts, 1)
        graph.stats["node_count"] = len(graph.nodes)
        graph.stats["edge_count"] = len(graph.edges)
        return graph

    # ── Cross-chain path finding ────────────────────────────────────

    def _find_cross_chain_path_sync(
        self,
        from_address: str,
        to_address: str,
        max_depth: int,
    ) -> TraceGraph | None:
        start = from_address.strip().lower()
        goal = to_address.strip().lower()
        start_chains = self._chain_activity(start) or ["ethereum"]

        from collections import deque

        queue: deque[tuple[str, str, int]] = deque()  # (chain, addr, depth)
        parent: dict[str, tuple[str, str, TraceEdge]] = {}
        visited: set[str] = set()
        for c in start_chains:
            key = _node_key(c, start)
            if key not in visited:
                visited.add(key)
                queue.append((c, start, 0))
                parent[key] = ("", "", TraceEdge(from_address=start, to_address=start, tx_hash="", chain=c, amount="0"))

        found: tuple[str, str] | None = None
        while queue:
            chain, addr, depth = queue.popleft()
            if addr == goal:
                found = (chain, addr)
                break
            if depth >= max_depth:
                continue

            try:
                outbound = self._outbound(addr, chain, limit=self.per_hop_limit)
            except Exception:  # noqa: BLE001
                outbound = []
            for evt in outbound:
                nxt = str(evt["to_address"])
                nkey = _node_key(chain, nxt)
                if nkey in visited:
                    continue
                visited.add(nkey)
                parent[nkey] = (
                    _node_key(chain, addr),
                    chain,
                    TraceEdge(
                        from_address=addr,
                        to_address=nxt,
                        tx_hash=str(evt["tx_hash"]),
                        chain=chain,
                        amount=str(evt.get("value") or "0"),
                        token=str(evt.get("token_address") or ""),
                        edge_type="transfer",
                        timestamp=self._fmt_ts(evt["block_timestamp"]),
                    ),
                )
                queue.append((chain, nxt, depth + 1))

            # Bridge-driven cross-chain hops (same-EOA + counterpart).
            for evt in outbound:
                bridge_addr = str(evt["to_address"])
                bridge_entry = self.registry.lookup(chain, bridge_addr)
                if bridge_entry is None:
                    continue
                ts = evt["block_timestamp"]
                match, dest_chain, conf = self._match_bridge_release(
                    addr,
                    str(evt.get("token_address") or ""),
                    _dec(evt.get("value")),
                    ts,
                    chain,
                    bridge_addr,
                    str(bridge_entry.get("name") or ""),
                    [c for c in self.registry.chains() if c != chain],
                )
                if match is None or dest_chain is None:
                    continue
                recipient = str(match.get("to_address") or addr)
                nkey = _node_key(dest_chain, recipient)
                if nkey in visited:
                    continue
                visited.add(nkey)
                parent[nkey] = (
                    _node_key(chain, addr),
                    dest_chain,
                    TraceEdge(
                        from_address=addr,
                        to_address=recipient,
                        tx_hash=str(match.get("tx_hash") or ""),
                        chain=dest_chain,
                        amount=str(match.get("value") or "0"),
                        token=str(match.get("token_address") or evt.get("token_address") or ""),
                        edge_type="bridge_withdraw",
                        timestamp=self._fmt_ts(match["block_timestamp"]),
                        protocol=str(bridge_entry.get("name") or ""),
                        confidence=conf,
                    ),
                )
                queue.append((dest_chain, recipient, depth + 1))

        if found is None:
            return None

        # Reconstruct the path.
        graph = TraceGraph(stats={"from": start, "to": goal, "found_on_chain": found[0], "direction": "path"})
        keys: list[str] = []
        k = _node_key(found[0], found[1])
        while k:
            keys.append(k)
            k = parent.get(k, ("", "", TraceEdge(from_address="", to_address="", tx_hash="", chain="", amount="0")))[0]
        keys.reverse()
        for key in keys:
            chain, addr = key.split(":", 1)
            graph.add_node(self._node(chain, addr))
        for i in range(1, len(keys)):
            _, _, edge = parent[keys[i]]
            graph.add_edge(edge)
        graph.stats["node_count"] = len(graph.nodes)
        graph.stats["edge_count"] = len(graph.edges)
        return graph

    # ── Bridge usage ────────────────────────────────────────────────

    def _detect_bridge_usage_sync(self, address: str) -> list[BridgeInteraction]:
        addr = address.strip().lower()
        interactions: list[BridgeInteraction] = []
        seen: set[tuple[str, Any, Any, str]] = set()
        for chain in self.registry.chains():
            bridges = self.registry.bridge_addresses(chain)
            if not bridges:
                continue
            try:
                rows = self._transfers_between([addr], bridges, chain, limit=100)
            except Exception:  # noqa: BLE001
                rows = []
            for r in rows:
                frm = str(r["from_address"])
                to = str(r["to_address"])
                direction = "deposit" if frm == addr else "withdraw"
                bridge_entry = self.registry.lookup(chain, to if direction == "deposit" else frm)
                bridge_name = bridge_entry["name"] if bridge_entry else "unknown-bridge"
                key = (chain, r["tx_hash"], r["log_index"], direction)
                if key in seen:
                    continue
                seen.add(key)
                ts = r["block_timestamp"]
                interaction = BridgeInteraction(
                    address=addr,
                    bridge_name=bridge_name,
                    direction=direction,
                    chain=chain,
                    tx_hash=str(r["tx_hash"]),
                    token=str(r["token_address"]),
                    amount=str(r.get("value") or "0"),
                    timestamp=self._fmt_ts(ts),
                )
                # Attempt counterpart matching for deposits.
                if direction == "deposit":
                    dest_chains = [c for c in self.registry.chains() if c != chain]
                    match, dest_chain, conf = self._match_bridge_release(
                        addr,
                        str(r.get("token_address") or ""),
                        _dec(r.get("value")),
                        ts,
                        chain,
                        to,
                        bridge_name,
                        dest_chains,
                    )
                    if match is not None and dest_chain is not None:
                        interaction = BridgeInteraction(
                            address=addr,
                            bridge_name=bridge_name,
                            direction=direction,
                            chain=chain,
                            tx_hash=str(r["tx_hash"]),
                            token=str(r["token_address"]),
                            amount=str(r.get("value") or "0"),
                            timestamp=self._fmt_ts(ts),
                            counterpart_chain=dest_chain,
                            counterpart_tx=str(match.get("tx_hash") or ""),
                            matched=True,
                            confidence=conf,
                        )
                interactions.append(interaction)
        interactions.sort(key=lambda i: i.timestamp, reverse=True)
        return interactions

    # ── CEX deposits ────────────────────────────────────────────────

    def _detect_cex_deposits_sync(self, address: str) -> list[CexDeposit]:
        addr = address.strip().lower()
        deposits: list[CexDeposit] = []
        seen: set[tuple[str, Any, Any]] = set()
        for chain in self.registry.chains():
            cex_addrs = self.registry.cex_addresses(chain)
            if not cex_addrs:
                continue
            try:
                rows = self._transfers_between([addr], cex_addrs, chain, limit=100)
            except Exception:  # noqa: BLE001
                rows = []
            for r in rows:
                key = (chain, r["tx_hash"], r["log_index"])
                if key in seen:
                    continue
                seen.add(key)
                cex_entry = self.registry.lookup(chain, str(r["to_address"]))
                deposits.append(
                    CexDeposit(
                        address=addr,
                        cex_name=cex_entry["name"] if cex_entry else str(r["to_address"])[:10],
                        chain=chain,
                        to_address=str(r["to_address"]),
                        tx_hash=str(r["tx_hash"]),
                        token=str(r["token_address"]),
                        amount=str(r.get("value") or "0"),
                        timestamp=self._fmt_ts(r["block_timestamp"]),
                        confidence=float(cex_entry["confidence"]) if cex_entry else 0.5,
                        pattern="direct",
                    )
                )
        deposits.sort(key=lambda d: d.timestamp, reverse=True)
        return deposits


def _dec(value: Any) -> Decimal:
    """Parse a CH string/numeric value into Decimal (0 on failure)."""
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return Decimal(0)


__all__ = [
    "BridgeInteraction",
    "CexDeposit",
    "FundTracer",
    "TraceEdge",
    "TraceGraph",
    "TraceNode",
]
