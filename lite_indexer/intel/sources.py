"""
Data sources for the wallet intelligence pipeline.

Design principle: **local-first, external best-effort.** The primary data
source is the indexer's own ClickHouse warehouse (``scam_addresses``,
``wallet_labels``, ``wallet_profiles``, ``wallet_entities``,
``deployer_events``, ``evm_transfers``) — always available, already
ingested from CryptoscamDB/Chainabuse/Revoke.cash/etherscan label sets.
External enrichments (GoPlus token security, Chainabuse API, rekt.news,
CryptoscamDB) are optional adapters that degrade gracefully: a timeout,
HTTP error, or missing API key marks the source "unavailable" without
breaking the report.

Every adapter exposes the same shape::

    async def lookup(address, chain) -> list[dict]   # one dict per finding
    def available() -> bool                           # sync availability probe

``ChQuery`` is the read contract every module depends on — a callable
``(sql, params) -> list[dict]``. The API passes ``api.db.query``; tests
inject a fake. This keeps the intel package free of hard database
imports (it works over ClickHouse, DuckDB, or mocks).
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol

import httpx

from lite_indexer.intel.config import (
    CHAINABUSE_API_KEY,
    EXCLUDED_SINK_ADDRESSES,
    EXPLOIT_THREAT_TYPES,
    INTEL_ANOMALY_MAX_RESULTS,
    INTEL_EXTERNAL_SOURCES_ENABLED,
    INTEL_HTTP_TIMEOUT,
    INTEL_MAX_FUNDERS,
    normalize_chain,
)

logger = logging.getLogger(__name__)

# ── Read contract ──────────────────────────────────────────────────────


class ChQuery(Protocol):
    """Execute a parameterized SELECT and return rows as dicts."""

    def __call__(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


def _graceful(default: Any = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator factory: wrap a local lookup so a missing table / missing
    grant returns ``default`` instead of raising.

    Fresh deployments may not have ``wallet_entities`` etc. provisioned or
    granted yet; a failed sub-lookup must not kill the whole report. The
    failure is logged at warning level so operators can spot grant gaps.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 — graceful degradation contract
                logger.warning("intel lookup %s failed: %s", fn.__name__, str(exc)[:200])
                return default
        return wrapper

    return decorator


def _chain_ids(chain: str | None) -> tuple[str, ...]:
    """Chain spellings to search in label/scam tables (short + long).

    ``wallet_labels`` / ``scam_addresses`` store ethereum addresses under
    both ``eth`` and ``ethereum``; other chains use their canonical name.
    A ``None`` chain returns an empty tuple (no filter).
    """
    normalized = normalize_chain(chain)
    if not normalized:
        return ()
    if normalized == "ethereum":
        return ("eth", "ethereum")
    return (normalized,)


# ── Local warehouse source (always available) ──────────────────────────


@dataclass
class LocalIntelSource:
    """Read-only access to the indexer's ClickHouse intel tables."""

    query: ChQuery
    max_funders: int = INTEL_MAX_FUNDERS
    anomaly_max: int = INTEL_ANOMALY_MAX_RESULTS

    # -- labels / scam ------------------------------------------------

    @_graceful(default=[])
    def labels(self, address: str, chain: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
        """Known labels for ``address`` from ``wallet_labels``."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT address, chain_id, label_name, label_category, source, is_sanctioned, "
            "label_subtype, loaded_at FROM wallet_memory.wallet_labels "
            "WHERE address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain_id IN %(chains)s"
            params["chains"] = chains
        sql += f" ORDER BY loaded_at DESC LIMIT {int(limit)}"
        return self.query(sql, params)

    @_graceful(default=[])
    def scam_lookup(self, address: str, chain: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        """Threat matches for ``address`` from ``scam_addresses``."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT address, chain_id, source, threat_type, confidence, first_seen_at, evidence "
            "FROM wallet_memory.scam_addresses WHERE address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain_id IN %(chains)s"
            params["chains"] = chains
        sql += f" ORDER BY first_seen_at DESC LIMIT {int(limit)}"
        return self.query(sql, params)

    @_graceful(default=None)
    def profile(self, address: str, chain: str | None = None) -> dict[str, Any] | None:
        """Precomputed wallet profile (risk score from the deployer pipeline)."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT wallet_address, chain_id, entity_id, entity_label, entity_category, "
            "total_transactions, total_volume, unique_counterparties, first_seen_at, "
            "last_seen_at, risk_score, risk_level, updated_at "
            "FROM wallet_memory.wallet_profiles WHERE wallet_address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain_id IN %(chains)s"
            params["chains"] = chains
        sql += " ORDER BY updated_at DESC LIMIT 1"
        rows = self.query(sql, params)
        return rows[0] if rows else None

    @_graceful(default=[])
    def entity_links(self, address: str, chain: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Precomputed entity-cluster memberships from ``wallet_entities``."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT entity_id, wallet_address, chain_id, heuristic_type, confidence_score, "
            "entity_label, entity_category, first_linked_at, last_updated_at "
            "FROM wallet_memory.wallet_entities WHERE entity_id IN ("
            "  SELECT entity_id FROM wallet_memory.wallet_entities WHERE wallet_address = %(address)s"
            ")"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain_id IN %(chains)s"
            params["chains"] = chains
        sql += f" ORDER BY last_updated_at DESC LIMIT {int(limit)}"
        return self.query(sql, params)

    # -- activity -----------------------------------------------------

    @_graceful(default={})
    def wallet_stats(self, address: str, chain: str | None = None) -> dict[str, Any]:
        """Aggregate activity stats from ``evm_transfers`` (ethereum-family)."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT count() AS tx_count, countDistinct(token_address) AS unique_tokens, "
            "countDistinct(from_address) AS unique_senders, "
            "countDistinct(to_address) AS unique_receivers, "
            "min(block_timestamp) AS first_seen, max(block_timestamp) AS last_seen "
            "FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(address)s OR to_address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        rows = self.query(sql, params)
        return rows[0] if rows else {}

    def recent_transfers(
        self, address: str, chain: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Most recent transfers touching ``address`` (for the report)."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT chain, block_number, block_timestamp, tx_hash, from_address, to_address, "
            "token_address, token_symbol, value "
            "FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(address)s OR to_address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        sql += f" ORDER BY block_timestamp DESC LIMIT {int(limit)}"
        return self.query(sql, params)

    @_graceful(default=[])
    def funders(self, address: str, chain: str | None = None) -> list[dict[str, Any]]:
        """First-hop funders of ``address`` (senders in inbound transfers).

        Returns the top funders by count, plus the first and last time each
        funded the wallet. Used by risk scoring (mixer/exploit funding) and
        by clustering (shared funding source).
        """
        chains = _chain_ids(chain)
        sql = (
            "SELECT from_address, count() AS fund_txs, min(block_timestamp) AS first_funded, "
            "max(block_timestamp) AS last_funded "
            "FROM wallet_memory.evm_transfers "
            "WHERE to_address = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        sql += " AND from_address != to_address GROUP BY from_address ORDER BY fund_txs DESC"
        sql += f" LIMIT {int(self.max_funders)}"
        return self.query(sql, params)

    def shared_recipients(
        self, funder: str, address: str, chain: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Other wallets funded by the same ``funder`` (cluster signal)."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT to_address, count() AS txs, min(block_timestamp) AS first_funded, "
            "max(block_timestamp) AS last_funded "
            "FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(funder)s AND to_address != %(funder)s AND to_address != %(address)s"
        )
        params: dict[str, Any] = {"funder": funder, "address": address.strip().lower()}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        sql += " GROUP BY to_address ORDER BY txs DESC"
        sql += f" LIMIT {int(limit)}"
        return self.query(sql, params)

    @_graceful(default=[])
    def chain_presence(self, address: str) -> list[str]:
        """Distinct chains the address is active on (cross-chain rug signal)."""
        sql = (
            "SELECT chain FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(address)s OR to_address = %(address)s "
            "GROUP BY chain ORDER BY chain"
        )
        rows = self.query(sql, {"address": address.strip().lower()})
        return [str(r["chain"]) for r in rows]

    @_graceful(default=[])
    def deployments(self, address: str, chain: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
        """Contracts deployed by ``address`` (deployer or deployer_eoa)."""
        chains = _chain_ids(chain)
        sql = (
            "SELECT chain, block_number, block_timestamp, tx_hash, deployer, deployer_eoa, "
            "token_address, token0, token1, pair_address, type, factory "
            "FROM wallet_memory.deployer_events "
            "WHERE deployer = %(address)s OR deployer_eoa = %(address)s"
        )
        params: dict[str, Any] = {"address": address.strip().lower()}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        sql += " ORDER BY block_timestamp DESC"
        sql += f" LIMIT {int(limit)}"
        return self.query(sql, params)

    # -- anomaly scans -------------------------------------------------

    @_graceful(default=[])
    def anomalous_deployers(self, chain: str | None = None, window_days: int = 7) -> list[dict[str, Any]]:
        """Deployers whose recent deploys are funded by high-risk sources.

        A deployer is anomalous when it deployed in the window AND either
        (a) is listed in ``scam_addresses``, or (b) was funded by an
        address in ``scam_addresses`` with an exploit-class threat.
        """
        chains = _chain_ids(chain)
        sql = (
            "SELECT d.chain, d.deployer_eoa AS address, count() AS deploys, "
            "max(d.block_timestamp) AS last_deploy, "
            "if(countIf(s.threat_type IS NOT NULL) > 0, 'self_tainted', 'funder_tainted') AS reason "
            "FROM wallet_memory.deployer_events d "
            "LEFT JOIN wallet_memory.scam_addresses s ON s.address = d.deployer_eoa "
            "WHERE d.block_timestamp >= now() - INTERVAL %(window)s DAY "
            "AND (s.threat_type IN %(threats)s OR d.deployer_eoa IN ("
            "  SELECT t.from_address FROM wallet_memory.evm_transfers t "
            "  JOIN wallet_memory.scam_addresses fs ON fs.address = t.from_address "
            "  WHERE t.to_address = d.deployer_eoa "
            "  AND fs.threat_type IN %(threats)s))"
        )
        params: dict[str, Any] = {
            "window": int(window_days),
            "threats": tuple(EXPLOIT_THREAT_TYPES),
        }
        if chains:
            sql += " AND d.chain IN %(chains)s"
            params["chains"] = chains
        sql += " GROUP BY d.chain, d.deployer_eoa ORDER BY last_deploy DESC"
        sql += f" LIMIT {int(self.anomaly_max)}"
        return self.query(sql, params)

    @_graceful(default=[])
    def anomalous_new_wallets(self, chain: str | None = None, min_txs: int = 10) -> list[dict[str, Any]]:
        """New wallets (first seen < 48h ago) already moving unusual volume.

        Approximated from ``evm_transfers`` with a 30-day lookback: an
        address is "new" when its *earliest* transfer in the lookback is
        within the last 2 days — i.e. it had no observed activity in the
        prior 28 days. This excludes persistent routers/sinks (zero
        address, DEX aggregators, CEX hot wallets) that would otherwise
        look new because ``min()`` is window-relative.
        """
        chains = _chain_ids(chain)
        sql = (
            "SELECT chain, from_address AS address, count() AS txs, "
            "countDistinct(to_address) AS counterparties, "
            "min(block_timestamp) AS first_seen, max(block_timestamp) AS last_seen "
            "FROM wallet_memory.evm_transfers "
            "WHERE block_timestamp >= now() - INTERVAL 30 DAY "
            "AND from_address NOT IN %(sinks)s "
            "AND from_address != to_address "
            "GROUP BY chain, from_address "
            "HAVING txs >= %(min_txs)s AND first_seen >= now() - INTERVAL 2 DAY "
            "ORDER BY txs DESC"
        )
        params: dict[str, Any] = {"min_txs": int(min_txs), "sinks": tuple(EXCLUDED_SINK_ADDRESSES)}
        if chains:
            sql += " AND chain IN %(chains)s"
            params["chains"] = chains
        sql += f" LIMIT {int(self.anomaly_max)}"
        return self.query(sql, params)


# ── External source adapters (best-effort) ─────────────────────────────


@dataclass
class GoPlusSource:
    """Token-security checks via the free GoPlus API (no auth).

    Probed live 2026-08-04: ``GET https://api.gopluslabs.io/api/v1/
    token_security/{chain_id}?contract_addresses=...`` returns a JSON
    object with honeypot / blacklist / tax / ownership flags per token.
    Used to screen tokens a wallet deployed or trades heavily.
    """

    base_url: str = "https://api.gopluslabs.io/api/v1/token_security"
    timeout: float = INTEL_HTTP_TIMEOUT
    client: httpx.AsyncClient | None = None

    # GoPlus chain id map (subset used by the indexer).
    CHAIN_IDS: ClassVar[dict[str, str]] = {
        "ethereum": "1",
        "bsc": "56",
        "polygon": "137",
        "arbitrum": "42161",
        "optimism": "10",
        "base": "8453",
        "avalanche": "43114",
        "fantom": "250",
        "linea": "59144",
        "scroll": "534352",
        "zksync": "324",
        "mantle": "5000",
        "gnosis": "100",
        "celo": "42220",
    }

    def available(self) -> bool:
        return INTEL_EXTERNAL_SOURCES_ENABLED

    async def token_security(
        self, token_address: str, chain: str | None = None
    ) -> dict[str, Any]:
        """Return GoPlus security flags for one token contract.

        Returns ``{"unavailable": True}`` when the chain is unsupported,
        the request fails, or external sources are disabled.
        """
        normalized = normalize_chain(chain)
        chain_id = self.CHAIN_IDS.get(normalized or "")
        if not chain_id:
            return {"unavailable": True, "reason": "chain_not_supported"}
        own_client = self.client is None
        client = self.client or httpx.AsyncClient(timeout=self.timeout)
        try:
            resp = await client.get(
                f"{self.base_url}/{chain_id}",
                params={"contract_addresses": token_address.strip().lower()},
            )
            resp.raise_for_status()
            payload = resp.json()
            result = (payload.get("result") or {}).get(token_address.strip().lower())
            if not result:
                return {"unavailable": True, "reason": "no_result"}
            return _goplus_flags(result)
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug("goplus lookup failed for %s: %s", token_address, exc)
            return {"unavailable": True, "reason": "request_failed"}
        finally:
            if own_client:
                await client.aclose()


def _goplus_flags(result: dict[str, Any]) -> dict[str, Any]:
    """Reduce a GoPlus result to the security-relevant flags."""
    risky = [
        flag
        for flag, value in (
            ("is_honeypot", result.get("is_honeypot")),
            ("is_blacklisted", result.get("is_blacklisted")),
            ("hidden_owner", result.get("hidden_owner")),
            ("owner_change_balance", result.get("owner_change_balance")),
            ("can_take_back_ownership", result.get("can_take_back_ownership")),
            ("is_mintable", result.get("is_mintable")),
            ("transfer_pausable", result.get("transfer_pausable")),
            ("personal_slippage_modifiable", result.get("personal_slippage_modifiable")),
            ("slippage_modifiable", result.get("slippage_modifiable")),
            ("selfdestruct", result.get("selfdestruct")),
            ("is_proxy", result.get("is_proxy")),
            ("external_call", result.get("external_call")),
        )
        if str(value) == "1"
    ]
    return {
        "token_name": result.get("token_name"),
        "token_symbol": result.get("token_symbol"),
        "holder_count": result.get("holder_count"),
        "buy_tax": result.get("buy_tax"),
        "sell_tax": result.get("sell_tax"),
        "creator_percent": result.get("creator_percent"),
        "owner_percent": result.get("owner_percent"),
        "is_open_source": result.get("is_open_source"),
        "risky_flags": risky,
        "risk_flag_count": len(risky),
        "unavailable": False,
    }


@dataclass
class ChainabuseSource:
    """Scam-report lookups via the Chainabuse API (needs API key).

    ``GET https://api.chainabuse.com/v0/reports?address=...``. Without
    ``CHAINABUSE_API_KEY`` the source reports itself unavailable.
    """

    base_url: str = "https://api.chainabuse.com/v0/reports"
    api_key: str = CHAINABUSE_API_KEY
    timeout: float = INTEL_HTTP_TIMEOUT
    client: httpx.AsyncClient | None = None

    def available(self) -> bool:
        return INTEL_EXTERNAL_SOURCES_ENABLED and bool(self.api_key)

    async def lookup(self, address: str, chain: str | None = None) -> list[dict[str, Any]]:
        if not self.available():
            return []
        own_client = self.client is None
        client = self.client or httpx.AsyncClient(timeout=self.timeout)
        try:
            resp = await client.get(
                self.base_url,
                params={"address": address.strip().lower()},
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
            resp.raise_for_status()
            payload = resp.json()
            reports = payload.get("data") or payload.get("results") or []
            return [
                {
                    "source": "chainabuse",
                    "address": address.strip().lower(),
                    "category": r.get("category", "scam"),
                    "description": (r.get("description") or "")[:300],
                    "reported_at": r.get("created_at") or r.get("reported_at"),
                }
                for r in reports
                if isinstance(r, dict)
            ][:10]
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug("chainabuse lookup failed: %s", exc)
            return []
        finally:
            if own_client:
                await client.aclose()


@dataclass
class CryptoscamDBSource:
    """CryptoscamDB public address list (dead/blocked on many networks).

    The official API (``api.cryptoscamdb.org``) returned 502 on probe
    2026-08-04. The adapter keeps the contract so it works when the
    service comes back; matches are also already ingested into the local
    ``scam_addresses`` table via label ingestion.
    """

    base_url: str = "https://api.cryptoscamdb.org/v1/addresses"
    timeout: float = INTEL_HTTP_TIMEOUT
    client: httpx.AsyncClient | None = None

    def available(self) -> bool:
        return INTEL_EXTERNAL_SOURCES_ENABLED

    async def lookup(self, address: str, chain: str | None = None) -> list[dict[str, Any]]:
        if not self.available():
            return []
        own_client = self.client is None
        client = self.client or httpx.AsyncClient(timeout=self.timeout)
        try:
            resp = await client.get(self.base_url)
            resp.raise_for_status()
            payload = resp.json()
            # Shape varies by deployment; tolerate list-of-dicts or dict-of-addresses.
            entries = payload.get("result") or payload.get("data") or payload
            if isinstance(entries, dict):
                row = entries.get(address.strip().lower())
                entries = [row] if row else []
            elif isinstance(entries, list):
                entries = [e for e in entries if isinstance(e, dict) and e.get("address") == address.strip().lower()]
            return [
                {
                    "source": "cryptoscamdb",
                    "address": address.strip().lower(),
                    "category": e.get("category", "scam"),
                    "description": (e.get("description") or "")[:300],
                }
                for e in entries
                if isinstance(e, dict)
            ][:10]
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug("cryptoscamdb lookup failed: %s", exc)
            return []
        finally:
            if own_client:
                await client.aclose()


@dataclass
class RektNewsSource:
    """rekt.news exploit leaderboard (best-effort; slow on some networks).

    ``GET https://api.rekt.news/leaderboard`` returns known DeFi exploit
    summaries. Used to tag hack entities by name. The endpoint timed out
    on probe 2026-08-04; the adapter degrades to unavailable.
    """

    base_url: str = "https://api.rekt.news/leaderboard"
    timeout: float = INTEL_HTTP_TIMEOUT
    client: httpx.AsyncClient | None = None

    def available(self) -> bool:
        return INTEL_EXTERNAL_SOURCES_ENABLED

    async def hacks(self, limit: int = 5) -> list[dict[str, Any]]:
        if not self.available():
            return []
        own_client = self.client is None
        client = self.client or httpx.AsyncClient(timeout=self.timeout)
        try:
            resp = await client.get(self.base_url)
            resp.raise_for_status()
            payload = resp.json()
            entries = payload.get("data") or payload.get("results") or payload
            if not isinstance(entries, list):
                return []
            return [
                {
                    "source": "rekt.news",
                    "name": e.get("name") or e.get("title"),
                    "amount_usd": e.get("amount") or e.get("usd"),
                    "date": e.get("date") or e.get("published_at"),
                }
                for e in entries[:limit]
                if isinstance(e, dict)
            ]
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug("rekt.news lookup failed: %s", exc)
            return []
        finally:
            if own_client:
                await client.aclose()


# ── Facade ─────────────────────────────────────────────────────────────


@dataclass
class DataSources:
    """Combined data source facade used by the intel modules."""

    query: ChQuery
    external_enabled: bool = INTEL_EXTERNAL_SOURCES_ENABLED
    local: LocalIntelSource = field(init=False)
    goplus: GoPlusSource = field(init=False)
    chainabuse: ChainabuseSource = field(init=False)
    cryptoscamdb: CryptoscamDBSource = field(init=False)
    rekt: RektNewsSource = field(init=False)

    def __post_init__(self) -> None:
        self.local = LocalIntelSource(self.query)
        self.goplus = GoPlusSource()
        self.chainabuse = ChainabuseSource()
        self.cryptoscamdb = CryptoscamDBSource()
        self.rekt = RektNewsSource()

    def availability(self) -> list[dict[str, Any]]:
        """Per-source availability report (used in intel responses)."""
        return [
            {"name": "wallet_labels", "local": True, "available": True, "source": "ClickHouse warehouse"},
            {"name": "scam_addresses", "local": True, "available": True, "source": "ClickHouse warehouse"},
            {"name": "wallet_profiles", "local": True, "available": True, "source": "ClickHouse warehouse"},
            {"name": "wallet_entities", "local": True, "available": True, "source": "ClickHouse warehouse"},
            {
                "name": "goplus_token_security",
                "local": False,
                "available": self.external_enabled,
                "source": "api.gopluslabs.io (free, no auth)",
            },
            {
                "name": "chainabuse",
                "local": False,
                "available": self.chainabuse.available(),
                "source": "api.chainabuse.com (CHAINABUSE_API_KEY)",
            },
            {
                "name": "cryptoscamdb",
                "local": False,
                "available": self.external_enabled,
                "source": "api.cryptoscamdb.org (public)",
            },
            {
                "name": "rekt_news",
                "local": False,
                "available": self.external_enabled,
                "source": "api.rekt.news (public)",
            },
        ]
