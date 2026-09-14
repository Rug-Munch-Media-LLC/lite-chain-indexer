"""
Provider quota ledger + degrade ladder.

An ADDITIVE layer on top of the per-endpoint throttling (``rpc/throttle.py``)
and the per-provider token bucket (``rpc/ratelimit.py``): those pace requests
from HTTP headers and request/s budgets, while this module tracks long-window
**account-level quota consumption** (Compute Units / credits / request units)
and returns a degrade level so the caller can back off *before* the 429.

Key differences from the other two layers:

1. **Quota is grouped PER ACCOUNT (``account_id``), NOT per key.** Keyed
   providers meter the free quota per ACCOUNT: N API keys belonging to one
   account draw ONE budget (extra keys buy rate, never quota). An account is
   the registrable domain by default — one account per provider — so multiple
   keys never fan the quota out N×. An operator with several real accounts on
   one host can map key/provider → account via
   ``LITE_INDEXER_ACCOUNT_IDS``; per-KEY rate limits (rps/concurrency) are
   tracked separately in ``rpc/budgets.py`` / ``rpc/provider_caps.py``.
   ProviderD (key-less) is per-IP and gets a per-domain bucket.
   ``account_id_for_endpoint()`` derives the account from the URL;
   ``provider_id_for_endpoint()`` derives the registrable domain; known
   free-tier quotas are seeded in :data:`KNOWN_QUOTAS`.

2. **Tariff-weighted consumption.** ``eth_getLogs`` (75 CU) costs 15x an
   ``eth_chainId`` (5 CU). Each budget carries a ``method_tariff`` mapping
   method → units; ``record_call`` increments ``consumed_units`` by the
   tariff weight, not by 1.

3. **Degrade ladder, not a hard block.** 70% → widen head-follow polls and
   narrow ``eth_getLogs`` windows; 85% → head-only (no backfill/getLogs);
   95% → park the key until ``reset_at``. The ledger *reports* the level —
   it never drops data on the floor. Unknown providers (no documented quota)
   are tracked for stats but never degrade.

State is persisted to ``QUOTA_LEDGER_FILE`` (default ``DATA_DIR/quota_ledger.json``)
with the same atomic-write + debounced-save pattern as ``rpc/throttle.py`` so a
restart does not reset counters mid-period.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lite_indexer.config import DATA_DIR
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Persisted ledger path (default: DATA_DIR/quota_ledger.json).
QUOTA_LEDGER_FILE: str = os.environ.get("QUOTA_LEDGER_FILE", str(DATA_DIR / "quota_ledger.json"))

# Unit types and periods understood by QuotaBudget.
UNIT_TYPES = ("CU", "credits", "request_units", "raw")
PERIODS = ("day", "30d", "month")

# One base tariff unit ≈ 5 CU (matches the documented eth_chainId @ 5 CU and
# eth_getLogs @ 75 CU example — a 15x spread). Used to scale the relative
# tariff table for CU/credits budgets; request-unit budgets use the raw
# relative weights.
CU_PER_UNIT: float = 5.0

# Relative per-method tariff weights. eth_getLogs is 15x eth_chainId. Tron
# method names (wallet/...) are included for request-unit providers.
BASE_TARIFF_UNITS: dict[str, float] = {
    # Cheap, high-frequency calls (1 unit).
    "eth_chainId": 1.0,
    "eth_blockNumber": 1.0,
    "eth_syncing": 1.0,
    "eth_gasPrice": 1.0,
    "eth_getBalance": 1.0,
    "eth_getTransactionCount": 1.0,
    "eth_getCode": 1.0,
    "net_version": 1.0,
    "web3_clientVersion": 1.0,
    "wallet/getnowblock": 1.0,
    "wallet/getblock": 1.0,
    # Moderate (2-4 units).
    "eth_getBlockByNumber": 2.0,
    "eth_getBlockByHash": 2.0,
    "eth_getTransactionByHash": 2.0,
    "eth_getTransactionReceipt": 2.0,
    "eth_getBlockReceipts": 4.0,
    "wallet/gettransactionbyid": 2.0,
    "wallet/getblockbylimitnext": 2.0,
    # eth_call (3 units) — cheap per call but hot in backfill.
    "eth_call": 3.0,
    "wallet/gettransactioninfobyid": 3.0,
    "wallet/gettriggercontractresult": 3.0,
    # Expensive (15-20 units).
    "eth_getLogs": 15.0,
    "eth_getLogsV2": 15.0,
    "trace_block": 20.0,
    "trace_filter": 20.0,
    "trace_transaction": 20.0,
    "eth_traceTransaction": 20.0,
    # Fallback for anything unlisted.
    "default": 1.0,
}

# Degrade ladder: (consumed_pct threshold, level, action description).
# Level 0 = normal; 3 = key parked until reset_at.
DEGRADE_LADDER: tuple[tuple[float, int, str], ...] = (
    (70.0, 1, "widen head-follow polls + narrow eth_getLogs windows"),
    (85.0, 2, "head-only: no backfill or getLogs"),
    (95.0, 3, "park key until reset_at"),
)


def degrade_action(level: int) -> str:
    """Human-readable action for a degrade level (0 = normal).

    Returns the action for the HIGHEST threshold met (a parked key reports
    the level-3 action, not the level-1 one).
    """
    for _threshold, lvl, action in reversed(DEGRADE_LADDER):
        if level >= lvl:
            return action
    return "normal operation"


@dataclass(frozen=True)
class QuotaSeed:
    """Documented free-tier quota used to seed a provider budget."""

    budget_units: int
    unit_type: str
    period: str


# Free-tier quota seeds, supplied by the operator (private) as a JSON object
# in ``LITE_INDEXER_QUOTA_SEEDS`` keyed by PROVIDER ID (the registrable
# domain, e.g. ``"example-rpc.com"``)::
#
#   {"example-rpc.com": {"budget_units": 30000000, "unit_type": "CU", "period": "month"}}
#
# Empty by default: endpoints are tracked but never degraded (the degrade
# ladder is opt-in via seeds). Per-key caps in ``rpc/provider_caps.py`` work
# regardless.
def _load_known_quotas() -> dict[str, QuotaSeed]:
    import json

    raw = os.environ.get("LITE_INDEXER_QUOTA_SEEDS", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("quota_seeds_parse_failed")
        return {}
    if not isinstance(data, dict):
        return {}
    seeds: dict[str, QuotaSeed] = {}
    for provider_id, spec in data.items():
        if not isinstance(spec, dict):
            continue
        try:
            seeds[str(provider_id)] = QuotaSeed(
                budget_units=int(spec["budget_units"]),
                unit_type=str(spec["unit_type"]),
                period=str(spec["period"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
    return seeds


KNOWN_QUOTAS: dict[str, QuotaSeed] = _load_known_quotas()


def _load_account_ids() -> dict[str, str]:
    """Operator-supplied ``key_id``/``provider_id`` → ``account_id`` overrides.

    Read from ``LITE_INDEXER_ACCOUNT_IDS`` (a JSON object). Both keys are
    non-secret: a ``provider_id`` (registrable domain) groups all of that
    provider's keys into one named account, while a ``key_id`` (the hashed
    id from :func:`key_id_for_endpoint`) maps one key to a specific account.
    Empty by default — every provider then collapses to a single account id
    equal to its ``provider_id`` (one account per provider).
    """
    raw = os.environ.get("LITE_INDEXER_ACCOUNT_IDS", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("account_ids_parse_failed")
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items() if str(v)}


KNOWN_ACCOUNT_IDS: dict[str, str] = _load_account_ids()

# Dataclass field names used to filter persisted JSON on load (survives
# schema drift by ignoring unknown keys).
_QUOTA_FIELDS = frozenset(
    {
        "provider_id",
        "account_id",
        "key_id",
        "unit_type",
        "period",
        "budget_units",
        "consumed_units",
        "method_tariff",
        "degrade_level",
        "reset_at",
        "updated_at",
    }
)


@dataclass
class QuotaBudget:
    """Persisted per-ACCOUNT quota state.

    ``budget_units`` is ``None`` for providers without a documented free
    quota — they are tracked for stats but never degrade (graceful by
    default). ``consumed_units`` is tariff-weighted: an ``eth_getLogs``
    at 75 CU costs 15x an ``eth_chainId`` at 5 CU.

    ``account_id`` is what the quota is keyed on (NOT ``key_id``): all keys
    of one account share this budget. It defaults to ``provider_id`` (one
    account per provider) so keys are never counted as separate capacity.
    ``key_id`` is retained as non-secret provenance only — per-key rate
    limits live in ``rpc/budgets.py`` / ``rpc/provider_caps.py``.
    """

    provider_id: str
    account_id: str = ""
    key_id: str = ""
    unit_type: str = "raw"  # CU | credits | request_units | raw
    period: str = "day"  # day | 30d | month
    budget_units: int | None = None
    consumed_units: float = 0.0
    method_tariff: dict[str, float] = field(default_factory=dict)
    degrade_level: int = 0
    reset_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        # One account per provider unless the operator named a real account.
        if not self.account_id:
            self.account_id = self.provider_id

    def consumed_pct(self) -> float:
        """Percent of the budget consumed (None-safe → 0.0, can exceed 100)."""
        if not self.budget_units:
            return 0.0
        return 100.0 * self.consumed_units / self.budget_units

    def parked(self, now: float | None = None) -> bool:
        """Whether this account is parked until ``reset_at`` (level 3)."""
        now = time.time() if now is None else now
        return self.degrade_level >= 3 and now < self.reset_at


def _registrable_domain(host: str) -> str:
    """Collapse ``*.example-rpc.com`` subdomains to ``example-rpc.com``.

    Generic account grouping without a baked-in provider list: all chain
    subdomains of one provider collapse to one budget; per-key separation is
    handled by :func:`key_id_for_endpoint`.
    """
    parts = [p for p in host.lower().split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else (host or "unknown")


def provider_id_for_endpoint(endpoint: str) -> str:
    """Derive the account-level provider id from an endpoint URL.

    The id is the registrable domain (``eth-mainnet.g.example.com`` →
    ``example.com``), so all chain subdomains of one provider share a budget
    while different providers stay separate. Key-less hosts group by their
    own domain and never degrade unless the operator seeds a quota.
    """
    from urllib.parse import urlparse

    return _registrable_domain(urlparse(endpoint).hostname or "")


#: Query params that carry a provider API key (generic, provider-agnostic).
_KEY_QUERY_PARAMS: tuple[str, ...] = ("dkey", "apikey", "api-key", "api_key", "key", "token")
#: A path segment this long and token-shaped is treated as an embedded key.
_KEY_PATH_RE = re.compile(r"^[A-Za-z0-9_\-]{16,}$")
#: ``name=<token>`` path segments (e.g. ``api=<KEY>``, ``key=<KEY>``).
_KEY_NAME_SEG_RE = re.compile(r"^[A-Za-z0-9_.\-]+=(.+)$")


def key_id_for_endpoint(endpoint: str) -> str:
    """Best-effort, NON-SECRET API-key id for a keyed endpoint.

    Keys are detected generically, in this precedence order:

    1. a recognised key query param
       (``dkey``/``apikey``/``api-key``/``api_key``/``key``/``token``);
    2. URL **userinfo** (``user[:pass]@host``) — the username is hashed, or
       ``user:pass`` when a password is present;
    3. the longest token-shaped path segment **anywhere** in the path
       (``/v2/<KEY>``, ``/rpc/<KEY>``, ``/<acct>/<KEY>/...``) or the value of
       a ``name=<token>`` segment (``api=<KEY>``).

    The raw value is HASHED (sha256, first 16 hex chars) so the id is stable
    per key-account but the secret is never persisted in the quota ledger or
    exposed in limiter state.

    Empty for key-less endpoints — they group by provider_id alone.
    """
    from urllib.parse import parse_qs, unquote, urlparse

    parsed = urlparse(endpoint)

    # 1. Explicit query-param key (highest precedence, existing behaviour).
    raw = ""
    qs = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    for param in _KEY_QUERY_PARAMS:
        values = qs.get(param)
        if values:
            raw = values[0]
            break

    # 2. URL userinfo (e.g. https://user:pass@host/...).
    if not raw and parsed.username is not None:
        user = unquote(parsed.username)
        password = unquote(parsed.password) if parsed.password is not None else ""
        raw = f"{user}:{password}" if password else user

    # 3. Path candidates: token-shaped segments or name=value values.
    if not raw:
        candidates: list[str] = []
        for segment in parsed.path.split("/"):
            if not segment:
                continue
            name_match = _KEY_NAME_SEG_RE.match(segment)
            if name_match:
                candidates.append(name_match.group(1))
            elif _KEY_PATH_RE.match(segment):
                candidates.append(segment)
        if candidates:
            raw = max(candidates, key=len)

    if not raw:
        return ""
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def account_id_for_endpoint(endpoint: str) -> str:
    """Resolve the quota ACCOUNT for an endpoint (never empty).

    Quota is metered per ACCOUNT, not per key: N keys belonging to one
    account share ONE budget, so extra keys add rate but never quota. The
    account is resolved as:

    1. a configured ``key_id`` override (:data:`KNOWN_ACCOUNT_IDS`) — lets an
       operator map one key to a named account;
    2. a configured ``provider_id`` override — groups every key of a provider
       into one named account;
    3. otherwise the ``provider_id`` itself (one account per provider).

    The ``key_id`` is NEVER returned, so quota can never be fanned out across
    keys. Per-key rate limits are tracked separately (``rpc/budgets.py``).
    """
    provider = provider_id_for_endpoint(endpoint)
    key_id = key_id_for_endpoint(endpoint)
    if key_id and key_id in KNOWN_ACCOUNT_IDS:
        return KNOWN_ACCOUNT_IDS[key_id]
    return KNOWN_ACCOUNT_IDS.get(provider, provider)


class ProviderLedger:
    """Persisted per-account quota ledger + degrade ladder.

    Single-writer, JSON-on-disk, debounced saves — same contract as
    :class:`~lite_indexer.rpc.throttle.RPCRateTracker`: every mutation
    bumps ``updated_at`` and the ledger flushes to :data:`QUOTA_LEDGER_FILE`
    at most once per ``persist_interval``. Load is best-effort; a corrupt
    file degrades to an empty ledger rather than crashing the indexer.

    The ledger is a **reporter**, not an enforcer: callers consult
    ``degrade_level_for`` / the return value of :meth:`record_call` and
    decide how to honor the ladder. Unknown providers are never degraded.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        persist_interval: float = 60.0,
    ) -> None:
        self._path = Path(path or QUOTA_LEDGER_FILE)
        self._persist_interval = persist_interval
        self._last_persist: float = 0.0
        self._budgets: dict[str, QuotaBudget] = {}
        self._load()

    # ── Keying ────────────────────────────────────────────────────────

    @staticmethod
    def _budget_key(provider_id: str, account_id: str | None = None) -> str:
        return f"{provider_id}:{account_id or provider_id}"

    # ── Persistence ───────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            for data in raw.values():
                if not isinstance(data, dict):
                    continue
                clean = {k: v for k, v in data.items() if k in _QUOTA_FIELDS}
                budget = QuotaBudget(**clean)
                key = self._budget_key(budget.provider_id, budget.account_id)
                existing = self._budgets.get(key)
                if existing is None:
                    self._budgets[key] = budget
                    continue
                # Migration: a pre-account ledger wrote one budget PER key.
                # Fold those into the single account budget so an upgrade
                # neither loses consumption nor resurrects the N× over-count.
                existing.consumed_units += budget.consumed_units
                existing.degrade_level = max(existing.degrade_level, budget.degrade_level)
                existing.reset_at = max(existing.reset_at, budget.reset_at)
                existing.updated_at = max(existing.updated_at, budget.updated_at)
            if self._budgets:
                logger.debug("quota_ledger_loaded", budgets=len(self._budgets), path=str(self._path))
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.warning("quota_ledger_load_failed", path=str(self._path), error=str(exc)[:80])

    def persist(self) -> None:
        """Atomically write the ledger state to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {key: asdict(budget) for key, budget in self._budgets.items()},
                indent=2,
            )
            tmp = self._path.with_suffix(".json.tmp")
            tmp.write_text(payload)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("quota_ledger_save_failed", path=str(self._path), error=str(exc)[:80])

    def persist_if_stale(self, now: float | None = None) -> None:
        """Persist when the last write is older than the interval.

        Debounces disk writes so a burst of ``record_call`` traffic flushes
        at most once per ``persist_interval`` (a crash loses at most that
        much consumption tracking).
        """
        now = time.time() if now is None else now
        if now - self._last_persist >= self._persist_interval:
            self._last_persist = now
            self.persist()

    # ── Budget lifecycle ──────────────────────────────────────────────

    @staticmethod
    def _build_tariff(unit_type: str) -> dict[str, float]:
        """Scale the relative tariff table for the budget's unit type.

        CU/credits budgets multiply by :data:`CU_PER_UNIT` so the base
        ``eth_chainId`` unit costs 5 CU and ``eth_getLogs`` 75 CU;
        request-unit/raw budgets use the relative weights directly.
        """
        scale = CU_PER_UNIT if unit_type in {"CU", "credits"} else 1.0
        return {method: round(weight * scale, 4) for method, weight in BASE_TARIFF_UNITS.items()}

    @staticmethod
    def _compute_reset_at(period: str, now: float | None = None) -> float:
        """Epoch time the quota window resets.

        ``day``/``month`` align to UTC calendar boundaries (provider windows
        reset on those); ``30d`` uses a rolling window from seeding time.
        """
        now = time.time() if now is None else now
        if period == "day":
            dt = datetime.fromtimestamp(now, UTC)
            return (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        if period == "month":
            dt = datetime.fromtimestamp(now, UTC)
            year = dt.year + (1 if dt.month == 12 else 0)
            month = dt.month % 12 + 1
            return datetime(year, month, 1, tzinfo=UTC).timestamp()
        days = 30 if period == "30d" else 1
        return now + days * 86_400

    def _seed_budget(self, provider_id: str, account_id: str, key_id: str | None = None) -> QuotaBudget:
        seed = KNOWN_QUOTAS.get(provider_id)
        now = time.time()
        if seed is None:
            budget_units = None
            unit_type = "raw"
            period = "day"
        else:
            budget_units = seed.budget_units
            unit_type = seed.unit_type
            period = seed.period
        return QuotaBudget(
            provider_id=provider_id,
            account_id=account_id or provider_id,
            key_id=key_id or "",
            unit_type=unit_type,
            period=period,
            budget_units=budget_units,
            consumed_units=0.0,
            method_tariff=self._build_tariff(unit_type),
            degrade_level=0,
            reset_at=self._compute_reset_at(period, now),
            updated_at=now,
        )

    def _ensure_budget(
        self,
        provider_id: str,
        key_id: str | None = None,
        account_id: str | None = None,
    ) -> QuotaBudget:
        """Return the ACCOUNT budget, seeding it once per account.

        ``key_id`` is provenance only; the budget is keyed on ``account_id``
        (defaulting to ``provider_id``), so N keys of one account share ONE
        budget instead of N copies of the same quota.
        """
        account = account_id or provider_id
        key = self._budget_key(provider_id, account)
        budget = self._budgets.get(key)
        if budget is None:
            budget = self._seed_budget(provider_id, account, key_id)
            self._budgets[key] = budget
            logger.debug("quota_budget_seeded", provider=provider_id, account_id=account, key_id=key_id or "")
        return budget

    def _maybe_reset(self, budget: QuotaBudget, now: float | None = None) -> None:
        """Roll a budget into a fresh window when ``reset_at`` has passed."""
        now = time.time() if now is None else now
        if now >= budget.reset_at:
            budget.consumed_units = 0.0
            budget.degrade_level = 0
            budget.reset_at = self._compute_reset_at(budget.period, now)
            budget.updated_at = now
            logger.info(
                "quota_period_reset",
                provider=budget.provider_id,
                account_id=budget.account_id,
                key_id=budget.key_id,
                next_reset_at=budget.reset_at,
            )

    @staticmethod
    def _degrade_level(pct: float) -> int:
        if pct >= 95.0:
            return 3
        if pct >= 85.0:
            return 2
        if pct >= 70.0:
            return 1
        return 0

    @staticmethod
    def _tariff_for(budget: QuotaBudget, method: str) -> float:
        return budget.method_tariff.get(method, budget.method_tariff.get("default", 1.0))

    # ── Public API ────────────────────────────────────────────────────

    def record_call(
        self,
        provider_id: str,
        method: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> int:
        """Record one call and return the resulting degrade level (0-3).

        Increments ``consumed_units`` by the method's tariff weight
        (e.g. ``eth_getLogs`` 15x ``eth_chainId`` for CU budgets) and
        recomputes the degrade ladder level.

        The call is charged to the ACCOUNT budget (``account_id``, defaulting
        to ``provider_id``): two keys of one account draw the same budget.

        Args:
            provider_id: Provider id (registrable domain).
            method: JSON-RPC method name (``"eth_getLogs"``, ``"eth_call"``).
            key_id: Optional API-key id — provenance only; NOT a quota key.
            now: Epoch time override (tests).
            account_id: Optional account id; defaults to ``provider_id`` so
                keys never split one account's quota.

        Returns:
            Degrade level: 0 normal, 1 (70%) widen polls/narrow getLogs,
            2 (85%) head-only, 3 (95%) park account until ``reset_at``.
        """
        now = time.time() if now is None else now
        budget = self._ensure_budget(provider_id, key_id, account_id)
        self._maybe_reset(budget, now)
        if budget.budget_units is not None:
            budget.consumed_units += self._tariff_for(budget, method)
        budget.updated_at = now
        budget.degrade_level = self._degrade_level(budget.consumed_pct())
        self.persist_if_stale(now)
        return budget.degrade_level

    def record_endpoint_call(self, endpoint: str, method: str, now: float | None = None) -> int:
        """Record a call from an endpoint URL (derives account/key id).

        Convenience wrapper for the RPC layer: ``account_id_for_endpoint``
        groups all keys of one provider account into a single budget while
        unknown/key-less hosts get a per-domain bucket (never degraded).
        """
        return self.record_call(
            provider_id_for_endpoint(endpoint),
            method,
            key_id_for_endpoint(endpoint),
            now,
            account_id=account_id_for_endpoint(endpoint),
        )

    def degrade_level_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> int:
        """Current degrade level (0-3) for an account without recording a call."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return 0
        now = time.time() if now is None else now
        self._maybe_reset(budget, now)
        budget.degrade_level = self._degrade_level(budget.consumed_pct())
        return budget.degrade_level

    def consumed_pct_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        *,
        account_id: str | None = None,
    ) -> float:
        """Percent of the account budget consumed (0.0 for unknown/unmanaged)."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return 0.0
        return budget.consumed_pct()

    def parked(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> bool:
        """Whether the account is parked (level 3) until ``reset_at``."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return False
        now = time.time() if now is None else now
        return budget.parked(now)

    def state_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> dict[str, Any]:
        """Public read-only view of one account's quota state."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return {}
        now = time.time() if now is None else now
        self._maybe_reset(budget, now)
        return {
            "provider_id": budget.provider_id,
            "account_id": budget.account_id,
            "key_id": budget.key_id,
            "unit_type": budget.unit_type,
            "period": budget.period,
            "budget_units": budget.budget_units,
            "consumed_units": round(budget.consumed_units, 2),
            "consumed_pct": round(budget.consumed_pct(), 1),
            "degrade_level": budget.degrade_level,
            "degrade_action": degrade_action(budget.degrade_level),
            "parked": budget.parked(now),
            "reset_at": budget.reset_at,
            "updated_at": budget.updated_at,
        }

    def snapshot(self, now: float | None = None) -> dict[str, dict[str, Any]]:
        """Full ledger view (for logging / metrics)."""
        return {
            key: self.state_for(budget.provider_id, budget.key_id, now, account_id=budget.account_id)
            for key, budget in self._budgets.items()
        }

    def reset(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> None:
        """Manually zero an ACCOUNT budget (e.g. operator top-up) and persist."""
        budget = self._ensure_budget(provider_id, key_id, account_id)
        now = time.time() if now is None else now
        budget.consumed_units = 0.0
        budget.degrade_level = 0
        budget.reset_at = self._compute_reset_at(budget.period, now)
        budget.updated_at = now
        self.persist()
        logger.info("quota_budget_reset", provider=provider_id, account_id=budget.account_id, key_id=key_id or "")


# Module-level ledger shared across the codebase (persists to disk).
LEDGER = ProviderLedger()


def record_call(provider_id: str, method: str, key_id: str | None = None) -> int:
    """Record a call in the shared ledger (convenience wrapper).

    Args:
        provider_id: Provider id (registrable domain).
        method: JSON-RPC method name.
        key_id: Optional API-key id (provenance; quota is per account).

    Returns:
        Degrade level (0-3) after recording.
    """
    return LEDGER.record_call(provider_id, method, key_id)


def record_endpoint_call(endpoint: str, method: str) -> int:
    """Record a call from an endpoint URL in the shared ledger.

    Derives the account id (per-account quota) and key id (provenance) from
    the URL.

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.

    Returns:
        Degrade level (0-3) after recording.
    """
    return LEDGER.record_endpoint_call(endpoint, method)


# Methods deferred at level 2 (85% consumed, head-only). ``debug_trace*``
# and ``trace_*`` family members match by prefix via :func:`_is_heavy_method`.
HEAVY_DEFER_METHODS: frozenset[str] = frozenset(
    {
        "eth_getLogs",
        "eth_getLogsV2",
        "eth_getBlockReceipts",
        "getSignaturesForAddress",
        "getBlock",
        "getTransaction",
        "trace_block",
        "trace_filter",
        "trace_transaction",
        "eth_traceTransaction",
    }
)

# Level-1 (70%) wide-scan threshold: ``eth_getLogs`` ranges wider than this
# many blocks are treated as backfill-style wide scans and deferred; narrow
# head-follow windows stay allowed.
LEVEL1_GETLOGS_RANGE_BLOCKS: int = 1000


def _is_heavy_method(method: str) -> bool:
    """Whether a method is in the level-2 heavy-defer set (prefix-aware)."""
    return method in HEAVY_DEFER_METHODS or method.startswith("debug_trace")


def defer_reason(
    provider_id: str,
    method: str,
    key_id: str | None = None,
    range_blocks: int | None = None,
    *,
    account_id: str | None = None,
) -> str | None:
    """Reason string when a call should be DEFERRED, else ``None``.

    Consults the shared ledger's current degrade level without recording a
    call — a deferred call was never dispatched, so it must not consume
    quota. This is the enforcement hook for the degrade ladder (the ledger
    itself stays reporter-only; callers decide how to honor the reason).

    Ladder:
      * level 3 (parked) → defer ALL methods
      * level 2 (85%)    → defer heavy methods only (see
                           :data:`HEAVY_DEFER_METHODS`)
      * level 1 (70%)    → defer backfill-style wide ``eth_getLogs`` scans
                           only (``range_blocks`` > 1000); head-follow allowed
      * level 0 / unknown providers (no seeded quota) → never deferred

    Args:
        provider_id: Provider id (registrable domain).
        method: JSON-RPC method name.
        key_id: Optional API-key id (provenance only; quota is per account).
        range_blocks: Optional block range for ``eth_getLogs`` so level 1
            can tell a wide backfill scan from a narrow head-follow window.
            Ignored for other methods; level-1 never defers when the range
            is unknown (``None``).
        account_id: Optional account id; defaults to ``provider_id``.

    Returns:
        Reason string when the call should be skipped, else ``None``.
        Never raises: on any ledger error it degrades to ``None`` (allow).
    """
    try:
        level = LEDGER.degrade_level_for(provider_id, key_id, account_id=account_id)
        if level <= 0:
            return None
        if level >= 3:
            return f"quota_deferred_level3_parked ({degrade_action(3)})"
        if level == 2:
            if _is_heavy_method(method):
                return f"quota_deferred_level2_head_only ({degrade_action(2)})"
            return None
        # level == 1 — wide backfill scans only; head-follow stays allowed.
        if (
            method in {"eth_getLogs", "eth_getLogsV2"}
            and range_blocks is not None
            and range_blocks > LEVEL1_GETLOGS_RANGE_BLOCKS
        ):
            return f"quota_deferred_level1_wide_scan ({degrade_action(1)})"
        return None
    except Exception as exc:  # noqa: BLE001 — enforcement must never raise
        logger.debug("quota_defer_reason_skipped", provider=provider_id, error=str(exc)[:80])
        return None


__all__ = [
    "CU_PER_UNIT",
    "DEGRADE_LADDER",
    "HEAVY_DEFER_METHODS",
    "KNOWN_ACCOUNT_IDS",
    "KNOWN_QUOTAS",
    "LEDGER",
    "LEVEL1_GETLOGS_RANGE_BLOCKS",
    "ProviderLedger",
    "QuotaBudget",
    "account_id_for_endpoint",
    "defer_reason",
    "degrade_action",
    "key_id_for_endpoint",
    "provider_id_for_endpoint",
    "record_call",
    "record_endpoint_call",
]
