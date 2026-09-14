"""
Endpoint capability probing — archive support, getLogs range, block receipts.

Discovers per-endpoint service capabilities beyond raw liveness so the
pool can make admission / chunk-sizing decisions that are currently
blind guesses:

  - ``archive`` — whether the node can serve state at genesis-era
    blocks (``eth_getBalance`` at block 1 / 256). Backfill and
    historical scans can only be routed to archive-capable endpoints.
  - ``getlogs_max_range`` — the largest ``eth_getLogs`` block range the
    endpoint accepts (binary-searched; a client-side timeout is
    classified as a range error because a slow endpoint is effectively
    range-limited at the requested size).
  - ``block_receipts`` — whether ``eth_getBlockReceipts`` is supported
    (one-call receipt fetches; unsupported endpoints must fall back to
    the per-tx loop in ``lite_indexer.strategies.evm.receipts``).
  - ``cu_tariff`` — seeded per-method compute-unit weights from
    provider docs (KeyedProvider-style CU table) for cost-aware endpoint
    ranking.

Each probe is best-effort and never raises: it records what it can and
leaves unknown fields at their defaults. Results are persisted per
endpoint in a module-level :class:`CapabilityRecord` registry keyed by
endpoint URL (the same registry the pool admission / routing layer can
consult via :func:`get_capability`).
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc.archive_routing import (
    ARCHIVE_METHOD_PREFIXES,
    ARCHIVE_METHODS,
    ArchiveRoutingError,
    NoArchiveEndpointError,
    archive_capable_endpoints,
    archive_guard_reason,
    is_archive_capable,
    method_requires_archive,
)

if TYPE_CHECKING:  # pragma: no cover — annotation-only, avoids import cycle
    from lite_indexer.rpc.client import RpcClient

logger = get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────
# Timeout for each individual capability probe call. Verification uses a
# separate, tighter liveness timeout; capability probes are allowed
# slightly more slack but must stay well under the RPC call timeout so a
# slow endpoint cannot stall the verification cycle.
CAPABILITY_PROBE_TIMEOUT = 8.0

# eth_getLogs range ladder probed before binary search kicks in.
GETLOGS_RANGE_LADDER: tuple[int, ...] = (10, 100, 1000, 10000)
# Hard cap on binary-search steps between two ladder rungs (bounds cost).
GETLOGS_BINARY_MAX_STEPS = 8

# Well-known, funded EOA (vitalik.eth) — a balance at genesis-era blocks
# only resolves on an archive-capable node; non-archive nodes error with
# a missing-trie-node / header-not-found response.
KNOWN_FUNDED_ADDRESS = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

# Standard ERC-20 Transfer topic — dense enough that a probe range is
# representative of real getLogs work while still returning fast.
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Seeded per-method compute-unit weights (provider-docs CU table, e.g.
# KeyedProvider-style pricing). eth_getBlockReceipts is not in most public CU
# tables — the ~50 estimate assumes ~5 receipt-equivalent units per
# returned receipt at typical block density; revisit when provider docs
# publish an authoritative figure.
SEEDED_CU_TARIFF: dict[str, float] = {
    "eth_chainId": 5.0,
    "eth_blockNumber": 5.0,
    "eth_getBalance": 10.0,
    "eth_getCode": 25.0,
    "eth_call": 26.0,
    "eth_getBlockByNumber": 16.0,
    "eth_getTransactionReceipt": 10.0,
    "eth_getLogs": 75.0,
    "eth_getBlockReceipts": 50.0,
}


class _GetLogsProbeStatus(enum.Enum):
    """Outcome of a single eth_getLogs probe request."""

    OK = "ok"
    RANGE_ERROR = "range_error"
    OTHER_ERROR = "other_error"


class GetLogsCapability(enum.Enum):
    """Broad eth_getLogs capability classification for an endpoint.

    Distinct from a transient failure: ``CANNOT_SERVE_LOGS`` is a permanent
    property of the endpoint for the getLogs method (the provider rejects
    the request shape/capability rather than merely a too-large range), so it
    must never be counted as an abuse signal / ban.
    """

    CAPABLE = "capable"
    CANNOT_SERVE_LOGS = "cannot_serve_logs"
    TRANSIENT = "transient"


# Capability-rejection signals: an endpoint that answers eth_blockNumber but
# structurally refuses broad topic-only eth_getLogs. Verified live
# (2026-09-14) the public pool contains such endpoints; without this gate
# they pollute the log path and get mis-counted as bans until the pool burns
# down to the few working endpoints.
#   - JSON-RPC error code -32701 ("Please specify an address ...")
#   - HTTP 405 (method not allowed)
#   - auth/tenant/key refusals and malformed-range refusals
CAPABILITY_HTTP_STATUSES: frozenset[int] = frozenset({405})
CAPABILITY_ERROR_CODES: frozenset[int] = frozenset({-32701})
CAPABILITY_ERROR_MARKERS: tuple[str, ...] = (
    "must authenticate",
    "unauthorized",
    "invalid block range",
    "api key disabled",
    "tenant disabled",
    "please specify an address",
)


def _error_code_and_message(payload: Any) -> tuple[int | None, str]:
    """Extract ``(code, message)`` from a JSON-RPC error payload.

    Handles both the spec object form and the plain-string form some RPCs
    emit (polygon-rpc.com ``"API key disabled, tenant disabled"``).
    """
    if not isinstance(payload, dict):
        return None, ""
    err = payload.get("error")
    if err is None:
        return None, ""
    if isinstance(err, dict):
        code = err.get("code")
        try:
            code_int = int(code) if code is not None else None
        except (TypeError, ValueError):
            code_int = None
        return code_int, str(err.get("message", err))
    return None, str(err)


def classify_logs_capability(http_status: int | None, payload: Any) -> GetLogsCapability:
    """Classify one eth_getLogs response for capability gating.

    Returns :data:`GetLogsCapability.CAPABLE` only when the response carries
    a real ``result`` list; ``CANNOT_SERVE_LOGS`` for structural capability
    refusals (see :data:`CAPABILITY_ERROR_CODES` / markers / HTTP 405);
    ``TRANSIENT`` for everything else (timeouts, 5xx, malformed payloads,
    genuine range limits — none of which should permanently exclude the
    endpoint).
    """
    if http_status in CAPABILITY_HTTP_STATUSES:
        return GetLogsCapability.CANNOT_SERVE_LOGS
    if not isinstance(payload, dict):
        return GetLogsCapability.TRANSIENT
    if isinstance(payload.get("result"), list):
        return GetLogsCapability.CAPABLE
    code, message = _error_code_and_message(payload)
    if code is not None and code in CAPABILITY_ERROR_CODES:
        return GetLogsCapability.CANNOT_SERVE_LOGS
    lowered = message.lower()
    if any(marker in lowered for marker in CAPABILITY_ERROR_MARKERS):
        return GetLogsCapability.CANNOT_SERVE_LOGS
    return GetLogsCapability.TRANSIENT


def is_logs_capability_failure(http_status: int | None, payload: Any) -> bool:
    """Whether a getLogs response is a structural capability refusal."""
    return classify_logs_capability(http_status, payload) is GetLogsCapability.CANNOT_SERVE_LOGS


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


# ── Registry ───────────────────────────────────────────────────────────
# endpoint URL -> CapabilityRecord. The pool admission / routing layer
# reads this via get_capability() to prefer archive endpoints for
# historical ranges and to size getLogs chunks within the probed maximum.
_CAPABILITY_RECORDS: dict[str, CapabilityRecord] = {}


def get_capability(endpoint: str) -> CapabilityRecord | None:
    """Return the persisted capability record for ``endpoint`` (or None)."""
    return _CAPABILITY_RECORDS.get(endpoint)


def set_capability(record: CapabilityRecord) -> None:
    """Persist a capability record for its endpoint URL."""
    _CAPABILITY_RECORDS[record.endpoint] = record


def capability_records() -> dict[str, CapabilityRecord]:
    """Return a copy of the endpoint capability registry."""
    return dict(_CAPABILITY_RECORDS)


def set_logs_capability(endpoint: str, chain: str, capable: bool) -> CapabilityRecord:
    """Persist the broad-getLogs capability for one endpoint.

    Creates a record when none exists (chain + endpoint only), preserving
    any previously probed archive / range fields.
    """
    record = _CAPABILITY_RECORDS.get(endpoint)
    if record is None:
        record = CapabilityRecord(endpoint=endpoint, chain=chain)
    record.logs_capable = capable
    record.probed_at = time.time()
    _CAPABILITY_RECORDS[endpoint] = record
    return record


def is_logs_capability_excluded(endpoint: str) -> bool:
    """Whether ``endpoint`` is known to reject broad eth_getLogs.

    A capability-excluded endpoint must never receive ``eth_getLogs`` (it is
    permanently tip/head-only). Unknown or transiently-failed endpoints
    return False so a single timeout never removes healthy capacity.
    """
    record = _CAPABILITY_RECORDS.get(endpoint)
    return record is not None and record.logs_capable is False


def logs_capable_endpoints(endpoints: list[str]) -> list[str]:
    """Filter ``endpoints`` to those not capability-excluded from getLogs."""
    return [ep for ep in endpoints if not is_logs_capability_excluded(ep)]


# ── Raw eth_getLogs probe helper ──────────────────────────────────────


async def _probe_getlogs_once(
    endpoint: str,
    from_block: int,
    to_block: int,
    topic0: str,
    timeout: float = CAPABILITY_PROBE_TIMEOUT,
) -> _GetLogsProbeStatus:
    """Issue a single topic-only eth_getLogs request over ``[from, to]``.

    Uses a fresh short-timeout httpx client (mirrors
    ``lite_indexer.rpc.health.verify_endpoint``). A client-side timeout is
    classified as a :data:`_GetLogsProbeStatus.RANGE_ERROR` — an endpoint
    that cannot answer a range in time is effectively range-limited at
    that size for the purposes of chunk sizing.

    Returns:
        OK for a valid list/null result, RANGE_ERROR for range/timeout
        failures, OTHER_ERROR for anything else (method not found etc.).
    """
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [topic0],
            }
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(endpoint, json=payload)
            data = resp.json()
    except httpx.TimeoutException:
        logger.debug("capability_getlogs_timeout", endpoint=endpoint[:60], from_block=from_block, to_block=to_block)
        return _GetLogsProbeStatus.RANGE_ERROR
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.debug(
            "capability_getlogs_transport_error",
            endpoint=endpoint[:60],
            error=str(exc)[:80],
        )
        return _GetLogsProbeStatus.OTHER_ERROR

    if not isinstance(data, dict):
        return _GetLogsProbeStatus.OTHER_ERROR
    if "result" in data and not data.get("error"):
        return _GetLogsProbeStatus.OK

    err = data.get("error") or {}
    err_msg = str(err.get("message", err)) if isinstance(err, dict) else str(err)
    lowered = err_msg.lower()
    if any(token in lowered for token in ("range", "limit", "too large", "exceeds", "timeout", "max")):
        return _GetLogsProbeStatus.RANGE_ERROR
    return _GetLogsProbeStatus.OTHER_ERROR


# ── Probes ─────────────────────────────────────────────────────────────


async def probe_logs_capability(
    endpoint: str,
    tip: int | None,
    timeout: float = CAPABILITY_PROBE_TIMEOUT,
) -> GetLogsCapability:
    """Probe broad, topic-only ``eth_getLogs`` over a real tip range.

    Issues exactly the request shape the indexer uses in production
    (``fromBlock = tip - 2``, ``toBlock = "latest"``, topic-only) and
    requires a ``result`` **list**. Endpoints that answer eth_blockNumber
    but reject this shape (publicnode's -32701, HTTP 405, auth/tenant
    refusals, malformed-range refusals) classify as
    :data:`GetLogsCapability.CANNOT_SERVE_LOGS`; transport/timeout/5xx
    classify as TRANSIENT so a flaky endpoint is not permanently excluded.
    When ``tip`` is unknown the probe cannot anchor a real range and returns
    TRANSIENT (never exclude on missing data).

    Args:
        endpoint: RPC endpoint URL.
        tip: Chain tip block number to anchor the probe range (None when
            unresolvable).
        timeout: Per-request timeout in seconds.

    Returns:
        The capability classification for this endpoint.
    """
    if tip is None:
        logger.debug("capability_logs_probe_no_tip", endpoint=endpoint[:60])
        return GetLogsCapability.TRANSIENT
    from_block = hex(max(0, int(tip) - 2))
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{"fromBlock": from_block, "toBlock": "latest"}],
    }
    status_code: int | None = None
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(endpoint, json=payload)
            status_code = resp.status_code
            data = resp.json()
    except httpx.TimeoutException:
        logger.debug("capability_logs_probe_timeout", endpoint=endpoint[:60], from_block=from_block)
        return GetLogsCapability.TRANSIENT
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.debug(
            "capability_logs_probe_transient",
            endpoint=endpoint[:60],
            error=str(exc)[:80],
        )
        return GetLogsCapability.TRANSIENT

    outcome = classify_logs_capability(status_code, data)
    logger.debug(
        "capability_logs_probe",
        endpoint=endpoint[:60],
        http_status=status_code,
        outcome=outcome.value,
    )
    return outcome


async def probe_archive(client: RpcClient, endpoint: str, chain: str) -> bool:
    """Probe whether ``endpoint`` can serve state at genesis-era blocks.

    Calls ``eth_getBalance`` at block 1 (falling back to block 256 when
    the node reports the block is unknown) for a known funded address.
    Any hex balance result — including ``0x0`` — proves the node can
    answer historical state queries; non-archive nodes return a
    missing-trie-node / header-not-found error instead.

    Args:
        client: RpcClient instance (call transport + pool accounting).
        endpoint: RPC endpoint URL to probe.
        chain: Chain name (logging only).

    Returns:
        True when the endpoint answered a genesis-era balance query.
    """
    for block in (1, 256):
        result = await client.call(
            endpoint,
            "eth_getBalance",
            [KNOWN_FUNDED_ADDRESS, hex(block)],
            timeout=CAPABILITY_PROBE_TIMEOUT,
        )
        if result is None:
            # Transport failure / timeout — retry at the next block, then
            # give up (not-archive) rather than raising into verification.
            continue
        if "result" in result and result["result"] is not None:
            logger.debug(
                "capability_archive_ok",
                endpoint=endpoint[:60],
                chain=chain,
                block=block,
            )
            return True
        err = result.get("error") or {}
        logger.debug(
            "capability_archive_rejected",
            endpoint=endpoint[:60],
            chain=chain,
            block=block,
            error=str(err.get("message", err))[:80] if isinstance(err, dict) else str(err)[:80],
        )
    return False


async def probe_getlogs_range(
    client: RpcClient,
    endpoint: str,
    topic0: str,
    latest_block: int | None = None,
    timeout: float = CAPABILITY_PROBE_TIMEOUT,
) -> int:
    """Binary-search the largest eth_getLogs block range ``endpoint`` serves.

    Probes the ladder ``10 → 100 → 1000 → 10000`` blocks; on the first
    rung that fails, binary-searches between the last good rung and the
    failed size (halving the search space, bounded by
    :data:`GETLOGS_BINARY_MAX_STEPS`). Client-side timeouts are treated
    as range errors, so slow endpoints get a conservative range.

    Args:
        client: RpcClient instance (used only for tip discovery).
        endpoint: RPC endpoint URL to probe.
        topic0: Event topic used for the probe filter.
        latest_block: Chain tip to anchor probe ranges against. When
            omitted, discovered via ``eth_blockNumber``; when that fails
            the probe anchors at block 1 (best-effort).
        timeout: Per-request probe timeout in seconds.

    Returns:
        The largest accepted range size (0 when even 10 blocks failed).
    """
    anchor = latest_block
    if anchor is None:
        anchor = await client.eth_block_number(endpoint)
    if anchor is None:
        anchor = 1

    async def probe_size(size: int) -> _GetLogsProbeStatus:
        from_block = max(1, anchor - size + 1)
        return await _probe_getlogs_once(endpoint, from_block, anchor, topic0, timeout=timeout)

    last_good = 0
    failed: int | None = None
    for size in GETLOGS_RANGE_LADDER:
        status = await probe_size(size)
        if status == _GetLogsProbeStatus.OK:
            last_good = size
            continue
        failed = size
        break

    if failed is None:
        # Everything through 10000 served — ladder ceiling is the answer.
        logger.debug(
            "capability_getlogs_range_ceiling",
            endpoint=endpoint[:60],
            max_range=GETLOGS_RANGE_LADDER[-1],
        )
        return GETLOGS_RANGE_LADDER[-1]
    if last_good == 0:
        logger.debug(
            "capability_getlogs_range_zero",
            endpoint=endpoint[:60],
            first_failed=failed,
        )
        return 0

    low, high = last_good, failed
    steps = 0
    while high - low > 1 and steps < GETLOGS_BINARY_MAX_STEPS:
        mid = (low + high) // 2
        if await probe_size(mid) == _GetLogsProbeStatus.OK:
            low = mid
        else:
            high = mid
        steps += 1

    logger.debug(
        "capability_getlogs_range_result",
        endpoint=endpoint[:60],
        max_range=low,
    )
    return low


async def probe_block_receipts(
    client: RpcClient,
    endpoint: str,
    block_number: int | None = None,
) -> bool:
    """Probe whether ``endpoint`` supports ``eth_getBlockReceipts``.

    Args:
        client: RpcClient instance.
        endpoint: RPC endpoint URL to probe.
        block_number: Block to probe (defaults to the chain tip).

    Returns:
        True when the endpoint returned a receipt list.
    """
    target = block_number
    if target is None:
        target = await client.eth_block_number(endpoint)
        if target is None:
            return False
    result = await client.call(
        endpoint,
        "eth_getBlockReceipts",
        [hex(target)],
        timeout=CAPABILITY_PROBE_TIMEOUT,
    )
    if result is None:
        return False
    # A recent block always has receipts, so a list result proves support;
    # null / error results mean unsupported or errored.
    return isinstance(result.get("result"), list)


async def probe_cu_tariff(client: RpcClient, endpoint: str) -> dict[str, float]:
    """Return seeded per-method compute-unit weights for ``endpoint``.

    The weights come from provider documentation (KeyedProvider-style CU
    table) rather than live measurement; they are intentionally static
    so cost-aware ranking is stable across verification cycles. The
    ``client``/``endpoint`` parameters keep the interface uniform with
    the other probes and reserve room for a live-measurement upgrade.

    Args:
        client: RpcClient instance (unused — interface uniformity).
        endpoint: RPC endpoint URL (unused — logging only).

    Returns:
        Copy of the seeded CU tariff keyed by JSON-RPC method name.
    """
    logger.debug("capability_cu_tariff_seeded", endpoint=endpoint[:60])
    return dict(SEEDED_CU_TARIFF)


async def probe_endpoint_capabilities(
    client: RpcClient,
    endpoint: str,
    chain: str,
    topic0: str = TRANSFER_TOPIC,
) -> CapabilityRecord:
    """Run the full capability probe suite for ``endpoint`` and persist it.

    Best-effort: individual probe failures leave their fields at the
    dataclass defaults instead of raising, so callers (verification,
    pool admission) can always get a record back.

    Args:
        client: RpcClient instance.
        endpoint: RPC endpoint URL to probe.
        chain: Chain name.
        topic0: Event topic used for the getLogs-range probe.

    Returns:
        The persisted :class:`CapabilityRecord`.
    """
    record = CapabilityRecord(
        endpoint=endpoint,
        chain=chain,
        archive=await probe_archive(client, endpoint, chain),
        getlogs_max_range=await probe_getlogs_range(client, endpoint, topic0),
        block_receipts=await probe_block_receipts(client, endpoint),
        cu_tariff=await probe_cu_tariff(client, endpoint),
        probed_at=time.time(),
    )
    set_capability(record)
    return record


__all__ = [
    "ARCHIVE_METHODS",
    "ARCHIVE_METHOD_PREFIXES",
    "CAPABILITY_PROBE_TIMEOUT",
    "CAPABILITY_ERROR_CODES",
    "CAPABILITY_ERROR_MARKERS",
    "CAPABILITY_HTTP_STATUSES",
    "ArchiveRoutingError",
    "CapabilityRecord",
    "GETLOGS_RANGE_LADDER",
    "GetLogsCapability",
    "NoArchiveEndpointError",
    "SEEDED_CU_TARIFF",
    "TRANSFER_TOPIC",
    "archive_capable_endpoints",
    "archive_guard_reason",
    "capability_records",
    "classify_logs_capability",
    "get_capability",
    "is_archive_capable",
    "is_logs_capability_excluded",
    "is_logs_capability_failure",
    "logs_capable_endpoints",
    "method_requires_archive",
    "probe_archive",
    "probe_block_receipts",
    "probe_cu_tariff",
    "probe_endpoint_capabilities",
    "probe_getlogs_range",
    "probe_logs_capability",
    "set_capability",
    "set_logs_capability",
]
