"""
RPC endpoint health tracking and verification.

Tracks per-endpoint verification status, provides functions to
verify endpoints against eth_getLogs, and caches results.
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx

from lite_indexer.chains import (
    ENDPOINT_VERIFY_CONCURRENCY,
    ENDPOINT_VERIFY_INTERVAL,
    ENDPOINT_VERIFY_TIMEOUT,
    VERIFY_BLOCK_DELTA,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import user_agent
from lite_indexer.rpc.budgets import budget_available
from lite_indexer.rpc.capabilities import is_logs_capability_failure
from lite_indexer.rpc.health_logs import PROBE_SPAN_BLOCKS, probe_logs_range

logger = get_logger(__name__)

# ── State ──────────────────────────────────────────────────────────────
_endpoint_last_verified: dict[str, float] = {}  # url -> last verify timestamp
_endpoint_verified_ok: dict[str, bool] = {}  # url -> last verify result

# D2 (2026-09-14): persist health verdicts so a restart does not reset the
# ENDPOINT_VERIFY_INTERVAL cache and re-probe the entire pool.
_HEALTH_KIND = "health"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _set_health(endpoint: str, ok: bool) -> None:
    """Record an endpoint health verdict in memory + the durable store."""
    _endpoint_verified_ok[endpoint] = ok
    at = _endpoint_last_verified.get(endpoint, 0.0)
    if ok:
        at = time.time()
        _endpoint_last_verified[endpoint] = at
    try:
        from lite_indexer.rpc import cache_db

        cache_db.state_put(
            _HEALTH_KIND,
            endpoint,
            json.dumps({"ok": bool(ok), "at": float(at)}),
            ttl=float(ENDPOINT_VERIFY_INTERVAL),
        )
    except Exception:  # noqa: BLE001 — persistence must never break probing
        logger.debug("health_persist_failed", endpoint=endpoint[:60])


def load_health_state() -> int:
    """Load persisted endpoint-health verdicts (daemon startup)."""
    try:
        from lite_indexer.rpc import cache_db

        stored = cache_db.state_load(_HEALTH_KIND)
    except Exception:  # noqa: BLE001
        return 0
    loaded = 0
    for endpoint, text in stored.items():
        try:
            data = json.loads(text)
            _endpoint_verified_ok[endpoint] = bool(data.get("ok", False))
            _endpoint_last_verified[endpoint] = float(data.get("at", 0.0) or 0.0)
            loaded += 1
        except (ValueError, TypeError):
            continue
    if loaded:
        logger.info("endpoint_health_loaded", records=loaded)
    return loaded


def clear_health_state() -> None:
    """Drop the in-memory health registry (tests / shutdown)."""
    _endpoint_verified_ok.clear()
    _endpoint_last_verified.clear()


# ── Verification ───────────────────────────────────────────────────────


async def verify_endpoint(
    endpoint: str,
    chain: str,
    latest_block: int | None = None,
) -> bool:
    """Verify that an endpoint can serve eth_getLogs before adding to rotation.

    Makes a small-range eth_getLogs call (100 blocks back from tip, or
    blocks 1-100 for chains without a tip). Returns True if the endpoint
    responds with a valid result (list or null — both are acceptable).

    Args:
        endpoint: RPC endpoint URL.
        chain: Chain name (for logging).
        latest_block: Optional latest block number. If provided, checks
            the most recent 100 blocks. Otherwise checks blocks 1-100.

    Returns:
        True if endpoint responded to eth_getLogs without error.
    """
    try:
        if latest_block:
            from_block = max(0, latest_block - VERIFY_BLOCK_DELTA)
            to_block = latest_block
        else:
            from_block = 1
            to_block = 100

        # Use a fresh client for verification
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(ENDPOINT_VERIFY_TIMEOUT),
        ) as client:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [
                    {
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "topics": [TRANSFER_TOPIC],
                    }
                ],
            }
            resp = await client.post(endpoint, json=payload)
            data = resp.json()

            if not isinstance(data, dict):
                _set_health(endpoint, False)
                metrics_incr("endpoints_failed_verify")
                return False

            # Accept any valid result (list or null — both mean endpoint works)
            if "result" in data and not ("error" in data and data.get("error")):
                _set_health(endpoint, True)
                metrics_incr("endpoints_verified")
                return True

            if "error" in data:
                # Capability refusal (JSON-RPC -32701, auth/tenant/key
                # disabled, malformed range, HTTP 405): a permanent property
                # of the endpoint for getLogs — reject, never accept it as a
                # range-limited success (that bug let publicnode-class
                # endpoints pollute the log pool).
                if is_logs_capability_failure(getattr(resp, "status_code", 200), data):
                    logger.debug(
                        "verify_endpoint_logs_capability_rejected",
                        endpoint=endpoint[:50],
                        chain=chain,
                    )
                    _set_health(endpoint, False)
                    metrics_incr("endpoints_failed_verify")
                    return False

                # 2026-09-02: some RPCs (e.g. polygon-rpc.com when the
                # tenant/API key is disabled) return ``error`` as a plain
                # STRING, not an object — ``.get()`` on it crashed the whole
                # chain's sync cycle every poll (polygon hard-stuck for 9
                # days, "daemon_chain_failed: 'str' object has no attribute
                # 'get'").
                err = data["error"]
                err_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                # Some endpoints restrict archive data but work for recent blocks
                if "archive" in err_msg.lower() or "range" in err_msg.lower() or "limit" in err_msg.lower():
                    logger.debug(
                        "verify_endpoint_range_limited",
                        endpoint=endpoint[:50],
                        chain=chain,
                        error=err_msg[:80],
                    )
                    # Still accept — it may work with appropriate ranges
                    _set_health(endpoint, True)
                    metrics_incr("endpoints_verified")
                    return True

            _set_health(endpoint, False)
            metrics_incr("endpoints_failed_verify")
            return False

    except httpx.TimeoutException:
        logger.debug("verify_endpoint_timeout", endpoint=endpoint[:50], chain=chain)
        _set_health(endpoint, False)
        metrics_incr("endpoints_failed_verify")
        return False
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.warning(
            "verify_endpoint_failed",
            endpoint=endpoint[:50],
            chain=chain,
            error=str(exc)[:80],
        )
        _set_health(endpoint, False)
        metrics_incr("endpoints_failed_verify")
        return False


async def verify_endpoints_for_chain(
    endpoints: list[str],
    chain: str,
    latest_block: int | None = None,
    *,
    semaphore: asyncio.Semaphore | None = None,
) -> list[str]:
    """Verify all endpoints for a chain, returning only healthy ones.

    Skips endpoints that have been verified within ENDPOINT_VERIFY_INTERVAL.
    Falls through to all endpoints if no healthy ones exist.

    Args:
        endpoints: List of RPC URLs for this chain.
        chain: Chain name.
        latest_block: Optional latest block for range-based verification.
        semaphore: Optional SHARED probe bound. The startup pass passes one
            global semaphore so in-flight probes stay memory-bounded across
            all chains; a standalone call creates its own small bound.

    Returns:
        List of verified healthy endpoints (maintains original order).
    """
    now = time.time()

    # Probe endpoints CONCURRENTLY (bounded). The liveness gate is one
    probe_sem = semaphore or asyncio.Semaphore(max(1, ENDPOINT_VERIFY_CONCURRENCY))

    async def _check(ep: str) -> tuple[str, bool]:
        # Skip unresolved ${ENV_VAR} placeholders — they can never be dialed
        # (DNS/JSON errors) and would otherwise retry every sync cycle.
        if "${" in ep:
            logger.debug("verify_skip_unresolved_env", endpoint=ep[:50], chain=chain)
            return ep, False

        # Honor the per-provider budget layer: never spend verification
        # probes on an identity that is pre-emptively cooled or has hit its
        # daily hard cap. Keeps the parallel startup pass inside quotas.
        if not budget_available(ep):
            logger.debug("verify_skip_budget_unavailable", endpoint=ep[:50], chain=chain)
            return ep, False

        # Skip if recently verified (reuse the cached verdict).
        last_v = _endpoint_last_verified.get(ep, 0)
        if (now - last_v) < ENDPOINT_VERIFY_INTERVAL:
            return ep, _endpoint_verified_ok.get(ep, False)

        async with probe_sem:
            return ep, await verify_endpoint(ep, chain, latest_block)

    outcomes = await asyncio.gather(*(_check(ep) for ep in endpoints))
    verified = [ep for ep, ok in outcomes if ok]

    # Fall through: if no endpoints passed, return all originals
    if not verified:
        logger.warning(
            "all_endpoints_failed_verify",
            chain=chain,
            count=len(endpoints),
        )
        return list(endpoints)

    return verified


def is_endpoint_healthy(endpoint: str) -> bool:
    """Check if an endpoint passed its last verification."""
    result = _endpoint_verified_ok.get(endpoint)
    if result is None:
        return True  # Not yet verified — assume healthy
    return result


# ── Periodic Health Monitor ───────────────────────────────────────────


def _pending_from_block(chain: str) -> int | None:
    """First block the sync loop for ``chain`` will request next.

    Reads the durable cursor directly (no StateManager logging) so the
    probe measures the real pending range instead of a synthetic tip span.
    """
    from lite_indexer.config import STATE_DIR

    path = STATE_DIR / f"lite_{chain}.json"
    try:
        return int(json.loads(path.read_text())["last_block"]) + 1
    except (OSError, ValueError, KeyError, TypeError):
        return None


async def _probe_tip(endpoint: str, timeout: float) -> int | None:
    """Read the chain tip, used only when no cursor exists yet."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), headers=user_agent.rpc_headers()) as client:
            resp = await client.post(
                endpoint, json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
            )
            return int(resp.json()["result"], 16)
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError):
        return None


async def _probe_endpoint(endpoint: str, timeout: float) -> int:
    """Light liveness probe for a single RPC endpoint (eth_blockNumber).

    Kept deliberately light (one JSON-RPC call, short timeout) so the
    monitor can sweep every configured endpoint without burning free-tier
    rate limits. The per-cycle eth_getLogs verification in
    ``verify_endpoints_for_chain`` remains the authoritative getLogs-level
    check.

    THE PROBE MUST SEND THE SAME HEADERS AS THE REAL CALL.
    Args:
        endpoint: RPC endpoint URL.
        timeout: Probe timeout in seconds.

    Returns:
        HTTP status code on a successful dial (200 for a valid
        eth_blockNumber answer, 4xx/5xx otherwise), or 0 when the probe
        failed at the transport level (timeout / connection error).
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), headers=user_agent.rpc_headers()) as client:
            resp = await client.post(
                endpoint,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
            )
            if resp.status_code != 200:
                return resp.status_code
            data = resp.json()
            return 200 if isinstance(data, dict) and bool(data.get("result")) else 0
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.debug(
            "rpc_probe_failed",
            endpoint=endpoint[:50],
            error=str(exc)[:80],
        )
        return 0


async def monitor_endpoint_health(
    interval: float = 300.0,
    chains: list[str] | None = None,
    timeout: float = 8.0,
) -> None:
    """Periodically probe configured endpoints and log up/down status.

    Runs forever (until cancelled). Each cycle probes every configured
    EVM endpoint with a light ``eth_blockNumber`` call and feeds the
    adaptive pool scoring:

      - ``_endpoint_verified_ok[url]`` is updated so
        :func:`is_endpoint_healthy` (used by ``get_best_endpoint``)
        deprioritizes dead endpoints (+10000 penalty).
      - Dead endpoints are recorded in the pool failure tracker via
        :func:`lite_indexer.rpc.pool.record_endpoint_failure` so the
        per-endpoint exponential backoff applies.

    Endpoints with unresolved ``${ENV_VAR}`` placeholders are skipped —
    they are only usable once the key is set in the environment, and
    marking them dead would pollute the pool scoring.

    Args:
        interval: Seconds between probe cycles (default: 300 = 5 min).
        chains: Chains to probe (default: all EVM chains in
            GET_LOGS_ENDPOINTS). Non-EVM chains (solana/tron) use their
            own RPC config and are not included.
        timeout: Per-endpoint probe timeout in seconds.
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.rpc.pool import record_endpoint_failure

    target_chains = chains if chains is not None else sorted(GET_LOGS_ENDPOINTS)
    logger.info(
        "endpoint_health_monitor_start",
        chains=target_chains,
        interval=interval,
    )

    while True:
        cycle_start = time.monotonic()
        up_total = 0
        down_total = 0

        for chain in target_chains:
            endpoints = [ep for ep in GET_LOGS_ENDPOINTS.get(chain, []) if "${" not in ep]
            if not endpoints:
                continue

            up: list[str] = []
            down: list[str] = []
            rate_limited: list[str] = []
            # 2026-10-23 bsc incident: liveness alone is a lie. Every
            # configured BSC endpoint answered eth_blockNumber while none
            # could serve eth_getLogs for the pending range (pruned history,
            # 25-block caps, 429s, archive-token walls). Probe the span the
            # sync loop asks for next — that is the capability that matters.
            pending_from = _pending_from_block(chain)
            tip = await _probe_tip(endpoints[0], timeout) if pending_from is None else None
            for url in endpoints:
                status = await _probe_endpoint(url, timeout)
                ok = status == 200
                # The getLogs verdict is recorded but NEVER turns into a
                # ban/circuit: a capability refusal (pruned history, range
                # cap) is a property of the endpoint, not a transient fault,
                # and banning on it would deepen a chain's outage. It feeds
                # the pool gauge and selection penalty only.
                if ok:
                    span_from = pending_from if pending_from is not None else max(0, (tip or 0) - PROBE_SPAN_BLOCKS)
                    await probe_logs_range(url, span_from, span_from + PROBE_SPAN_BLOCKS - 1, timeout=timeout)
                _set_health(url, ok)
                if ok:
                    up.append(url)
                    up_total += 1
                else:
                    down.append(url)
                    down_total += 1
                    record_endpoint_failure(url)
                    # 403/429 = provider rate-limit / blacklist: hard-ban
                    # the endpoint for an exponential window instead of
                    # just a 300s cooldown.
                    if status in (403, 429):
                        # D4 (2026-09-14): the probe path records health /
                        # soft cooldown only — it must NEVER hard-ban. A 403
                        # is usually a capability/geo/Cloudflare refusal, and
                        # a 429 is a rate limit that lifts in seconds; hard
                        # bans here caused the ban/rebirth storm.
                        rate_limited.append(url)

            # Per-chain endpoint health metrics for monitoring: emitted on
            # every cycle with per-chain up/down + the endpoint list, plus
            # Prometheus counters so per-chain health is graphable.
            metrics_incr(f"endpoint_chain_up_{chain}", len(up))
            metrics_incr(f"endpoint_chain_down_{chain}", len(down))
            if rate_limited:
                metrics_incr(f"endpoint_chain_banned_{chain}", len(rate_limited))
            logger.info(
                "endpoint_health_chain",
                chain=chain,
                up=len(up),
                down=len(down),
                rate_limited=len(rate_limited),
                total=len(endpoints),
                down_endpoints=[ep[:60] for ep in down],
            )

            if down:
                logger.warning(
                    "endpoint_health_down",
                    chain=chain,
                    up=len(up),
                    down=len(down),
                    endpoints=down,
                )
            else:
                logger.info("endpoint_health_ok", chain=chain, endpoints=len(up))

        logger.info(
            "endpoint_health_cycle",
            up=up_total,
            down=down_total,
            duration_s=round(time.monotonic() - cycle_start, 2),
        )

        await asyncio.sleep(interval)
