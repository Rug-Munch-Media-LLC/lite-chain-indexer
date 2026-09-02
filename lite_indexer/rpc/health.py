"""
RPC endpoint health tracking and verification.

Tracks per-endpoint verification status, provides functions to
verify endpoints against eth_getLogs, and caches results.
"""

from __future__ import annotations

import asyncio
import time

import httpx

from lite_indexer.chains import ENDPOINT_VERIFY_INTERVAL, ENDPOINT_VERIFY_TIMEOUT, VERIFY_BLOCK_DELTA
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ── State ──────────────────────────────────────────────────────────────
_endpoint_last_verified: dict[str, float] = {}  # url -> last verify timestamp
_endpoint_verified_ok: dict[str, bool] = {}  # url -> last verify result

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


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
                _endpoint_verified_ok[endpoint] = False
                metrics_incr("endpoints_failed_verify")
                return False

            # Accept any valid result (list or null — both mean endpoint works)
            if "result" in data and not ("error" in data and data.get("error")):
                _endpoint_verified_ok[endpoint] = True
                _endpoint_last_verified[endpoint] = time.time()
                metrics_incr("endpoints_verified")
                return True

            if "error" in data:
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
                    _endpoint_verified_ok[endpoint] = True
                    _endpoint_last_verified[endpoint] = time.time()
                    metrics_incr("endpoints_verified")
                    return True

            _endpoint_verified_ok[endpoint] = False
            metrics_incr("endpoints_failed_verify")
            return False

    except httpx.TimeoutException:
        logger.debug("verify_endpoint_timeout", endpoint=endpoint[:50], chain=chain)
        _endpoint_verified_ok[endpoint] = False
        metrics_incr("endpoints_failed_verify")
        return False
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
        logger.warning(
            "verify_endpoint_failed",
            endpoint=endpoint[:50],
            chain=chain,
            error=str(exc)[:80],
        )
        _endpoint_verified_ok[endpoint] = False
        metrics_incr("endpoints_failed_verify")
        return False


async def verify_endpoints_for_chain(
    endpoints: list[str],
    chain: str,
    latest_block: int | None = None,
) -> list[str]:
    """Verify all endpoints for a chain, returning only healthy ones.

    Skips endpoints that have been verified within ENDPOINT_VERIFY_INTERVAL.
    Falls through to all endpoints if no healthy ones exist.

    Args:
        endpoints: List of RPC URLs for this chain.
        chain: Chain name.
        latest_block: Optional latest block for range-based verification.

    Returns:
        List of verified healthy endpoints (maintains original order).
    """
    now = time.time()
    verified: list[str] = []

    for ep in endpoints:
        # Skip unresolved ${ENV_VAR} placeholders — they can never be dialed
        # (DNS/JSON errors) and would otherwise retry every sync cycle.
        if "${" in ep:
            logger.debug("verify_skip_unresolved_env", endpoint=ep[:50], chain=chain)
            continue

        # Skip if recently verified
        last_v = _endpoint_last_verified.get(ep, 0)
        if (now - last_v) < ENDPOINT_VERIFY_INTERVAL:
            if _endpoint_verified_ok.get(ep, False):
                verified.append(ep)
            continue

        ok = await verify_endpoint(ep, chain, latest_block)
        if ok:
            verified.append(ep)

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


async def _probe_endpoint(endpoint: str, timeout: float) -> int:
    """Light liveness probe for a single RPC endpoint (eth_blockNumber).

    Kept deliberately light (one JSON-RPC call, short timeout) so the
    monitor can sweep every configured endpoint without burning free-tier
    rate limits. The per-cycle eth_getLogs verification in
    ``verify_endpoints_for_chain`` remains the authoritative getLogs-level
    check.

    Args:
        endpoint: RPC endpoint URL.
        timeout: Probe timeout in seconds.

    Returns:
        HTTP status code on a successful dial (200 for a valid
        eth_blockNumber answer, 4xx/5xx otherwise), or 0 when the probe
        failed at the transport level (timeout / connection error).
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
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
    from lite_indexer.rpc.pool import ban_endpoint, record_endpoint_failure

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
            for url in endpoints:
                status = await _probe_endpoint(url, timeout)
                ok = status == 200
                _endpoint_verified_ok[url] = ok
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
                        rate_limited.append(url)
                        ban_endpoint(url, reason=f"probe_http_{status}")

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
