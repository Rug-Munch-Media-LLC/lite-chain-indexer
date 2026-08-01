"""
RPC endpoint health tracking and verification.

Tracks per-endpoint verification status, provides functions to
verify endpoints against eth_getLogs, and caches results.
"""

from __future__ import annotations

import time

import httpx

from lite_indexer.chains import ENDPOINT_VERIFY_INTERVAL, ENDPOINT_VERIFY_TIMEOUT, VERIFY_BLOCK_DELTA
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── State ──────────────────────────────────────────────────────────────
_endpoint_last_verified: dict[str, float] = {}  # url -> last verify timestamp
_endpoint_verified_ok: dict[str, bool] = {}  # url -> last verify result
_metrics: dict[str, int] = {
    "endpoints_verified": 0,
    "endpoints_failed_verify": 0,
}

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
                _metrics["endpoints_failed_verify"] += 1
                return False

            # Accept any valid result (list or null — both mean endpoint works)
            if "result" in data and not ("error" in data and data.get("error")):
                _endpoint_verified_ok[endpoint] = True
                _endpoint_last_verified[endpoint] = time.time()
                _metrics["endpoints_verified"] += 1
                return True

            if "error" in data:
                err_msg = data["error"].get("message", str(data["error"]))
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
                    _metrics["endpoints_verified"] += 1
                    return True

            _endpoint_verified_ok[endpoint] = False
            _metrics["endpoints_failed_verify"] += 1
            return False

    except httpx.TimeoutException:
        logger.debug("verify_endpoint_timeout", endpoint=endpoint[:50], chain=chain)
        _endpoint_verified_ok[endpoint] = False
        _metrics["endpoints_failed_verify"] += 1
        return False
    except Exception as exc:
        logger.debug(
            "verify_endpoint_failed",
            endpoint=endpoint[:50],
            chain=chain,
            error=str(exc)[:80],
        )
        _endpoint_verified_ok[endpoint] = False
        _metrics["endpoints_failed_verify"] += 1
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
