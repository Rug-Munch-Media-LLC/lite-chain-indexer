"""Freemium (keyed) endpoint classification + backfill-only selection.

Policy: the keyless free pool is reserved for the live tip. Historical
backfill must dispatch ONLY to keyed/freemium accounts (Alchemy, Infura,
QuickNode, ...), whose provider-level caps (see ``rpc/provider_caps.py``
and ``rpc/ledger.py``) bound consumption per account. That keeps the free
pool's shared rate budget available for head-follow.

Classification is intentionally conservative. An endpoint is freemium when
ANY of these hold:

  * its URL still contains an unresolved ``${VAR}`` key placeholder
    (raw template passed straight through — never dialable, but recognisable);
  * it carries an account key the ledger can derive (``key_id_for_endpoint``
    covers Alchemy/Infura ``/v2/<key>`` / ``/v3/<key>`` paths);
  * its query string carries a recognised key parameter (``dkey=``,
    ``apiKey=``, ``api-key=``);
  * its host belongs to a provider whose endpoints are keyed by default,
    with explicit exceptions for a provider's keyless tiers (Ankr's
    one-segment ``/celo`` path, BlockPI's ``/public`` path).

Everything else is treated as free/keyless and is NEVER selected for
backfill.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import urlparse

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.logging import get_logger
from lite_indexer.rpc.archive_routing import archive_guard_reason, method_requires_archive
from lite_indexer.rpc.ledger import key_id_for_endpoint

logger = get_logger(__name__)

# Method every backfill selection targets (transfer logs).
BACKFILL_METHOD = "eth_getLogs"

# Hosts whose endpoints are keyed/freemium by default. Order does not matter.
_FREEMIUM_HOST_MARKERS: tuple[str, ...] = (
    "alchemy.com",
    "infura.io",
    "helius.xyz",
    "helius-rpc.com",
    "quiknode.pro",
    "getblock.io",
    "chainstack.com",
    "nownodes.io",
    "blockpi.network",
    "blockvision.org",
    "uniblock.dev",
    "nodereal.io",
    "ankr.com",
)

# Query-string key parameters that mark a keyed endpoint even on a shared host.
_FREEMIUM_QUERY_MARKERS: tuple[str, ...] = (
    "dkey=",
    "apikey=",
    "api-key=",
    "api_key=",
)


def is_freemium_endpoint(endpoint: str) -> bool:
    """Whether ``endpoint`` is a keyed/freemium endpoint eligible for backfill.

    Fail-safe: an empty/unknown URL returns False, so backfill never falls
    back to the free pool. See the module docstring for the classification
    rules.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        True when the endpoint is keyed/freemium, else False.
    """
    if not endpoint:
        return False
    if "${" in endpoint:
        return True
    if key_id_for_endpoint(endpoint):
        return True

    parsed = urlparse(endpoint)
    host = (parsed.hostname or "").lower()
    query = (parsed.query or "").lower()
    if any(marker in query for marker in _FREEMIUM_QUERY_MARKERS):
        return True

    segments = [seg for seg in parsed.path.split("/") if seg]
    for marker in _FREEMIUM_HOST_MARKERS:
        if marker not in host:
            continue
        # Keyless exceptions: Ankr serves one-segment public paths
        # (rpc.ankr.com/celo) and BlockPI a literal /public path.
        keyless = (marker == "ankr.com" and len(segments) < 2) or (
            marker == "blockpi.network" and bool(segments) and segments[-1] == "public"
        )
        return not keyless
    return False


def freemium_endpoints(endpoints: list[str]) -> list[str]:
    """Filter ``endpoints`` down to the keyed/freemium ones (order preserved)."""
    return [ep for ep in endpoints if is_freemium_endpoint(ep)]


def _pool_best_endpoint() -> Callable[[list[str]], str]:
    """Late import of the pool selector (avoids an import cycle)."""
    from lite_indexer.rpc.pool import get_best_endpoint

    return get_best_endpoint


def select_freemium_endpoint(
    endpoints: list[str],
    method: str = BACKFILL_METHOD,
    *,
    base_select: Callable[[list[str]], str] | None = None,
) -> str | None:
    """Pick a freemium, archive-safe endpoint for a backfill call.

    Candidates are first narrowed to freemium endpoints, then — for
    ARCHIVE-class methods — to endpoints the archive guard would NOT reject
    (unprobed endpoints are allowed so the pipeline runs before the
    capability registry is populated; KNOWN non-archive endpoints are
    excluded in the same way as the live routing path).

    Args:
        endpoints: Candidate endpoint URLs.
        method: RPC method the backfill will issue (default ``eth_getLogs``).
        base_select: Optional selector with :func:`get_best_endpoint`'s
            signature (injectable for tests).

    Returns:
        A freemium endpoint URL, or None when none qualifies. Never raises.
    """
    candidates = freemium_endpoints(endpoints)
    if method_requires_archive(method):
        candidates = [ep for ep in candidates if archive_guard_reason(ep, method) is None]
    if not candidates:
        return None
    select = base_select or _pool_best_endpoint()
    return select(candidates)


def select_freemium_endpoint_for_chain(
    chain: str,
    method: str = BACKFILL_METHOD,
    *,
    base_select: Callable[[list[str]], str] | None = None,
) -> str | None:
    """Backfill endpoint for ``chain`` from the live configured pool.

    Reads :data:`lite_indexer.chains.GET_LOGS_ENDPOINTS` (the same dict the
    verifier swaps per cycle) and delegates to
    :func:`select_freemium_endpoint`.
    """
    return select_freemium_endpoint(GET_LOGS_ENDPOINTS.get(chain, []), method, base_select=base_select)


__all__ = [
    "BACKFILL_METHOD",
    "freemium_endpoints",
    "is_freemium_endpoint",
    "select_freemium_endpoint",
    "select_freemium_endpoint_for_chain",
]
