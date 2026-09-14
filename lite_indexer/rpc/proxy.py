"""Opt-in residential-proxy routing for RPC HTTP calls.

Free/public RPC tiers rate-limit **per IP**, so all workers sharing one
egress IP burn the same budget. Routing calls through a pool of residential
proxies spreads the load across many egress IPs. This module is OPT-IN and
ships disabled: with ``RPC_PROXY_ENABLED`` unset/0 the OSS behaviour is a
direct connection, unchanged.

Configuration (all from the environment; secret values are never logged):

* ``WEBSHARE_PROXY_LIST`` — one or more proxies, whitespace/comma/semicolon
  separated. Accepted shapes::

      host:port
      host:port:user:pass
      http://user:pass@host:port
      socks5://host:port

* ``RPC_PROXY_ENABLED`` — ``1`` enables proxy routing (default ``0``).
* ``RPC_PROXY_MODE`` — ``sticky`` (default) or ``rotate``.
* ``RPC_PROXY_FALLBACK_ONLY`` — ``1`` makes the DIRECT connection primary
  and the proxy a fallback used only after a direct transport failure
  (default ``0`` = proxy-primary with a direct fallback, the earlier
  behaviour). Recommented for quota work: direct-first keeps the proxy
  pool's per-IP budget untouched until a direct IP is blocked.
* ``RPC_PROXY_KEYLESS_ONLY`` — ``1`` routes **key-less** endpoints (no API
  key in the URL) through the proxy pool while **keyed** endpoints stay
  direct-first with the proxy used only after a transport failure. This is
  the correct lever: key-less providers limit per IP (proxy helps), while
  keyed providers meter quota per account/key (protocol hop adds latency
  and burns proxy GB for no quota gain). Default ``0`` = today's
  ``RPC_PROXY_FALLBACK_ONLY`` behaviour. Env-gated and opt-in.

``sticky`` pins one proxy per RPC endpoint until that proxy fails, then
fails over to another. This is the recommended mode because providers
usually limit per (IP, key): a stable IP per endpoint keeps the account's
observed rate/identity consistent, while a rotating IP per request can
trip anti-abuse heuristics. ``rotate`` cycles the pool on every call.

Only a masked ``host:port`` label is ever logged — never userinfo, never
the raw list.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_ALLOWED_SCHEMES = ("http", "https", "socks5", "socks5h")
_VALID_MODES = ("sticky", "rotate")


@dataclass(frozen=True)
class ProxySpec:
    """One parsed proxy: the httpx proxy URL plus a secret-free label."""

    url: str
    label: str


def parse_proxy_entry(entry: str) -> ProxySpec | None:
    """Parse one proxy string into a :class:`ProxySpec`.

    Accepts ``host:port``, ``host:port:user:pass``,
    ``http://user:pass@host:port`` and ``socks5://host:port``. Returns
    ``None`` for anything malformed — a bad entry is skipped, never fatal.
    Credentials are URL-encoded into the proxy URL and never included in
    the label.
    """
    raw = entry.strip()
    if not raw:
        return None

    if "://" not in raw:
        parts = raw.split(":")
        if len(parts) == 2:
            host, port = parts
            url = f"http://{host}:{port}"
        elif len(parts) == 4:
            host, port, user, password = parts
            url = f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"
        else:
            return None
    else:
        url = raw

    try:
        parsed = urlparse(url)
        parsed_port = parsed.port
    except ValueError:
        return None
    if not parsed.hostname or parsed_port is None:
        return None
    if (parsed.scheme or "http") not in _ALLOWED_SCHEMES:
        return None
    return ProxySpec(url=url, label=f"{parsed.hostname}:{parsed_port}")


def parse_proxy_list(raw: str) -> list[ProxySpec]:
    """Parse a delimited proxy list, de-duplicating exact URLs."""
    specs: list[ProxySpec] = []
    seen: set[str] = set()
    for entry in re.split(r"[\s,;]+", raw or ""):
        spec = parse_proxy_entry(entry)
        if spec is not None and spec.url not in seen:
            seen.add(spec.url)
            specs.append(spec)
    return specs


def is_keyless_endpoint(endpoint: str) -> bool:
    """Whether an endpoint carries NO API key (per-IP limited).

    Key-less providers meter requests per egress IP, so the residential proxy
    pool is the right lever there. Keyed endpoints meter per account/key, so
    proxying them burns GB for no quota gain. Detection reuses the generic,
    non-secret key detection in ``rpc/ledger.py`` (imported lazily to avoid an
    import cycle); an unresolvable endpoint is treated as keyed (direct) so a
    routing error never pushes unknown traffic through the paid proxy.
    """
    try:
        from lite_indexer.rpc.ledger import key_id_for_endpoint

        return not key_id_for_endpoint(endpoint)
    except Exception:  # noqa: BLE001 — routing must never raise
        return False


class ProxyRouter:
    """Assign proxies to endpoints (``sticky``) or round-robin (``rotate``).

    A proxy that has failed ``max_failures`` consecutive times is taken out
    of rotation (but the last-resort fallback keeps the pool usable if every
    proxy has failed). ``sticky`` pins an endpoint to one proxy until that
    proxy fails, at which point the endpoint is re-pinned to the next
    healthy proxy.
    """

    def __init__(
        self,
        specs: list[ProxySpec],
        *,
        enabled: bool = False,
        mode: str = "sticky",
        fallback_only: bool = False,
        keyless_only: bool = False,
        max_failures: int = 1,
    ) -> None:
        self._specs = list(specs)
        self._enabled = bool(enabled) and bool(self._specs)
        self._mode = mode if mode in _VALID_MODES else "sticky"
        self._fallback_only = bool(fallback_only)
        self._keyless_only = bool(keyless_only)
        self._max_failures = max(1, int(max_failures))
        self._pins: dict[str, ProxySpec] = {}
        self._failed: dict[str, int] = {}
        self._rr = 0

    @classmethod
    def from_env(cls) -> ProxyRouter:
        """Build a router from the process environment (never logs values)."""
        raw = os.environ.get("WEBSHARE_PROXY_LIST", "")
        enabled = os.environ.get("RPC_PROXY_ENABLED", "0") != "0"
        mode = (os.environ.get("RPC_PROXY_MODE", "sticky").strip().lower()) or "sticky"
        fallback_only = os.environ.get("RPC_PROXY_FALLBACK_ONLY", "0") != "0"
        keyless_only = os.environ.get("RPC_PROXY_KEYLESS_ONLY", "0") != "0"
        router = cls(
            parse_proxy_list(raw),
            enabled=enabled,
            mode=mode,
            fallback_only=fallback_only,
            keyless_only=keyless_only,
        )
        if router.enabled:
            logger.info(
                "rpc_proxy_pool_loaded",
                proxies=len(router._specs),
                mode=router._mode,
                fallback_only=router._fallback_only,
                keyless_only=router._keyless_only,
            )
        return router

    @property
    def enabled(self) -> bool:
        """Whether proxy routing is active (enabled AND a non-empty pool)."""
        return self._enabled

    @property
    def mode(self) -> str:
        """Configured routing mode (``sticky`` or ``rotate``)."""
        return self._mode

    @property
    def fallback_only(self) -> bool:
        """Whether the direct connection is primary and the proxy fallback-only."""
        return self._fallback_only

    @property
    def keyless_only(self) -> bool:
        """Whether only key-less endpoints are proxied (keyed stay direct-first)."""
        return self._keyless_only

    def proxy_count(self) -> int:
        """Number of parsed proxies (no values exposed)."""
        return len(self._specs)

    def _direct_first(self, endpoint: str, keyless: bool | None = None) -> bool:
        """Whether ``endpoint`` must be tried direct-first (proxy = fallback).

        True in global ``fallback_only`` mode, or when ``keyless_only`` is on
        and the endpoint is keyed. ``keyless`` may be supplied to avoid
        re-detecting (callers that already know the endpoint shape).
        """
        if self._fallback_only:
            return True
        if self._keyless_only:
            if keyless is None:
                keyless = is_keyless_endpoint(endpoint)
            return not keyless
        return False

    def _healthy(self) -> list[ProxySpec]:
        return [s for s in self._specs if self._failed.get(s.url, 0) < self._max_failures]

    def _pick(self) -> ProxySpec | None:
        healthy = self._healthy() or self._specs
        if not healthy:
            return None
        spec = healthy[self._rr % len(healthy)]
        self._rr += 1
        return spec

    def proxy_for(self, endpoint: str, *, keyless: bool | None = None) -> ProxySpec | None:
        """Return the proxy to use for ``endpoint`` (None = direct).

        Direct (None) in ``fallback_only`` mode, and for a keyed endpoint in
        ``keyless_only`` mode — callers then use :meth:`fallback_proxy_for`
        only after a direct transport failure.
        """
        if not self._enabled or self._direct_first(endpoint, keyless):
            return None
        if self._mode == "sticky":
            pinned = self._pins.get(endpoint)
            if pinned is not None and self._failed.get(pinned.url, 0) < self._max_failures:
                return pinned
            spec = self._pick()
            if spec is not None:
                self._pins[endpoint] = spec
            return spec
        return self._pick()

    def fallback_proxy_for(self, endpoint: str, *, keyless: bool | None = None) -> ProxySpec | None:
        """Proxy to retry with after a DIRECT failure (direct-first endpoints only).

        Returns None when the pool is disabled or the endpoint is
        proxy-primary (where a direct retry is already handled). In
        ``keyless_only`` mode keyed endpoints are direct-first, so they DO
        get a proxy fallback here.
        """
        if not self._enabled or not self._direct_first(endpoint, keyless):
            return None
        pinned = self._pins.get(endpoint)
        if pinned is not None and self._failed.get(pinned.url, 0) < self._max_failures:
            return pinned
        spec = self._pick()
        if spec is not None:
            self._pins[endpoint] = spec
        return spec

    def mark_failed(self, endpoint: str, spec: ProxySpec | None = None) -> None:
        """Mark a proxy failed and unpin the endpoint (failover on next pick)."""
        if spec is None:
            spec = self._pins.get(endpoint)
        if spec is None:
            return
        self._failed[spec.url] = self._failed.get(spec.url, 0) + 1
        self._pins.pop(endpoint, None)

    def mark_success(self, endpoint: str, spec: ProxySpec | None = None) -> None:
        """Clear a proxy's consecutive-failure count after a success."""
        if spec is None:
            spec = self._pins.get(endpoint)
        if spec is not None:
            self._failed.pop(spec.url, None)

    def snapshot(self) -> dict[str, object]:
        """Secret-free status view (counts only, never URLs)."""
        return {
            "enabled": self._enabled,
            "mode": self._mode,
            "fallback_only": self._fallback_only,
            "keyless_only": self._keyless_only,
            "proxy_count": len(self._specs),
            "pinned_endpoints": len(self._pins),
            "failed_proxies": len(self._failed),
        }


# Module-level router shared across the RPC layer.
PROXY_ROUTER = ProxyRouter.from_env()


def proxy_for_endpoint(endpoint: str) -> ProxySpec | None:
    """Proxy to use for ``endpoint`` (None → direct connection)."""
    return PROXY_ROUTER.proxy_for(endpoint)


def fallback_proxy_for(endpoint: str) -> ProxySpec | None:
    """Proxy to retry with after a direct failure (fallback-only mode)."""
    return PROXY_ROUTER.fallback_proxy_for(endpoint)


def mark_proxy_failed(endpoint: str, spec: ProxySpec | None = None) -> None:
    """Record a proxy transport failure for ``endpoint`` (unpins it)."""
    PROXY_ROUTER.mark_failed(endpoint, spec)


def mark_proxy_success(endpoint: str, spec: ProxySpec | None = None) -> None:
    """Record a proxy success for ``endpoint``."""
    PROXY_ROUTER.mark_success(endpoint, spec)


__all__ = [
    "PROXY_ROUTER",
    "ProxyRouter",
    "ProxySpec",
    "fallback_proxy_for",
    "is_keyless_endpoint",
    "mark_proxy_failed",
    "mark_proxy_success",
    "parse_proxy_entry",
    "parse_proxy_list",
    "proxy_for_endpoint",
]
