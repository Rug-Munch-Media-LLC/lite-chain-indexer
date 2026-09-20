"""SSRF protection for webhook targets (audit SEC-3a, 2026-09-19).

Pure helper shared by webhook registration (``webhooks/router.py``) and
delivery (``webhooks/delivery/sender.py``). A target host is safe only if
EVERY address it resolves to is public:

Blocked classes: loopback (127/8, ::1), private RFC1918 (10/8, 172.16/12,
192.168/16), link-local (169.254/16, fe80::/10), CGNAT (100.64/10),
unique-local IPv6 (fc00::/7), unspecified (0.0.0.0, ::).

Port rule: only 443 (or any port >= 1024) is allowed — ports < 1024 other
than 443 are rejected to keep targets on standard HTTPS.

The resolver is injectable for tests (DNS-rebinding simulation) without
monkeypatching call sites.
"""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable
from urllib.parse import urlparse

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

Resolver = Callable[[str, int], list]

#: Module-level default resolver — tests may swap this (shrink-only seam).
_default_resolver: Resolver = socket.getaddrinfo


def _is_blocked_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True when the address is in any blocked (non-public) class."""
    if isinstance(addr, ipaddress.IPv4Address) and addr in ipaddress.ip_network("100.64.0.0/10"):
        return True  # CGNAT — not always covered by is_private
    return (
        addr.is_loopback
        or addr.is_private
        or addr.is_link_local
        or addr.is_unspecified
        or addr.is_multicast
        or addr.is_reserved
    )


def _port_allowed(port: int | None) -> bool:
    """HTTPS default (443) or any unprivileged port (>= 1024)."""
    if port is None:
        return True  # default https port
    return port == 443 or port >= 1024


def resolve_and_check(host: str, *, resolver: Resolver | None = None) -> bool:
    """Resolve ``host`` and return True only if ALL addresses are public.

    Uses the injected ``resolver`` (default: :func:`socket.getaddrinfo`).
    Unresolvable hosts are rejected (fail-closed).
    """
    try:
        infos = (resolver or _default_resolver)(host, 0, socket.SOCK_STREAM)
    except (socket.gaierror, OSError) as exc:
        logger.debug("ssrf_resolve_failed", host=host, error=str(exc)[:80])
        return False
    if not infos:
        return False
    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except ValueError:
            logger.debug("ssrf_bad_address", host=host)
            return False
        if _is_blocked_ip(addr):
            return False
    return True


def is_safe_webhook_url(url: str, *, resolver: Resolver | None = None) -> bool:
    """Full URL safety check: https scheme, allowed port, public host."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    if not _port_allowed(parsed.port):
        return False
    return resolve_and_check(parsed.hostname, resolver=resolver)


__all__ = ["is_safe_webhook_url", "resolve_and_check"]
