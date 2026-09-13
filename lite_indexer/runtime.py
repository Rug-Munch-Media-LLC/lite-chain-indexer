"""
Runtime performance hooks — optional uvloop event-loop installation.

``uvloop`` is a drop-in libuv-based asyncio event loop that speeds up the
I/O-heavy RPC/DB fan-out without code changes. It is OPTIONAL and platform
dependent (Linux/macOS): when it is absent or fails to install, callers fall
back to the default ``asyncio`` loop and NEVER hard-fail.

Install via the ``perf`` extra::

    pip install "litechainindexer[perf]"
"""

from __future__ import annotations

import asyncio

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def install_event_loop_policy() -> bool:
    """Install the uvloop event-loop policy when available.

    Graceful fallback: returns ``False`` (leaving the default asyncio policy
    untouched) when uvloop is missing, unsupported, or raises while
    installing. Safe to call unconditionally from any entrypoint.

    Returns:
        True when the uvloop policy was installed, else False.
    """
    try:
        import uvloop
    except ImportError:
        logger.debug("uvloop_unavailable")
        return False
    try:
        policy_cls = getattr(uvloop, "EventLoopPolicy", None)
        if policy_cls is not None:
            asyncio.set_event_loop_policy(policy_cls())
        else:  # pragma: no cover — very old/new uvloop without the policy class
            uvloop.install()
    except Exception as exc:  # noqa: BLE001 — never hard-fail on loop setup
        logger.warning("uvloop_install_failed", error=str(exc)[:80])
        return False
    logger.info("uvloop_installed")
    return True


__all__ = ["install_event_loop_policy"]
