"""Injection seams for routing internals (no monkeypatching required).

The shared circuit breaker, the P2C selector, and the EWMA damping
constant are read through these seams. Production uses the defaults;
tests/ops inject fakes via the setters and reset them afterwards.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

_SELECTOR: Callable[[list[str]], str] | None = None
_BREAKER: Any | None = None
_DAMPING: float | None = None


def set_selector(fn: Callable[[list[str]], str] | None) -> None:
    """Swap the P2C selector (None restores the real one)."""
    global _SELECTOR
    _SELECTOR = fn


def selector() -> Callable[[list[str]], str]:
    """The P2C selector in effect (injected fake or the real one)."""
    if _SELECTOR is not None:
        return _SELECTOR
    from lite_indexer.rpc.routing.balancer import select_p2c

    return select_p2c


def set_circuit_breaker(breaker: Any | None) -> None:
    """Swap the shared circuit breaker (None restores the default)."""
    global _BREAKER
    _BREAKER = breaker


def circuit_breaker() -> Any:
    """The shared circuit breaker (injected fake or the default instance)."""
    if _BREAKER is not None:
        return _BREAKER
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    return CIRCUIT_BREAKER


def set_damping(value: float | None) -> None:
    """Swap the EWMA error-rate damping constant (None restores config)."""
    global _DAMPING
    _DAMPING = value


def damping() -> float:
    """The damping constant in effect (injected value or env config)."""
    if _DAMPING is not None:
        return _DAMPING
    from lite_indexer.rpc.routing import ERROR_RATE_DAMPING

    return float(ERROR_RATE_DAMPING)
