"""Plain in-process metric stand-ins used when prometheus_client is absent.

``lite_indexer.metrics`` documents graceful degradation to plain counters so the
package still imports on a box without ``prometheus-client``. The alert-contract
metrics are *labeled* and are primed at import time (``_prime_labeled_metrics``
calls ``.labels(...)``), so the fallback objects must expose the same surface the
Prometheus objects do — otherwise the degraded path is the one that crashes.

The fallbacks are for local inspection only (``snapshot()``/``get()``), not for
scraping: label sets are aggregated into a single value instead of being kept as
separate series.
"""

from __future__ import annotations

from typing import Any

LabelKey = tuple[tuple[str, str], ...]


class _PlainCounter:
    """Minimal ``Counter`` stand-in: ``inc``, ``set``, ``get``, labeled children."""

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value = 0.0

    def labels(self, **_labels: str) -> _PlainCounter:
        """Return a child counter (aggregates every label set into one value)."""
        return self

    def inc(self, amount: float = 1.0) -> None:
        """Increment the counter by ``amount`` (default 1.0)."""
        self._value += amount

    def get(self) -> float:
        """Return the current counter value."""
        return self._value

    def set(self, value: float) -> None:
        """Set the counter to an absolute ``value``."""
        self._value = value


class _PlainGauge:
    """Minimal labeled ``Gauge`` stand-in (per-label-set values)."""

    __slots__ = ("_key", "_values")

    def __init__(self, values: dict[LabelKey, float] | None = None) -> None:
        self._values: dict[LabelKey, float] = {} if values is None else values
        self._key: LabelKey = ()

    def labels(self, **labels: str) -> _PlainGauge:
        """Return a child gauge bound to one label set."""
        child = _PlainGauge(self._values)
        child._key = tuple(sorted(labels.items()))
        return child

    def set(self, value: float) -> None:
        """Set the gauge to an absolute ``value``."""
        self._values[self._key] = float(value)

    def get(self) -> float:
        """Return the current gauge value."""
        return self._values.get(self._key, 0.0)


def plain_metric(kind: str, values: dict[LabelKey, float] | None = None) -> Any:
    """Build the fallback object for ``kind`` (``counter`` or ``gauge``)."""
    return _PlainCounter() if kind == "counter" else _PlainGauge(values)


__all__ = ["LabelKey", "_PlainCounter", "_PlainGauge", "plain_metric"]
