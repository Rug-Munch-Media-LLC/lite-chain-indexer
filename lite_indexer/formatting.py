"""Human-readable formatting helpers for lag and reconciliation output.

Extracted 2026-09-25 from ``health`` + ``storage.reconciliation`` (structure
gate: both are allowlisted shrink-only files and carried the helpers inline,
growing them past baseline).

Why this module exists: operators read lag and reconciliation output directly.
Raw seconds and raw ratio floats are unreadable and — for very large values —
Python/JSON render them in scientific notation (``1e+04%``, ``544257900s``),
which reads as a bug in the metric rather than a real number. Both helpers
below make those values legible and bounded.
"""

from __future__ import annotations

# Above this magnitude a reconciliation ratio is not a meaningful percentage:
# it means the denominator (on-chain balance) is tiny relative to the
# numerator — the mixed-decimal / near-zero-balance regime. Reporting e.g.
# "9.6e+28%" is noise, so callers clamp and label it. 1e6% == 10,000x is
# already far past "MISMATCH"; the exact number adds nothing beyond "absolute".
MAX_MEANINGFUL_PCT = 1e6


def format_pct(value: float) -> str:
    """Render a percentage human-readably (never scientific notation).

    Percentages at or above :data:`MAX_MEANINGFUL_PCT` are labelled
    ``">1e6% (absolute)"`` instead of printing a meaningless huge ratio, so a
    near-zero denominator can never surface as ``1e+04%`` or ``9.6e+28%``.

    Args:
        value: Percentage value (may be negative or huge).

    Returns:
        Human-readable string, e.g. ``"0.5000%"``, ``"900%"``, ``"9.0k%"``,
        ``">1e6% (absolute)"``.
    """
    v = abs(float(value))
    if v >= MAX_MEANINGFUL_PCT:
        return ">1e6% (absolute)"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k%"
    if v >= 10:
        return f"{v:.0f}%"
    if v >= 1:
        return f"{v:.2f}%"
    return f"{v:.4f}%"


def format_duration(seconds: float) -> str:
    """Render a seconds figure as a compact human-readable duration.

    Lag estimates for fast-block chains can be enormous; a raw integer
    seconds count is unreadable (and a raw ``// 60`` turns 143 *days* into
    "143 min"). Renders ``d`` / ``h`` / ``m`` / ``s`` with a single unit and
    clamps negatives to ``0s``.

    Args:
        seconds: Duration in seconds (negatives clamp to 0).

    Returns:
        Compact string such as ``"3d 4h"``, ``"2h 23m"``, ``"42s"``.
    """
    total = int(max(0.0, seconds))
    if total >= 86400:
        days, rem = divmod(total, 86400)
        hours = rem // 3600
        return f"{days}d" + (f" {hours}h" if hours else "")
    if total >= 3600:
        hours, rem = divmod(total, 3600)
        minutes = rem // 60
        return f"{hours}h" + (f" {minutes}m" if minutes else "")
    if total >= 60:
        return f"{total // 60}m"
    return f"{total}s"


__all__ = ["MAX_MEANINGFUL_PCT", "format_duration", "format_pct"]
