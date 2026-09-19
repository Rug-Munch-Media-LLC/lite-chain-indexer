"""Tests for the cgroup-cap memory recycle guard (2026-09-19 OOM fix).

A transient systemd override set ``MemoryMax=4G`` while the unit declared
``MEMORY_RECYCLE_MB=5800`` — the in-process recycle guard could never fire
and the kernel OOM-killed the daemon. These tests pin the clamp math, the
``memory.max`` reader, and the incident regression. NO monkeypatching:
the functions take explicit ``path=`` / ``cap_mb=`` seams.
"""

from __future__ import annotations

from pathlib import Path

from lite_indexer.indexer._memory_cap import (
    cgroup_memory_max_mb,
    clamp_recycle_mb,
    resolve_recycle_mb,
)


def _write(tmp_path: Path, content: str) -> str:
    f = tmp_path / "memory.max"
    f.write_text(content)
    return str(f)


def test_clamp_incident_regression() -> None:
    """5800 declared under a 4096 cap → 4096 - max(512, 614) = 3482."""
    assert clamp_recycle_mb(5800, 4096) == 3482


def test_clamp_unchanged_when_below_ceiling() -> None:
    assert clamp_recycle_mb(5800, 7168) == 5800  # 5800 <= 7168-1075=6093


def test_clamp_disabled_declared() -> None:
    assert clamp_recycle_mb(0, 4096) == 0


def test_clamp_no_cap() -> None:
    assert clamp_recycle_mb(3072, 0) == 3072


def test_clamp_tiny_cap_still_positive() -> None:
    # 600 cap → ceiling 600-512=88 → positive, never 0/negative.
    assert clamp_recycle_mb(5800, 600) == 88


def test_clamp_declared_equals_ceiling_boundary() -> None:
    cap = 4096
    ceiling = cap - max(512, int(cap * 0.15))
    assert clamp_recycle_mb(ceiling, cap) == ceiling
    assert clamp_recycle_mb(ceiling + 1, cap) == ceiling


def test_cgroup_memory_max_unlimited(tmp_path: Path) -> None:
    assert cgroup_memory_max_mb(path=_write(tmp_path, "max\n")) == 0


def test_cgroup_memory_max_bytes(tmp_path: Path) -> None:
    assert cgroup_memory_max_mb(path=_write(tmp_path, "7516192768\n")) == 7168


def test_cgroup_memory_max_missing(tmp_path: Path) -> None:
    assert cgroup_memory_max_mb(path=str(tmp_path / "nope")) == 0


def test_cgroup_memory_max_garbage(tmp_path: Path) -> None:
    assert cgroup_memory_max_mb(path=_write(tmp_path, "not-a-number\n")) == 0


def test_resolve_recycle_mb_incident_regression() -> None:
    """Declared 5800 under a 4096 cap resolves to 3482 (this incident)."""
    assert resolve_recycle_mb(cap_mb=4096, declared_mb=5800) == 3482


def test_resolve_recycle_mb_no_cap_uses_declared() -> None:
    from lite_indexer.config import MEMORY_RECYCLE_MB

    assert resolve_recycle_mb(cap_mb=0) == MEMORY_RECYCLE_MB
