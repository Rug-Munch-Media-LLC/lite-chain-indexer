"""cgroup v2 reader regression tests (2026-10-24 post-mortem).

Production read 0 bytes for ``memory.current`` and 0 MB for ``memory.max``
inside the container: ``/proc/self/cgroup`` was ``0::/`` (cgroup ROOT, the
normal case in a private cgroup namespace) and the old resolver treated an
empty relative path as a failure. Both readers must also fail OPEN — a
"no cap known" answer, never a fabricated 0 cap that would pause ingestion.
"""

from __future__ import annotations

from pathlib import Path

from lite_indexer.indexer import _cgroup
from lite_indexer.indexer._cgroup import read_bytes, read_mb
from lite_indexer.indexer._memory_cap import cgroup_memory_max_mb


def _root(tmp_path: Path, proc_line: str, files: dict[str, str] | None = None) -> Path:
    """Fake cgroup root + fake /proc/self/cgroup (monkeypatch-free seam)."""
    root = tmp_path / "cgroup"
    root.mkdir()
    for name, content in (files or {"memory.current": "1048576\n"}).items():
        (root / name).write_text(content)
    proc = tmp_path / "proc_cgroup"
    proc.write_text(proc_line)
    _cgroup.PROC_CGROUP = str(proc)
    _cgroup.reset_warnings()
    return root


def test_root_cgroup_resolves(tmp_path: Path, monkeypatch) -> None:
    """``0::/`` means the process IS the root — the container case."""
    root = _root(tmp_path, "0::/\n", {"memory.current": "4294967296\n"})
    assert str(_cgroup.cgroup_dir(str(root))) == str(root)
    assert read_mb("memory.current", str(root)) == 4096


def test_nested_cgroup_resolves(tmp_path: Path) -> None:
    """Systemd-style path still resolves under the root."""
    root = _root(tmp_path, "0::/system.slice/lite-indexer.service\n")
    unit = root / "system.slice" / "lite-indexer.service"
    unit.mkdir(parents=True)
    (unit / "memory.current").write_text("2097152\n")
    assert read_mb("memory.current", str(root)) == 2


def test_missing_controller_fails_open(tmp_path: Path) -> None:
    """No memory.current → unknown (0), one warning, no exception."""
    root = tmp_path / "cgroup"
    root.mkdir()
    proc = tmp_path / "proc"
    proc.write_text("0::/gone.service\n")
    _cgroup.PROC_CGROUP = str(proc)
    _cgroup.reset_warnings()
    assert read_mb("memory.current", str(root)) == 0
    assert read_bytes("memory.max", str(root)) is None


def test_limit_unlimited_is_no_cap(tmp_path: Path) -> None:
    """``memory.max == max`` → 0 (no cap known), not a crash."""
    root = _root(tmp_path, "0::/\n", {"memory.current": "1\n", "memory.max": "max\n"})
    assert read_bytes("memory.max", str(root)) is None
    assert read_mb("memory.max", str(root)) == 0


def test_limit_bytes(tmp_path: Path) -> None:
    """A real cap is reported in MiB through the public helper."""
    root = _root(tmp_path, "0::/\n", {"memory.current": "1\n", "memory.max": "4294967296\n"})
    assert read_bytes("memory.max", str(root)) == 4294967296
    assert read_mb("memory.max", str(root)) == 4096


def test_guard_clamps_when_root_cap_readable(tmp_path: Path) -> None:
    """4 GiB cap must clamp a 5800 MB declared ceiling below the cap."""
    root = _root(tmp_path, "0::/\n", {"memory.current": "1\n", "memory.max": "4294967296\n"})
    _cgroup.CGROUP_ROOT = str(root)
    assert cgroup_memory_max_mb() == 4096
