"""Tests for the shared env-file loader (lite_indexer/env.py).

Covers:

1. KEY=VALUE lines are loaded into os.environ
2. Existing environment variables win (setdefault semantics)
3. Empty values are skipped (would otherwise produce keyless URLs)
4. Comments / blanks / malformed lines are ignored
5. Missing / unreadable file is a silent no-op
6. Default-path load is guarded to run once per process
"""

from __future__ import annotations

# Test methods intentionally omit return annotations (repo-wide test
# convention); scope mypy's no-untyped-def to this file only.
# mypy: disable-error-code="no-untyped-def"
import os
from pathlib import Path

import pytest

from lite_indexer.env import load_env_file


def test_loads_key_value_lines(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_file = tmp_path / "test.env"
    env_file.write_text("ALCHEMY_KEY=abc123\nINFURA_KEY=xyz789\n")
    monkeypatch.delenv("ALCHEMY_KEY", raising=False)
    monkeypatch.delenv("INFURA_KEY", raising=False)

    load_env_file(env_file)

    assert os.environ.get("ALCHEMY_KEY") == "abc123"
    assert os.environ.get("INFURA_KEY") == "xyz789"


def test_existing_env_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_file = tmp_path / "test.env"
    env_file.write_text("ALCHEMY_KEY=from-file\n")
    monkeypatch.setenv("ALCHEMY_KEY", "from-process")

    load_env_file(env_file)

    assert os.environ.get("ALCHEMY_KEY") == "from-process"


def test_empty_value_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_file = tmp_path / "test.env"
    env_file.write_text("ALCHEMY_KEY=\nINFURA_KEY=ok\n")
    monkeypatch.delenv("ALCHEMY_KEY", raising=False)
    monkeypatch.delenv("INFURA_KEY", raising=False)

    load_env_file(env_file)

    assert "ALCHEMY_KEY" not in os.environ
    assert os.environ.get("INFURA_KEY") == "ok"


def test_comments_blanks_and_malformed_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env_file = tmp_path / "test.env"
    env_file.write_text("# comment\n\nALCHEMY_KEY=abc\nno-equals-here\n   \n   HELIUS_KEY=  padded  \n")
    # The host's real env file may have loaded ALCHEMY_KEY via the default
    # path already — clear it so the temp file is authoritative.
    monkeypatch.delenv("ALCHEMY_KEY", raising=False)
    monkeypatch.delenv("HELIUS_KEY", raising=False)

    load_env_file(env_file)

    assert os.environ.get("ALCHEMY_KEY") == "abc"
    assert os.environ.get("HELIUS_KEY") == "padded"


def test_missing_file_is_silent_noop(tmp_path: Path):
    load_env_file(tmp_path / "does-not-exist.env")  # must not raise


def test_default_path_loaded_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The canonical default path is loaded at most once per process."""
    import lite_indexer.env as env_module

    env_file = tmp_path / "default.env"
    env_file.write_text("ALCHEMY_KEY=first\n")
    monkeypatch.setenv("LITE_INDEXER_ENV_FILE", str(env_file))
    monkeypatch.delenv("ALCHEMY_KEY", raising=False)
    # Other test modules may have already triggered the default load;
    # reset the guard so this test controls the default-path call.
    env_module._loaded_default = False

    load_env_file()  # default path
    first = os.environ.get("ALCHEMY_KEY")

    # Overwrite the file — the second default-path call must NOT re-read it.
    env_file.write_text("ALCHEMY_KEY=second\n")
    load_env_file()

    assert os.environ.get("ALCHEMY_KEY") == first == "first"
