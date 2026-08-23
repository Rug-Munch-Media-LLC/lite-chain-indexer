"""Tests for the plugin manager CLI (plugin/manager.py).

Scope: enable/disable marker semantics, exit-code contract, and arg
parsing — the operator-facing surface for managing indexer plugins.
Uses temp plugin dirs; no registry/network access.

Contracts probed against actual behavior first (fleet doctrine).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from lite_indexer.plugin.manager import _parse_args, cmd_disable, cmd_enable


def _ns(td: str, name: str) -> argparse.Namespace:
    return argparse.Namespace(plugins_dir=td, name=name)


def _make_plugin(td: Path, name: str, disabled: bool = False) -> Path:
    p = td / name
    p.mkdir()
    (p / "plugin.toml").write_text(f'name = "{name}"')
    if disabled:
        (p / ".disabled").write_text("")
    return p


class TestEnableDisable:
    def test_enable_removes_disabled_marker(self, tmp_path):
        _make_plugin(tmp_path, "myplug", disabled=True)
        assert cmd_enable(_ns(str(tmp_path), "myplug")) == 0
        assert not (tmp_path / "myplug" / ".disabled").exists()

    def test_disable_creates_marker(self, tmp_path):
        _make_plugin(tmp_path, "myplug")
        assert cmd_disable(_ns(str(tmp_path), "myplug")) == 0
        assert (tmp_path / "myplug" / ".disabled").exists()

    def test_double_disable_is_idempotent_success(self, tmp_path):
        # Probed: disabling an already-disabled plugin succeeds (rc 0) —
        # operators scripting cleanup must not trip on repeat calls.
        _make_plugin(tmp_path, "x", disabled=True)
        assert cmd_disable(_ns(str(tmp_path), "x")) == 0

    def test_missing_plugin_fails_with_rc1(self, tmp_path):
        assert cmd_disable(_ns(str(tmp_path), "ghost")) == 1
        assert cmd_enable(_ns(str(tmp_path), "ghost")) == 1


class TestArgParsing:
    def test_list_defaults(self):
        args = _parse_args(["list"])
        assert args.command == "list"
        assert args.plugins_dir is None  # falls back to PLUGIN_DIR

    def test_install_from_local_path(self):
        args = _parse_args(["install", "--from", "/some/dir", "myplug"])
        assert args.command == "install"
        assert args.source_path == "/some/dir"
        assert args.name == "myplug"

    def test_missing_command_rejected(self):
        with pytest.raises(SystemExit):
            _parse_args([])
