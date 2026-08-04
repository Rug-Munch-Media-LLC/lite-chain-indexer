"""
Plugin discovery, validation, and loading.

:class:`PluginLoader` scans a plugins directory for plugin folders
(a directory containing ``manifest.yaml`` or ``manifest.yml``), parses
and validates each manifest, loads the handler module (default
``plugin.py``), instantiates the ``BasePlugin`` subclass, and verifies
that every handler named in the manifest exists and is callable.

Disabled plugins are skipped: a plugin folder containing a ``.disabled``
marker file is ignored (the :mod:`lite_indexer.plugin.manager` CLI
creates/removes that marker for ``enable``/``disable``).

Plugins are loaded as isolated modules via ``importlib`` so a failing
plugin never breaks the indexer process — load errors are logged and the
plugin is skipped.
"""

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
from typing import Any

import yaml

from lite_indexer.config import PLUGIN_DIR
from lite_indexer.logging import get_logger
from lite_indexer.plugin.base import (
    BasePlugin,
    PluginManifest,
    PluginValidationError,
    parse_manifest,
)

logger = get_logger(__name__)

_MANIFEST_FILENAMES = ("manifest.yaml", "manifest.yml")
_DISABLED_MARKER = ".disabled"
_DEFAULT_HANDLER_MODULE = "plugin.py"


def is_disabled(plugin_dir: Path) -> bool:
    """Return True when a plugin folder carries the ``.disabled`` marker."""
    return (plugin_dir / _DISABLED_MARKER).exists()


def find_manifest(plugin_dir: Path) -> Path | None:
    """Return the manifest file path for a plugin folder, or None."""
    for name in _MANIFEST_FILENAMES:
        candidate = plugin_dir / name
        if candidate.is_file():
            return candidate
    return None


class PluginLoader:
    """Discovers and loads plugins from a directory.

    Args:
        plugins_dir: Directory containing plugin folders. Defaults to
            ``PLUGIN_DIR`` (``LITE_INDEXER_PLUGIN_DIR`` env, falling back
            to ``DATA_DIR/plugins``). When the directory does not exist
            the loader returns an empty set (plugins are optional).
    """

    def __init__(self, plugins_dir: str | Path | None = None) -> None:
        self.plugins_dir = Path(plugins_dir) if plugins_dir is not None else PLUGIN_DIR

    # ── Discovery ────────────────────────────────────────────────────

    def discover(self) -> list[Path]:
        """Return plugin folders present in the plugins directory.

        A plugin folder is a directory containing a manifest file. Folders
        with the ``.disabled`` marker are excluded.
        """
        if not self.plugins_dir.is_dir():
            return []
        folders: list[Path] = []
        for entry in sorted(self.plugins_dir.iterdir()):
            if not entry.is_dir():
                continue
            if is_disabled(entry):
                continue
            if find_manifest(entry) is not None:
                folders.append(entry)
        return folders

    # ── Loading ──────────────────────────────────────────────────────

    def load_all(self, chain: str | None = None) -> list[BasePlugin]:
        """Load every discoverable plugin (optionally filtered by chain).

        A plugin whose manifest is invalid, whose handler module fails to
        import, or whose handlers are incomplete is skipped with a
        warning — the indexer keeps running without it.

        Args:
            chain: When given, only plugins for this chain are loaded.

        Returns:
            List of loaded :class:`BasePlugin` instances.
        """
        plugins: list[BasePlugin] = []
        for plugin_dir in self.discover():
            plugin = self.load_plugin(plugin_dir)
            if plugin is None:
                continue
            if chain is not None and plugin.chain != chain:
                continue
            plugins.append(plugin)
        if plugins:
            logger.info(
                "plugins_loaded",
                plugins_dir=str(self.plugins_dir),
                count=len(plugins),
                names=[p.name for p in plugins],
            )
        return plugins

    def load_plugin(self, plugin_dir: Path) -> BasePlugin | None:
        """Load a single plugin from a plugin folder.

        Returns None (with a logged warning) when the folder is not a
        valid plugin — the caller skips it.

        Args:
            plugin_dir: Directory containing ``manifest.yaml`` + handler.

        Returns:
            Instantiated :class:`BasePlugin` subclass, or None.
        """
        manifest_file = find_manifest(plugin_dir)
        if manifest_file is None:
            logger.warning("plugin_manifest_missing", dir=str(plugin_dir))
            return None

        try:
            manifest = self._parse_manifest_file(manifest_file)
        except (PluginValidationError, yaml.YAMLError) as exc:
            logger.warning(
                "plugin_manifest_invalid",
                dir=str(plugin_dir),
                error=str(exc)[:200],
            )
            return None

        if not manifest.enabled:
            logger.debug("plugin_disabled_in_manifest", name=manifest.name)
            return None

        module_file = plugin_dir / manifest.module
        if not module_file.is_file():
            logger.warning(
                "plugin_module_missing",
                plugin=manifest.name,
                module=manifest.module,
            )
            return None

        module = self._import_module(module_file, manifest.name)
        if module is None:
            return None

        plugin_class = self._find_plugin_class(module, manifest.name)
        if plugin_class is None:
            return None

        try:
            plugin = plugin_class(manifest=manifest)
        except Exception as exc:  # noqa: BLE001 — plugin constructors must not kill the indexer
            logger.warning("plugin_construct_failed", plugin=manifest.name, error=str(exc)[:200])
            return None

        missing = [spec.handler for spec in manifest.events if getattr(plugin, spec.handler, None) is None]
        if missing:
            logger.warning(
                "plugin_handler_missing",
                plugin=manifest.name,
                handlers=missing,
            )
            return None

        return plugin

    # ── Internals ────────────────────────────────────────────────────

    def _parse_manifest_file(self, manifest_file: Path) -> PluginManifest:
        """Read + parse + validate a manifest file."""
        raw = yaml.safe_load(manifest_file.read_text(encoding="utf-8"))
        if raw is None:
            raise PluginValidationError(f"manifest {manifest_file} is empty")
        return parse_manifest(raw)

    def _import_module(self, module_file: Path, plugin_name: str) -> Any | None:
        """Import the plugin handler module as an isolated module."""
        module_name = f"lite_plugin_{plugin_name.replace('-', '_')}"
        try:
            spec = importlib.util.spec_from_file_location(module_name, module_file)
            if spec is None or spec.loader is None:
                logger.warning("plugin_module_spec_failed", plugin=plugin_name)
                return None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as exc:  # noqa: BLE001 — plugin code must not kill the indexer
            logger.warning("plugin_import_failed", plugin=plugin_name, error=str(exc)[:200])
            return None

    def _find_plugin_class(self, module: Any, plugin_name: str) -> type[BasePlugin] | None:
        """Find the BasePlugin subclass defined in a handler module."""
        candidates: list[type[BasePlugin]] = []
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BasePlugin:
                continue
            if issubclass(obj, BasePlugin):
                candidates.append(obj)
        if not candidates:
            logger.warning("plugin_no_base_class", plugin=plugin_name)
            return None
        return candidates[0]


__all__ = ["PluginLoader", "is_disabled", "find_manifest"]
