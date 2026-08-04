"""
Plugin manager CLI — install / list / enable / disable.

Usage::

    python -m lite_indexer.plugin.manager list
    python -m lite_indexer.plugin.manager install uniswap-v3-positions
    python -m lite_indexer.plugin.manager install --from /path/to/plugin my-plugin
    python -m lite_indexer.plugin.manager enable uniswap-v3-positions
    python -m lite_indexer.plugin.manager disable uniswap-v3-positions

Plugins are installed into the plugins directory (``LITE_INDEXER_PLUGIN_DIR``
env var, default ``DATA_DIR/plugins``). Enabling/disabling toggles a
``.disabled`` marker file inside the plugin folder — the loader skips
folders with that marker.

Exit codes: 0 success, 1 usage/install error.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from lite_indexer.config import PLUGIN_DIR
from lite_indexer.logging import get_logger
from lite_indexer.plugin.loader import PluginLoader, is_disabled
from lite_indexer.plugin.registry import bundled_path, find_community

logger = get_logger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="lite-indexer-plugin",
        description="Install, list, enable, and disable lite-indexer plugins",
    )
    parser.add_argument(
        "--plugins-dir",
        type=str,
        default=None,
        help=f"Plugins directory (default: {PLUGIN_DIR})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list", help="List installed + community plugins")
    list_parser.add_argument("--all", action="store_true", help="Include community registry")

    install_parser = sub.add_parser("install", help="Install a plugin")
    install_parser.add_argument("name", help="Plugin name (community registry) or target name")
    install_parser.add_argument(
        "--from",
        dest="source_path",
        type=str,
        default=None,
        help="Install from a local plugin folder instead of the registry",
    )

    enable_parser = sub.add_parser("enable", help="Enable a plugin")
    enable_parser.add_argument("name", help="Plugin name")

    disable_parser = sub.add_parser("disable", help="Disable a plugin")
    disable_parser.add_argument("name", help="Plugin name")

    return parser.parse_args(argv)


def _plugins_dir(args: argparse.Namespace) -> Path:
    return Path(args.plugins_dir) if args.plugins_dir else PLUGIN_DIR


def _installed_plugin_dir(plugins_dir: Path, name: str) -> Path:
    return plugins_dir / name


def cmd_list(args: argparse.Namespace) -> int:
    """Print installed plugins (and optionally the community registry)."""
    plugins_dir = _plugins_dir(args)
    loader = PluginLoader(plugins_dir=plugins_dir)

    installed = loader.discover()
    print(f"plugins_dir: {plugins_dir}")  # noqa: T201
    if not installed:
        print("(no plugins installed)")  # noqa: T201
    for folder in installed:
        marker = " [disabled]" if is_disabled(folder) else ""
        manifest_file = folder / "manifest.yaml" if (folder / "manifest.yaml").is_file() else folder / "manifest.yml"
        name = manifest_file.parent.name
        print(f"  - {name}{marker}")  # noqa: T201
        if manifest_file.is_file():
            import yaml

            raw = yaml.safe_load(manifest_file.read_text(encoding="utf-8")) or {}
            print(  # noqa: T201
                f"      v{raw.get('version', '?')} · chain={raw.get('chain', '?')} · {raw.get('description', '')}"
            )

    if args.all:
        print("\ncommunity registry:")  # noqa: T201
        from lite_indexer.plugin.registry import community_plugins

        for entry in community_plugins():
            print(  # noqa: T201
                f"  - {entry['name']} v{entry['version']} (chain={entry['chain']}) "
                f"- {entry['description']} [{entry['source_type']}]"
            )
    return 0


def cmd_install(args: argparse.Namespace) -> int:
    """Install a plugin from the registry or a local folder."""
    plugins_dir = _plugins_dir(args)
    plugins_dir.mkdir(parents=True, exist_ok=True)
    target = _installed_plugin_dir(plugins_dir, args.name)
    if target.exists():
        print(f"error: plugin already installed at {target}")  # noqa: T201
        return 1

    if args.source_path:
        source = Path(args.source_path).expanduser()
        if not source.is_dir():
            print(f"error: source folder does not exist: {source}")  # noqa: T201
            return 1
        return _copy_plugin(source, target, args.name)

    entry = find_community(args.name)
    if entry is None:
        print(f"error: unknown plugin {args.name!r} — see `list --all`")  # noqa: T201
        return 1

    if entry.get("source_type") == "bundled":
        bundled = bundled_path(args.name)
        if bundled is None:
            print(f"error: bundled source for {args.name!r} not found in examples/plugins")  # noqa: T201
            return 1
        return _copy_plugin(bundled, target, args.name)

    if entry.get("source_type") == "git":
        url = entry.get("source_url", "")
        if not url:
            print(f"error: registry entry {args.name!r} has no source_url")  # noqa: T201
            return 1
        print(f"cloning {url} -> {target}")  # noqa: T201
        import subprocess

        result = subprocess.run(  # noqa: S603
            ["git", "clone", "--depth", "1", url, str(target)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"error: git clone failed: {result.stderr[:300]}")  # noqa: T201
            return 1
        print(f"installed {args.name} from git")  # noqa: T201
        return 0

    print(f"error: plugin {args.name!r} is manual-install only — copy it into {plugins_dir}")  # noqa: T201
    return 1


def _copy_plugin(source: Path, target: Path, name: str) -> int:
    """Copy a plugin folder into the plugins directory."""
    shutil.copytree(source, target)
    print(f"installed {name} -> {target}")  # noqa: T201
    return 0


def cmd_enable(args: argparse.Namespace) -> int:
    """Remove the ``.disabled`` marker from a plugin folder."""
    plugins_dir = _plugins_dir(args)
    target = _installed_plugin_dir(plugins_dir, args.name)
    if not target.is_dir():
        print(f"error: plugin {args.name!r} is not installed")  # noqa: T201
        return 1
    marker = target / ".disabled"
    if marker.exists():
        marker.unlink()
    print(f"enabled {args.name}")  # noqa: T201
    return 0


def cmd_disable(args: argparse.Namespace) -> int:
    """Add the ``.disabled`` marker to a plugin folder."""
    plugins_dir = _plugins_dir(args)
    target = _installed_plugin_dir(plugins_dir, args.name)
    if not target.is_dir():
        print(f"error: plugin {args.name!r} is not installed")  # noqa: T201
        return 1
    marker = target / ".disabled"
    marker.touch(exist_ok=True)
    print(f"disabled {args.name}")  # noqa: T201
    return 0


def main(argv: list[str] | None = None) -> int:
    """Plugin manager entry point."""
    args = _parse_args(argv)
    if args.command == "list":
        return cmd_list(args)
    if args.command == "install":
        return cmd_install(args)
    if args.command == "enable":
        return cmd_enable(args)
    if args.command == "disable":
        return cmd_disable(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
