"""
Community plugin registry.

A curated, static list of community plugins that the ``manager`` CLI can
install. Each entry carries install metadata:

* ``name`` — plugin folder name (matches the manifest ``name``).
* ``version`` / ``author`` / ``description`` — display metadata.
* ``chain`` — chain the plugin indexes.
* ``source_type`` — how to install:

  - ``"bundled"`` — ships inside the repo under ``examples/plugins``;
    ``manager install`` copies it from there. This is the zero-network
    path for plugins that live in this repository.
  - ``"git"`` — clone from ``source_url`` at install time.
  - ``"manual"`` — no automated install; the operator copies the plugin
    folder into the plugins directory themselves.

Entries are intentionally plain dicts so the registry stays trivially
extensible and diff-friendly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# Repo-relative location of bundled example plugins. Computed from this
# file: lite_indexer/plugin/registry.py → repo root → examples/plugins.
_BUNDLED_PLUGINS_DIR = Path(__file__).resolve().parents[2] / "examples" / "plugins"

COMMUNITY_PLUGINS: list[dict[str, Any]] = [
    {
        "name": "uniswap-v3-positions",
        "version": "1.0.0",
        "author": "RugMunch",
        "description": "Tracks Uniswap V3 liquidity positions (Increase/DecreaseLiquidity)",
        "chain": "ethereum",
        "source_type": "bundled",
    },
    # Add community plugins here, e.g.:
    # {
    #     "name": "aave-v3-borrows",
    #     "version": "0.1.0",
    #     "author": "CommunityMember",
    #     "description": "Tracks Aave V3 Borrow events",
    #     "chain": "ethereum",
    #     "source_type": "git",
    #     "source_url": "https://github.com/example/aave-v3-borrows",
    # },
]


def community_plugins() -> list[dict[str, Any]]:
    """Return a copy of the community plugin registry."""
    return [dict(entry) for entry in COMMUNITY_PLUGINS]


def find_community(name: str) -> dict[str, Any] | None:
    """Return the registry entry for a plugin name, or None."""
    for entry in COMMUNITY_PLUGINS:
        if entry["name"] == name:
            return dict(entry)
    return None


def bundled_path(name: str) -> Path | None:
    """Return the bundled source folder for a plugin, or None.

    Only meaningful for entries with ``source_type == "bundled"`` —
    returns the corresponding folder under ``examples/plugins`` when it
    exists.
    """
    candidate = _BUNDLED_PLUGINS_DIR / name
    return candidate if candidate.is_dir() else None


__all__ = ["COMMUNITY_PLUGINS", "bundled_path", "community_plugins", "find_community"]
