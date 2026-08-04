"""
Plugin system — user-extensible contract indexing.

Plugins let users add custom contract indexing without modifying core
code: a plugin is a folder with a ``manifest.yaml`` (declaring which
contracts/events to index and which handler methods to call) and a
Python module extending :class:`BasePlugin`. The indexer loop dispatches
decoded events to registered plugins.

Public API:

* :class:`BasePlugin` — the class plugin authors extend.
* :class:`PluginLoader` — discovers, validates, and loads plugins.
* :class:`PluginManifest` / :func:`parse_manifest` — manifest model.
* :func:`event_topic` — keccak256 topic hash for an event signature.
* :class:`DecodedEvent` / :class:`PluginContext` — handler arguments.
* :func:`community_plugins` / :func:`find_community` — registry helpers.
* ``python -m lite_indexer.plugin.manager`` — install/list/enable/disable.
"""

from __future__ import annotations

from lite_indexer.plugin.base import (
    BasePlugin,
    ContractSpec,
    DecodedEvent,
    EventSpec,
    PluginContext,
    PluginManifest,
    PluginSchema,
    PluginValidationError,
    decode_event_log,
    event_topic,
    map_ch_type,
    parse_manifest,
)
from lite_indexer.plugin.loader import PluginLoader
from lite_indexer.plugin.registry import community_plugins, find_community

__all__ = [
    "BasePlugin",
    "ContractSpec",
    "DecodedEvent",
    "EventSpec",
    "PluginContext",
    "PluginLoader",
    "PluginManifest",
    "PluginSchema",
    "PluginValidationError",
    "community_plugins",
    "decode_event_log",
    "event_topic",
    "find_community",
    "map_ch_type",
    "parse_manifest",
]
