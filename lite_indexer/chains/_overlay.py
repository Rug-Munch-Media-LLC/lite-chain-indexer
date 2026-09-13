"""
Runtime endpoint overlay — separate curated/freemium providers from the
public OSS endpoint pool.

The public repository ships only FREE, KEYLESS public RPC endpoints plus
this extension point. Curated providers that need an API key or a
provisioned URL (``${PROVIDER_KEY}`` templates) live in a PRIVATE JSON
overlay OUTSIDE the repo, supplied at runtime via the
``LITE_INDEXER_ENDPOINT_OVERLAY`` environment variable. The overlay file is
never committed and contains no secret values — only URL templates whose
placeholders resolve from the environment.

Overlay JSON format (both shapes accepted per chain)::

    {
      "ethereum": {
        "endpoints": [
          "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}"
        ],
        "ws": [
          "wss://mainnet.example-rpc.com/ws/${YOUR_PROVIDER_KEY}"
        ],
        "notes": {
          "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}": "T4 — optional curated provider"
        }
      },
      "solana": ["https://solana.example-rpc.com/?api-key=${YOUR_PROVIDER_KEY}"]
    }

``endpoints`` preserves the recorded order (highest tier first); ``ws`` is an
optional websocket list (or single string) for the real-time head pool; a
``ws://`` / ``wss://`` entry listed under ``endpoints`` is also recognised as
a websocket (and never dialed by the HTTP client). ``notes`` is optional
metadata (tier annotations / comments). A flat list is shorthand for
``{"endpoints": [...]}``.

Merge semantics: builtin (public, free/keyless) endpoints keep their order and
come first; overlay endpoints are appended in their recorded order, minus any
URL already present. Missing/unreadable overlay → ``{}`` (public free-only
defaults), never an exception.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

DEFAULT_OVERLAY_PATH = "/srv/secrets/lite-indexer-endpoints.json"

_WS_SCHEMES = ("ws://", "wss://")


def _is_ws_url(url: str) -> bool:
    """Whether ``url`` uses a websocket scheme (``ws://`` / ``wss://``)."""
    return url.lower().startswith(_WS_SCHEMES)


def _overlay_path(path: str | Path | None = None) -> Path:
    """Resolve the overlay path (explicit arg → env → default)."""
    if path is not None:
        return Path(path)
    return Path(os.environ.get("LITE_INDEXER_ENDPOINT_OVERLAY", DEFAULT_OVERLAY_PATH))


def _chain_entry(value: Any) -> tuple[list[str], dict[str, str]]:
    """Normalise one chain value to ``(endpoints, notes)``.

    Accepts a flat list (endpoint templates) or a mapping with an
    ``endpoints`` list and optional ``notes`` mapping. Websocket URLs are
    removed from the HTTP endpoint list (they are exposed separately by
    :func:`ws_head_endpoints`) so the HTTP client never dials a ``ws://``
    URL. Anything malformed yields empty lists (skip, don't crash).
    """
    if isinstance(value, list):
        endpoints = [str(u) for u in value if isinstance(u, str) and not _is_ws_url(str(u))]
        return endpoints, {}
    if isinstance(value, dict):
        raw = value.get("endpoints", [])
        endpoints = (
            [str(u) for u in raw if isinstance(u, str) and not _is_ws_url(str(u))] if isinstance(raw, list) else []
        )
        raw_notes = value.get("notes", {})
        notes = {str(k): str(v) for k, v in raw_notes.items()} if isinstance(raw_notes, dict) else {}
        return endpoints, notes
    return [], {}


def _raw_ws_urls(value: Any) -> list[str]:
    """Collect websocket URL templates from one overlay chain entry.

    Sources, in order: the ``ws`` key (string or list) plus any ``ws://`` /
    ``wss://`` entries in the ``endpoints`` list (or a flat list entry).
    """
    urls: list[str] = []
    if isinstance(value, list):
        urls.extend(str(u) for u in value if isinstance(u, str))
    elif isinstance(value, dict):
        raw_endpoints = value.get("endpoints", [])
        if isinstance(raw_endpoints, list):
            urls.extend(str(u) for u in raw_endpoints if isinstance(u, str))
        raw_ws = value.get("ws")
        if isinstance(raw_ws, str):
            urls.append(raw_ws)
        elif isinstance(raw_ws, list):
            urls.extend(str(u) for u in raw_ws if isinstance(u, str))
    return [url for url in urls if _is_ws_url(url)]


def load_endpoint_overlay(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Load the private endpoint overlay.

    Args:
        path: Explicit overlay path (tests). Defaults to
            ``$LITE_INDEXER_ENDPOINT_OVERLAY`` or
            :data:`DEFAULT_OVERLAY_PATH`.

    Returns:
        Mapping ``{chain: {"endpoints": [...], "notes": {...}}}``. Empty
        dict when the file is missing, unreadable, or not valid JSON —
        callers fall back to the public free-only defaults. Never raises.
    """
    resolved = _overlay_path(path)
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.debug("endpoint_overlay_absent", path=str(resolved))
        return {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("endpoint_overlay_load_failed", path=str(resolved), error=str(exc)[:100])
        return {}

    if isinstance(raw, dict) and isinstance(raw.get("chains"), dict):
        raw = raw["chains"]
    if not isinstance(raw, dict):
        logger.warning("endpoint_overlay_bad_shape", path=str(resolved))
        return {}

    overlay: dict[str, dict[str, Any]] = {}
    for chain, value in raw.items():
        endpoints, notes = _chain_entry(value)
        ws_urls = _raw_ws_urls(value)
        if endpoints or ws_urls:
            overlay[str(chain)] = {"endpoints": endpoints, "notes": notes, "ws": ws_urls}
    logger.debug("endpoint_overlay_loaded", path=str(resolved), chains=len(overlay))
    return overlay


def resolve_endpoint_urls(urls: list[str]) -> list[str]:
    """Expand ``${ENV_VAR}`` placeholders and drop unresolved entries.

    A URL whose placeholder cannot be resolved can never be dialed (it would
    only produce DNS/JSON errors), so it is excluded from the active list.
    """
    resolved: list[str] = []
    for url in urls:
        expanded = os.path.expandvars(url)
        if "${" in expanded:
            logger.debug("endpoint_unresolved_env_skipped", url=url)
            continue
        resolved.append(expanded)
    return resolved


def ws_head_endpoints(chain: str, overlay: dict[str, Any] | None = None) -> list[str]:
    """Resolve the overlay websocket endpoints for one chain.

    Websocket endpoints are **overlay-only** — the public tree ships no
    provider websockets. A chain entry contributes websockets from its
    ``ws`` list/string and from any ``ws://`` / ``wss://`` entry in
    ``endpoints``. Order is preserved, duplicates are dropped,
    ``${ENV_VAR}`` placeholders are resolved, and unresolved entries are
    excluded. Returns ``[]`` when the overlay is absent — the caller then
    keeps HTTP polling as the sole tip source.

    Args:
        chain: Chain name (e.g. ``ethereum``).
        overlay: Loaded overlay mapping; loaded from
            ``LITE_INDEXER_ENDPOINT_OVERLAY`` when ``None``.

    Returns:
        Dialable ``wss://`` (or ``ws://``) URLs, possibly empty.
    """
    source = overlay if overlay is not None else load_endpoint_overlay()
    entry = (source or {}).get(chain)
    if not entry:
        return []
    raw_ws = _raw_ws_urls(entry)
    deduped = list(dict.fromkeys(raw_ws))
    return resolve_endpoint_urls(deduped)


def merge_endpoints(
    builtin: dict[str, list[str]],
    overlay: dict[str, dict[str, Any]] | None,
) -> dict[str, list[str]]:
    """Append overlay endpoints after builtin free/keyless endpoints.

    Builtin order is preserved (free-first); overlay entries are appended in
    their recorded order. Duplicates (by exact URL template) are dropped,
    keeping the first occurrence. Chains present only in the overlay are
    included.

    Args:
        builtin: Builtin pool ``{chain: [url template, ...]}``.
        overlay: Loaded overlay mapping (or ``None``/``{}``).

    Returns:
        New mapping with merged, de-duplicated, order-preserving lists.
    """
    merged: dict[str, list[str]] = {chain: list(urls) for chain, urls in builtin.items()}
    for chain, entry in (overlay or {}).items():
        endpoints, _notes = _chain_entry(entry)
        target = merged.setdefault(chain, [])
        seen = set(target)
        for url in endpoints:
            if url not in seen:
                seen.add(url)
                target.append(url)
    return merged


__all__ = [
    "DEFAULT_OVERLAY_PATH",
    "load_endpoint_overlay",
    "merge_endpoints",
    "resolve_endpoint_urls",
    "ws_head_endpoints",
]
