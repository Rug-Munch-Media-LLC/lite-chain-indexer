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
        "notes": {
          "https://mainnet.example-rpc.com/v1/${YOUR_PROVIDER_KEY}": "T4 — optional curated provider"
        }
      },
      "solana": ["https://solana.example-rpc.com/?api-key=${YOUR_PROVIDER_KEY}"]
    }

``endpoints`` preserves the recorded order (highest tier first); ``notes`` is
optional metadata (tier annotations / comments). A flat list is shorthand for
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


def _overlay_path(path: str | Path | None = None) -> Path:
    """Resolve the overlay path (explicit arg → env → default)."""
    if path is not None:
        return Path(path)
    return Path(os.environ.get("LITE_INDEXER_ENDPOINT_OVERLAY", DEFAULT_OVERLAY_PATH))


def _chain_entry(value: Any) -> tuple[list[str], dict[str, str]]:
    """Normalise one chain value to ``(endpoints, notes)``.

    Accepts a flat list (endpoint templates) or a mapping with an
    ``endpoints`` list and optional ``notes`` mapping. Anything malformed
    yields empty lists (skip, don't crash).
    """
    if isinstance(value, list):
        endpoints = [str(u) for u in value if isinstance(u, str)]
        return endpoints, {}
    if isinstance(value, dict):
        raw = value.get("endpoints", [])
        endpoints = [str(u) for u in raw if isinstance(u, str)] if isinstance(raw, list) else []
        raw_notes = value.get("notes", {})
        notes = {str(k): str(v) for k, v in raw_notes.items()} if isinstance(raw_notes, dict) else {}
        return endpoints, notes
    return [], {}


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
        if endpoints:
            overlay[str(chain)] = {"endpoints": endpoints, "notes": notes}
    logger.debug("endpoint_overlay_loaded", path=str(resolved), chains=len(overlay))
    return overlay


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
]
