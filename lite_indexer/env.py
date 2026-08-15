"""
Shared environment-file loading for LiteChainIndexer.

The daemon's systemd unit sources ``/var/data/chain-indexer/lite-indexer.env``
(``EnvironmentFile``), so RPC API-key placeholders (``${ALCHEMY_KEY}``,
``${INFURA_KEY}``, ``${HELIUS_KEY}``, ...) resolve at import time for the
daemon. Other entry points — the health module, integrity checks, cron
jobs, manual invocations — do NOT inherit that systemd environment, so the
same placeholders stay unresolved and those endpoints are silently skipped
(``endpoint_unresolved_env_skipped``).

:func:`load_env_file` closes that gap: it reads the same env file the
daemon uses (path overridable via ``LITE_INDEXER_ENV_FILE``) into the
current process environment **before** ``lite_indexer.chains.endpoints``
performs its module-level placeholder resolution. It is:

- **idempotent** — runs once per process (module-level guard);
- **non-destructive** — existing environment variables always win
  (``setdefault`` semantics), so a systemd ``EnvironmentFile`` or an
  explicitly exported key is never overridden;
- **silent on failure** — a missing or unreadable file is skipped, never
  an error (the file is root-only 0600 and may be unreadable to the API
  service's unprivileged user).
"""

from __future__ import annotations

import os
from pathlib import Path

# Canonical env file written by ops (mirrors the daemon's EnvironmentFile).
_DEFAULT_ENV_FILE = "/var/data/chain-indexer/lite-indexer.env"

# Guards the DEFAULT-path load so it runs once per process. Explicit
# ``env_file`` calls (tests) always load.
_loaded_default = False


def load_env_file(env_file: str | os.PathLike[str] | None = None) -> None:
    """Load KEY=VALUE lines from the indexer env file into os.environ.

    Only keys that are not already set are applied, and only non-empty
    values are applied (an empty value would leave a ``${VAR}``-free but
    keyless URL that gets dialed pointlessly). Safe to call repeatedly.

    Args:
        env_file: Path to the env file. Defaults to ``LITE_INDEXER_ENV_FILE``
            or the canonical ``/var/data/chain-indexer/lite-indexer.env``.
            The default path is loaded at most once per process; explicit
            paths are always read (tests).
    """
    global _loaded_default
    if env_file is None:
        if _loaded_default:
            return
        _loaded_default = True
    path = env_file or os.environ.get("LITE_INDEXER_ENV_FILE") or _DEFAULT_ENV_FILE
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, ValueError):
        return  # missing / unreadable / binary — nothing to load

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        os.environ.setdefault(key, value)


__all__ = ["load_env_file"]
