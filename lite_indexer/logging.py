"""
Structured logging setup for lite-chain-indexer.

Replaces app.core.logging from rmi-backend with a standalone
structlog-based logger for the standalone package.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import MutableMapping
from typing import Any, cast

import structlog

# ── Secret redaction ───────────────────────────────────────────────────
# Our own events log endpoint URLs, which embed provider API keys/tokens
# (query params, path segments, userinfo). Scrub them before they reach
# journald/CI. 2026-09-13 incident: keys were visible in capability logs.
_SECRET_QUERY = re.compile(r"(?i)(apikey|api_key|dkey|key|token|access_token|secret|password)=([^&\s\"']+)")
_USERINFO = re.compile(r"://[^/@:\s]+:[^/@\s]+@")
# 16+ char URL path segments are key-like (provider keys run 16–64 chars, e.g.
# an 18-char provider key leaked with a 20-char threshold on 2026-09-13).
_PATH_SEG = re.compile(r"/[A-Za-z0-9_\-]{16,}")


def _redact_secrets(value: str) -> str:
    """Redact secret-looking tokens from a string.

    Query-param secrets and URL userinfo are always masked; key-like URL
    path segments are masked only when the string is a URL, so ordinary
    filesystem paths stay readable.
    """
    value = _USERINFO.sub("://***@", value)
    value = _SECRET_QUERY.sub(lambda m: f"{m.group(1)}=***", value)
    if "://" in value:
        value = _PATH_SEG.sub("/***", value)
    return value


def _redact_value(value: Any) -> Any:
    """Recursively redact strings inside scalars, lists, tuples, and dicts."""
    if isinstance(value, str):
        return _redact_secrets(value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, dict):
        return {k: _redact_value(v) for k, v in value.items()}
    return value


def redact_secrets(_logger: Any, _method: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """structlog processor: mask secrets anywhere in the event payload.

    Handles top-level fields, nested lists (e.g. ``down_endpoints``) and
    dicts, so provider keys never reach journald/CI logs.
    """
    for key, value in list(event_dict.items()):
        event_dict[key] = _redact_value(value)
    return event_dict


def setup_logging(level: str | None = None) -> None:
    """Configure structlog for the lite-chain-indexer.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
               Defaults to the LOG_LEVEL env var or INFO.
    """
    log_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()

    # Standard library logging — capture warnings too
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level, logging.INFO),
        stream=sys.stderr,
    )

    # httpx/httpcore emit "HTTP Request: POST <full url>" at INFO — and our
    # endpoint URLs embed provider API keys/tokens. Silence them so
    # credentials never land in journald/CI logs. (2026-09-13 incident.)
    for _noisy in ("httpx", "httpcore"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # Scrub provider secrets from logged URLs before rendering.
            redact_secrets,
            # JSON rendering for production
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a structlog logger instance.

    Args:
        name: Logger name (defaults to caller's module name).

    Returns:
        A structlog BoundLogger.
    """
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name or __name__))
