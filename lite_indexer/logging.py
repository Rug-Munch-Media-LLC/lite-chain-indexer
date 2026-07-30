"""
Structured logging setup for lite-chain-indexer.

Replaces app.core.logging from rmi-backend with a standalone
structlog-based logger for the standalone package.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog


def setup_logging(level: str | None = None) -> None:
    """Configure structlog for the lite-chain-indexer.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
               Defaults to the LOG_LEVEL env var or INFO.
    """
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()

    # Standard library logging — capture warnings too
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level, logging.INFO),
        stream=sys.stderr,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
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
    return structlog.get_logger(name or __name__)
