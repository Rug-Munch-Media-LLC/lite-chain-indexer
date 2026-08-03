"""Uvicorn entrypoint for the Chain Query API.

``python -m api.main`` or ``uvicorn api.main:app``.
"""

from __future__ import annotations

import uvicorn

from api.config import LOG_LEVEL


def main() -> None:
    """Run the API server with uvicorn."""
    uvicorn.run(
        "api.app:app",
        host="0.0.0.0",
        port=8100,
        log_level=LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
