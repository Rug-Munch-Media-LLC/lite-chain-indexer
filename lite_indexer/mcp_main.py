"""Console entry point for the Lite Chain Indexer MCP server."""

from __future__ import annotations


def main() -> None:
    """Start the FastMCP server.

    Imports lazily so a missing ``fastmcp`` extra reports at run time, not at
    import of every module that touches this package.
    """
    from lite_indexer.mcp_server import mcp

    mcp.run()


if __name__ == "__main__":
    main()


__all__ = ["main"]
