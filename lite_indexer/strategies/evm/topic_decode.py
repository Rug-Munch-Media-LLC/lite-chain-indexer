"""Decode one ``eth_getLogs`` response for a DEX topic fetch.

Split out of ``strategies/evm/dex_topic.py`` so that module stays under its
shrink-only size ratchet, and so the silent-truncation rule — the one thing
here that can silently corrupt the index — is testable on its own.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.dex.log_context import LogDecodeContext
from lite_indexer.dex.lp_logs import parse_dex_logs
from lite_indexer.logging import get_logger
from lite_indexer.strategies.evm.log_budget import is_truncated_log_response

logger = get_logger(__name__)

#: Refuse to accept a page with fewer than this many bytes as a real answer.
#: A genuine empty result serialises to "[]"; anything shorter is a stub or
#: an error page that happens to parse as JSON.
MIN_RESULT_BYTES = 10


def decode_topic_page(
    resp_text: str,
    ctx: LogDecodeContext,
    label: str,
    endpoint: str,
) -> list[dict[str, Any]] | None:
    """Decode a getLogs response, or return ``None`` to rotate the endpoint.

    Returning ``None`` means "this endpoint did not serve a complete page".
    That covers three distinct cases, and the caller must treat all of them the
    same way: penalise the endpoint and try the next one.

    * a body too short to be a real result (a stub or an error page);
    * malformed JSON;
    * **silent truncation** — a capped public RPC answers HTTP 200 with a
      SHORT log list and no error. Accepting it lets the watermark advance
      past blocks that were never queried, so the page is rejected and the
      endpoint is penalised.

    Args:
        resp_text: The response body, already read off the socket.
        ctx: Chain, block range, topic and pair tokens for this fetch.
        label: Human label for logging ("v2", "v3", "lp_v2", ...).
        endpoint: Endpoint URL, for the log record.

    Returns:
        The parsed event dicts, or ``None`` when the caller should rotate.
    """
    if not resp_text or len(resp_text) < MIN_RESULT_BYTES:
        return None

    import json

    try:
        data = json.loads(resp_text)
    except (json.JSONDecodeError, ValueError):
        return None

    if not (isinstance(data, dict) and "result" in data):
        return None
    logs = data["result"]
    if not isinstance(logs, list):
        return None

    if is_truncated_log_response(logs, ctx.from_block, ctx.to_block):
        logger.warning(
            "dex_getlogs_truncated",
            chain=ctx.chain,
            label=label,
            endpoint=endpoint[:50],
            returned=len(logs),
            from_block=ctx.from_block,
            to_block=ctx.to_block,
        )
        return None

    # parse_dex_logs dispatches per topic, so it covers both swap legs and the
    # LP Mint/Burn legs, with `type` derived from the topic rather than
    # hardcoded per decoder.
    return parse_dex_logs(logs, ctx)


__all__ = ["MIN_RESULT_BYTES", "decode_topic_page"]
