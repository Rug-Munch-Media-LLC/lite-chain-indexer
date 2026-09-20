"""Provider-rotation streaming for :class:`SQDSource` (split for size policy).

Extracted 2026-09-21 from sqd_source.py so the source module stays within the
structure ratchet while the rotation logic lives on its own.
"""

from __future__ import annotations

import json
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_source import SQDPortalError

logger = get_logger(__name__)

#: Bounded rotation: max providers tried per stream before giving up.
_MAX_LAKE_ATTEMPTS = 3


def stream_with_rotation(
    source: Any,
    *,
    dataset: str,
    name: str,
    type_value: str,
    from_block: int,
    to_block: int | None,
    fields: dict[str, Any],
    selectors: dict[str, Any] | None,
    max_batches: int,
    timeout: float,
) -> list[dict[str, Any]]:
    """Bounded provider rotation around one full pagination pass."""
    router = source._router()
    tried: set[str] = set()
    last_error: SQDPortalError | None = None
    for _ in range(_MAX_LAKE_ATTEMPTS):
        prov = router.select(dataset, "sqd", exclude=tried)
        if prov is None:
            logger.warning("sqd_no_provider", dataset=dataset, tried=sorted(tried))
            break
        tried.add(prov.name)
        try:
            blocks = _stream_once(
                source,
                name,
                type_value,
                from_block,
                to_block,
                fields,
                selectors,
                max_batches,
                timeout,
                router,
                prov.name,
            )
        except SQDPortalError as exc:
            last_error = exc
            continue
        router.report_success(prov.name)
        if blocks:
            return blocks
        # empty stream (dataset head reached) -> fall through to next candidate
    if not tried:
        raise SQDPortalError(f"lake router: no provider available for {dataset} (quota/health)")
    if last_error is not None:
        raise last_error
    return []


def _stream_once(
    source: Any,
    name: str,
    type_value: str,
    from_block: int,
    to_block: int | None,
    fields: dict[str, Any],
    selectors: dict[str, Any] | None,
    max_batches: int,
    timeout: float,
    router: Any,
    prov_name: str,
) -> list[dict[str, Any]]:
    """One full pagination pass against a single provider."""
    blocks: list[dict[str, Any]] = []
    cursor = int(from_block)
    for _ in range(max(1, int(max_batches))):
        body: dict[str, Any] = {"type": type_value, "fromBlock": cursor, "fields": fields}
        if to_block is not None:
            body["toBlock"] = int(to_block)
        if selectors:
            body.update(selectors)
        status, text = source._request("POST", f"{source.PORTAL}/datasets/{name}/stream", body, timeout=timeout)
        if status == 204:  # above dataset height - done
            break
        if status != 200:
            router.report_failure(prov_name, error=f"HTTP {status}")
            raise SQDPortalError(f"portal stream error HTTP {status}: {text[:160]}")
        batch = [json.loads(line) for line in text.splitlines() if line.strip()]
        if not batch:
            break
        blocks.extend(batch)
        last = max(int(b.get("header", {}).get("number", cursor)) for b in batch)
        if to_block is not None and last >= int(to_block):
            break
        cursor = last + 1
    return blocks
