"""Typed failures for the RPC edge — never an empty success.

Two shapes only, because a caller must always be able to tell *why* it has no
data:

``RpcUnavailable``
    Every provider was tried and none produced a usable answer, and there is
    no last-known-good value (or it is older than the caller allows). It is
    deliberately NOT ``None``: an empty result that looks like data is how a
    stalled chain reports "healthy".

``QuotaExceeded``
    The request violates the tier policy in
    :mod:`lite_indexer.rpc.tier_quota`. Carries the tier and the read class so
    the caller can degrade (serve cached/aggregated) instead of retrying.

Both carry enough context to log without re-deriving it at the call site.
"""

from __future__ import annotations


class RpcUnavailable(RuntimeError):  # noqa: N818 — name fixed by the public contract
    """No provider answered and no acceptable last-known-good value existed.

    Attributes:
        chain: Chain name whose pool was exhausted.
        age_s: Age of the last-known-good value in seconds, or ``None`` when
            nothing has ever been recorded for the chain.
        detail: Short human-readable reason (for logs, not for control flow).
    """

    def __init__(self, chain: str, *, age_s: float | None = None, detail: str = "") -> None:
        self.chain = chain
        self.age_s = age_s
        self.detail = detail
        suffix = "" if age_s is None else f" (last known good {age_s:.0f}s old)"
        super().__init__(f"RPC unavailable for {chain}{suffix}{': ' + detail if detail else ''}")


class QuotaExceeded(RuntimeError):  # noqa: N818 — name fixed by the public contract
    """The tier policy refused this read, before any provider was dialled.

    Attributes:
        tier: ``"free"`` or ``"paid"``.
        read_class: The refused read class (``head``/``historical``/``aggregated``).
        retry_after_s: Seconds to wait before the same request may be retried;
            ``0.0`` when the refusal is structural (the read class is simply
            not allowed on that tier) rather than rate-based.
    """

    def __init__(
        self,
        tier: str,
        read_class: str,
        *,
        retry_after_s: float = 0.0,
        detail: str = "",
    ) -> None:
        self.tier = tier
        self.read_class = read_class
        self.retry_after_s = retry_after_s
        self.detail = detail
        super().__init__(
            f"quota exceeded: tier={tier} read_class={read_class}"
            + (f" retry_after={retry_after_s:.2f}s" if retry_after_s else "")
            + (f": {detail}" if detail else "")
        )


__all__ = ["QuotaExceeded", "RpcUnavailable"]
