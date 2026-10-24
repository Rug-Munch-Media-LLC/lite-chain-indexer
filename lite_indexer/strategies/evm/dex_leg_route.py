"""Per-leg endpoint rotation for the EVM dex topic fetcher.

Split out of ``dex_topic`` on 2026-10-24 (structure canon: one primary
function per module).

Why this exists: every DEX topic leg used to start its endpoint rotation at
``get_best_endpoint(endpoints)`` — the SAME endpoint for all six legs of a
range (V2 swap, V3 swap, and the four LP Mint/Burn legs). Two consequences,
both measured on bsc 2026-10-24 against ``rpc-bsc.48.club`` with the six
legs of one range fanned out in parallel:

  * 20 blocks: 3.06 s wall clock for all six legs (226/28/8/0/9/0 logs);
  * 100 blocks: 13.91 s wall clock (1164/204/24/3/15/0 logs).

That is the real price of the leg. Production measured 30.1 s for the same
shape, because all six legs started on the same "best" endpoint and that
endpoint (``bsc.api.pocket.network``) was unreachable that hour: every leg
paid the dead-endpoint penalty before falling through to the working one.

Spreading the start index per leg bounds a dead endpoint's blast radius to
one leg and spreads load across the pool instead of concentrating it on one
provider account.
"""

from __future__ import annotations

#: Connect timeout for ONE getLogs attempt inside a dex leg.
#:
#: A dead endpoint must cost a few seconds, not the whole attempt deadline.
#: The retry loop is sequential over the rotated endpoint list, so a 30 s
#: connect penalty per dead endpoint is paid by every chunk of every leg. A
#: healthy endpoint connects in under 100 ms (measured 2026-10-24); 3 s is
#: ~30x that and still collapses a dead endpoint's cost by an order of
#: magnitude.
DEX_LEG_CONNECT_TIMEOUT_S: float = 3.0

#: Leg order used for rotation. Stable (not hashed) so a leg always starts on
#: the same relative endpoint across restarts, which keeps behaviour reviewable.
_LEG_ORDER: tuple[str, ...] = (
    "v2",
    "v3",
    "lp_v2_mint",
    "lp_v2_burn",
    "lp_v3_mint",
    "lp_v3_burn",
)


def leg_start_index(label: str, endpoint_count: int) -> int:
    """Endpoint offset for one dex topic leg.

    Args:
        label: Leg label ("v2", "v3", "lp_v2_mint", ...). An unknown label is
            treated as the first leg, so a new leg degrades to the old
            behaviour instead of raising inside a fetch path.
        endpoint_count: Number of endpoints in the rotation.

    Returns:
        Offset in ``[0, endpoint_count)`` to add to the pool's best-endpoint
        index (modulo the rotated list length). ``0`` when the pool is empty.
    """
    if endpoint_count <= 0:
        return 0
    try:
        return _LEG_ORDER.index(label) % endpoint_count
    except ValueError:
        return 0


__all__ = ["DEX_LEG_CONNECT_TIMEOUT_S", "leg_start_index"]
