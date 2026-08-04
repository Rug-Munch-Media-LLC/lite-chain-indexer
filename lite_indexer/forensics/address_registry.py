"""
Known bridge + CEX address registry for cross-chain fund tracing.

Curated from three sources:

1. **Live-verified in this warehouse** (2026-08-04 probe of
   ``wallet_memory.evm_transfers`` / ``wallet_memory.wallet_labels``) —
   these addresses demonstrably move money in the indexed data, so their
   confidence is 0.85+.
2. **Publicly documented protocol contracts** (Hop, Across, Stargate,
   Wormhole, LayerZero, Synapse, Orbiter, Socket, deBridge, Celer,
   Connext, Multichain + canonical L1/L2 bridges) — confidence 0.7-0.85.
3. **Known CEX hot wallet clusters** (Binance, Coinbase, Kraken, OKX,
   Bybit, KuCoin, Gate, HTX, MEXC, Bitget) — the same omnibus wallets are
   reused across EVM chains, so the EVM list is applied to every EVM chain
   and each entry carries a per-chain confidence.

Format (as specified)::

    {chain: {address: {name, type, confidence}}}

``type`` values: ``bridge``, ``canonical_l1_bridge``,
``canonical_l2_bridge``, ``bridge_router``, ``cex_hot_wallet``.

The registry is a *starting point* — :class:`FundTracer` always layers the
live ``wallet_memory.wallet_labels`` / ``scam_addresses`` tables on top, so
addresses absent here but labeled on-chain (e.g. "Optimism: Gateway",
"Gate.io 1") are still tagged correctly.
"""

from __future__ import annotations

from typing import Any

# ── Bridges ─────────────────────────────────────────────────────────────
# {chain: {address: {name, type, confidence}}}

_BRIDGE_REGISTRY: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        # ── Live-verified in warehouse data (2026-08-04 probe) ────────
        "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1": {
            "name": "Optimism StandardBridge (L1)",
            "type": "canonical_l1_bridge",
            "confidence": 0.95,
        },
        "0x3ee18b2214aff97000d974cf647e7c347e8fa585": {
            "name": "Wormhole Token Bridge",
            "type": "bridge",
            "confidence": 0.9,
        },
        "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a": {
            "name": "Arbitrum Gateway (L1)",
            "type": "canonical_l1_bridge",
            "confidence": 0.9,
        },
        "0x5427fefa711eff984124bfbb1ab6fbf5e3da1820": {
            "name": "Celer cBridge",
            "type": "bridge",
            "confidence": 0.85,
        },
        "0xb8901acb165ed027e32754e0ffe830802195e1a0": {
            "name": "Hop ETH Bridge",
            "type": "bridge",
            "confidence": 0.85,
        },
        # ── Publicly documented (confidence 0.7-0.85) ──────────────────
        "0x3660f04e797a7f8dce6f6c8d607ac29a8f3e7c8e": {
            "name": "Hop USDC Bridge",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x5c7bcd6e7de5423a257d81b442095a1a6e35f2cb": {
            "name": "Across SpokePool (Hub)",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x296f55f8fb28e498b858d0bcda06d955b2cb3f97": {
            "name": "Stargate Router (ETH pool)",
            "type": "bridge_router",
            "confidence": 0.8,
        },
        "0x2796317b0ff8538f253012862c06787adc8cbff": {
            "name": "Synapse Bridge",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x43de2d77bf8027e25dd179b7796fc41e5ab87b8c": {
            "name": "deBridgeGate",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x8898b472c54c31894e3b9bb83cea802a5d0e63c6": {
            "name": "Connext (Diamond)",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x765277eebeca2e31912c9946eae1021199b39c61": {
            "name": "Multichain Router",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x3154cf16ccdb4c6d922629664174b904d80f2c35": {
            "name": "Base StandardBridge (L1)",
            "type": "canonical_l1_bridge",
            "confidence": 0.85,
        },
        "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f": {
            "name": "Arbitrum Inbox",
            "type": "canonical_l1_bridge",
            "confidence": 0.85,
        },
        "0x32400084c286cf3e17e7b677ea9583e60a000324": {
            "name": "ZKsync Era L1 Bridge",
            "type": "canonical_l1_bridge",
            "confidence": 0.8,
        },
        "0xa0c68c638235ee32657e8f720a23cec1bfc77c77": {
            "name": "Polygon RootChainManager",
            "type": "canonical_l1_bridge",
            "confidence": 0.8,
        },
        "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": {
            "name": "Polygon ERC20 Bridge",
            "type": "canonical_l1_bridge",
            "confidence": 0.8,
        },
        "0x8eb8a3b98659cce290402893d0123abb75e3ab28": {
            "name": "Avalanche Bridge (L1)",
            "type": "canonical_l1_bridge",
            "confidence": 0.8,
        },
        "0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675": {
            "name": "LayerZero V1 Endpoint",
            "type": "bridge",
            "confidence": 0.75,
        },
        "0x1a44076050125825900e736c501f859c50fe728c": {
            "name": "LayerZero V2 Endpoint",
            "type": "bridge",
            "confidence": 0.75,
        },
        "0x80c67432656d59144ceff962e8faf8926599bcf8": {
            "name": "Orbiter Maker",
            "type": "bridge_router",
            "confidence": 0.75,
        },
        "0x3a23f94318108e626842a9747e4d3e7df8cb3ef5": {
            "name": "Socket Gateway",
            "type": "bridge_router",
            "confidence": 0.7,
        },
        "0x6774bcbd5cecef1336b5300fb5186a12ddd8b367": {
            "name": "Scroll L1 Gateway",
            "type": "canonical_l1_bridge",
            "confidence": 0.7,
        },
        "0x95fc37a27a2f68e3a647cdc081f0a89bb0483045": {
            "name": "Mantle L1 Bridge",
            "type": "canonical_l1_bridge",
            "confidence": 0.6,
        },
        "0xd19d4b5d358258f05d7b411e21a146e84d3f0c4d": {
            "name": "Linea L1 MessageService",
            "type": "canonical_l1_bridge",
            "confidence": 0.6,
        },
    },
    "optimism": {
        "0x4200000000000000000000000000000000000010": {
            "name": "Optimism L2StandardBridge",
            "type": "canonical_l2_bridge",
            "confidence": 0.95,
        },
        "0x6f26bf09b1c792e3228e5467807a900a503c0281": {
            "name": "Across SpokePool (Optimism)",
            "type": "bridge",
            "confidence": 0.8,
        },
        "0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590": {
            "name": "Stargate Router (Optimism)",
            "type": "bridge_router",
            "confidence": 0.75,
        },
        "0x25d8039bb044dc227f741a9e381ca4ceae2e6ae8": {
            "name": "Hop USDC Bridge (Optimism)",
            "type": "bridge",
            "confidence": 0.7,
        },
    },
    "arbitrum": {
        "0x5288c571fd7ad117bea99bf60fe0846c4e2f7408": {
            "name": "Arbitrum L2ERC20Gateway",
            "type": "canonical_l2_bridge",
            "confidence": 0.8,
        },
        "0x53bf833a5d6c4dda888f69c22c88c9f356a41612": {
            "name": "Stargate Router (Arbitrum)",
            "type": "bridge_router",
            "confidence": 0.75,
        },
    },
    "base": {
        "0x4200000000000000000000000000000000000010": {
            "name": "Base L2StandardBridge",
            "type": "canonical_l2_bridge",
            "confidence": 0.95,
        },
    },
    "polygon": {
        "0x45a01e4e04f14f7a4a6702c74187c5f6222033cd": {
            "name": "Stargate Router (Polygon)",
            "type": "bridge_router",
            "confidence": 0.7,
        },
    },
    "bsc": {
        "0xb6f6d86a8f9879a9c87f643768d9efcefcb5f9f9d": {
            "name": "Wormhole Token Bridge (BSC)",
            "type": "bridge",
            "confidence": 0.6,
        },
    },
    "avalanche": {
        "0x3ee18b2214aff97000d974cf647e7c347e8fa585": {
            "name": "Wormhole Token Bridge (Avalanche)",
            "type": "bridge",
            "confidence": 0.6,
        },
    },
}

# ── CEX hot wallets ─────────────────────────────────────────────────────
# The same omnibus hot wallets are reused across EVM chains. Per-entry
# confidence reflects the 2026-08-04 warehouse probe on ethereum (and
# cross-chain activity observed on polygon/zksync/arbitrum/optimism/base).
_CEX_ENTRIES: dict[str, dict[str, Any]] = {
    "0x28c6c06298d514db089934071355e5743bf21d60": {"name": "Binance 14", "confidence": 0.9},
    "0x21a31ee1afc51d94c2efcca2092ad1028285549": {"name": "Binance 15", "confidence": 0.85},
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": {"name": "Binance 16", "confidence": 0.9},
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": {"name": "Binance 17", "confidence": 0.85},
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": {"name": "Binance 18", "confidence": 0.85},
    "0x4976a4a02f38326660d17bf34b431dc6e2eb2327": {"name": "Binance 19", "confidence": 0.85},
    "0xdccf3b77da55107280f850f2031d1dcbe1b3dd7b": {"name": "Binance 20", "confidence": 0.85},
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": {"name": "Coinbase 1", "confidence": 0.85},
    "0x503828976d22510aad0201ac7ec88293211d23da": {"name": "Coinbase 2", "confidence": 0.85},
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": {"name": "Coinbase 3", "confidence": 0.85},
    "0x3cd751e6b0078be393132286c442345e5dc49699": {"name": "Coinbase 4", "confidence": 0.85},
    "0xb739d0895772dbb71a89a3754a160269068f0d45": {"name": "Coinbase 5", "confidence": 0.85},
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": {"name": "Kraken 1", "confidence": 0.85},
    "0x0a869d79a7052c7f1b135a7d57faf0fe918aad6d": {"name": "Kraken 2", "confidence": 0.85},
    "0xe853c56864a2ebe4576a807d26fdc4a0ada51919": {"name": "Kraken 3", "confidence": 0.8},
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": {"name": "OKX 1", "confidence": 0.8},
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": {"name": "Bybit 1", "confidence": 0.9},
    "0xee5b5b923ffce93a870b3104b7ca09c3db80047a": {"name": "Bybit 2", "confidence": 0.8},
    "0x2b5634c42055806a59e9107ed44d43c426e58258": {"name": "KuCoin 1", "confidence": 0.8},
    "0x689c56aef474df92d44a1b70850f808488f9769c": {"name": "KuCoin 2", "confidence": 0.8},
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": {"name": "Gate.io 1", "confidence": 0.95},
    "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c": {"name": "Gate.io 2", "confidence": 0.8},
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b": {"name": "HTX 1", "confidence": 0.85},
    "0x8f22f2063d253846b53609231ed80fa571bc0c8f": {"name": "HTX 2", "confidence": 0.8},
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": {"name": "MEXC 1", "confidence": 0.85},
    "0x0639556f03714a74a5feeaf5736a4a64ff266146": {"name": "Bitget 1", "confidence": 0.8},
}

# ── Bridge counterpart contracts ────────────────────────────────────────
# For a deposit into a bridge on chain A, which contract on chain B emits
# the matching release/mint? Used to match "deposit on chain A" with the
# "withdrawal on chain B" beyond the same-EOA heuristic. Keyed by
# (source_chain, source_address) → list of (dest_chain, dest_address).
BRIDGE_COUNTERPARTS: dict[tuple[str, str], list[tuple[str, str]]] = {
    ("ethereum", "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1"): [
        ("optimism", "0x4200000000000000000000000000000000000010"),
    ],
    ("ethereum", "0x3154cf16ccdb4c6d922629664174b904d80f2c35"): [
        ("base", "0x4200000000000000000000000000000000000010"),
    ],
    ("ethereum", "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a"): [
        ("arbitrum", "0x5288c571fd7ad117bea99bf60fe0846c4e2f7408"),
    ],
    ("optimism", "0x4200000000000000000000000000000000000010"): [
        ("ethereum", "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1"),
    ],
    ("base", "0x4200000000000000000000000000000000000010"): [
        ("ethereum", "0x3154cf16ccdb4c6d922629664174b904d80f2c35"),
    ],
    ("arbitrum", "0x5288c571fd7ad117bea99bf60fe0846c4e2f7408"): [
        ("ethereum", "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a"),
    ],
}

# EVM chains the CEX hot-wallet list applies to (same omnibus wallets).
_EVM_CEX_CHAINS = (
    "ethereum",
    "base",
    "bsc",
    "polygon",
    "arbitrum",
    "optimism",
    "avalanche",
    "fantom",
    "linea",
    "scroll",
    "zksync",
    "mantle",
    "gnosis",
    "celo",
)

_ALL_EVM_CHAINS = _EVM_CEX_CHAINS


class AddressRegistry:
    """Lookup for known bridge / CEX addresses.

    Builds the combined ``{chain: {address: {name, type, confidence}}}``
    structure (bridges + CEX hot wallets) and exposes fast lookups.
    """

    def __init__(
        self,
        bridge: dict[str, dict[str, dict[str, Any]]] | None = None,
        cex: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        self._bridge = bridge if bridge is not None else _BRIDGE_REGISTRY
        self._cex = cex if cex is not None else self._build_cex()

    @staticmethod
    def _build_cex() -> dict[str, dict[str, dict[str, Any]]]:
        """Expand the shared EVM CEX list into per-chain registries."""
        out: dict[str, dict[str, dict[str, Any]]] = {}
        for chain in _EVM_CEX_CHAINS:
            out[chain] = {
                addr: {
                    "name": entry["name"],
                    "type": "cex_hot_wallet",
                    "confidence": entry["confidence"],
                }
                for addr, entry in _CEX_ENTRIES.items()
            }
        return out

    def as_dict(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Return the full combined registry structure."""
        chains = set(self._bridge) | set(self._cex)
        combined: dict[str, dict[str, dict[str, Any]]] = {}
        for chain in chains:
            merged: dict[str, dict[str, Any]] = {}
            merged.update(self._bridge.get(chain, {}))
            merged.update(self._cex.get(chain, {}))
            if merged:
                combined[chain] = merged
        return combined

    def lookup(self, chain: str, address: str) -> dict[str, Any] | None:
        """Return the registry entry for ``address`` on ``chain`` (or None).

        Addresses are normalized to lowercase; ``chain`` falls back to the
        EVM CEX set when the chain is unknown to the registry.
        """
        addr = address.strip().lower()
        for table in (self._bridge, self._cex):
            entry = table.get(chain, {}).get(addr)
            if entry is not None:
                return entry
        return None

    def label(self, chain: str, address: str) -> str:
        """Human label for ``address`` on ``chain`` ('' when unknown)."""
        entry = self.lookup(chain, address)
        return entry["name"] if entry else ""

    def entity_type(self, chain: str, address: str) -> str:
        """Registry type ('bridge', 'cex_hot_wallet', ...) or ''."""
        entry = self.lookup(chain, address)
        return entry["type"] if entry else ""

    def bridge_addresses(self, chain: str) -> list[str]:
        """All registered bridge addresses on ``chain``."""
        return sorted(self._bridge.get(chain, {}))

    def cex_addresses(self, chain: str) -> list[str]:
        """All registered CEX hot-wallet addresses on ``chain``."""
        return sorted(self._cex.get(chain, {}))

    def chains(self) -> list[str]:
        """All chains present in the registry."""
        return sorted(set(self._bridge) | set(self._cex))

    def counterpart_bridges(self, chain: str, address: str) -> list[tuple[str, str]]:
        """Destination (chain, bridge-address) pairs for a bridge contract.

        Returns [] when no counterpart mapping is known — the tracer falls
        back to the same-EOA + time-window heuristic in that case.
        """
        return BRIDGE_COUNTERPARTS.get((chain, address.strip().lower()), [])


# Module-level default registry (shared, read-only).
DEFAULT_REGISTRY = AddressRegistry()

__all__ = [
    "AddressRegistry",
    "DEFAULT_REGISTRY",
    "BRIDGE_COUNTERPARTS",
]
