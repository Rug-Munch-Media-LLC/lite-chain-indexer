"""Tron address helpers for the SQD Portal readers.

Hex normalization, log-topic padding and base58check encoding — shared by
:mod:`lite_indexer.query.sqd_tron`, and re-exported on ``SQDSource`` for
callers/tests.
"""

from __future__ import annotations

import hashlib


def tron_hex40(address: str) -> str:
    """Normalize a Tron address (``0x..`` / ``41..`` / plain hex) to 40 hex chars."""
    a = address.strip().lower()
    if a.startswith("0x"):
        a = a[2:]
    if a.startswith("41") and len(a) == 42:
        a = a[2:]
    if len(a) != 40 or any(c not in "0123456789abcdef" for c in a):
        raise ValueError(f"invalid tron address: {address!r}")
    return a


def tron_topic(address: str) -> str:
    """Left-pad a Tron wallet to a 32-byte log topic."""
    return "0" * 24 + tron_hex40(address)


def tron_base58(address: str) -> str:
    """Tron hex (40 chars, optional 0x/41) → base58check T-address."""
    hex40 = address.strip().lower()
    if hex40.startswith("0x"):
        hex40 = hex40[2:]
    if hex40.startswith("41") and len(hex40) == 42:
        hex40 = hex40[2:]
    payload = bytes.fromhex("41" + hex40)
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    num = int.from_bytes(payload + checksum, "big")
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    out = ""
    while num:
        num, rem = divmod(num, 58)
        out = alphabet[rem] + out
    return out
