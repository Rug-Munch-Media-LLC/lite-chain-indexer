"""
Cross-chain address-activity parsers (EVM + TRON).

Expands parsed EVM log events and TRON TRC20 events into the same
address-activity row shape as the Solana parser, so every chain feeds the
single ``wallet_memory.address_activity`` keystone table. Kept in its own
module so ``address_activity.py`` stays under the 500-line file cap.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

# ── Cross-chain helpers (EVM + TRON) ──────────────────────────────────


def _coerce_block_number(raw: Any) -> int:
    """Coerce an EVM/TRON block number (decimal, hex string, int) to int."""
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith(("0x", "0X")):
            try:
                return int(text, 16)
            except ValueError:
                return 0
        raw = text
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _coerce_block_time(raw: Any) -> datetime:
    """Coerce a timestamp (datetime / ISO string / epoch s or ms) to aware UTC.

    Falls back to ``now(UTC)`` for missing or unparseable values, matching
    the Solana parser's best-effort behaviour.
    """
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    if isinstance(raw, (int, float)):
        value = int(raw)
        # TronGrid reports milliseconds; EVM enrichment reports seconds.
        if value > 10**12:
            value //= 1000
        try:
            return datetime.fromtimestamp(value, tz=UTC)
        except (ValueError, TypeError, OverflowError):
            return datetime.now(UTC)
    if isinstance(raw, str) and raw.strip():
        text = raw.strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except (ValueError, TypeError):
            return datetime.now(UTC)
    return datetime.now(UTC)


def _dedupe(values: list[str]) -> list[str]:
    """Return the values in first-seen order with blanks/duplicates removed."""
    return list(dict.fromkeys(v for v in (str(x).strip() for x in values) if v))


def _collect_contracts(event: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    """Collect the ordered-unique contract/emitter addresses of an event.

    For EVM this is the token/pair/emitter set (the closest analogue to
    Solana ``program_ids``); for TRON it is the TRC20 contract address.
    """
    contracts: list[str] = []
    for key in keys:
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            contracts.append(value)
    for list_key in ("contractAddresses", "programIds"):
        extra = event.get(list_key)
        if isinstance(extra, (list, tuple)):
            contracts.extend(str(item) for item in extra)
    return _dedupe(contracts)


def parse_evm_address_activity(event: Any) -> list[dict[str, Any]]:
    """Expand one parsed EVM event into one address-activity row per address.

    The EVM pipeline is log-based (``eth_getLogs``) — it never fetches the
    full transaction body — so the input is a parsed event dict rather than
    a raw ``eth_getTransactionByHash`` result. Recognized keys:

    * ``chain`` / ``txHash`` or ``transactionHash`` (signature)
    * ``blockNumber`` (int or hex string), ``blockTimestamp``
    * ``from`` / ``to`` — ERC-20 transfer endpoints
    * ``sender`` / ``recipient`` — DEX swap endpoints
    * ``tokenAddress`` / ``contractAddress`` / ``pairAddress`` / ``address``
    * ``token0`` / ``token1`` — swap token contracts
    * ``involvedAccounts`` / ``addresses`` — extra addresses touched by
      logs or internal calls (optional)
    * ``signer`` / ``txFrom`` — the transaction sender, when the caller
      resolved it (e.g. via ``eth_getTransactionByHash``). EVM logs do not
      carry the tx sender, so ``is_signer`` is True only for an explicitly
      supplied signer; it is never guessed from a token-transfer ``from``
      (which may be a contract).

    Args:
        event: Parsed EVM transfer/swap/log event dict.

    Returns:
        One row per distinct address touched, or ``[]`` when the payload
        has no transaction signature or no addresses.
    """
    if not isinstance(event, dict):
        return []
    chain = str(event.get("chain") or "ethereum")
    tx_signature = str(event.get("txHash") or event.get("transactionHash") or event.get("hash") or "")
    if not tx_signature:
        return []

    addresses: list[str] = []
    for key in (
        "from",
        "to",
        "sender",
        "recipient",
        "tokenAddress",
        "contractAddress",
        "pairAddress",
        "address",
        "token0",
        "token1",
    ):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            addresses.append(value)
    extras = event.get("involvedAccounts") or event.get("addresses") or []
    if isinstance(extras, (list, tuple)):
        addresses.extend(str(item) for item in extras)
    involved = _dedupe(addresses)
    if not involved:
        return []

    signer = str(event.get("signer") or event.get("txFrom") or "").strip()
    contracts = _collect_contracts(
        event,
        ("tokenAddress", "contractAddress", "pairAddress", "address", "token0", "token1"),
    )
    slot = _coerce_block_number(event.get("blockNumber"))
    block_time = _coerce_block_time(event.get("blockTimestamp"))

    return [
        {
            "chain": chain,
            "address": address,
            "tx_signature": tx_signature,
            "slot": slot,
            "block_time": block_time,
            "involved_accounts": involved,
            "program_ids": contracts,
            "is_signer": bool(signer) and address == signer,
        }
        for address in involved
    ]


def parse_tron_address_activity(event: Any) -> list[dict[str, Any]]:
    """Expand one parsed TRON (TRC20) event into address-activity rows.

    Accepts the dict produced by
    :func:`lite_indexer.strategies.tron.parsing.parse_tron_transfer_event`
    (``txHash`` / ``blockNumber`` / ``blockTimestamp`` / ``from`` / ``to`` /
    ``tokenAddress``) as well as a raw TronGrid event
    (``transaction_id`` / ``block_number`` / ``block_timestamp`` /
    ``from_address`` / ``to_address`` / ``contract_address``).

    TronGrid's ``from_address`` is the transaction owner, so ``from`` is
    treated as the signer (``is_signer`` True).

    Args:
        event: Parsed or raw TRON TRC20 event dict.

    Returns:
        One row per distinct address touched, or ``[]`` when the payload
        has no transaction id or no addresses.
    """
    if not isinstance(event, dict):
        return []
    chain = str(event.get("chain") or "tron")
    tx_signature = str(
        event.get("txHash") or event.get("txID") or event.get("transaction_id") or event.get("txid") or ""
    )
    if not tx_signature:
        return []

    from_addr = event.get("from") or event.get("from_address") or ""
    to_addr = event.get("to") or event.get("to_address") or ""
    contract = event.get("tokenAddress") or event.get("contract_address") or event.get("contractAddress") or ""

    addresses = [str(from_addr), str(to_addr), str(contract)]
    extras = event.get("involvedAccounts") or event.get("addresses") or []
    if isinstance(extras, (list, tuple)):
        addresses.extend(str(item) for item in extras)
    involved = _dedupe(addresses)
    if not involved:
        return []

    contracts = _dedupe([str(contract)]) if str(contract).strip() else []
    signer = str(from_addr).strip()
    slot = _coerce_block_number(event.get("blockNumber") or event.get("block_number"))
    block_time = _coerce_block_time(event.get("blockTimestamp") or event.get("block_timestamp"))

    return [
        {
            "chain": chain,
            "address": address,
            "tx_signature": tx_signature,
            "slot": slot,
            "block_time": block_time,
            "involved_accounts": involved,
            "program_ids": contracts,
            "is_signer": bool(signer) and address == signer,
        }
        for address in involved
    ]


__all__ = [
    "parse_evm_address_activity",
    "parse_tron_address_activity",
]
