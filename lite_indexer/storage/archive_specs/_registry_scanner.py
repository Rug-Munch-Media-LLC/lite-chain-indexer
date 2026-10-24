"""Archive specs for the scanner / label tables.

These were the silent-loss set: five small tables with no export path at all,
so their B2 history was a 1-9 day island surrounded by nothing. They are
chain-agnostic (``chain_id`` mixes ``eth``/``ETH``/``ethereum``, so it is a
label, not a dimension) and therefore archive under ``_global`` — the same
bucket ``deployer_history`` already uses.

``cursor`` for append-only label feeds (a day is final once written);
``snapshot`` for the two tables whose rows are mutated in place, where no
date is honest.
"""

from __future__ import annotations

from lite_indexer.storage.archive_specs._spec import CURSOR, SNAPSHOT, TableSpec

SCANNER_SPECS: dict[str, TableSpec] = {
    "scam_addresses": TableSpec(
        "scam_addresses",
        "labels",
        "first_seen_at",
        CURSOR,
        None,
        "first_seen_at",
        0,
        ("address", "chain_id", "source", "threat_type", "confidence", "first_seen_at", "evidence"),
        chain_literal="_global",
    ),
    "rag_scan_labels": TableSpec(
        "rag_scan_labels",
        "labels",
        "scanned_at",
        CURSOR,
        None,
        "scanned_at",
        0,
        (
            "row_id",
            "address",
            "chain_id",
            "doc_id",
            "kind",
            "symbol",
            "name",
            "risk_score",
            "safety_score",
            "risk_level",
            "risk_flags",
            "scanned_at",
            "source",
            "provenance",
            "licence",
            "qdrant_point_id",
        ),
        chain_literal="_global",
    ),
    "sanctions_crypto": TableSpec(
        "sanctions_crypto",
        "labels",
        "ingested_at",
        CURSOR,
        None,
        "ingested_at",
        0,
        (
            "address",
            "address_lower",
            "chain",
            "uid",
            "name",
            "program",
            "sdn_type",
            "country",
            "source",
            "ingested_at",
        ),
        chain_literal="_global",
    ),
    "wallet_entities": TableSpec(
        "wallet_entities",
        "labels",
        "",
        SNAPSHOT,
        None,
        "",
        0,
        (
            "entity_id",
            "wallet_address",
            "chain_id",
            "heuristic_type",
            "confidence_score",
            "entity_label",
            "entity_category",
            "first_linked_at",
            "last_updated_at",
        ),
        chain_literal="_global",
    ),
    "deployer_history": TableSpec(
        "deployer_history",
        "labels",
        "",
        SNAPSHOT,
        None,
        "",
        0,
        (
            "deployer_address",
            "chain_id",
            "token_address",
            "token_name",
            "token_symbol",
            "deployed_at",
            "outcome",
            "lifespan_days",
            "max_market_cap",
            "is_scam_related",
            "scan_risk_score",
        ),
        chain_literal="_global",
    ),
}
