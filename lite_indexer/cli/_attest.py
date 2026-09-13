"""attest subcommand handlers — on-chain-verifiable index attestations.

``attest verify <manifest>`` recomputes the Merkle inclusion proof from the
manifest alone (plus an optional trusted root) and reports validity.
``attest show --chain X`` prints the persisted accumulator state (root,
leaf count, bounded frontier size, committed ranges).

Extracted from ``lite_indexer.cli`` alongside the other subcommand modules.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lite_indexer.attestation import RangeAttestor, verify_manifest


def run_attest_cli(args: argparse.Namespace) -> int:
    """Run the ``attest`` subcommand.

    Args:
        args: Parsed CLI args (``attest_action`` + ``manifest`` /
            ``expected_root`` / ``chain``).

    Returns:
        Process exit code (0 = ok, 1 = usage/IO error, 2 = invalid manifest).
    """
    action = getattr(args, "attest_action", None)

    if action == "verify":
        manifest_path = Path(args.manifest)
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            print(json.dumps({"status": "error", "error": str(exc)[:120]}))  # noqa: T201
            return 1
        expected_root = getattr(args, "expected_root", None)
        ok = verify_manifest(manifest, expected_root=expected_root)
        print(  # noqa: T201
            json.dumps(
                {
                    "status": "ok" if ok else "invalid",
                    "chain": manifest.get("chain"),
                    "from_block": manifest.get("from_block"),
                    "to_block": manifest.get("to_block"),
                    "root": manifest.get("root"),
                    "expected_root": expected_root,
                },
                default=str,
            )
        )
        return 0 if ok else 2

    if action == "show":
        attestor = RangeAttestor(args.chain)
        print(json.dumps({"status": "ok", **attestor.summary()}, default=str))  # noqa: T201
        return 0

    print(json.dumps({"status": "error", "error": "missing attest action (verify|show)"}))  # noqa: T201
    return 1


__all__ = ["run_attest_cli"]
