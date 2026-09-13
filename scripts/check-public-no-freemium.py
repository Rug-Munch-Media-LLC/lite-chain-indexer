#!/usr/bin/env python3
"""CI gate: the public tree must not expose the curated freemium RPC stack.

The public repo ships free/keyless RPCs plus a private overlay extension
point. Our curated provider stack (provider hosts, per-account keys, tier
lists, verified rate caps) lives in the runtime overlay
(``LITE_INDEXER_ENDPOINT_OVERLAY``), never in git. This gate fails the build
if a curated provider name/host appears in any tracked public file.

Run:  python3 scripts/check-public-no-freemium.py   (exit 0 = clean)
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# Providers whose bare name never collides with an RPC method name.
PLAIN_TOKENS = (
    "alchemy",
    "drpc",
    "uniblock",
    "blockpi",
    "blockvision",
    "chainstack",
    "helius",
    "nodereal",
    "infura",
    "ankr",
    "quicknode",
    "moralis",
    "nownodes",
)

# Names that collide with common RPC methods (getBlock, getBlockByNumber) are
# anchored to the provider host / key placeholder instead of the bare word.
ANCHORED_PATTERNS = (
    re.compile(r"getblock\.io", re.IGNORECASE),
    re.compile(r"getblock_key", re.IGNORECASE),
)

# tests legitimately assert the absence of these names; scripts may define the
# denylist itself.
EXCLUDE_PREFIXES = ("tests/", ".git/", "scripts/check-public-no-freemium.py")


def _tracked_files() -> list[str]:
    out = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout
    return [f for f in out.splitlines() if f and not f.startswith(EXCLUDE_PREFIXES)]


def main() -> int:
    hits: list[tuple[str, str]] = []
    files = _tracked_files()
    for name in files:
        path = Path(name)
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        lowered = text.lower()
        for token in PLAIN_TOKENS:
            if token in lowered:
                hits.append((name, token))
        for pattern in ANCHORED_PATTERNS:
            if pattern.search(text):
                hits.append((name, pattern.pattern))

    if hits:
        print("PUBLIC-LEAK GATE FAILED — curated provider stack found in the public tree:")
        for name, token in hits:
            print(f"  {name}: {token}")
        print(
            "\nMove provider-specific config to the private runtime overlay "
            "(LITE_INDEXER_ENDPOINT_OVERLAY); keep the public tree free/keyless."
        )
        return 1
    print(f"PUBLIC-LEAK GATE PASSED — scanned {len(files)} tracked files, 0 curated-provider hits.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
