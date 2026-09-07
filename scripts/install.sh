#!/usr/bin/env bash
# Install LiteChainIndexer as a systemd service.
# Usage: sudo bash scripts/install.sh
# Customize DATA_DIR before installation.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${DATA_DIR:-/var/data/chain-indexer}"

echo "==> Installing lite-chain-indexer..."
pip install -e "$REPO_DIR"

echo "==> Creating data directory..."
mkdir -p "$DATA_DIR/state"

# Runtime env file for RPC API keys. Referenced from the systemd unit via
# EnvironmentFile (never inline Environment= lines — those leak key names
# into `systemctl show` and encourage world-readable unit state). The file
# is root-only 0600. Keys already exported into THIS shell (as the old
# inline Environment= lines required) are seeded into a freshly created
# file so existing install flows keep working; unset keys are left as
# commented placeholders. Keys are resolved by the indexer at import time
# (lite_indexer/chains/endpoints.py + non_evm.py), so changing this file
# requires `systemctl restart lite-indexer`.
echo "==> Ensuring env file ($DATA_DIR/lite-indexer.env, 0600)..."
ENV_FILE="$DATA_DIR/lite-indexer.env"
if [ ! -f "$ENV_FILE" ]; then
    umask 177
    {
        echo "# lite-chain-indexer runtime environment (EnvironmentFile, 0600)."
        echo "# API-key free-tier RPC endpoints (\${ENV_VAR} placeholders in"
        echo "# lite_indexer/chains/endpoints.py + non_evm.py are resolved from here)."
        echo "# Fill in the keys you use (NODEREAL_KEY is REQUIRED) and restart the service."
        for k in NODEREAL_KEY ALCHEMY_KEY GETBLOCK_KEY CHAINSTACK_KEY QUICKNODE_ENDPOINT INFURA_KEY HELIUS_KEY; do
            v="${!k:-}"
            if [ -n "$v" ]; then
                printf '%s=%s\n' "$k" "$v"
            else
                printf '#%s=\n' "$k"
            fi
        done
    } >"$ENV_FILE"
    chmod 600 "$ENV_FILE"
else
    chmod 600 "$ENV_FILE" 2>/dev/null || true
fi

echo "==> Applying ClickHouse grants (rmi user)..."
if command -v clickhouse-client &>/dev/null; then
    clickhouse-client --queries-file "$REPO_DIR/migrations/001_grants.sql" 2>/dev/null ||
        echo "   (non-fatal: grants may already exist or ClickHouse is not running)"
fi

echo "==> Installing systemd service..."
cat >/etc/systemd/system/lite-indexer.service <<SERVICE
[Unit]
Description=LiteChainIndexer — Lightweight multi-chain blockchain indexer
After=network.target clickhouse-server.service
Wants=clickhouse-server.service

[Service]
Type=simple
User=root
WorkingDirectory=$DATA_DIR
Environment=DATA_DIR=$DATA_DIR
Environment=LOG_LEVEL=INFO
# RPC API keys are read from the 0600 env file (see above) — never inlined
# here, so key names/values stay out of systemctl unit dumps.
EnvironmentFile=$DATA_DIR/lite-indexer.env
# head-mode (--start-from-head) is one-shot only; the daemon resumes from
# its state file.
ExecStart=/usr/local/bin/lite-indexer --daemon --dex --deployers
Restart=always
RestartSec=30
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable lite-indexer
systemctl start lite-indexer

echo "==> Done! lite-indexer service is running."
echo "    Check logs: journalctl -u lite-indexer -f"
