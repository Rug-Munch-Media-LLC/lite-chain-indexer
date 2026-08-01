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
# API-key free-tier RPC endpoints (${ENV_VAR} placeholders in
# lite_indexer/chains/endpoints.py + non_evm.py are resolved from these).
Environment=ALCHEMY_KEY=${ALCHEMY_KEY:-}
Environment=GETBLOCK_KEY=${GETBLOCK_KEY:-}
Environment=CHAINSTACK_KEY=${CHAINSTACK_KEY:-}
Environment=QUICKNODE_ENDPOINT=${QUICKNODE_ENDPOINT:-}
Environment=INFURA_KEY=${INFURA_KEY:-}
Environment=HELIUS_KEY=${HELIUS_KEY:-}
ExecStart=/usr/local/bin/lite-indexer --daemon --dex --deployers --start-from-head
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
