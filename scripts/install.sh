#!/usr/bin/env bash
# Install LiteChainIndexer as a systemd service.
# Usage: sudo bash scripts/install.sh

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Installing lite-chain-indexer..."
pip install -e "$REPO_DIR"

echo "==> Creating data directory..."
mkdir -p /srv/data/chain-indexer/state

echo "==> Installing systemd service..."
cat > /etc/systemd/system/lite-indexer.service << 'SERVICE'
[Unit]
Description=LiteChainIndexer — Lightweight multi-chain blockchain indexer
After=network.target clickhouse-server.service
Wants=clickhouse-server.service

[Service]
Type=simple
User=root
WorkingDirectory=/srv/data/chain-indexer
Environment=DATA_DIR=/srv/data/chain-indexer
Environment=LOG_LEVEL=INFO
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
