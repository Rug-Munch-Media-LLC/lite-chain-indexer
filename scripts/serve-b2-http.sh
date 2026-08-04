#!/usr/bin/env bash
# Serves B2 parquet files over HTTP for ClickHouse's url() table function.
#
# This is the cold half of the Transparent Historical Access (THA) layer:
# ClickHouse's *_historical views (migrations/006_historical_views.sql)
# read cold parquet via url('http://localhost:8090/parquet/...') — this
# proxy makes every file in b2-crypt:chain-data reachable at that URL.
#
# The Python layer (HistoricalAccess) reads the SAME archive directly via
# DuckDB (rclone mount or rclone cat); the proxy is only needed for the
# ClickHouse-side unified views.
#
# Usage:
#   ./scripts/serve-b2-http.sh                 # foreground
#   nohup ./scripts/serve-b2-http.sh &         # background
#
# Environment:
#   THA_HTTP_PORT    port to bind (default 8090, matches B2_HTTP_PROXY_PORT)
#   THA_B2_REMOTE    rclone remote to serve (default b2-crypt:chain-data)
#
# systemd unit (fleet-infra convention):
#   [Unit]
#   Description=RugMunch B2 parquet HTTP proxy (THA)
#   After=network-online.target
#   [Service]
#   ExecStart=/srv/work/repos/lite-chain-indexer/scripts/serve-b2-http.sh
#   Restart=always
#   [Install]
#   WantedBy=multi-user.target

set -euo pipefail

PORT="${THA_HTTP_PORT:-8090}"
REMOTE="${THA_B2_REMOTE:-b2-crypt:chain-data}"

echo "==> rclone serve http ${REMOTE} on :${PORT} (read-only, no auth)"

exec rclone serve http "${REMOTE}" \
    --addr ":${PORT}" \
    --read-only \
    --no-auth
