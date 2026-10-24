#!/usr/bin/env bash
# Install the archive timers, rendering @REPO@ to this checkout.
#
# The units carry no workspace path of their own: the repo location is an
# environment fact, and hard-coding it would publish the deploy layout. The
# path is rendered here, at install time, from this checkout.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for unit in archive-export.service archive-export.timer archive-stream.service archive-stream.timer; do
	sed "s|@REPO@|${repo}|g" "scripts/systemd/${unit}" >"/etc/systemd/system/${unit}"
done
chmod 644 /etc/systemd/system/archive-*.service /etc/systemd/system/archive-*.timer
systemctl daemon-reload
systemctl enable --now archive-export.timer archive-stream.timer
systemctl list-timers 'archive-*' --no-pager
