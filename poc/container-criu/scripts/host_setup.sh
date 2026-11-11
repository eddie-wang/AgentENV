#!/bin/bash
# One-time host prerequisites for the container backend (idempotent).
set -euo pipefail
sudo modprobe ublk_drv
sudo chmod 0755 /home/aenv-ct 2>/dev/null || true   # server's capability-scoped spawns must traverse AENV_HOME
sudo mkdir -p /etc/criu /home/aenv-ct-shared/snapshots
# CRIU options for runc checkpoint/restore. Host security agents attach uprobes to processes.
printf 'allow-uprobes\n' | sudo tee /etc/criu/runc.conf >/dev/null
command -v criu >/dev/null || sudo test -x /usr/local/sbin/criu || { echo "build CRIU: scripts/build_criu.sh"; exit 1; }
sudo /usr/local/sbin/criu check >/dev/null 2>&1 && echo "criu check OK"
runc --version | head -1
