#!/bin/bash
# Start/stop one AgentENV node server (container backend prototype) as root.
#   [SCHED=http://10.250.0.1:9090] [NETNS=aenv-n2] node.sh start <name> [api_addr]
#   node.sh stop <name>
# Per-node state: AENV_HOME=/home/aenv-ct (n1) or /home/aenv-ct-<name>; config $POC_STATE/nodes/<name>/config.toml (cluster_up.sh writes it).
# NETNS runs the node in a host network namespace (several nodes on one host: separate veths, iptables, slots).
set -euo pipefail
CT=${POC_STATE:-$HOME/aenv-poc}; POC=$(cd "$(dirname "$0")/.." && pwd); cmd=$1; name=$2; addr=${3:-127.0.0.1:8000}
home=/home/aenv-ct; [ "$name" = n1 ] || home=/home/aenv-ct-$name
mkdir -p $CT/logs; log=$CT/logs/server-$name.log
case $cmd in
start)
  sudo modprobe ublk_drv
  bin=${SERVER_BIN:-$POC/../../target/release/server}
  wrap=(); [ -n "${NETNS:-}" ] && wrap=(nsenter --net=/run/netns/$NETNS)
  extra=(); [ -n "${SCHED:-}" ] && extra=(AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true AENV_OBSERVABILITY_SCHEDULER_ENDPOINT=$SCHED)
  sudo env API_ADDR=$addr AENV_CONFIG_PATH=$CT/nodes/$name/config.toml AENV_HOME_PATH=$home \
    AENV_RUNTIME_PATH=/run/aenv-$name AENV_VIRTUALIZATION_MODE=kvm HOME=/root AENV_NODE_ID=$name \
    AENV_SANDBOX_BACKEND=${AENV_SANDBOX_BACKEND:-container} AENV_CT_BASE_DIR=$home/container \
    AENV_CT_RUNC_ROOT=/run/aenv-ct-$name/runc RUST_LOG=${RUST_LOG:-info,netlink_packet_route=error} "${extra[@]}" \
    setsid nohup "${wrap[@]}" $bin > $log 2>&1 < /dev/null &
  for i in $(seq 120); do curl -s -o /dev/null -w '%{http_code}' http://$addr/health 2>/dev/null | grep -q 204 && { echo "node $name up on $addr after ${i}s"; exit 0; }; sleep 1; done
  echo "node $name not healthy"; tail -20 $log; exit 1;;
stop)
  for p in $(sudo pgrep -f "target/release/server" || true); do
    if sudo cat /proc/$p/environ 2>/dev/null | tr '\0' '\n' | grep -qx "AENV_CONFIG_PATH=$CT/nodes/$name/config.toml"; then sudo kill -INT $p; echo "sent SIGINT to $p"; fi
  done
  for i in $(seq 30); do
    alive=0; for p in $(sudo pgrep -f "target/release/server" || true); do sudo cat /proc/$p/environ 2>/dev/null | tr '\0' '\n' | grep -qx "AENV_CONFIG_PATH=$CT/nodes/$name/config.toml" && alive=1; done
    [ $alive = 0 ] && { echo "node $name stopped"; exit 0; }; sleep 1
  done; echo "node $name still running";;
esac
