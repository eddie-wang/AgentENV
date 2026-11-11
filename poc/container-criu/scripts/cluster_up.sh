#!/bin/bash
# Two-node AgentENV cluster on one host, both nodes on the container backend:
#   scheduler (gRPC :9090) + gateway (HTTP :8080) on the host, shared POSIX snapshot repository,
#   node n1 on the host at 127.0.0.1:8000, node n2 in netns aenv-n2 at 10.250.0.2:8000.
# Prereqs: AgentENV v0.2.4 installed with `sudo AENV_HOME_PATH=/home/aenv-ct bash install.sh` (deps + base config),
#          scripts/host_setup.sh, scripts/build.sh, and gateway/scheduler binaries (AgentENV services/) in GATEWAY_BIN/SCHEDULER_BIN.
set -euo pipefail
POC=$(cd "$(dirname "$0")/.." && pwd); CT=${POC_STATE:-$HOME/aenv-poc}; export POC_STATE=$CT
GATEWAY_BIN=${GATEWAY_BIN:?path to the AgentENV gateway binary}; SCHEDULER_BIN=${SCHEDULER_BIN:?path to the AgentENV scheduler binary}
BASE=${AENV_BASE_CONFIG:-/home/aenv-ct/config/config.toml}
p() { echo "PHASE $1 $(date -u +%T)"; }
mkdir -p $CT/nodes/n1 $CT/nodes/n2 $CT/logs
p config
sudo cat /home/aenv-ct/secrets/api-key > $CT/nodes/api-key; chmod 600 $CT/nodes/api-key
sudo cat $BASE | python3 $POC/config/node_config.py n1 > $CT/nodes/n1/config.toml
sudo cat $BASE | python3 $POC/config/node_config.py n2 > $CT/nodes/n2/config.toml
p netns
if ! ip netns list | grep -qw aenv-n2; then
  sudo ip netns add aenv-n2
  sudo ip link add aenvn2-h type veth peer name eth0 netns aenv-n2
  sudo ip addr add 10.250.0.1/30 dev aenvn2-h; sudo ip link set aenvn2-h up
  sudo ip -n aenv-n2 addr add 10.250.0.2/30 dev eth0; sudo ip -n aenv-n2 link set eth0 up; sudo ip -n aenv-n2 link set lo up
  sudo ip -n aenv-n2 route add default via 10.250.0.1
fi
p n2-home   # same deps and the same secrets (API key, access-token seed) as n1
if ! sudo test -d /home/aenv-ct-n2/deps; then
  sudo install -d -m 0755 /home/aenv-ct-n2
  sudo cp -a /home/aenv-ct/deps /home/aenv-ct/ublk /home/aenv-ct/secrets /home/aenv-ct-n2/
fi
p control-plane
cat > $CT/nodes/control-plane.json <<JSON
{"log_level": "info", "log_format": "json",
 "scheduler": {"grpc_listen_addr": "0.0.0.0:9090", "metrics_listen_addr": "127.0.0.1:9101", "strategy": "round_robin",
   "report_ttl": "30s", "binding_ttl": "30s", "discovery": {"mode": "static"},
   "nodes": [{"id": "n1", "endpoint": "http://127.0.0.1:8000"}, {"id": "n2", "endpoint": "http://10.250.0.2:8000"}]},
 "gateway": {"http_listen_addr": "127.0.0.1:8080", "metrics_listen_addr": "127.0.0.1:9102", "scheduler_addr": "127.0.0.1:9090",
   "request_timeout": "600s", "forward_response_size": 4194304, "sandbox_proxy_domains": []}}
JSON
K=$(cat $CT/nodes/api-key)
for bin in $SCHEDULER_BIN $GATEWAY_BIN; do
  pkill -f "$bin -config" 2>/dev/null || true; sleep 0.5
  AENV_API_KEY="$K" setsid nohup $bin -config $CT/nodes/control-plane.json > $CT/logs/$(basename $bin).log 2>&1 < /dev/null &
  sleep 1
done
p nodes
SCHED=http://127.0.0.1:9090 $POC/scripts/node.sh start n1 127.0.0.1:8000
SCHED=http://10.250.0.1:9090 NETNS=aenv-n2 $POC/scripts/node.sh start n2 10.250.0.2:8000
sleep 8
echo "gateway health $(curl -s -o /dev/null -w '%{http_code}' 127.0.0.1:8080/health)"
curl -s -H "X-API-Key: $K" 127.0.0.1:8080/nodes | python3 -c 'import sys,json; [print("NODE", n["id"], n["status"]) for n in json.load(sys.stdin)]'
p done
