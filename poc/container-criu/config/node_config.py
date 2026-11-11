"""Derive a container-backend node config from AgentENV's installed default config (stdin -> stdout).

Changes vs the stock v0.2.4 default: no pre-spawned Firecracker pool, a longer envd init timeout (cold app boots),
a POSIX snapshot repository shared by every node, and a per-node ublk metrics port.
usage: sudo cat /home/aenv-ct/config/config.toml | python3 node_config.py n1 > n1.toml
"""
import re, sys
node = sys.argv[1]
s = sys.stdin.read()
edits = [
    (r"(\[pool\.firecracker\][^\[]*?enabled = )true", r"\1false"),
    (r"init_timeout_secs = \d+", "init_timeout_secs = 120"),
    (r'# \[backend\.posix_fs\]\n# snapshot_store = "[^"]*"', '[backend.posix_fs]\nsnapshot_store = "/home/aenv-ct-shared/snapshots"'),
]
if node != "n1":
    edits.append((r'daemon_metrics_listen_addr = "[^"]*"', 'daemon_metrics_listen_addr = "127.0.0.1:9104"'))
for pat, rep in edits:
    s, n = re.subn(pat, rep, s, count=1, flags=re.S)
    assert n == 1, f"config edit did not apply: {pat}"
sys.stdout.write(s)
