"""App-agnostic lifecycle E2E for AgentENV (any backend), through a node API or the cluster gateway.

The app is described by shell commands run inside the sandbox via envd (E2B SDK):
  --start-cmd   starts the app in the background (cold sandbox only)
  --ready-cmd   exits 0 once the app is ready
  --check-cmd   deterministic output; every restored/forked/resumed sandbox must print what the source printed
  --write-cmd   mutates app state;  --look-cmd exits 0 iff that mutation is visible

phases (--phases, in order): cold, template, spawn, isolation, fork, pause, cleanup
Each phase prints `PHASE <name> PASS|FAIL {json}`; a JSON summary goes to --out.

example (a tiny web app):
  e2e_lifecycle.py --api http://127.0.0.1:8080 --image 127.0.0.1:5000/demo/app:v1 \
    --start-cmd 'python3 -m http.server 8080 >/dev/null 2>&1 &' --ready-cmd 'curl -fs localhost:8080 >/dev/null' \
    --check-cmd 'curl -fs localhost:8080/ | sha256sum' --write-cmd 'echo x > /srv/marker' --look-cmd 'test -f /srv/marker'
"""
import argparse
import concurrent.futures as cf
import json
import os
import sys
import time

import requests
from e2b import Sandbox

S = {"phases": {}}


class Api:
    def __init__(self, url, key):
        self.url, self.h = url.rstrip("/"), {"X-API-Key": key, "Content-Type": "application/json"}

    def req(self, method, path, body=None, timeout=900, ok=(200, 201, 204)):
        r = requests.request(method, self.url + path, headers=self.h, json=body, timeout=timeout)
        if r.status_code not in ok:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:800]}")
        return r.json() if r.text.strip() else None


def sh(sid, cmd, timeout=600):
    r = Sandbox.connect(sid).commands.run(cmd, timeout=timeout)
    return r.exit_code, r.stdout


def try_sh(sid, cmd):
    try:
        return sh(sid, cmd)[0] == 0
    except Exception:  # noqa: BLE001 - non-zero exit raises in the SDK
        return False


def wait_ready(sid, cmd, t0, deadline=900):
    tries = 0
    while not try_sh(sid, cmd):
        tries += 1
        if time.time() - t0 > deadline:
            return None, tries
        time.sleep(0.2)
    return round(time.time() - t0, 3), tries + 1


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return round(xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))], 3) if xs else None


def phase(name, ok, **kw):
    print(f"PHASE {name} {'PASS' if ok else 'FAIL'} {json.dumps(kw, separators=(',', ':'))}", flush=True)
    S["phases"][name] = {"ok": ok, **kw}


def node_of(nodes, sid):
    for n in nodes:
        try:
            if any(s.get("sandboxID") == sid for s in n.req("GET", "/sandboxes", timeout=30)):
                return n.url
        except Exception:  # noqa: BLE001
            pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    ap.add_argument("--key-file", default=os.path.expanduser("~/aenv-poc/nodes/api-key"))
    ap.add_argument("--nodes", default="", help="comma-separated node APIs, to report placement")
    ap.add_argument("--image", required=True)
    ap.add_argument("--start-cmd", required=True)
    ap.add_argument("--ready-cmd", required=True)
    ap.add_argument("--check-cmd", required=True)
    ap.add_argument("--write-cmd", required=True)
    ap.add_argument("--look-cmd", required=True)
    ap.add_argument("--phases", default="cold,template,spawn,isolation,fork,pause,cleanup")
    ap.add_argument("--template", default="e2e-app")
    ap.add_argument("--n", type=int, default=8)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--fork-n", type=int, default=4)
    ap.add_argument("--cpu", type=int, default=4)
    ap.add_argument("--mem", type=int, default=16384)
    ap.add_argument("--out", default="e2e-summary.json")
    a = ap.parse_args()
    key = open(a.key_file).read().strip()
    os.environ.update(E2B_API_URL=a.api, E2B_SANDBOX_URL=a.api, E2B_API_KEY=key)
    api, nodes = Api(a.api, key), [Api(u, key) for u in a.nodes.split(",") if u]
    phases, created, src, restored = a.phases.split(","), [], None, []
    expected = None

    if "cold" in phases:
        t0 = time.time()
        src = api.req("POST", "/sandboxes-cold", {"image": a.image, "cpuCount": a.cpu, "memoryMB": a.mem,
                                                   "timeout": 7200, "autoPause": False})["sandboxID"]
        created.append(src)
        t_create = time.time() - t0
        sh(src, f"setsid nohup sh -c '{a.start_cmd}' >/dev/null 2>&1 </dev/null &", timeout=60)
        ready, tries = wait_ready(src, a.ready_cmd, t0)
        expected = sh(src, a.check_cmd)[1] if ready else None
        phase("cold", ready is not None, sandbox=src, node=node_of(nodes, src), t_create_s=round(t_create, 3),
              t_ready_s=ready, tries=tries, kernel=sh(src, "uname -r")[1].strip())

    if "template" in phases:
        t0 = time.time()
        r = api.req("POST", f"/sandboxes/{src}/snapshots", {"name": a.template})
        dt = time.time() - t0
        phase("template", sh(src, a.check_cmd)[1] == expected, snapshot=r.get("snapshotID"), snapshot_s=round(dt, 3))

    if "spawn" in phases:
        def one(_):
            t0 = time.time()
            sid = api.req("POST", "/sandboxes", {"templateID": a.template, "timeout": 3600, "autoPause": False})["sandboxID"]
            rec = {"sandbox": sid, "t_create_s": round(time.time() - t0, 3)}
            rec["t_ready_s"], _ = wait_ready(sid, a.ready_cmd, t0)
            out = sh(sid, a.check_cmd)[1]
            rec["match"] = expected is None or out == expected
            return rec, out
        t0 = time.time()
        with cf.ThreadPoolExecutor(a.concurrency) as ex:
            res = list(ex.map(one, range(a.n)))
        wall = time.time() - t0
        if expected is None:
            expected = res[0][1]
        rows = [r for r, _ in res]
        placement = {}
        for r in rows:
            created.append(r["sandbox"]); restored.append(r["sandbox"])
            r["match"] = r["match"] and res[rows.index(r)][1] == expected
            r["node"] = node_of(nodes, r["sandbox"]) if nodes else None
            placement[r["node"]] = placement.get(r["node"], 0) + 1
        phase("spawn", all(r["t_ready_s"] and r["match"] for r in rows), n=a.n, concurrency=a.concurrency,
              wall_s=round(wall, 3), create_p50=pct([r["t_create_s"] for r in rows], 50),
              ready_p50=pct([r["t_ready_s"] for r in rows], 50), ready_p90=pct([r["t_ready_s"] for r in rows], 90),
              placement=placement)

    if "isolation" in phases:
        sa, sb = restored[0], restored[1]
        sh(sa, a.write_cmd)
        fresh = api.req("POST", "/sandboxes", {"templateID": a.template, "timeout": 3600, "autoPause": False})["sandboxID"]
        created.append(fresh)
        wait_ready(fresh, a.ready_cmd, time.time())
        iso = {"a_sees": try_sh(sa, a.look_cmd), "b_sees": try_sh(sb, a.look_cmd), "fresh_sees": try_sh(fresh, a.look_cmd),
               "source_sees": try_sh(src, a.look_cmd) if src else None,
               "a_node": node_of(nodes, sa), "b_node": node_of(nodes, sb)}
        phase("isolation", iso["a_sees"] and not iso["b_sees"] and not iso["fresh_sees"] and not iso["source_sees"], **iso)

    if "fork" in phases:
        base = restored[-1]
        t0 = time.time()
        r = api.req("POST", f"/sandboxes/{base}/fork", {"count": a.fork_n, "timeout": 3600})
        t_fork = time.time() - t0
        kids = [e["sandbox"]["sandboxID"] for e in r if "sandbox" in e]
        created.extend(kids)
        ok_kids = [sh(k, a.check_cmd)[1] == expected for k in kids]
        phase("fork", len(kids) == a.fork_n and all(ok_kids) and sh(base, a.check_cmd)[1] == expected,
              fork_api_s=round(t_fork, 3), children=len(kids), errors=[e for e in r if "sandbox" not in e][:2])

    if "pause" in phases:
        sid = restored[0]   # carries the isolation write, which must survive
        t0 = time.time(); api.req("POST", f"/sandboxes/{sid}/pause", {}); t_pause = time.time() - t0
        state = api.req("GET", f"/sandboxes/{sid}", timeout=30, ok=(200,)).get("state")
        t0 = time.time(); api.req("POST", f"/sandboxes/{sid}/resume", {"timeout": 3600}); t_resume = time.time() - t0
        ready, _ = wait_ready(sid, a.ready_cmd, t0)
        kept = try_sh(sid, a.look_cmd) if "isolation" in phases else True
        phase("pause", ready is not None and kept, pause_s=round(t_pause, 3), state=state, resume_s=round(t_resume, 3),
              t_ready_s=ready, write_survived=kept)

    if "cleanup" in phases:
        for sid in created:
            try:
                api.req("DELETE", f"/sandboxes/{sid}", timeout=120, ok=(204, 404))
            except Exception as e:  # noqa: BLE001
                print("cleanup", sid, e)
        phase("cleanup", True, deleted=len(created))

    json.dump(S, open(a.out, "w"), indent=1)
    ok = all(p["ok"] for p in S["phases"].values())
    print("SUMMARY", a.out, "ALL_PASS" if ok else "SOME_FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
