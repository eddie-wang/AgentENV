# Container backend (runc + CRIU) for AgentENV: proof of concept

This branch adds an **OCI container sandbox backend** to AgentENV v0.2.4. It runs next to the Firecracker backend, behind the same `SandboxBackend` / `SandboxBackendFactory` traits. The API, E2B SDK compatibility, templates, snapshots, gateway and scheduler are unchanged.

Status: prototype. It was validated end to end with a real data-agent environment image on a 2-node cluster on one host; see [RESULTS.md](RESULTS.md).

## Design

The backend keeps the node's existing plumbing and replaces only the VMM. Code: `src/sandbox/container/` (about 1.5k lines).

| Piece | Firecracker backend | Container backend |
|---|---|---|
| Image → rootfs | OCI → OverlayBD (ext4) over ublk, as virtio-blk | **the same ublk device**, mounted ext4 on the host as the runc root |
| Rootfs snapshot | restack (seal the writable upper) | **the same restack**, after `runc pause` and `fsfreeze` |
| Memory snapshot | VM state + a memory OverlayBD layer | CRIU images (`runc checkpoint --leave-running`) tarred into the snapshot's `vm_state` blob; the memory image is an empty placeholder layer |
| Network | slot netns; `tap0` → VM | slot netns; `tap0` is a **veth** to a per-sandbox guest netns with the same guest IP as a VM. CRIU, including established TCP, therefore restores into any slot on any node |
| Guest agent | envd from the tools drive | **the same envd binary**: the tools drive is mounted read-only on the host and bind-mounted at `/agentenv`. A small init runs it under runsv |

Lifecycle mapping:
- **create from an image**: `runc run`;
- **create from a template**: `runc restore`, from the CRIU archive, unpacked once per node and shared;
- **snapshot**: freeze, seal the rootfs, CRIU dump, publish;
- **pause**: the same capture into the paused-sandbox store;
- **resume**: restore;
- **fork(n)**: a local capture, then n restores;
- **delete**: kill, unmount, release the device, give `tap0` back to the slot.

### Backend selection (one node runs both)

- **`POST /sandboxes-cold` takes `"backend": "firecracker" | "container"`.** If it's omitted, the node default applies: `AENV_SANDBOX_BACKEND`, normally `firecracker`. An unknown or unavailable backend returns 400 before any image work.
- **Snapshots and templates** (`POST /sandboxes`) go to the backend named in the snapshot manifest (`backend: "container"` or `"firecracker"`).
- **Forks and resumes** go to the backend that captured them.
- **The server flag `--enable-container`** (or `AENV_ENABLE_CONTAINER=1`) turns the container backend on. Container requests fail with a clear error when it's off.
- **Without KVM:** when the container backend is enabled, the node starts without KVM. The startup KVM check only warns, and the Firecracker pool prime is skipped. Every microVM operation checks KVM lazily (in `FirecrackerSandbox::build`: cold create, template or snapshot create, fork, resume, template build) and fails just that request. The server stays up and keeps serving containers. The check result is cached once KVM works.

Tested matrix (`scripts/node.sh`, with `MASK_KVM=1` simulating a host without KVM):

| Node | cold `firecracker` | cold `container` | cold, no backend | microVM template | container template | server |
|---|---|---|---|---|---|---|
| KVM, `--enable-container` | 201, microVM | 201, container | 201, microVM | 201 | 201 | up; VMs and containers run side by side |
| no KVM, `--enable-container` | 400 "microVM sandboxes are unavailable on this node: …" | 201 | 400 | 500 "… unavailable …" | 201 | up, `/health` 204, existing containers keep working |
| no KVM, container off | — | — | — | — | — | refuses to start (stock behaviour) |
| KVM, container off | 201 | 400 "container backend is not enabled …" | 201 | 201 | 500 "not enabled" | up |

### Checkpointability guard rails, learned the hard way

- **io_uring:** CRIU can't dump it, so seccomp returns `ENOSYS` for `io_uring_*`. Libraries fall back, as they do under Docker's default profile.
- **MPTCP:** CRIU can't dump MPTCP sockets either. Go enables MPTCP by default, so seccomp rejects `socket(..., IPPROTO_MPTCP)` and envd runs with `GODEBUG=multipathtcp=0`.
- **uprobes:** host agents may attach uprobes, so `/etc/criu/runc.conf` sets `allow-uprobes`.
- **stdio:** the container's stdio is `/dev/null`. A host pipe would block `runc run --detach`'s caller, and a host file can't be restored.
- **Capture dirs:** they live under the managed snapshot base. The OverlayBD restack then adopts inherited layers, so every pause, fork or snapshot dir is self-contained.

## Layout

```
src/sandbox/container/      backend: sandbox.rs (lifecycle), runtime.rs (netns, mounts, OCI spec, init, CRIU archive), factory.rs
poc/container-criu/
  scripts/build.sh          cargo build of the node server
  scripts/build_criu.sh     CRIU v4.3 from the pinned release tarball → /usr/local/sbin/criu
  scripts/host_setup.sh     ublk module, AENV_HOME permissions, /etc/criu/runc.conf
  scripts/node.sh           start/stop a node server (root); NETNS= runs it in a network namespace, SCHED= enables scheduler reports
  scripts/cluster_up.sh     scheduler + gateway + n1 (host :8000) + n2 (netns, 10.250.0.2:8000), shared POSIX repository
  scripts/cleanup.sh        force-remove leftover containers, mounts and netns after a crash
  scripts/oci_layout_registry.py  minimal read-only registry serving a local OCI layout (for `aenv pull` / cold create)
  config/node_config.py     derives a node config from the installed default
  e2e/e2e_lifecycle.py      app-agnostic E2E: cold, template, spawn, isolation, fork, pause, cleanup
```

## Run

```bash
sudo AENV_HOME_PATH=/home/aenv-ct bash install.sh    # stock v0.2.4 install (deps + default config); the unit is not used
poc/container-criu/scripts/build_criu.sh && poc/container-criu/scripts/host_setup.sh
poc/container-criu/scripts/build.sh
GATEWAY_BIN=... SCHEDULER_BIN=... poc/container-criu/scripts/cluster_up.sh
python3 poc/container-criu/e2e/e2e_lifecycle.py --api http://127.0.0.1:8080 --nodes http://127.0.0.1:8000,http://10.250.0.2:8000 \
  --image <registry>/<app>:<tag> --start-cmd '...' --ready-cmd '...' --check-cmd '...' --write-cmd '...' --look-cmd '...'
```

The node server runs as root (runc, CRIU, mounts). `criu` must be on root's PATH: runc 1.3 ignores `--criu`.

## Limitations / next steps

- The template builder (`aenv pull/build --start-cmd --ready-cmd`) is still Firecracker-only, because `template/runner.rs` constructs `FirecrackerSandbox` directly. For now a container template is made by a cold create, starting the app, then `POST /sandboxes/{id}/snapshots`.
- There are no volumes or attached drives in container mode.
- Restore is eager: each copy holds its full private heap and nothing is shared. Lazy pages and page sharing are open.
- The CRIU archive is one uncompressed tar per snapshot.
- The scheduler doesn't know backends. In a cluster where some nodes have no KVM, placement needs capability filtering: nodes report `kvm` / `container`, and the gateway passes the request's backend in the scheduling hint. Until then, keep node pools uniform.
- A microVM request on a node without KVM fails with 400 (cold create) or 500 (template); it isn't retried on another node.
- Containers share the host kernel and run as root with Docker's default capabilities plus `SYS_PTRACE`, under an allow-by-default seccomp profile, with no user namespace. Secure sandboxes (`secure: true`, envd access tokens across restore) are untested.
