# Results (2026-11-11)

**Host:** AWS `m5d.metal` (bare metal, 96 vCPU, 377 GB RAM), Ubuntu 24.04, kernel 7.0. runc 1.3.6, CRIU 4.3.

**Workload:** a Spark/Delta data-agent environment. A Python tool server drives a local Spark JVM over a read-only dataset baked into the image. The image is about 5.6 GB; sandboxes get 4 CPU / 16 GiB. Its cold boot (Spark init and table registration) takes about 95 s, so a template that snapshots the booted app is the whole point.

## Lifecycle, 2-node cluster

Both nodes run the container backend. The nodes are on one host: n2 runs in its own network namespace with its own ublk daemon, sharing one POSIX snapshot repository. Requests go through the stock gateway and scheduler (round robin).

| Step | Result |
|---|---|
| Cold create from the OCI image | API 0.27–0.31 s; app ready after about 98 s (app boot) |
| App correctness | 5 reference tool calls match the host-boot outputs on cold, template-restored, forked and resumed sandboxes |
| Snapshot → template | 9.5–13 s: seal about 60 ms, CRIU dump about 3 s, then tar and publish of a 4–5.6 GB archive. The source keeps running |
| Create from template, sequential | 2.3 s p50, almost all of it eager CRIU restore. The first create on a node adds about 3 s to unpack the archive |
| Create from template, 16 at 8 concurrent, via gateway | all ready, 8 per node, 12.4 s wall |
| Cross-node | a template built on n1 restores on n2 |
| Isolation | a write in sandbox A is invisible to B (other node), to a fresh spawn and to the source |
| Fork ×4 | 4.9–7.7 s API time; children correct; the source is fine afterwards |
| Pause / resume | about 3 s / 2.5 s; state survives |
| Paused sandbox across a node restart | resumes with data intact |
| One node, both backends | image launch gives a microVM (guest kernel 6.1); a container template gives a container (host kernel) |
| Cleanup | no leftover containers, netns or mounts |

## Full-trajectory replay: microVM vs container

Both templates were built the same way on one node: cold create, app ready, snapshot. We replayed 100 recorded agent rollouts at 10 concurrent, after 10 warm-up rollouts, over 2 rounds. Each rollout makes about 14 tool calls; most of the time is in Spark code-execution and notebook-cell calls. App calls go through the node's reverse proxy.

| Mode | Backend | Makespan, round 1 / round 2 | e2e per rollout | setup p50 | tool time per rollout | teardown |
|---|---|---|---|---|---|---|
| **On demand**: create from template on the rollout's clock, wait ready, replay, delete | microVM | 234.2 / 230.7 s | 20.95 s | 0.09 s | 20.36 s | 0.43 s |
| | container | 230.7 / 232.0 s | 21.33 s | 2.84 s | 17.39 s | 1.04 s |
| **Pre-warmed**: single-use pool of 10 ready sandboxes, refilled in the background | microVM | 235.8 / 237.8 s | 21.28 s | 0 | 20.77 s | 0.43 s |
| | container | **213.0 / 217.9 s** | **19.10 s** | 0 | **17.94 s** | 1.10 s |

Spark code-execution calls (n=403) take 3.52 s mean / 7.3 s p90 in the container, against 4.05 / 9.6 s in the microVM. Notebook-cell runs (n=106) take 2.94 / 7.2 s against 3.68 / 10.1 s.

Reading the numbers:
- **The container runs tools about 15% faster.** The rootfs is read through the host page cache, and there's no virtualization layer.
- **On demand, the gain is cancelled out.** Eager CRIU restore costs about 2.8 s at 10 concurrent, versus 0.09 s for a microVM snapshot restore, and teardown is 0.6 s slower. Makespans are equal within ±1%.
- **With a pre-warmed pool, the container is about 9% faster** (215.5 vs 236.8 s).
- The comparison runs on bare metal with 4 KiB pages, which is the microVM's best case. On nested-virtualization hosts the microVM is much slower without huge pages, so the container's lead would likely be larger there.
- The app's search tools ran without their embedding backend, in both arms.

## Memory: 10 sandboxes from the template, microVM vs container

Same node, page cache dropped before each arm. The host delta is `MemTotal - MemAvailable` against the arm's baseline (reclaimable page cache excluded). "Busy" is measured after each sandbox replayed one recorded rollout, with the sandboxes still alive.

| | microVM | container |
|---|---|---|
| Idle, just restored | **+2.05 GiB (0.21 per sandbox)**; PSS 1.95 GiB (template pages shared) | **+41.2 GiB (4.1 per sandbox)**: the full checkpointed heap (4.07 GiB) copied into each sandbox |
| After 1 rollout each | **+25.9 GiB (2.6 per sandbox)** | **+42.2 GiB (4.2 per sandbox)** |
| Wall time of those 10 rollouts | 37.0 s | 28.8 s |

Restored containers cost about 20× more memory while idle and about 1.6× after a short rollout. MicroVM clones converge toward the container figure as the app rewrites its heap. Closing this gap needs lazy, shared memory restore; see the README's limitations.
