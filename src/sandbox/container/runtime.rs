//! Host-side helpers for the container backend: commands, network plumbing,
//! mounts, the OCI runtime spec and the in-container init script.
//!
//! Everything here shells out to standard tools (`ip`, `nsenter`, `mount`,
//! `fsfreeze`, `runc`, `tar`). The node server already runs privileged, so the
//! prototype keeps the moving parts visible instead of re-implementing them.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::cfg::ConfigManager;
use crate::sandbox::ublk::{SharedReadOnlyDevice, UblkCreateSpec, UblkDeviceManager};

/// Name of the in-container directory the init script is bind-mounted at.
pub(super) const SUPPORT_MOUNT: &str = "/.agentenv-ct";

/// Node-wide settings, read from the environment once.
#[derive(Clone, Debug)]
pub(crate) struct ContainerSettings {
    /// `runc` executable.
    pub runc: String,
    /// runc state root (`runc --root`).
    pub runc_root: PathBuf,
    /// Node-local directory for mounts, support files and per-sandbox work dirs.
    pub base_dir: PathBuf,
    /// Extra arguments appended to `runc checkpoint`.
    pub checkpoint_args: Vec<String>,
    /// Extra arguments appended to `runc restore`.
    pub restore_args: Vec<String>,
}

impl ContainerSettings {
    pub(crate) fn global() -> &'static ContainerSettings {
        static SETTINGS: LazyLock<ContainerSettings> = LazyLock::new(ContainerSettings::from_env);
        &SETTINGS
    }

    fn from_env() -> Self {
        let var = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let split = |value: String| {
            value
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        let default_base = ConfigManager::global_config()
            .home_path
            .join("container")
            .display()
            .to_string();
        Self {
            runc: var("AENV_CT_RUNC", "runc"),
            runc_root: PathBuf::from(var("AENV_CT_RUNC_ROOT", "/run/aenv-ct/runc")),
            base_dir: PathBuf::from(var("AENV_CT_BASE_DIR", &default_base)),
            checkpoint_args: split(var(
                "AENV_CT_CHECKPOINT_ARGS",
                "--tcp-established --file-locks --ext-unix-sk",
            )),
            restore_args: split(var(
                "AENV_CT_RESTORE_ARGS",
                "--tcp-established --file-locks --ext-unix-sk --manage-cgroups-mode ignore",
            )),
        }
    }

    pub(super) fn work_root(&self) -> PathBuf {
        self.base_dir.join("work")
    }

    /// Local capture dirs (snapshot, fork, pause without an artifact root).
    /// They live under the node's managed snapshot base so the OverlayBD
    /// restack treats their layers as runtime-owned: a later capture adopts
    /// (hard-links) them, and every capture dir stays self-contained.
    pub(super) fn snapshot_root(&self) -> PathBuf {
        crate::sandbox::firecracker::managed_snapshot_base().join("container")
    }

    pub(super) fn support_dir(&self) -> PathBuf {
        self.base_dir.join("support")
    }

    pub(super) fn checkpoint_cache_dir(&self) -> PathBuf {
        self.base_dir.join("checkpoint-cache")
    }
}

/// Run a command to completion; fail with its stderr on a non-zero exit.
pub(super) async fn run(program: &str, args: &[&str]) -> Result<String> {
    debug!(program, ?args, "container backend command");
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("spawn {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} {} failed ({}): {}{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
            String::from_utf8_lossy(&output.stdout).trim(),
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Run a command inside the network namespace bound at `netns`.
pub(super) async fn run_in_netns(netns: &Path, args: &[&str]) -> Result<String> {
    let net = format!("--net={}", netns.display());
    let mut full = vec![net.as_str(), "--"];
    full.extend_from_slice(args);
    run("nsenter", &full).await
}

/// Run `runc` with the node's state root and CRIU binary.
pub(super) async fn runc(args: &[&str]) -> Result<String> {
    let settings = ContainerSettings::global();
    let root = settings.runc_root.display().to_string();
    let mut full = vec!["--root", root.as_str()];
    full.extend_from_slice(args);
    run(&settings.runc, &full).await
}

/// Run `runc run|restore --detach`. The container inherits the caller's stdio,
/// so it gets `/dev/null` (a host pipe or file would also be unrestorable by
/// CRIU); runc's own errors go to `log`.
pub(super) async fn runc_detached(args: &[&str], log: &Path) -> Result<()> {
    let settings = ContainerSettings::global();
    let root = settings.runc_root.display().to_string();
    let log_arg = log.display().to_string();
    let mut full = vec!["--root", root.as_str(), "--log", log_arg.as_str()];
    full.extend_from_slice(args);
    debug!(?full, "container backend detached runc");
    let status = Command::new(&settings.runc)
        .args(&full)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("spawn runc")?;
    if !status.success() {
        let log_text = tokio::fs::read_to_string(log).await.unwrap_or_default();
        let tail: Vec<&str> = log_text.lines().rev().take(8).collect();
        bail!(
            "runc {} failed ({status}): {}",
            args.first().copied().unwrap_or_default(),
            tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
        );
    }
    Ok(())
}

// ── Network ─────────────────────────────────────────────────────────────────

/// Replace the slot's `tap0` with a veth whose peer (`eth0`) lives in a new
/// per-sandbox network namespace, configured like a Firecracker guest NIC.
///
/// Keeping the slot-side name `tap0` means every existing iptables rule,
/// DNAT to the guest IP and the egress proxy apply unchanged. Every guest
/// uses the same IP, so a CRIU image restores into any slot.
pub(super) async fn attach_guest_netns(
    slot_netns: &Path,
    guest_netns: &str,
    guest_ip: Ipv4Addr,
    gateway_ip: Ipv4Addr,
    prefix: u8,
) -> Result<()> {
    run("ip", &["netns", "add", guest_netns]).await?;
    let gw_cidr = format!("{gateway_ip}/{prefix}");
    let guest_cidr = format!("{guest_ip}/{prefix}");
    let gw = gateway_ip.to_string();
    run_in_netns(slot_netns, &["ip", "link", "del", "tap0"])
        .await
        .context("remove slot tap0")?;
    run_in_netns(
        slot_netns,
        &[
            "ip", "link", "add", "tap0", "type", "veth", "peer", "name", "eth0", "netns",
            guest_netns,
        ],
    )
    .await
    .context("create guest veth")?;
    run_in_netns(slot_netns, &["ip", "addr", "add", &gw_cidr, "dev", "tap0"]).await?;
    run_in_netns(slot_netns, &["ip", "link", "set", "tap0", "up"]).await?;
    let n = guest_netns;
    run("ip", &["-n", n, "addr", "add", &guest_cidr, "dev", "eth0"]).await?;
    run("ip", &["-n", n, "link", "set", "eth0", "up"]).await?;
    run("ip", &["-n", n, "link", "set", "lo", "up"]).await?;
    run("ip", &["-n", n, "route", "add", "default", "via", &gw]).await?;
    Ok(())
}

/// Undo [`attach_guest_netns`]: drop the guest namespace and give the slot back
/// a `tap0` tap device, so the slot can be reused by any backend.
pub(super) async fn detach_guest_netns(
    slot_netns: Option<&Path>,
    guest_netns: &str,
    gateway_ip: Ipv4Addr,
    prefix: u8,
) -> Result<()> {
    if let Err(error) = run("ip", &["netns", "del", guest_netns]).await {
        warn!(%error, guest_netns, "failed to delete guest netns");
    }
    let Some(slot_netns) = slot_netns else {
        return Ok(());
    };
    // Deleting the namespace deletes eth0 and with it the slot-side peer.
    let _ = run_in_netns(slot_netns, &["ip", "link", "del", "tap0"]).await;
    let gw_cidr = format!("{gateway_ip}/{prefix}");
    run_in_netns(slot_netns, &["ip", "tuntap", "add", "tap0", "mode", "tap"]).await?;
    run_in_netns(slot_netns, &["ip", "addr", "add", &gw_cidr, "dev", "tap0"]).await?;
    run_in_netns(slot_netns, &["ip", "link", "set", "tap0", "up"]).await?;
    Ok(())
}

// ── Mounts ──────────────────────────────────────────────────────────────────

pub(super) async fn mount_ext4(device: &Path, target: &Path, read_only: bool) -> Result<()> {
    tokio::fs::create_dir_all(target)
        .await
        .with_context(|| format!("create mount point {}", target.display()))?;
    let options = if read_only { "ro,noload" } else { "rw" };
    run(
        "mount",
        &[
            "-t",
            "ext4",
            "-o",
            options,
            &device.display().to_string(),
            &target.display().to_string(),
        ],
    )
    .await
    .map(drop)
}

pub(super) async fn umount(target: &Path) -> Result<()> {
    let target = target.display().to_string();
    if run("umount", &[&target]).await.is_err() {
        run("umount", &["-l", &target]).await?;
    }
    Ok(())
}

/// Flush the filesystem and block writers, so its block device can be sealed.
pub(super) async fn fsfreeze(target: &Path, freeze: bool) -> Result<()> {
    let flag = if freeze { "-f" } else { "-u" };
    run("fsfreeze", &[flag, &target.display().to_string()])
        .await
        .map(drop)
}

/// Node-wide read-only mounts of the tools drive, one per release version.
/// The mount (and the shared ublk device behind it) lives as long as the node.
static TOOLS_MOUNTS: LazyLock<Mutex<HashMap<String, (SharedReadOnlyDevice, PathBuf)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The `/agentenv` directory of the tools drive (`envd`, `busybox`), mounted
/// read-only on the host. It is bind-mounted into every container.
pub(super) async fn tools_agentenv_dir(version: &str) -> Result<PathBuf> {
    let mut mounts = TOOLS_MOUNTS.lock().await;
    if let Some((_, path)) = mounts.get(version) {
        return Ok(path.join("agentenv"));
    }
    let config = ConfigManager::global_config();
    let image_config = crate::sandbox::resolve_tools_image(config, version)
        .await?
        .with_context(|| {
            format!("tools drive {version} is not an OverlayBD image; the container backend needs one")
        })?;
    let device = UblkDeviceManager::global()
        .get_or_create_shared_tools(&UblkCreateSpec::Overlaybd {
            image_config,
            global_config: config.ublk.overlaybd.global_config_path.clone(),
        })
        .await?;
    let target = ContainerSettings::global()
        .base_dir
        .join("tools")
        .join(version);
    // A previous server process may have left the mount behind.
    let _ = umount(&target).await;
    mount_ext4(device.device_path(), &target, true).await?;
    mounts.insert(version.to_string(), (device, target.clone()));
    Ok(target.join("agentenv"))
}

/// Unmount the node's tools-drive mounts and release their ublk devices.
/// Called at server shutdown, before the ublk daemon stops: a mounted device
/// would otherwise keep the daemon from deleting it.
pub(crate) async fn shutdown() {
    let mut mounts = TOOLS_MOUNTS.lock().await;
    for (version, (device, path)) in mounts.drain() {
        if let Err(error) = umount(&path).await {
            warn!(%error, %version, "failed to unmount tools drive");
        }
        if let Err(error) = device.release().await {
            warn!(%error, %version, "failed to release tools drive device");
        }
    }
}

// ── Container init and OCI spec ─────────────────────────────────────────────

/// PID 1 inside the container: the container counterpart of the tools drive's
/// `pivot-init`. It prepares the same guest integration points, supervises
/// envd under runsv and stays as a reaping `runsvdir`.
const INIT_SCRIPT: &str = r#"#!/agentenv/bin/busybox sh
BB=/agentenv/bin/busybox
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/root
$BB mkdir -p /usr/local/bin /usr/bin /bin /tmp /var/run /run/lock /var/log/agentenv/envd /run/sv/envd/log
$BB chmod 1777 /tmp 2>/dev/null
$BB ln -sf /agentenv/envd /usr/local/bin/envd
[ -x /usr/bin/nice ] || $BB ln -sf /agentenv/bin/busybox /usr/bin/nice
[ -x /usr/bin/ionice ] || $BB ln -sf /agentenv/bin/busybox /usr/bin/ionice
[ -x /bin/sh ] || $BB ln -sf /agentenv/bin/busybox /bin/sh
if [ ! -e /etc/passwd ]; then
    $BB mkdir -p /etc /root
    $BB printf 'root:x:0:0:root:/root:/bin/sh\n' > /etc/passwd
fi
[ -s /etc/hosts ] || $BB printf '127.0.0.1 localhost\n::1 localhost\n' > /etc/hosts
$BB cat > /run/sv/envd/run << 'RUNSV_EOF'
#!/agentenv/bin/busybox sh
exec 2>>/var/log/agentenv/envd.stderr.log
# CRIU cannot checkpoint MPTCP sockets, which Go enables by default for listeners.
export GODEBUG=multipathtcp=0
exec /agentenv/envd
RUNSV_EOF
$BB cat > /run/sv/envd/log/run << 'RUNSV_EOF'
#!/agentenv/bin/busybox sh
exec /agentenv/bin/busybox svlogd -tt /var/log/agentenv/envd
RUNSV_EOF
$BB chmod 0755 /run/sv/envd/run /run/sv/envd/log/run
echo "AGENTENV CONTAINER INIT $($BB date -u +%FT%TZ)" >> /var/log/agentenv/container-init.log
$BB runsv /run/sv/envd >/var/log/agentenv/envd-sv.log 2>&1 &
# PID 1 must reap orphans: runsvdir waits on any child. Its scan dir stays empty.
$BB mkdir -p /run/agentenv-reaper
exec $BB runsvdir -P /run/agentenv-reaper
"#;

/// Write the node's support directory (init script) once.
pub(super) async fn ensure_support_dir() -> Result<PathBuf> {
    static READY: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));
    let dir = ContainerSettings::global().support_dir();
    let mut ready = READY.lock().await;
    if !*ready {
        tokio::fs::create_dir_all(&dir).await?;
        let init = dir.join("init");
        tokio::fs::write(&init, INIT_SCRIPT).await?;
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&init, std::fs::Permissions::from_mode(0o755)).await?;
        *ready = true;
    }
    Ok(dir)
}

/// Prepare a freshly created rootfs: DNS, and the mount points the spec uses.
pub(super) async fn prepare_fresh_rootfs(rootfs: &Path, dns: Ipv4Addr) -> Result<()> {
    for dir in ["agentenv", ".agentenv-ct", "proc", "dev", "sys", "run", "tmp", "etc"] {
        tokio::fs::create_dir_all(rootfs.join(dir)).await?;
    }
    let resolv = rootfs.join("etc/resolv.conf");
    // Images often ship resolv.conf as a symlink into a runtime dir.
    let _ = tokio::fs::remove_file(&resolv).await;
    tokio::fs::write(&resolv, format!("nameserver {dns}\n")).await?;
    Ok(())
}

pub(super) struct SpecParams<'a> {
    pub rootfs: &'a Path,
    pub guest_netns: &'a str,
    pub cgroups_path: &'a str,
    pub cpu_count: u32,
    pub memory_mib: u32,
    pub tools_dir: &'a Path,
    pub support_dir: &'a Path,
}

/// OCI runtime spec for a sandbox container.
///
/// It must be identical between the dump and every restore except for host
/// paths (rootfs, bind sources, netns, cgroup): CRIU restores the mount
/// namespace by destination.
pub(super) fn oci_spec(params: &SpecParams<'_>) -> Value {
    const CAPS: &[&str] = &[
        "CAP_CHOWN",
        "CAP_DAC_OVERRIDE",
        "CAP_FSETID",
        "CAP_FOWNER",
        "CAP_MKNOD",
        "CAP_NET_RAW",
        "CAP_SETGID",
        "CAP_SETUID",
        "CAP_SETFCAP",
        "CAP_SETPCAP",
        "CAP_NET_BIND_SERVICE",
        "CAP_SYS_CHROOT",
        "CAP_KILL",
        "CAP_AUDIT_WRITE",
        "CAP_SYS_PTRACE",
    ];
    let shm_size = format!("size={}m", (params.memory_mib / 2).max(64));
    let cpu_quota = i64::from(params.cpu_count.max(1)) * 100_000;
    let memory_limit = i64::from(params.memory_mib) * 1024 * 1024;
    json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": {"uid": 0, "gid": 0},
            "args": [format!("{SUPPORT_MOUNT}/init")],
            "env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "HOME=/root"
            ],
            "cwd": "/",
            "capabilities": {
                "bounding": CAPS, "effective": CAPS, "permitted": CAPS
            },
            "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1048576u64, "soft": 1048576u64}],
            "noNewPrivileges": false
        },
        "root": {"path": params.rootfs.display().to_string(), "readonly": false},
        "hostname": "instance",
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs",
             "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]},
            {"destination": "/dev/pts", "type": "devpts", "source": "devpts",
             "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"]},
            {"destination": "/dev/shm", "type": "tmpfs", "source": "shm",
             "options": ["nosuid", "noexec", "nodev", "mode=1777", shm_size]},
            {"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue",
             "options": ["nosuid", "noexec", "nodev"]},
            {"destination": "/sys", "type": "sysfs", "source": "sysfs",
             "options": ["nosuid", "noexec", "nodev", "ro"]},
            {"destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup",
             "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]},
            {"destination": "/run", "type": "tmpfs", "source": "tmpfs",
             "options": ["nosuid", "nodev", "mode=755"]},
            {"destination": "/agentenv", "type": "bind",
             "source": params.tools_dir.display().to_string(), "options": ["rbind", "ro"]},
            {"destination": SUPPORT_MOUNT, "type": "bind",
             "source": params.support_dir.display().to_string(), "options": ["rbind", "ro"]}
        ],
        "linux": {
            "cgroupsPath": params.cgroups_path,
            "resources": {
                "devices": [{"allow": false, "access": "rwm"}],
                "cpu": {"quota": cpu_quota, "period": 100000u64},
                "memory": {"limit": memory_limit}
            },
            "namespaces": [
                {"type": "pid"},
                {"type": "ipc"},
                {"type": "uts"},
                {"type": "mount"},
                {"type": "cgroup"},
                {"type": "network", "path": format!("/run/netns/{}", params.guest_netns)}
            ],
            // Checkpointability guard rails, applied to every process in the
            // sandbox: CRIU cannot dump io_uring instances or MPTCP sockets, so
            // refuse to create them (callers fall back, as under Docker's
            // default profile, which also blocks io_uring).
            "seccomp": {
                "defaultAction": "SCMP_ACT_ALLOW",
                "architectures": ["SCMP_ARCH_X86_64", "SCMP_ARCH_X86", "SCMP_ARCH_X32"],
                "syscalls": [
                    {"names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
                     "action": "SCMP_ACT_ERRNO", "errnoRet": 38u32},
                    {"names": ["socket"], "action": "SCMP_ACT_ERRNO", "errnoRet": 93u32,
                     "args": [{"index": 2u32, "value": 262u64, "op": "SCMP_CMP_EQ"}]}
                ]
            },
            "maskedPaths": [
                "/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
                "/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware",
                "/proc/scsi"
            ],
            "readonlyPaths": [
                "/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"
            ]
        }
    })
}

pub(super) async fn write_bundle(bundle: &Path, spec: &Value) -> Result<()> {
    tokio::fs::create_dir_all(bundle).await?;
    tokio::fs::write(bundle.join("config.json"), serde_json::to_vec_pretty(spec)?)
        .await
        .context("write OCI config.json")
}

// ── CRIU image archives ─────────────────────────────────────────────────────

/// Pack a CRIU image directory into the single `vm_state` blob a snapshot
/// record carries.
pub(super) async fn pack_checkpoint(images_dir: &Path, archive: &Path) -> Result<()> {
    run(
        "tar",
        &[
            "-C",
            &images_dir.display().to_string(),
            "-cf",
            &archive.display().to_string(),
            ".",
        ],
    )
    .await
    .map(drop)
}

/// Unpack a snapshot's CRIU archive once per node and share it between every
/// restore of that snapshot (CRIU only reads the image directory).
pub(super) async fn unpack_checkpoint(archive: &Path) -> Result<PathBuf> {
    static LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    let meta = tokio::fs::metadata(archive)
        .await
        .with_context(|| format!("stat checkpoint archive {}", archive.display()))?;
    use std::os::unix::fs::MetadataExt;
    let key = format!(
        "{:x}",
        fnv1a(&format!(
            "{}:{}:{}",
            archive.display(),
            meta.len(),
            meta.mtime()
        ))
    );
    let target = ContainerSettings::global()
        .checkpoint_cache_dir()
        .join(key);
    let lock = LOCKS
        .lock()
        .await
        .entry(target.clone())
        .or_default()
        .clone();
    let _guard = lock.lock().await;
    if tokio::fs::try_exists(target.join("inventory.img")).await? {
        return Ok(target);
    }
    let staging = target.with_extension("tmp");
    let _ = tokio::fs::remove_dir_all(&staging).await;
    tokio::fs::create_dir_all(&staging).await?;
    run(
        "tar",
        &[
            "-C",
            &staging.display().to_string(),
            "-xf",
            &archive.display().to_string(),
        ],
    )
    .await?;
    tokio::fs::rename(&staging, &target).await?;
    Ok(target)
}

/// Stable, dependency-free 64-bit FNV-1a hash for cache keys.
fn fnv1a(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
