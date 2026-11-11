//! [`ContainerSandbox`]: an OCI container run by `runc`, checkpointed with CRIU.
//!
//! The container keeps the node's existing sandbox plumbing and swaps only the
//! VMM:
//!
//! | Piece            | Firecracker backend                | Container backend                                  |
//! |------------------|------------------------------------|----------------------------------------------------|
//! | Rootfs           | OverlayBD over ublk, virtio-blk    | the same ublk device, mounted ext4 on the host     |
//! | Rootfs snapshot  | restack (seal upper)               | the same restack, after freezing tasks + fs        |
//! | Memory snapshot  | VM state + memory OverlayBD layer  | CRIU images, stored as the `vm_state` blob         |
//! | Network          | slot netns, `tap0` → guest         | slot netns, `tap0` is a veth → guest netns         |
//! | Guest agent      | envd from the tools drive          | the same envd binary, bind-mounted read-only       |
//!
//! Every guest has the same IP, so a CRIU image (including established TCP
//! connections inside the sandbox) restores into any slot on any node.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use firecracker_client::models::DirtyMemoryRanges;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use super::runtime::{self, ContainerSettings, SpecParams};
use crate::cfg::ConfigManager;
use crate::overlaybd::OverlaybdCompactOutput;
use crate::sandbox::backend::{
    CapturedSandboxSnapshot, PausedSandboxState, SandboxBackend, SandboxCaptureError,
    SandboxCaptureResult, SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
};
use crate::sandbox::envd::EnvdInstance;
use crate::sandbox::firecracker::{
    build_mem_snapshot_image_config, convert_dirty_memory_to_overlaybd,
    restack_snapshot_overlaybd_rootfs,
};
use crate::sandbox::network::{NetworkManager, SandboxNetworkPolicy, Slot};
use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};
use crate::sandbox::{
    CustomExtensionParams, EnvdAccessToken, FirecrackerCommonConfig, OverlaybdConfig,
    CONTAINER_BACKEND,
};
use crate::snapshot::SandboxSnapshotManifest;
use crate::types::{RuntimeArtifactSet, SandboxId};

const CHECKPOINT_ARCHIVE: &str = "vm_state.bin";
const CHECKPOINT_DIR: &str = "criu";

/// Where a launch's CRIU images come from.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "path", rename_all = "camelCase")]
pub enum ContainerCheckpoint {
    /// A node-local CRIU image directory (pause, fork).
    Dir(PathBuf),
    /// A committed snapshot's `vm_state` blob: a tar of the image directory.
    Archive(PathBuf),
}

/// Everything needed to (re)create a container sandbox.
///
/// `common` is the node's sandbox launch config shared with the Firecracker
/// backend (rootfs image, env, workdir, user, tools version, network policy,
/// envd port and timeouts); the VM-only fields in it are ignored.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerLaunch {
    pub common: FirecrackerCommonConfig,
    pub cpu_count: u32,
    pub memory_mib: u32,
    /// `None` boots the image; `Some` restores CRIU images over the rootfs.
    pub checkpoint: Option<ContainerCheckpoint>,
}

/// Deletes a directory once the last owner is gone. A sandbox's local captures
/// are lowers of its own (and its fork children's) live rootfs, so they must
/// outlive every runtime that stacks on them.
#[derive(Debug)]
pub(crate) struct DirGuard(PathBuf);

impl DirGuard {
    pub(crate) fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(%error, path = %self.0.display(), "failed to remove container capture dir");
            }
        }
    }
}

/// Paused state: the launch that restores the captured images.
#[derive(Debug)]
pub struct ContainerPausedState {
    launch: ContainerLaunch,
    _root: Option<Arc<DirGuard>>,
}

#[derive(Serialize, Deserialize)]
struct EncodedPausedState {
    backend: String,
    launch: ContainerLaunch,
}

impl ContainerPausedState {
    pub(crate) fn new(launch: ContainerLaunch, root: Option<Arc<DirGuard>>) -> Self {
        Self {
            launch,
            _root: root,
        }
    }

    pub(crate) fn launch(&self) -> &ContainerLaunch {
        &self.launch
    }

    /// Whether `state` is an encoded container paused state.
    pub(crate) fn is_encoded(state: &serde_json::Value) -> bool {
        state.get("backend").and_then(|value| value.as_str()) == Some(CONTAINER_BACKEND)
    }

    pub(crate) fn decode(state: serde_json::Value) -> Result<Self> {
        let encoded: EncodedPausedState =
            serde_json::from_value(state).context("decode container paused state")?;
        Ok(Self::new(encoded.launch, None))
    }
}

impl PausedSandboxState for ContainerPausedState {
    fn encode(&self) -> Result<serde_json::Value> {
        serde_json::to_value(EncodedPausedState {
            backend: CONTAINER_BACKEND.to_string(),
            launch: self.launch.clone(),
        })
        .context("serialize container paused state")
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(
            self.launch
                .common
                .rootfs_image_config
                .iter()
                .map(|config| config.image_config_path.clone())
                .collect(),
        )
    }

    fn control_plane_port(&self) -> Option<u16> {
        let port = self.launch.common.control_plane_port;
        (port != 0).then_some(port)
    }
}

pub struct ContainerSandbox {
    id: SandboxId,
    launch: ContainerLaunch,
    work_dir: PathBuf,
    /// This sandbox's local captures (snapshot, fork). Created lazily.
    capture_root: Option<Arc<DirGuard>>,
    /// Capture dirs of the snapshot this sandbox was forked or resumed from.
    inherited_roots: Vec<Arc<DirGuard>>,
    slot: Option<Slot>,
    guest_netns: Option<String>,
    rootfs: Option<OverlaybdRuntimeHandle>,
    rootfs_mounted: bool,
    container_id: Option<String>,
    envd: Option<EnvdInstance>,
    current_network_policy: Option<SandboxNetworkPolicy>,
    current_custom_extension_params: Option<CustomExtensionParams>,
}

impl ContainerSandbox {
    pub(crate) fn new(id: SandboxId, launch: ContainerLaunch) -> Result<Self> {
        let work_dir = ContainerSettings::global().work_root().join(id.to_string());
        Ok(Self {
            id,
            current_network_policy: launch.common.network_policy.clone(),
            current_custom_extension_params: launch.common.custom_extension_params.clone(),
            launch,
            work_dir,
            capture_root: None,
            inherited_roots: Vec::new(),
            slot: None,
            guest_netns: None,
            rootfs: None,
            rootfs_mounted: false,
            container_id: None,
            envd: None,
        })
    }

    pub(crate) fn with_inherited_root(mut self, root: Option<Arc<DirGuard>>) -> Self {
        self.inherited_roots.extend(root);
        self
    }

    pub(crate) fn with_access_token(mut self, token: Option<EnvdAccessToken>) -> Self {
        self.launch.common.envd_access_token = token;
        self
    }

    fn rootfs_mount(&self) -> PathBuf {
        self.work_dir.join("rootfs")
    }

    fn bundle_dir(&self) -> PathBuf {
        self.work_dir.join("bundle")
    }

    fn container_name(&self) -> String {
        format!("aenv-{}", self.id)
    }

    fn container_id(&self) -> Result<&str> {
        self.container_id
            .as_deref()
            .context("container is not running")
    }

    async fn start_inner(&mut self) -> Result<()> {
        let started = std::time::Instant::now();
        tokio::fs::create_dir_all(&self.work_dir).await?;
        let tools_dir = runtime::tools_agentenv_dir(&self.launch.common.tools_drive_version)
            .await
            .context("prepare tools drive")?;
        let support_dir = runtime::ensure_support_dir().await?;

        // ── Network: slot netns + guest netns behind a veth named tap0 ──
        let slot = NetworkManager::global()
            .allocate_any()
            .context("allocate network slot")?;
        let slot = self.slot.insert(slot);
        slot.set_egress_policy(self.launch.common.network_policy.as_ref())
            .context("configure sandbox egress policy")?;
        let (guest_ip, gateway_ip, prefix) = slot.guest_link();
        let slot_netns = slot.namespace_path();
        let interaction_ip = slot.host_interaction_ip;
        let dns = slot.guest_dns_server();
        let guest_netns = format!("aenvct-{}", self.id);
        self.guest_netns = Some(guest_netns.clone());
        runtime::attach_guest_netns(&slot_netns, &guest_netns, guest_ip, gateway_ip, prefix)
            .await
            .context("attach guest network namespace")?;

        // ── Rootfs: the OverlayBD ublk device, mounted on the host ──
        let rootfs_config = self
            .launch
            .common
            .rootfs_image_config
            .clone()
            .context("container rootfs image config is missing")?;
        let restoring = self.launch.checkpoint.is_some();
        let (requested, known) = if restoring {
            (
                self.launch.common.rootfs_virtual_size,
                self.launch.common.rootfs_virtual_size,
            )
        } else {
            (self.launch.common.rootfs_virtual_size, None)
        };
        let global_config_path = ConfigManager::global_config()
            .ublk
            .overlaybd
            .global_config_path
            .clone();
        let device = UblkDeviceManager::global()
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: &rootfs_config.image_config_path,
                global_config: &global_config_path,
                runtime_dir: &self.work_dir.join("overlaybd"),
                read_only: false,
                runtime_upper_mode: rootfs_config.runtime_upper_mode,
                requested_virtual_size: requested,
                known_source_virtual_size: known,
                allow_shrink: !restoring && self.launch.common.rootfs_allow_shrink,
            })
            .await
            .context("create rootfs runtime device")?;
        let device_path = device.device.device_path().to_path_buf();
        self.launch.common.rootfs_virtual_size = Some(device.actual_virtual_size);
        self.rootfs = Some(OverlaybdRuntimeHandle {
            device: device.device,
            image_config_path: device.image_config_path,
            actual_virtual_size: device.actual_virtual_size,
        });
        let rootfs = self.rootfs_mount();
        runtime::mount_ext4(&device_path, &rootfs, false)
            .await
            .context("mount rootfs")?;
        self.rootfs_mounted = true;

        // ── OCI bundle ──
        let cgroups_path = format!("/aenv-ct/{}", self.id);
        let spec = runtime::oci_spec(&SpecParams {
            rootfs: &rootfs,
            guest_netns: &guest_netns,
            cgroups_path: &cgroups_path,
            cpu_count: self.launch.cpu_count,
            memory_mib: self.launch.memory_mib,
            tools_dir: &tools_dir,
            support_dir: &support_dir,
        });
        let mut spec = spec;
        if restoring {
            // Restore-only CRIU options via runc's per-container config annotation
            // (replaces /etc/criu/runc.conf for this restore). mntns-compat-mode avoids
            // mount-v2 failing to copy bind-mount sharing when the node's mount
            // namespace or propagation differs from the dumping node's.
            let conf = self.work_dir.join("criu-restore.conf");
            tokio::fs::write(&conf, "allow-uprobes\nmntns-compat-mode\n").await?;
            spec["annotations"] =
                serde_json::json!({"org.criu.config": conf.display().to_string()});
        }
        let bundle = self.bundle_dir();
        runtime::write_bundle(&bundle, &spec).await?;
        let bundle_arg = bundle.display().to_string();
        let name = self.container_name();
        let pid_file = self.work_dir.join("init.pid").display().to_string();

        match self.launch.checkpoint.clone() {
            None => {
                runtime::prepare_fresh_rootfs(&rootfs, dns).await?;
                runtime::runc_detached(
                    &[
                        "run", "--detach", "--bundle", &bundle_arg, "--pid-file", &pid_file,
                        &name,
                    ],
                    &self.work_dir.join("runc.log"),
                )
                .await
                .context("runc run")?;
            }
            Some(checkpoint) => {
                let images = match checkpoint {
                    ContainerCheckpoint::Dir(dir) => dir,
                    ContainerCheckpoint::Archive(archive) => {
                        runtime::unpack_checkpoint(&archive).await?
                    }
                };
                let work = self.work_dir.join("criu-restore");
                tokio::fs::create_dir_all(&work).await?;
                let images_arg = images.display().to_string();
                let work_arg = work.display().to_string();
                let settings = ContainerSettings::global();
                let mut args = vec![
                    "restore",
                    "--detach",
                    "--bundle",
                    &bundle_arg,
                    "--image-path",
                    &images_arg,
                    "--work-path",
                    &work_arg,
                    "--pid-file",
                    &pid_file,
                ];
                args.extend(settings.restore_args.iter().map(String::as_str));
                args.push(&name);
                let restore_started = std::time::Instant::now();
                runtime::runc_detached(&args, &self.work_dir.join("runc.log"))
                    .await
                    .with_context(|| format!("runc restore (CRIU log: {work_arg}/restore.log)"))?;
                info!(
                    sandbox_id = %self.id,
                    restore_ms = restore_started.elapsed().as_millis() as u64,
                    "container restored from CRIU images"
                );
            }
        }
        self.container_id = Some(name);

        let base_url = format!(
            "http://{}:{}",
            interaction_ip, self.launch.common.control_plane_port
        );
        self.envd = Some(EnvdInstance::new(
            base_url,
            self.launch.common.envd_access_token.clone(),
        ));
        debug!(
            sandbox_id = %self.id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "container sandbox started"
        );
        Ok(())
    }

    async fn wait_ready_inner(&self) -> Result<()> {
        let envd = self.envd.as_ref().context("envd instance not initialized")?;
        let policy = &self.launch.common.runtime_policy;
        envd.wait_for_ready(policy.envd_timeout, policy.envd_poll_interval)
            .await?;
        envd.init(
            self.launch.common.env_vars.clone(),
            self.launch.common.default_workdir.clone(),
            self.launch.common.default_user.clone(),
        )
        .await
    }

    /// Release every host resource. Safe to call repeatedly.
    async fn release(&mut self) -> Result<()> {
        if let Some(envd) = self.envd.take() {
            envd.invalidate();
        }
        if let Some(name) = self.container_id.take() {
            // A paused (frozen) container must be thawed for SIGKILL to land.
            let _ = runtime::runc(&["resume", &name]).await;
            let _ = runtime::runc(&["kill", &name, "KILL"]).await;
            if let Err(error) = runtime::runc(&["delete", "--force", &name]).await {
                warn!(%error, container = %name, "runc delete failed");
            }
        }
        if self.rootfs_mounted {
            if let Err(error) = runtime::umount(&self.rootfs_mount()).await {
                warn!(%error, "failed to unmount container rootfs");
            }
            self.rootfs_mounted = false;
        }
        if let Some(rootfs) = self.rootfs.take() {
            if let Err(error) = UblkDeviceManager::global()
                .release_device(&rootfs.device)
                .await
            {
                warn!(%error, "failed to release container rootfs device");
            }
        }
        if let Some(guest_netns) = self.guest_netns.take() {
            let (slot_netns, link) = match &self.slot {
                Some(slot) => (Some(slot.namespace_path()), Some(slot.guest_link())),
                None => (None, None),
            };
            let (_, gateway_ip, prefix) =
                link.unwrap_or((std::net::Ipv4Addr::UNSPECIFIED, std::net::Ipv4Addr::UNSPECIFIED, 0));
            if let Err(error) =
                runtime::detach_guest_netns(slot_netns.as_deref(), &guest_netns, gateway_ip, prefix)
                    .await
            {
                warn!(%error, "failed to restore slot tap0; dropping the slot");
                // Do not hand a half-configured slot back to the warm pool.
                if let Some(slot) = self.slot.take() {
                    let _ = NetworkManager::global().cleanup_allocated_slot(slot, true);
                }
            }
        }
        if let Some(slot) = self.slot.take() {
            NetworkManager::global()
                .release(slot)
                .context("release network slot")?;
        }
        let _ = tokio::fs::remove_dir_all(&self.work_dir).await;
        Ok(())
    }

    fn capture_root(&mut self) -> Arc<DirGuard> {
        self.capture_root
            .get_or_insert_with(|| {
                DirGuard::new(
                    ContainerSettings::global()
                        .snapshot_root()
                        .join(self.id.to_string()),
                )
            })
            .clone()
    }

    /// Capture rootfs + CRIU images into `dir`. Tasks are left frozen.
    ///
    /// Order: freeze the cgroup, freeze the filesystem, seal the rootfs
    /// upper, thaw the filesystem, dump the frozen tasks. Rootfs and memory
    /// therefore describe the same instant.
    async fn capture_frozen(&mut self, dir: &Path, archive: bool) -> Result<ContainerLaunch> {
        let started = std::time::Instant::now();
        let name = self.container_id()?.to_string();
        tokio::fs::create_dir_all(dir).await?;
        runtime::runc(&["pause", &name]).await.context("runc pause")?;
        let rootfs_mount = self.rootfs_mount();
        runtime::fsfreeze(&rootfs_mount, true).await?;
        let rootfs = self.rootfs.as_ref().context("rootfs device missing")?;
        let restack = restack_snapshot_overlaybd_rootfs(
            &rootfs.device,
            false,
            &rootfs.image_config_path,
            dir,
        )
        .await;
        if let Err(error) = runtime::fsfreeze(&rootfs_mount, false).await {
            warn!(%error, "failed to thaw container rootfs");
        }
        let rootfs_image = restack.context("seal rootfs upper")?;
        let sealed_ms = started.elapsed().as_millis() as u64;

        let images = dir.join(CHECKPOINT_DIR);
        let work = dir.join("criu-work");
        tokio::fs::create_dir_all(&images).await?;
        tokio::fs::create_dir_all(&work).await?;
        let images_arg = images.display().to_string();
        let work_arg = work.display().to_string();
        let mut args = vec![
            "checkpoint",
            "--leave-running",
            "--image-path",
            &images_arg,
            "--work-path",
            &work_arg,
        ];
        args.extend(
            ContainerSettings::global()
                .checkpoint_args
                .iter()
                .map(String::as_str),
        );
        args.push(&name);
        runtime::runc(&args)
            .await
            .with_context(|| format!("runc checkpoint (CRIU log: {work_arg}/dump.log)"))?;
        let dumped_ms = started.elapsed().as_millis() as u64;

        let checkpoint = if archive {
            let archive_path = dir.join(CHECKPOINT_ARCHIVE);
            runtime::pack_checkpoint(&images, &archive_path).await?;
            // The archive is what gets published; drop the duplicate images.
            let _ = tokio::fs::remove_dir_all(&images).await;
            ContainerCheckpoint::Archive(archive_path)
        } else {
            ContainerCheckpoint::Dir(images)
        };
        info!(
            sandbox_id = %self.id,
            sealed_ms,
            dumped_ms,
            total_ms = started.elapsed().as_millis() as u64,
            "container capture finished"
        );

        let mut launch = self.launch.clone();
        launch.common.rootfs_image_config = Some(OverlaybdConfig {
            image_config_path: rootfs_image,
            read_only: false,
            runtime_upper_mode: self
                .launch
                .common
                .rootfs_image_config
                .as_ref()
                .map(|config| config.runtime_upper_mode)
                .unwrap_or(overlaybd::config::UpperMode::LogStructured),
        });
        launch.common.network_policy = self.current_network_policy.clone();
        launch.common.custom_extension_params = self.current_custom_extension_params.clone();
        launch.common.envd_access_token = None;
        launch.checkpoint = Some(checkpoint);
        Ok(launch)
    }

    async fn thaw(&self) -> Result<()> {
        runtime::runc(&["resume", self.container_id()?])
            .await
            .context("runc resume")
            .map(drop)
    }

    /// Recover a failed capture: the tasks may still be frozen.
    async fn recover<T>(&self, what: &str, error: anyhow::Error) -> SandboxCaptureResult<T> {
        warn!(error = %format_args!("{error:#}"), what, "container capture failed");
        if let Err(thaw_error) = self.thaw().await {
            return Err(SandboxCaptureError::terminal(
                error.context(format!("{what}; thaw also failed: {thaw_error:#}")),
            ));
        }
        Err(SandboxCaptureError::recoverable(error))
    }

    /// The manifest a published container snapshot carries.
    async fn manifest_for(dir: &Path, launch: &ContainerLaunch) -> Result<SandboxSnapshotManifest> {
        // Records require a memory image. Containers keep memory in the CRIU
        // archive, so publish an empty 4 KiB layer.
        let (layer, size) = convert_dirty_memory_to_overlaybd(
            nix::unistd::Pid::this(),
            &DirtyMemoryRanges {
                page_size: 4096,
                memory_size: 4096,
                ranges: Vec::new(),
            },
            &dir.join("mem_overlaybd"),
            OverlaybdCompactOutput::Raw,
        )
        .await
        .context("write placeholder memory layer")?;
        let mem_config =
            build_mem_snapshot_image_config(None, &layer, dir, OverlaybdCompactOutput::Raw)
                .await?;
        let mem_config_path = dir.join("mem_image.json");
        tokio::fs::write(&mem_config_path, serde_json::to_vec_pretty(&mem_config)?).await?;
        let Some(ContainerCheckpoint::Archive(archive)) = &launch.checkpoint else {
            bail!("a published container snapshot needs a checkpoint archive");
        };
        let rootfs = launch
            .common
            .rootfs_image_config
            .as_ref()
            .context("captured rootfs missing")?;
        SandboxSnapshotManifest::new(
            CONTAINER_BACKEND,
            archive.clone(),
            mem_config_path,
            size,
            rootfs.image_config_path.clone(),
            launch
                .common
                .rootfs_virtual_size
                .context("rootfs virtual size unknown")?,
            &[],
        )
    }
}

#[async_trait]
impl SandboxBackend for ContainerSandbox {
    fn envd(&self) -> Option<Arc<dyn crate::sandbox::EnvdApi>> {
        let envd = self.envd.clone()?;
        Some(Arc::new(envd))
    }

    async fn start(&mut self) -> Result<()> {
        self.start_nowait().await?;
        self.wait_for_ready().await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        let result = self.start_inner().await;
        if let Err(error) = &result {
            warn!(error = %format_args!("{error:#}"), sandbox_id = %self.id, "container start failed");
            let _ = self.release().await;
        }
        result
    }

    async fn wait_for_ready(&self) -> Result<()> {
        self.wait_ready_inner().await
    }

    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
    ) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        let (dir, root) = match artifact_root {
            Some(root) => (root.join(Uuid::now_v7().to_string()), None),
            None => {
                let root = self.capture_root();
                (root.path().join(Uuid::now_v7().to_string()), Some(root))
            }
        };
        match self.capture_frozen(&dir, false).await {
            // Tasks stay frozen; stop() (or resume()) follows.
            Ok(launch) => Ok(Arc::new(ContainerPausedState::new(launch, root))),
            Err(error) => self.recover("pause", error).await,
        }
    }

    async fn resume(&mut self) -> Result<()> {
        if self.container_id.is_some() {
            self.thaw().await?;
        }
        Ok(())
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let root = self.capture_root();
        let dir = root.path().join(Uuid::now_v7().to_string());
        let launch = match self.capture_frozen(&dir, true).await {
            Ok(launch) => launch,
            Err(error) => return self.recover("snapshot", error).await,
        };
        self.thaw().await.map_err(SandboxCaptureError::terminal)?;
        let manifest = Self::manifest_for(&dir, &launch)
            .await
            .map_err(SandboxCaptureError::recoverable)?;
        Ok(CapturedSandboxSnapshot::new(manifest, root))
    }

    async fn snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        Ok(())
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        let root = self.capture_root();
        let dir = root.path().join(Uuid::now_v7().to_string());
        let launch = match self.capture_frozen(&dir, false).await {
            Ok(launch) => launch,
            Err(error) => return self.recover("fork", error).await,
        };
        self.thaw().await.map_err(SandboxCaptureError::terminal)?;

        let children = spec.iter().map(|child| {
            let launch = launch.clone();
            let root = root.clone();
            async move {
                anyhow::ensure!(
                    child.extra_drives.is_empty() && child.replace_drive_ids.is_empty(),
                    "the container backend does not support drives on fork"
                );
                let mut sandbox = ContainerSandbox::new(child.sandbox_id, launch)?
                    .with_inherited_root(Some(root))
                    .with_access_token(child.envd_access_token.clone());
                match sandbox.start().await {
                    Ok(()) => Ok(Box::new(sandbox) as Box<dyn SandboxBackend>),
                    Err(error) => {
                        let _ = sandbox.stop().await;
                        Err(error.context("start forked container"))
                    }
                }
            }
        });
        Ok(futures::future::join_all(children).await)
    }

    async fn capture_to_dir(
        &mut self,
        at: &Path,
    ) -> SandboxCaptureResult<(SandboxSnapshotManifest, Option<Box<dyn std::any::Any + Send>>)>
    {
        let launch = match self.capture_frozen(at, true).await {
            Ok(launch) => launch,
            Err(error) => return self.recover("capture", error).await,
        };
        let manifest = Self::manifest_for(at, &launch)
            .await
            .map_err(SandboxCaptureError::recoverable)?;
        Ok((manifest, Some(Box::new(launch))))
    }

    async fn stop(&mut self) -> Result<()> {
        self.release().await
    }

    async fn freeze_and_snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        Ok(())
    }

    async fn thaw_volumes(&mut self) -> Result<()> {
        Ok(())
    }

    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        self.slot.as_ref().map(|slot| slot.host_interaction_ip)
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        SandboxRuntimeInfo {
            rootfs_virtual_size: self.launch.common.rootfs_virtual_size,
            runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(
                self.rootfs
                    .iter()
                    .map(|rootfs| rootfs.image_config_path.clone())
                    .collect(),
            ),
        }
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(
            self.launch
                .common
                .rootfs_image_config
                .iter()
                .map(|config| config.image_config_path.clone())
                .collect(),
        )
    }

    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        match self.slot.as_mut() {
            Some(slot) => {
                slot.set_egress_policy(policy.as_ref())
                    .context("configure sandbox network policy")?;
                self.current_network_policy = policy;
                Ok(())
            }
            None if policy.is_none() => {
                self.current_network_policy = None;
                Ok(())
            }
            None => bail!("sandbox has no active network slot"),
        }
    }

    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>) {
        self.current_custom_extension_params = params;
    }
}

impl Drop for ContainerSandbox {
    fn drop(&mut self) {
        if self.container_id.is_some() || self.slot.is_some() {
            warn!(sandbox_id = %self.id, "container sandbox dropped without stop()");
        }
    }
}
