//! Factory for [`ContainerSandbox`] and the node-wide backend choice.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use serde_json::Value;

use super::sandbox::{ContainerCheckpoint, ContainerLaunch, ContainerPausedState, ContainerSandbox};
use crate::sandbox::backend::{PausedSandboxState, SandboxBackend, SandboxBackendFactory};
use crate::sandbox::{
    EnvdAccessToken, FirecrackerSandboxConfig, FirecrackerSnapshotConfig, FreshSandboxBuildSpec,
    OverlaybdConfig, SandboxLaunchConfig, CONTAINER_BACKEND,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::SandboxId;

/// Backend used for sandboxes created from an image on this node
/// (`AENV_SANDBOX_BACKEND=container|firecracker`, default firecracker).
/// Snapshots and paused sandboxes always go back to the backend that took them.
pub fn fresh_backend_is_container() -> bool {
    std::env::var("AENV_SANDBOX_BACKEND")
        .map(|value| value.trim().eq_ignore_ascii_case(CONTAINER_BACKEND))
        .unwrap_or(false)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ContainerSandboxFactory;

impl ContainerSandboxFactory {
    pub fn new() -> Self {
        Self
    }

    pub fn handles_snapshot(snapshot: &RunnableSnapshot) -> bool {
        snapshot.manifest().backend == CONTAINER_BACKEND
    }

    pub fn handles_encoded_paused_state(state: &Value) -> bool {
        ContainerPausedState::is_encoded(state)
    }

    pub fn handles_paused_state(state: &dyn PausedSandboxState) -> bool {
        state.downcast_ref::<ContainerPausedState>().is_some()
    }
}

impl SandboxBackendFactory for ContainerSandboxFactory {
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        ensure!(
            build_spec.extra_drives.is_empty() && launch_config.extra_drives.is_empty(),
            "the container backend does not support attached drives or volumes yet"
        );
        let user_image_config = OverlaybdConfig {
            image_config_path: build_spec.image_config_path,
            read_only: false,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        };
        let mut config =
            FirecrackerSandboxConfig::from_global_config_with_user_image(user_image_config)?;
        config.common.rootfs_virtual_size = if build_spec.resources.disk_size_mib == 0 {
            None
        } else {
            Some(
                u64::from(build_spec.resources.disk_size_mib)
                    .checked_mul(1024 * 1024)
                    .context("requested rootfs size overflows bytes")?,
            )
        };
        config.common.env_vars = (!build_spec.context.env_vars.is_empty())
            .then_some(build_spec.context.env_vars.clone());
        config.common.default_workdir = Some(build_spec.context.workdir.clone());
        config.common.default_user = build_spec.context.user.clone();
        let config = config.apply_launch_config(&launch_config);
        let launch = ContainerLaunch {
            common: config.common,
            cpu_count: build_spec.resources.cpu_count,
            memory_mib: build_spec.resources.memory_mib,
            checkpoint: None,
        };
        Ok(Box::new(ContainerSandbox::new(
            launch_config.sandbox_id,
            launch,
        )?))
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
    ) -> Result<Box<dyn SandboxBackend>> {
        ensure!(
            launch_config.extra_drives.is_empty(),
            "the container backend does not support launch-time volumes yet"
        );
        let snapshot_config =
            FirecrackerSnapshotConfig::from_runnable_snapshot_for_backend(snapshot, CONTAINER_BACKEND)?;
        let mut common = snapshot_config.common;
        common.envd_access_token = launch_config.envd_access_token.clone();
        common.network_policy = launch_config.network.clone();
        common.custom_extension_params = launch_config
            .custom_extension_params
            .clone()
            .or_else(|| snapshot.committed().custom_extension_params.clone());
        if let Some(env_vars) = &launch_config.env_vars {
            common
                .env_vars
                .get_or_insert_default()
                .extend(env_vars.clone());
        }
        let resources = *snapshot.resources();
        let launch = ContainerLaunch {
            common,
            cpu_count: resources.cpu_count,
            memory_mib: resources.memory_mib,
            checkpoint: Some(ContainerCheckpoint::Archive(snapshot_config.vm_state_path)),
        };
        Ok(Box::new(ContainerSandbox::new(
            launch_config.sandbox_id,
            launch,
        )?))
    }

    fn decode_paused_state(
        &self,
        _artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>> {
        Ok(Arc::new(ContainerPausedState::decode(state)?))
    }

    fn build_from_paused_state(
        &self,
        sandbox_id: SandboxId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>> {
        let paused = state
            .downcast_ref::<ContainerPausedState>()
            .context("the provided PausedSandboxState is not a container paused state")?;
        let sandbox = ContainerSandbox::new(sandbox_id, paused.launch().clone())?
            .with_access_token(envd_access_token);
        Ok(Box::new(sandbox))
    }
}
