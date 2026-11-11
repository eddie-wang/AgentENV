//! OCI container sandbox backend: `runc` for isolation, CRIU for memory
//! snapshots, and the node's existing OverlayBD/ublk rootfs, network slots and
//! envd agent for everything else. See [`sandbox`] for the mapping.

mod factory;
mod runtime;
mod sandbox;

pub use factory::{
    container_enabled, container_enabled_from_env, default_fresh_backend,
    ensure_container_enabled, ensure_microvm_available, fresh_backend_is_container,
    set_container_enabled, ContainerSandboxFactory, SandboxBackendKind,
};
pub use sandbox::{ContainerCheckpoint, ContainerLaunch, ContainerPausedState, ContainerSandbox};

/// Release node-wide container backend resources (tools-drive mounts).
pub async fn shutdown_container_backend() {
    runtime::shutdown().await;
}
