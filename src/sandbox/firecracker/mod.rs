//! Firecracker-based sandbox backend implementation.
//!
//! Provides [`FirecrackerSandbox`] (the concrete VM-backed sandbox) and
//! [`FirecrackerSandboxFactory`] which wires sandbox configuration from the
//! global [`ConfigManager`][crate::cfg::ConfigManager].

mod config;
mod connector;
mod factory;
mod instance;
mod mmds;
mod overlaybd_snapshot;
mod pool;
mod process_vm_reader;
mod resources;
mod sandbox;
mod socket;
mod startup_pack;

pub use config::{
    FirecrackerCommonConfig, FirecrackerRuntimePolicy, FirecrackerSandboxConfig,
    FirecrackerSnapshotConfig,
};
pub use factory::FirecrackerSandboxFactory;
pub(super) use instance::FirecrackerInstance;
pub use pool::FirecrackerPool;
pub use sandbox::{FirecrackerCaptureArtifacts, FirecrackerPausedState, FirecrackerSandbox};
pub(crate) use sandbox::managed_snapshot_base;
pub(crate) use startup_pack::record_startup_pack;
// Shared with the container backend, which keeps the same rootfs storage path.
pub(crate) use overlaybd_snapshot::{
    build_mem_snapshot_image_config, convert_dirty_memory_to_overlaybd,
    restack_snapshot_overlaybd_rootfs,
};
