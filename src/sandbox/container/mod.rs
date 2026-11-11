//! OCI container sandbox backend: `runc` for isolation, CRIU for memory
//! snapshots, and the node's existing OverlayBD/ublk rootfs, network slots and
//! envd agent for everything else. See [`sandbox`] for the mapping.

mod factory;
mod runtime;
mod sandbox;

pub use factory::{fresh_backend_is_container, ContainerSandboxFactory};
pub use sandbox::{ContainerCheckpoint, ContainerLaunch, ContainerPausedState, ContainerSandbox};
