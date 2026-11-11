//! Node-local bookkeeping for read-only volume mounts.
//!
//! The shared catalog records one `node-<uuid>` lease per (node, read-only
//! volume) instead of one entry per sandbox, so mounting and releasing a
//! read-only volume on a node that already holds its lease touches no catalog
//! document. The per-sandbox owners live in a node-local ledger that survives
//! a server restart. The node lease keeps cross-node deletes safe: a volume is
//! deletable only after every node has dropped its lease, which happens once
//! the node has had no local owner for the configured idle period.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{is_valid_volume_component, repository_error, VolumeCatalog, VolumeError};
use crate::repository::Repository;

const NODE_OWNER_FILE: &str = "node-owner";
const NODE_OWNER_PREFIX: &str = "node-";
const LEDGER_ENTRY_EXTENSION: &str = "json";

#[derive(Default)]
struct NodeLease {
    /// The catalog lists this node in the volume's `read_only_mounts`.
    held: bool,
    /// Local sandboxes (or pending creates) mounting the volume.
    owners: BTreeSet<String>,
    /// Bumped by every acquire and idle transition, cancelling older idle timers.
    idle_generation: u64,
}

#[derive(Deserialize, Serialize)]
struct LedgerEntry {
    owners: BTreeSet<String>,
}

pub(super) struct ReadOnlyMountLedger {
    directory: PathBuf,
    node_owner: String,
    repository: Arc<dyn Repository>,
    idle_release: Duration,
    leases: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<NodeLease>>>>,
}

impl ReadOnlyMountLedger {
    /// Loads the ledger left by a previous server run. Every persisted entry
    /// is a node lease that may still be listed in the catalog.
    pub(super) fn open(
        directory: PathBuf,
        repository: Arc<dyn Repository>,
        idle_release: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        use anyhow::Context;

        std::fs::create_dir_all(&directory)
            .with_context(|| format!("create read-only mount ledger {}", directory.display()))?;
        let node_owner = load_or_create_node_owner(&directory.join(NODE_OWNER_FILE))?;
        let mut leases = HashMap::new();
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("read read-only mount ledger {}", directory.display()))?
        {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str())
                != Some(LEDGER_ENTRY_EXTENSION)
            {
                continue;
            }
            let Some(volume_id) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|stem| is_valid_volume_component(stem))
                .map(str::to_owned)
            else {
                continue;
            };
            let bytes = std::fs::read(&path)
                .with_context(|| format!("read read-only mount entry {}", path.display()))?;
            let entry: LedgerEntry = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse read-only mount entry {}", path.display()))?;
            leases.insert(
                volume_id,
                Arc::new(tokio::sync::Mutex::new(NodeLease {
                    held: true,
                    owners: entry.owners,
                    idle_generation: 0,
                })),
            );
        }
        if !leases.is_empty() {
            info!(
                node_owner = %node_owner,
                volume_count = leases.len(),
                "loaded node-local read-only volume mounts"
            );
        }
        Ok(Arc::new(Self {
            directory,
            node_owner,
            repository,
            idle_release,
            leases: std::sync::Mutex::new(leases),
        }))
    }

    pub(super) fn node_owner(&self) -> &str {
        &self.node_owner
    }

    fn lease(&self, volume_id: &str) -> Arc<tokio::sync::Mutex<NodeLease>> {
        Arc::clone(
            self.leases
                .lock()
                .unwrap()
                .entry(volume_id.to_owned())
                .or_default(),
        )
    }

    fn existing_lease(&self, volume_id: &str) -> Option<Arc<tokio::sync::Mutex<NodeLease>>> {
        self.leases.lock().unwrap().get(volume_id).cloned()
    }

    /// Whether the catalog lists this node as mounting `volume_id`. While it
    /// does, the volume cannot be deleted and its read-only record cannot change.
    pub(super) async fn holds(&self, volume_id: &str) -> bool {
        match self.existing_lease(volume_id) {
            Some(lease) => lease.lock().await.held,
            None => false,
        }
    }

    /// Adds a local owner. Only the node's first owner writes the catalog;
    /// concurrent first owners wait on the per-volume lock and reuse the lease.
    pub(super) async fn acquire(
        self: &Arc<Self>,
        volume_id: &str,
        owner: &str,
    ) -> Result<(), VolumeError> {
        let lease = self.lease(volume_id);
        let mut lease = lease.lock().await;
        if !lease.held {
            // Record the intent first: a crash after the catalog write must
            // still find this lease on restart.
            self.persist(volume_id, &lease.owners)?;
            if let Err(error) = self
                .repository
                .reserve_read_only_volume(volume_id, &self.node_owner)
                .await
            {
                if lease.owners.is_empty() {
                    self.forget(volume_id);
                }
                return Err(repository_error(error));
            }
            lease.held = true;
            debug!(volume_id, node_owner = %self.node_owner, "node read-only volume lease acquired");
        }
        lease.idle_generation += 1;
        if !lease.owners.contains(owner) {
            let mut owners = lease.owners.clone();
            owners.insert(owner.to_owned());
            if let Err(error) = self.persist(volume_id, &owners) {
                if lease.owners.is_empty() {
                    self.schedule_idle_release(volume_id, &mut lease).await;
                }
                return Err(error);
            }
            lease.owners = owners;
        }
        Ok(())
    }

    /// Releases or renames a local owner without touching the catalog.
    /// Returns `false` when this node does not track `owner` for the volume,
    /// so the caller falls back to the catalog (exclusive volumes and
    /// per-sandbox entries written by earlier releases).
    pub(super) async fn replace_owner(
        self: &Arc<Self>,
        volume_id: &str,
        owner: &str,
        new_owner: Option<&str>,
    ) -> Result<bool, VolumeError> {
        let Some(lease) = self.existing_lease(volume_id) else {
            return Ok(false);
        };
        let mut lease = lease.lock().await;
        if !lease.owners.contains(owner) {
            return Ok(false);
        }
        let mut owners = lease.owners.clone();
        owners.remove(owner);
        if let Some(new_owner) = new_owner {
            owners.insert(new_owner.to_owned());
        }
        self.persist(volume_id, &owners)?;
        lease.owners = owners;
        if lease.owners.is_empty() {
            self.schedule_idle_release(volume_id, &mut lease).await;
        }
        Ok(true)
    }

    /// Drops this node's idle lease so a local delete can proceed at once.
    pub(super) async fn release_for_delete(&self, volume_id: &str) -> Result<(), VolumeError> {
        let Some(lease) = self.existing_lease(volume_id) else {
            return Ok(());
        };
        let mut lease = lease.lock().await;
        if let Some(owner) = lease.owners.first() {
            return Err(VolumeError::Reserved(owner.clone()));
        }
        if lease.held {
            self.release_node_lease(volume_id, &mut lease).await?;
        }
        Ok(())
    }

    /// Drops owners that did not survive a restart and starts idle release
    /// for leases left without owners. Runs before the server accepts creates.
    pub(super) async fn reconcile(self: &Arc<Self>, is_live: impl Fn(&str, &str) -> bool) {
        let leases = self
            .leases
            .lock()
            .unwrap()
            .iter()
            .map(|(volume_id, lease)| (volume_id.clone(), Arc::clone(lease)))
            .collect::<Vec<_>>();
        for (volume_id, lease) in leases {
            let mut lease = lease.lock().await;
            let owners = lease
                .owners
                .iter()
                .filter(|owner| is_live(owner, &volume_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            if owners != lease.owners {
                let dropped = lease.owners.len() - owners.len();
                match self.persist(&volume_id, &owners) {
                    Ok(()) => {
                        info!(volume_id = %volume_id, dropped, "dropped stale read-only volume mounts after restart");
                        lease.owners = owners;
                    }
                    Err(error) => {
                        warn!(volume_id = %volume_id, %error, "failed to persist reconciled read-only volume mounts");
                        lease.owners = owners;
                    }
                }
            }
            if lease.held && lease.owners.is_empty() {
                self.schedule_idle_release(&volume_id, &mut lease).await;
            }
        }
    }

    async fn schedule_idle_release(self: &Arc<Self>, volume_id: &str, lease: &mut NodeLease) {
        if !lease.held {
            self.forget(volume_id);
            return;
        }
        lease.idle_generation += 1;
        if self.idle_release.is_zero() {
            if let Err(error) = self.release_node_lease(volume_id, lease).await {
                warn!(volume_id, %error, "failed to release idle node read-only volume lease");
            }
            return;
        }
        let generation = lease.idle_generation;
        let ledger = Arc::clone(self);
        let volume_id = volume_id.to_owned();
        let Some(lease) = self.existing_lease(&volume_id) else {
            return;
        };
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(ledger.idle_release).await;
                let mut lease = lease.lock().await;
                if !lease.held || !lease.owners.is_empty() || lease.idle_generation != generation
                {
                    return;
                }
                match ledger.release_node_lease(&volume_id, &mut lease).await {
                    Ok(()) => return,
                    Err(error) => {
                        warn!(volume_id = %volume_id, %error, "failed to release idle node read-only volume lease; retrying");
                    }
                }
            }
        });
    }

    async fn release_node_lease(
        &self,
        volume_id: &str,
        lease: &mut NodeLease,
    ) -> Result<(), VolumeError> {
        self.repository
            .replace_volume_owner_for(volume_id, &self.node_owner, None)
            .await
            .map_err(repository_error)?;
        lease.held = false;
        lease.idle_generation += 1;
        self.forget(volume_id);
        debug!(volume_id, node_owner = %self.node_owner, "node read-only volume lease released");
        Ok(())
    }

    /// Removes the persisted entry; the in-memory lease stays so a waiter
    /// holding its lock re-acquires through the catalog.
    fn forget(&self, volume_id: &str) {
        let path = self.entry_path(volume_id);
        if let Err(error) = std::fs::remove_file(&path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %path.display(), %error, "failed to remove read-only mount entry");
            }
        }
    }

    fn entry_path(&self, volume_id: &str) -> PathBuf {
        self.directory
            .join(format!("{volume_id}.{LEDGER_ENTRY_EXTENSION}"))
    }

    fn persist(&self, volume_id: &str, owners: &BTreeSet<String>) -> Result<(), VolumeError> {
        let bytes = serde_json::to_vec(&LedgerEntry {
            owners: owners.clone(),
        })
        .map_err(|error| VolumeError::Storage(error.to_string()))?;
        write_atomically(&self.entry_path(volume_id), &bytes)
            .map_err(|error| VolumeError::Storage(format!("persist read-only mounts: {error}")))
    }
}

fn load_or_create_node_owner(path: &Path) -> anyhow::Result<String> {
    use anyhow::Context;

    match std::fs::read_to_string(path) {
        Ok(owner) => {
            let owner = owner.trim().to_owned();
            anyhow::ensure!(
                owner.starts_with(NODE_OWNER_PREFIX) && is_valid_volume_component(&owner),
                "invalid node owner in {}",
                path.display()
            );
            Ok(owner)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let owner = format!("{NODE_OWNER_PREFIX}{}", Uuid::now_v7().simple());
            write_atomically(path, owner.as_bytes())
                .with_context(|| format!("write node owner {}", path.display()))?;
            Ok(owner)
        }
        Err(error) => Err(error).with_context(|| format!("read node owner {}", path.display())),
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension(format!("tmp-{}", Uuid::now_v7().simple()));
    if let Err(error) = std::fs::write(&temporary, bytes) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    std::fs::rename(&temporary, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&temporary);
    })
}
