use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use overlaybd::config::{ImageConfig, LayerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::repository::{Repository, RepositoryError};
use crate::snapshot::OverlaybdLayerRef;
use read_only_mounts::ReadOnlyMountLedger;

mod catalog;
mod read_only_mounts;
pub use crate::types::VolumeMode;
pub use catalog::{VolumeCatalog, VolumeRecordPage};

pub const DEFAULT_VOLUME_SIZE_MB: u64 = 64 * 1024;
pub const DEFAULT_VOLUME_PAGE_SIZE: usize = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

pub(crate) use crate::repository::is_valid_volume_component;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumePage {
    pub records: Vec<VolumeRecord>,
    pub next_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeStatus {
    #[default]
    Ready,
    Uploading,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRecord {
    pub id: String,
    pub name: String,
    pub mode: VolumeMode,
    pub size_mb: u64,
    pub status: VolumeStatus,
    pub reserved_by_sandbox_id: Option<String>,
    #[serde(skip)]
    pub backing_image_config: Option<PathBuf>,
    /// Repository-owned layers; the image config path is only a node-local cache.
    pub backing_layers: Vec<OverlaybdLayerRef>,
    pub read_only_mounts: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) deleting: bool,
}

impl VolumeRecord {
    pub(crate) fn mounted_by(&self, owner: &str) -> bool {
        self.reserved_by_sandbox_id.as_deref() == Some(owner)
            || self.read_only_mounts.iter().any(|entry| entry == owner)
    }

    pub(crate) fn replace_owner(&mut self, from: &str, to: Option<&str>) -> bool {
        let mut changed = false;
        if self.reserved_by_sandbox_id.as_deref() == Some(from) {
            self.reserved_by_sandbox_id = to.map(str::to_owned);
            changed = true;
        }
        if self.read_only_mounts.iter().any(|owner| owner == from) {
            self.read_only_mounts.retain(|owner| owner != from);
            if let Some(to) = to {
                if !self.read_only_mounts.iter().any(|owner| owner == to) {
                    self.read_only_mounts.push(to.to_owned());
                }
            }
            changed = true;
        }
        changed
    }

    pub(crate) fn validate_catalog_update(&self, next: &Self) -> Result<(), String> {
        if self.deleting {
            return Err(format!("volume '{}' is being deleted", self.id));
        }
        if self.name != next.name || self.mode != next.mode || self.size_mb != next.size_mb {
            return Err("volume identity fields cannot be changed".to_owned());
        }
        if self.backing_layers != next.backing_layers && self.status != VolumeStatus::Uploading {
            return Err(format!(
                "volume '{}' must enter uploading state before replacing its backing",
                self.id
            ));
        }
        if self.status == VolumeStatus::Failed && next.status == VolumeStatus::Ready {
            return Err(format!(
                "volume '{}' must enter uploading state before becoming ready",
                self.id
            ));
        }
        Ok(())
    }
}

fn current_local_backing(
    remote: &VolumeRecord,
    local: &HashMap<String, VolumeRecord>,
) -> Option<PathBuf> {
    local
        .get(&remote.id)
        .filter(|record| record.backing_layers == remote.backing_layers)
        .and_then(|record| record.backing_image_config.clone())
        .filter(|path| path.exists())
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum VolumeError {
    #[error("volume name must contain only letters, numbers, underscores, or hyphens")]
    InvalidName,
    #[error("volume name already exists: {0}")]
    NameConflict(String),
    #[error("volume not found: {0}")]
    NotFound(String),
    #[error("volume is reserved by sandbox {0}")]
    Reserved(String),
    #[error("volume is uploading and not usable: {0}")]
    Uploading(String),
    #[error("volume publication failed and is not usable: {0}")]
    Failed(String),
    #[error("fromVolume and image cannot be used together")]
    MultipleSources,
    #[error("volume size must be greater than zero")]
    InvalidSize,
    #[error("volume size exceeds the configured maximum of {max_size_mb} MiB")]
    SizeLimitExceeded { max_size_mb: u64 },
    #[error("volume child size must match its source size")]
    SizeMismatch,
    #[error("sandbox cannot mount more than {max_count} volumes")]
    TooManyMountedVolumes { max_count: usize },
    #[error("source volume not found: {0}")]
    SourceNotFound(String),
    #[error("invalid volume next token")]
    InvalidNextToken,
    #[error("volume page limit must be greater than zero")]
    InvalidPageLimit,
    #[error("volume catalog storage failed: {0}")]
    Storage(String),
}

#[derive(Clone)]
pub struct VolumeManager {
    records: Arc<RwLock<HashMap<String, VolumeRecord>>>,
    root: PathBuf,
    repository: Arc<dyn Repository>,
    limits: VolumeLimits,
    read_only_mounts: Arc<ReadOnlyMountLedger>,
    /// One in-flight get-or-create per snapshot read-only volume name.
    snapshot_volume_gates: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct VolumeLimits {
    pub max_size_mb: u64,
    pub max_mounts: usize,
    /// Seconds a node keeps its catalog lease on a read-only volume after its
    /// last local mount is released. `0` releases it immediately.
    pub read_only_node_lease_idle_secs: u64,
}

impl Default for VolumeLimits {
    fn default() -> Self {
        let config = crate::cfg::VolumeConfig::default();
        Self {
            max_size_mb: config.max_size_mb,
            max_mounts: config.max_volume_count,
            read_only_node_lease_idle_secs: config.read_only_node_lease_idle_secs,
        }
    }
}

impl VolumeManager {
    pub async fn open_with_repository(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn Repository>,
    ) -> anyhow::Result<Self> {
        Self::open_with_repository_and_limits(path, repository, VolumeLimits::default()).await
    }

    pub async fn open_with_repository_and_limits(
        path: impl Into<std::path::PathBuf>,
        repository: Arc<dyn Repository>,
        mut limits: VolumeLimits,
    ) -> anyhow::Result<Self> {
        let path = path.into();
        let root = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("volume catalog path has no parent"))?
            .to_path_buf();
        limits.max_mounts = limits.max_mounts.min(crate::cfg::MAX_VOLUME_MOUNTS);
        let read_only_mounts = ReadOnlyMountLedger::open(
            root.join("read-only-mounts"),
            Arc::clone(&repository),
            std::time::Duration::from_secs(limits.read_only_node_lease_idle_secs),
        )?;
        Ok(Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            root,
            repository,
            limits,
            read_only_mounts,
            snapshot_volume_gates: Arc::default(),
        })
    }

    pub fn limits(&self) -> VolumeLimits {
        self.limits
    }

    pub async fn get(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let mut record = self
            .repository
            .get_volume(reference)
            .await
            .map_err(repository_error)?
            .ok_or_else(|| VolumeError::NotFound(reference.to_owned()))?;
        validate_volume_id(&record.id)?;
        let local = self.records.read().await;
        record.backing_image_config = current_local_backing(&record, &local);
        Ok(record)
    }

    pub async fn list_page(
        &self,
        next_token: Option<&str>,
        limit: usize,
    ) -> Result<VolumePage, VolumeError> {
        if limit == 0 {
            return Err(VolumeError::InvalidPageLimit);
        }
        let after_volume_id = next_token.map(decode_volume_cursor).transpose()?;
        let mut page = self
            .repository
            .list_volumes_page(after_volume_id.as_deref(), limit)
            .await
            .map_err(repository_error)?;
        let local = self.records.read().await;
        for record in &mut page.records {
            validate_volume_id(&record.id)?;
            record.backing_image_config = current_local_backing(record, &local);
        }
        Ok(VolumePage {
            records: page.records,
            next_token: page.next_volume_id.map(encode_volume_cursor),
        })
    }

    pub fn data_dir(&self, volume_id: &str) -> PathBuf {
        self.root.join("data").join(volume_id)
    }

    pub async fn materialize_backing(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let mut record = self.get_for_mount(reference).await?;
        if record
            .backing_image_config
            .as_ref()
            .is_some_and(|path| path.exists())
            || record.backing_layers.is_empty()
        {
            return Ok(record);
        }
        let destination = self.data_dir(&record.id).join("image.json");
        debug!(
            volume_id = %record.id,
            layer_count = record.backing_layers.len(),
            "materializing volume backing"
        );
        let path = crate::repository::materialize_volume_backing(
            self.repository.as_ref(),
            &record.backing_layers,
            &destination,
        )
        .await
        .map_err(repository_error)?;
        debug!(
            volume_id = %record.id,
            path = %path.display(),
            "volume backing materialized"
        );
        record.backing_image_config = Some(path);
        self.cache_record(record.clone()).await;
        Ok(record)
    }

    pub async fn create(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<String>,
        source_config: Option<PathBuf>,
        size_mb: u64,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            mode,
            from_volume.map(|reference| (reference, None)),
            source_config,
            size_mb,
            None,
        )
        .await
    }

    /// Creates a private writable child without publishing its unchanged backing first.
    /// The caller holds a read lease on the immutable seed until this child is released.
    pub(crate) async fn create_build_cache(
        &self,
        name: String,
        seed: Option<String>,
        size_mb: u64,
        owner: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            VolumeMode::Exclusive,
            seed.map(|id| (id, None)),
            None,
            size_mb,
            Some(owner),
        )
        .await
    }

    async fn create_with_source_owner(
        &self,
        name: String,
        mode: VolumeMode,
        from_volume: Option<(String, Option<&str>)>,
        source_config: Option<PathBuf>,
        size_mb: u64,
        reserved_owner: Option<&str>,
    ) -> Result<VolumeRecord, VolumeError> {
        validate_name(&name)?;
        if size_mb == 0 {
            return Err(VolumeError::InvalidSize);
        }
        if size_mb > self.limits.max_size_mb {
            return Err(VolumeError::SizeLimitExceeded {
                max_size_mb: self.limits.max_size_mb,
            });
        }
        if from_volume.is_some() && source_config.is_some() {
            return Err(VolumeError::MultipleSources);
        }
        match self.get(&name).await {
            Ok(_) => return Err(VolumeError::NameConflict(name)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let id = format!("vol_{}", Uuid::now_v7().simple());
        debug!(
            volume_id = %id,
            mode = ?mode,
            size_mb,
            "creating volume"
        );
        let mut backing_layers = Vec::new();
        let backing_image_config = if let Some((reference, source_owner)) = from_volume {
            let mut parent = match self.get(&reference).await {
                Ok(parent) => parent,
                Err(VolumeError::NotFound(_)) => {
                    return Err(VolumeError::SourceNotFound(reference))
                }
                Err(error) => return Err(error),
            };
            match parent.status {
                VolumeStatus::Uploading => return Err(VolumeError::Uploading(parent.id.clone())),
                VolumeStatus::Failed => return Err(VolumeError::Failed(parent.id.clone())),
                VolumeStatus::Ready => {}
            }
            if let Some(owner) = parent.reserved_by_sandbox_id.as_deref() {
                if source_owner != Some(owner) {
                    return Err(VolumeError::Reserved(owner.to_owned()));
                }
            }
            if parent.size_mb != size_mb {
                return Err(VolumeError::SizeMismatch);
            }
            if parent.reserved_by_sandbox_id.is_none() && !parent.backing_layers.is_empty() {
                // Committed layers are immutable and repository-owned. Materializing
                // the child on mount preserves their descriptors without copying or
                // republishing the parent's local files. Reserved parents may have
                // newer local writes, so they still need the local clone below.
                backing_layers = parent.backing_layers;
                None
            } else {
                if parent.backing_image_config.is_none() && !parent.backing_layers.is_empty() {
                    parent = self.materialize_backing(&parent.id).await?;
                }
                match parent.backing_image_config.as_ref() {
                    Some(path) => Some(self.create_child_backing(&id, path).await?),
                    None => None,
                }
            }
        } else if let Some(source_config) = source_config {
            Some(self.create_child_backing(&id, &source_config).await?)
        } else {
            Some(self.create_empty_backing(&id, size_mb).await?)
        };

        let mut record = VolumeRecord {
            id,
            name,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: reserved_owner.map(str::to_owned),
            backing_image_config,
            backing_layers,
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        let create_result = async {
            if reserved_owner.is_none() {
                self.publish_backing(&mut record).await?;
            }
            self.repository
                .create_volume(record.clone())
                .await
                .map_err(repository_error)?;
            self.cache_record(record.clone()).await;
            Ok(())
        }
        .await;
        if let Err(error) = create_result {
            if let Some(config) = record.backing_image_config.as_ref() {
                if let Some(directory) = config.parent() {
                    let _ = tokio::fs::remove_dir_all(directory).await;
                }
            }
            return Err(error);
        }
        info!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "volume created"
        );
        Ok(record)
    }

    pub async fn delete(&self, reference: &str) -> Result<(), VolumeError> {
        let record = self.get(reference).await?;
        if let Some(owner) = record.reserved_by_sandbox_id.as_deref() {
            return Err(VolumeError::Reserved(owner.to_owned()));
        }
        let node_owner = self.read_only_mounts.node_owner();
        if let Some(owner) = record
            .read_only_mounts
            .iter()
            .find(|owner| owner.as_str() != node_owner)
        {
            return Err(VolumeError::Reserved(owner.clone()));
        }
        if record.read_only_mounts.iter().any(|owner| owner == node_owner) {
            // Only this node's idle lease remains; drop it instead of making
            // the caller wait for the idle release.
            self.read_only_mounts.release_for_delete(&record.id).await?;
        }
        debug!(volume_id = %record.id, "deleting volume");
        self.repository
            .delete_volume(&record.id)
            .await
            .map_err(repository_error)?;
        let backing_directory = self.data_dir(&record.id);
        self.records.write().await.remove(&record.id);
        if let Err(error) = tokio::fs::remove_dir_all(&backing_directory).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    volume_id = %record.id,
                    path = %backing_directory.display(),
                    %error,
                    "failed to clean deleted volume's node-local backing"
                );
            }
        }
        info!(volume_id = %record.id, "volume deleted");
        Ok(())
    }

    pub(crate) async fn create_child_for_owner(
        &self,
        reference: &str,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        source_owner: &str,
        child_owner: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        self.create_with_source_owner(
            name,
            mode,
            Some((reference.to_owned(), Some(source_owner))),
            None,
            size_mb,
            Some(child_owner),
        )
        .await
    }

    pub(crate) async fn create_from_snapshot(
        &self,
        name: String,
        mode: VolumeMode,
        size_mb: u64,
        backing_layers: Vec<OverlaybdLayerRef>,
    ) -> Result<VolumeRecord, VolumeError> {
        validate_name(&name)?;
        if size_mb == 0 || backing_layers.is_empty() {
            return Err(VolumeError::InvalidSize);
        }
        if size_mb > self.limits.max_size_mb {
            return Err(VolumeError::SizeLimitExceeded {
                max_size_mb: self.limits.max_size_mb,
            });
        }
        match self.get(&name).await {
            Ok(_) => return Err(VolumeError::NameConflict(name)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let record = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name,
            mode,
            size_mb,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers,
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        debug!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "creating volume from snapshot"
        );
        self.repository
            .create_volume(record.clone())
            .await
            .map_err(repository_error)?;
        self.cache_record(record.clone()).await;
        info!(
            volume_id = %record.id,
            mode = ?record.mode,
            size_mb = record.size_mb,
            "volume created from snapshot"
        );
        Ok(record)
    }

    pub(crate) async fn snapshot_volume_state(
        &self,
        reference: &str,
    ) -> Result<VolumeRecord, VolumeError> {
        let mut parent = self.get(reference).await?;
        // A read-only volume's committed layers are its whole, immutable content,
        // so the snapshot references them as-is. Re-deriving them from the
        // node-local config is not a round trip: on OSS a materialized managed
        // lower reads back as an external layer, which the catalog rejects.
        if parent.mode == VolumeMode::ReadOnly {
            return Ok(parent);
        }
        // A running sandbox restacks its volume upper into this same local
        // image config before capture. Publish that latest state before making
        // the logical volume snapshot.
        let owner = parent.reserved_by_sandbox_id.clone();
        if let Some(owner) = owner.as_deref() {
            self.recover_and_publish_backings(owner, std::slice::from_ref(&parent.id))
                .await?;
            parent = self.get(reference).await?;
        } else {
            self.publish_backing(&mut parent).await?;
            self.persist_catalog(&parent).await?;
            self.cache_record(parent.clone()).await;
        }
        Ok(parent)
    }

    pub async fn reserve(&self, reference: &str, owner: &str) -> Result<(), VolumeError> {
        let mut record = self.get(reference).await?;
        match record.status {
            VolumeStatus::Uploading => return Err(VolumeError::Uploading(record.id.clone())),
            VolumeStatus::Failed => return Err(VolumeError::Failed(record.id.clone())),
            VolumeStatus::Ready => {}
        }
        if record.mode == VolumeMode::ReadOnly {
            self.repository
                .reserve_read_only_volume(&record.id, owner)
                .await
                .map_err(repository_error)?;
            if !record.read_only_mounts.iter().any(|entry| entry == owner) {
                record.read_only_mounts.push(owner.to_owned());
            }
            debug!(volume_id = %record.id, "read-only volume reserved");
            self.cache_record(record).await;
            return Ok(());
        }
        if let Some(existing) = self
            .repository
            .reserve_volume(&record.id, owner)
            .await
            .map_err(repository_error)?
        {
            return Err(VolumeError::Reserved(existing));
        }
        record.reserved_by_sandbox_id = Some(owner.to_owned());
        debug!(volume_id = %record.id, "exclusive volume reserved");
        self.cache_record(record).await;
        Ok(())
    }

    /// Reserves `record`, as just resolved by [`Self::get_for_mount`], for a
    /// sandbox mount. Exclusive volumes use [`Self::reserve`]; read-only
    /// volumes are tracked in the node-local ledger and write the catalog only
    /// for the node's first mount.
    pub async fn reserve_mount(&self, record: &VolumeRecord, owner: &str) -> Result<(), VolumeError> {
        if record.mode != VolumeMode::ReadOnly {
            return self.reserve(&record.id, owner).await;
        }
        match record.status {
            VolumeStatus::Uploading => return Err(VolumeError::Uploading(record.id.clone())),
            VolumeStatus::Failed => return Err(VolumeError::Failed(record.id.clone())),
            VolumeStatus::Ready => {}
        }
        if !is_valid_volume_component(owner) {
            return Err(VolumeError::Storage(format!("invalid volume owner '{owner}'")));
        }
        self.read_only_mounts.acquire(&record.id, owner).await?;
        debug!(volume_id = %record.id, %owner, "read-only volume mounted on node");
        // Keep a concurrent mount's materialized backing path.
        self.records
            .write()
            .await
            .entry(record.id.clone())
            .or_insert_with(|| record.clone());
        Ok(())
    }

    /// Resolves a volume for mounting. A read-only volume whose node lease
    /// this node holds is served from the local cache: the lease blocks its
    /// deletion, and read-only identity, layers, and status never change.
    pub async fn get_for_mount(&self, reference: &str) -> Result<VolumeRecord, VolumeError> {
        let cached = {
            let records = self.records.read().await;
            records
                .get(reference)
                .or_else(|| records.values().find(|record| record.name == reference))
                .filter(|record| record.mode == VolumeMode::ReadOnly)
                .cloned()
        };
        if let Some(mut record) = cached {
            if self.read_only_mounts.holds(&record.id).await {
                record.backing_image_config =
                    record.backing_image_config.filter(|path| path.exists());
                return Ok(record);
            }
        }
        self.get(reference).await
    }

    /// Returns the read-only volume shared by every restore of a captured
    /// read-only volume with these layers, creating it once per cluster.
    /// `None` means an unrelated volume holds the derived name.
    pub(crate) async fn read_only_volume_for_snapshot(
        &self,
        size_mb: u64,
        backing_layers: Vec<OverlaybdLayerRef>,
    ) -> Result<Option<VolumeRecord>, VolumeError> {
        let name = snapshot_read_only_volume_name(size_mb, &backing_layers)?;
        let gate = Arc::clone(
            self.snapshot_volume_gates
                .lock()
                .unwrap()
                .entry(name.clone())
                .or_default(),
        );
        let _gate = gate.lock().await;
        let matches = |record: &VolumeRecord| {
            record.mode == VolumeMode::ReadOnly
                && record.size_mb == size_mb
                && record.backing_layers == backing_layers
                && record.status == VolumeStatus::Ready
        };
        match self.get_for_mount(&name).await {
            Ok(record) => return Ok(matches(&record).then_some(record)),
            Err(VolumeError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        match self
            .create_from_snapshot(
                name.clone(),
                VolumeMode::ReadOnly,
                size_mb,
                backing_layers.clone(),
            )
            .await
        {
            Ok(record) => Ok(Some(record)),
            // Another node created it first; reuse theirs.
            Err(VolumeError::NameConflict(_)) => {
                let record = self.get(&name).await?;
                Ok(matches(&record).then_some(record))
            }
            Err(error) => Err(error),
        }
    }

    /// Drops node-local read-only mounts whose owner did not survive a
    /// restart. `live_mounts` maps each restored sandbox ID to its volume IDs.
    pub async fn reconcile_read_only_mounts(
        &self,
        live_mounts: &HashMap<String, Vec<String>>,
    ) {
        self.read_only_mounts
            .reconcile(|owner, volume_id| {
                live_mounts
                    .get(owner)
                    .is_some_and(|volume_ids| volume_ids.iter().any(|id| id == volume_id))
            })
            .await;
    }

    pub async fn replace_owner_for(
        &self,
        owner: &str,
        new_owner: Option<&str>,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        for volume_id in volume_ids {
            if self
                .read_only_mounts
                .replace_owner(volume_id, owner, new_owner)
                .await?
            {
                debug!(volume_id = %volume_id, %owner, new_owner = ?new_owner, "node-local read-only volume mount updated");
                continue;
            }
            self.repository
                .replace_volume_owner_for(volume_id, owner, new_owner)
                .await
                .map_err(repository_error)?;
            if let Some(record) = self.records.write().await.get_mut(volume_id) {
                record.replace_owner(owner, new_owner);
            }
            match new_owner {
                Some(new_owner) => debug!(
                    volume_id = %volume_id,
                    previous_owner = %owner,
                    new_owner = %new_owner,
                    "volume reservation owner replaced"
                ),
                None => debug!(volume_id = %volume_id, %owner, "volume reservation released"),
            }
        }
        Ok(())
    }

    /// Keep an incomplete capture unavailable after its sandbox is stopped.
    pub(crate) async fn fail_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        for volume_id in volume_ids {
            let mut record = self.get(volume_id).await?;
            if record.reserved_by_sandbox_id.as_deref() == Some(owner) {
                record.status = VolumeStatus::Failed;
                self.persist_catalog(&record).await?;
                self.cache_record(record).await;
            }
        }
        Ok(())
    }

    pub async fn publish_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        let mut records = Vec::with_capacity(volume_ids.len());
        for volume_id in volume_ids {
            let record = self.get(volume_id).await?;
            if record.mounted_by(owner) {
                records.push(record);
            }
        }
        self.publish_records(owner, records).await
    }

    /// Restores deterministic node-local config paths after a server restart.
    pub async fn recover_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        for volume_id in volume_ids {
            let mut record = self.get(volume_id).await?;
            if record.reserved_by_sandbox_id.as_deref() != Some(owner) {
                continue;
            }
            if record.backing_image_config.is_none() {
                let path = self.data_dir(&record.id).join("image.json");
                if !path.exists() {
                    record.status = VolumeStatus::Failed;
                    if let Err(error) = self.persist_catalog(&record).await {
                        warn!(
                            volume_id = %record.id,
                            %error,
                            "failed to persist volume failure status"
                        );
                    }
                    self.cache_record(record.clone()).await;
                    warn!(
                        volume_id = %record.id,
                        path = %path.display(),
                        "reserved volume backing is missing"
                    );
                    return Err(VolumeError::Storage(format!(
                        "local backing for reserved volume '{}' is missing",
                        record.id
                    )));
                }
                record.backing_image_config = Some(path);
                debug!(volume_id = %record.id, %owner, "volume backing recovered");
                self.cache_record(record).await;
            }
        }
        Ok(())
    }

    pub async fn recover_and_publish_backings(
        &self,
        owner: &str,
        volume_ids: &[String],
    ) -> Result<(), VolumeError> {
        self.recover_backings(owner, volume_ids).await?;
        self.publish_backings(owner, volume_ids).await
    }

    async fn publish_records(
        &self,
        owner: &str,
        records: Vec<VolumeRecord>,
    ) -> Result<(), VolumeError> {
        for mut record in records {
            if record.reserved_by_sandbox_id.as_deref() != Some(owner) {
                continue;
            }
            // Publish the state transition before uploading any writable
            // upper layer so other nodes cannot mount stale content.
            record.status = VolumeStatus::Uploading;
            if let Err(error) = self.persist_catalog(&record).await {
                self.mark_publication_failed(record, "mark_uploading", &error)
                    .await;
                return Err(error);
            }
            self.cache_record(record.clone()).await;
            debug!(volume_id = %record.id, "publishing volume backing");
            if let Err(error) = self.publish_backing(&mut record).await {
                self.mark_publication_failed(record, "publish_backing", &error)
                    .await;
                return Err(error);
            }
            record.status = VolumeStatus::Ready;
            if let Err(error) = self.persist_catalog(&record).await {
                self.mark_publication_failed(record, "mark_ready", &error)
                    .await;
                return Err(error);
            }
            info!(
                volume_id = %record.id,
                layer_count = record.backing_layers.len(),
                "volume backing published"
            );
            self.cache_record(record).await;
        }
        Ok(())
    }

    async fn mark_publication_failed(
        &self,
        mut record: VolumeRecord,
        stage: &'static str,
        error: &VolumeError,
    ) {
        record.status = VolumeStatus::Failed;
        if let Err(status_error) = self.persist_catalog(&record).await {
            warn!(
                volume_id = %record.id,
                error = %status_error,
                "failed to persist volume failure status"
            );
        }
        warn!(
            volume_id = %record.id,
            stage,
            %error,
            "volume backing publication failed"
        );
        self.cache_record(record).await;
    }

    async fn cache_record(&self, record: VolumeRecord) {
        self.records.write().await.insert(record.id.clone(), record);
    }

    async fn publish_backing(&self, record: &mut VolumeRecord) -> Result<(), VolumeError> {
        if let Some(path) = record.backing_image_config.as_deref() {
            record.backing_layers =
                crate::snapshot::store::store_volume_layers(self.repository.as_ref(), path)
                    .await
                    .map_err(repository_error)?;
        }
        Ok(())
    }

    async fn persist_catalog(&self, record: &VolumeRecord) -> Result<(), VolumeError> {
        self.repository
            .put_volume(record.clone())
            .await
            .map_err(repository_error)
    }

    async fn create_empty_backing(
        &self,
        volume_id: &str,
        size_mb: u64,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let raw = directory.join("base.ext4");
        let commit = directory.join("base.commit");
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let result = async {
            let file = tokio::fs::File::create(&raw)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            file.set_len(
                size_mb
                    .checked_mul(BYTES_PER_MB)
                    .ok_or(VolumeError::InvalidSize)?,
            )
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;
            drop(file);
            let status = Command::new("mkfs.ext4")
                .args(["-q", "-F"])
                .arg(&raw)
                .status()
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            if !status.success() {
                return Err(VolumeError::Storage(format!(
                    "mkfs.ext4 exited with status {status}"
                )));
            }
            overlaybd::tools::package_raw_as_overlaybd(&raw, &commit)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let config = ImageConfig {
                lowers: vec![LayerConfig {
                    file: commit.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                }],
                ..ImageConfig::default()
            };
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            tokio::fs::write(&image_config, bytes)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let _ = tokio::fs::remove_file(&raw).await;
            Ok(image_config)
        }
        .await;

        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }

    async fn create_child_backing(
        &self,
        volume_id: &str,
        parent_config: &Path,
    ) -> Result<PathBuf, VolumeError> {
        let directory = self.data_dir(volume_id);
        let image_config = directory.join("image.json");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| VolumeError::Storage(error.to_string()))?;

        let result = async {
            let bytes = tokio::fs::read(parent_config)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            let mut config: ImageConfig = serde_json::from_slice(&bytes)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            for (index, layer) in config.lowers.iter_mut().enumerate() {
                if layer.file.is_empty() {
                    continue;
                }
                let source = Path::new(&layer.file);
                let extension = source
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .unwrap_or("layer");
                let destination = directory.join(format!("lower-{index}.{extension}"));
                if let Err(link_error) = tokio::fs::hard_link(source, &destination).await {
                    tokio::fs::copy(source, &destination).await.map_err(|copy_error| {
                        VolumeError::Storage(format!(
                            "clone volume layer '{}' (hard link failed: {link_error}; copy failed: {copy_error})",
                            source.display()
                        ))
                    })?;
                }
                layer.file = destination.to_string_lossy().into_owned();
            }
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            tokio::fs::write(&image_config, bytes)
                .await
                .map_err(|error| VolumeError::Storage(error.to_string()))?;
            Ok(image_config.clone())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&directory).await;
        }
        result
    }
}

/// Content-derived so every restore of the same captured read-only volume,
/// on any node, resolves the same record.
fn snapshot_read_only_volume_name(
    size_mb: u64,
    backing_layers: &[OverlaybdLayerRef],
) -> Result<String, VolumeError> {
    use sha2::{Digest, Sha256};

    let layers = serde_json::to_vec(backing_layers)
        .map_err(|error| VolumeError::Storage(error.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(size_mb.to_le_bytes());
    hasher.update(&layers);
    let digest = hex::encode(hasher.finalize());
    Ok(format!("volume-ro-{}", &digest[..32]))
}

fn repository_error(error: RepositoryError) -> VolumeError {
    match error {
        RepositoryError::VolumeNotFound { lookup } => VolumeError::NotFound(lookup),
        RepositoryError::VolumeNameConflict { name } => VolumeError::NameConflict(name),
        error => VolumeError::Storage(error.to_string()),
    }
}

fn encode_volume_cursor(volume_id: String) -> String {
    URL_SAFE_NO_PAD.encode(volume_id)
}

fn decode_volume_cursor(token: &str) -> Result<String, VolumeError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| VolumeError::InvalidNextToken)?;
    let volume_id = String::from_utf8(bytes).map_err(|_| VolumeError::InvalidNextToken)?;
    validate_volume_id(&volume_id).map_err(|_| VolumeError::InvalidNextToken)?;
    Ok(volume_id)
}

fn validate_name(name: &str) -> Result<(), VolumeError> {
    if is_valid_volume_component(name) {
        Ok(())
    } else {
        Err(VolumeError::InvalidName)
    }
}

fn validate_volume_id(id: &str) -> Result<(), VolumeError> {
    if is_valid_volume_component(id) {
        Ok(())
    } else {
        Err(VolumeError::Storage(format!("invalid volume id '{id}'")))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use overlaybd::config::{ImageConfig, LayerConfig};
    use tempfile::NamedTempFile;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::{VolumeError, VolumeLimits, VolumeManager, VolumeMode, VolumeRecord, VolumeStatus};
    use crate::digest::sha256_digest;
    use crate::repository::{
        BlobBody, BlobKey, BlobLocation, DocumentChange, DocumentCollection, DocumentKey,
        DocumentUpdate, Repository, RepositoryResult,
    };
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{ManagedLayer, OverlaybdLayerRef};

    async fn manager_at(root: &Path) -> VolumeManager {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("cache")),
            runtime_cache_root: Some(root.join("runtime")),
        })
        .expect("POSIX snapshot backend");
        VolumeManager::open_with_repository(root.join("volumes/catalog"), backend.repository())
            .await
            .expect("volume manager")
    }

    async fn published_volume(root: &Path, manager: &VolumeManager) -> VolumeRecord {
        let layer = root.join("base.commit");
        std::fs::write(&layer, b"published volume layer").unwrap();
        let config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: layer.to_string_lossy().into_owned(),
                    ..LayerConfig::default()
                },
                LayerConfig {
                    repo_blob_url: "https://registry.example.test/v2/cache/blobs".to_owned(),
                    digest: sha256_digest(b"external volume layer"),
                    size: 21,
                    ..LayerConfig::default()
                },
            ],
            ..ImageConfig::default()
        };
        let path = root.join("source.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        manager
            .create(
                "parent".to_owned(),
                VolumeMode::Exclusive,
                None,
                Some(path),
                1024,
            )
            .await
            .expect("publish initial volume")
    }

    fn append_local_layer(record: &VolumeRecord) {
        let path = record.backing_image_config.as_ref().unwrap();
        let layer = path.parent().unwrap().join("unpublished.commit");
        std::fs::write(&layer, b"new volume writes").unwrap();
        let mut config = overlaybd::config::load_image_config(path).unwrap();
        config.lowers.push(LayerConfig {
            file: layer.to_string_lossy().into_owned(),
            ..LayerConfig::default()
        });
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn published_volume_clones_survive_parent_deletion_without_local_files() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        // A stale local config must not override the committed backing, even
        // when the manager still has the config path in its in-memory cache.
        std::fs::write(
            parent.backing_image_config.as_ref().unwrap(),
            b"stale config",
        )
        .unwrap();
        let cold_manager = manager_at(temp.path()).await;
        let mut children = Vec::new();
        for (index, (manager, mode)) in [
            (&manager, VolumeMode::ReadOnly),
            (&cold_manager, VolumeMode::Exclusive),
        ]
        .into_iter()
        .enumerate()
        {
            let child = manager
                .create(
                    format!("child-{index}"),
                    mode,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .expect("clone committed volume without reading local files");
            assert_eq!(child.backing_layers, parent.backing_layers);
            assert_eq!(child.mode, mode);
            assert!(child.backing_image_config.is_none());
            assert!(!manager.data_dir(&child.id).exists());
            children.push(child);
        }
        manager.delete(&parent.id).await.unwrap();
        let restarted = manager_at(temp.path()).await;
        for child in children {
            let child = restarted.materialize_backing(&child.id).await.unwrap();
            let config =
                overlaybd::config::load_image_config(child.backing_image_config.unwrap()).unwrap();
            assert_eq!(
                std::fs::read(&config.lowers[0].file).unwrap(),
                b"published volume layer"
            );
            assert_eq!(
                config.lowers[0].digest,
                sha256_digest(b"published volume layer")
            );
            assert_eq!(config.lowers[0].size, 22);
            assert!(config.lowers[1].file.is_empty());
            assert_eq!(
                config.lowers[1].repo_blob_url,
                "https://registry.example.test/v2/cache/blobs"
            );
            assert_eq!(
                config.lowers[1].digest,
                sha256_digest(b"external volume layer")
            );
        }
    }

    #[tokio::test]
    async fn volume_generations_preserve_inherited_descriptors_and_new_writes() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        let seed = manager
            .create(
                "seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(parent.id.clone()),
                None,
                1024,
            )
            .await
            .unwrap();
        manager.reserve(&seed.id, "builder").await.unwrap();
        let child = manager
            .create_child_for_owner(
                &seed.id,
                "work".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "builder",
                "builder",
            )
            .await
            .unwrap();
        assert_eq!(child.backing_layers, parent.backing_layers);
        let child = manager.materialize_backing(&child.id).await.unwrap();
        let config =
            overlaybd::config::load_image_config(child.backing_image_config.as_ref().unwrap())
                .unwrap();
        assert_eq!(
            config.lowers[0].digest,
            sha256_digest(b"published volume layer")
        );
        assert_eq!(config.lowers[0].size, 22);
        append_local_layer(&child);
        manager
            .recover_and_publish_backings("builder", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        manager
            .replace_owner_for("builder", None, &[seed.id, child.id.clone()])
            .await
            .unwrap();
        let published = manager.get(&child.id).await.unwrap();
        assert_eq!(
            &published.backing_layers[..2],
            parent.backing_layers.as_slice()
        );
        assert_eq!(
            published.backing_layers[2],
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: sha256_digest(b"new volume writes"),
                size: 17,
                uuid: None,
            })
        );
        let next_seed = manager
            .create(
                "next-seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(child.id),
                None,
                1024,
            )
            .await
            .unwrap();
        assert_eq!(next_seed.backing_layers, published.backing_layers);
        assert!(next_seed.backing_image_config.is_none());
    }

    #[tokio::test]
    async fn reserved_volume_clone_retains_unpublished_local_layers() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        manager.reserve(&parent.id, "source").await.unwrap();
        append_local_layer(&parent);
        let child = manager
            .create_child_for_owner(
                &parent.id,
                "fork".to_owned(),
                VolumeMode::Exclusive,
                1024,
                "source",
                "child",
            )
            .await
            .unwrap();
        assert!(child.backing_layers.is_empty());
        manager
            .replace_owner_for("source", None, std::slice::from_ref(&parent.id))
            .await
            .unwrap();
        manager.delete(&parent.id).await.unwrap();
        manager
            .recover_and_publish_backings("child", std::slice::from_ref(&child.id))
            .await
            .unwrap();
        let child = manager.get(&child.id).await.unwrap();
        assert_eq!(child.backing_layers.len(), 3);
        assert_eq!(
            child.backing_layers[2],
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: sha256_digest(b"new volume writes"),
                size: 17,
                uuid: None,
            })
        );
    }

    #[tokio::test]
    async fn published_volume_clone_validates_source_state_and_size() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let mut parent = published_volume(temp.path(), &manager).await;
        assert_eq!(
            manager
                .create(
                    "wrong-size".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    2048
                )
                .await
                .unwrap_err(),
            VolumeError::SizeMismatch
        );
        manager.reserve(&parent.id, "owner").await.unwrap();
        assert_eq!(
            manager
                .create(
                    "reserved".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024
                )
                .await
                .unwrap_err(),
            VolumeError::Reserved("owner".to_owned())
        );
        for status in [VolumeStatus::Uploading, VolumeStatus::Failed] {
            parent.status = status;
            manager.persist_catalog(&parent).await.unwrap();
            let error = manager
                .create(
                    "unready".to_owned(),
                    VolumeMode::ReadOnly,
                    Some(parent.id.clone()),
                    None,
                    1024,
                )
                .await
                .unwrap_err();
            assert_eq!(
                error,
                match status {
                    VolumeStatus::Uploading => VolumeError::Uploading(parent.id.clone()),
                    VolumeStatus::Failed => VolumeError::Failed(parent.id.clone()),
                    VolumeStatus::Ready => unreachable!(),
                }
            );
        }
    }

    const REMOTE_MANAGED_LAYERS_URL: &str = "s3://bucket/prefix/managed-layers";

    /// Stores through the POSIX backend but addresses managed layers by URL, as
    /// the OSS backend does, so materialized volume configs carry remote lowers.
    struct RemoteManagedLayers {
        inner: Arc<dyn Repository>,
    }

    #[async_trait]
    impl Repository for RemoteManagedLayers {
        fn managed_layers_repo_blob_url(&self) -> Option<String> {
            Some(REMOTE_MANAGED_LAYERS_URL.to_owned())
        }

        async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
            self.inner.put_blob(key, body).await
        }

        fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
            Ok(BlobLocation::Remote(format!("{REMOTE_MANAGED_LAYERS_URL}/{}", key.path())))
        }

        fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
            self.inner.staging_file()
        }

        async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
            self.inner.get_document(key).await
        }

        async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
            self.inner.put_document(key, bytes).await
        }

        async fn update_document(
            &self,
            key: DocumentKey,
            update: DocumentUpdate,
        ) -> RepositoryResult<Option<Vec<u8>>> {
            self.inner.update_document(key, update).await
        }

        async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
            self.inner.delete_document(key).await
        }

        async fn list_documents(
            &self,
            collection: DocumentCollection,
            after: Option<&str>,
            limit: usize,
        ) -> RepositoryResult<Vec<String>> {
            self.inner.list_documents(collection, after, limit).await
        }
    }

    #[tokio::test]
    async fn read_only_clone_snapshot_keeps_committed_layers_with_remote_managed_layers() {
        let temp = tempfile::tempdir().unwrap();
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: temp.path().join("repository"),
            cache_root: Some(temp.path().join("cache")),
            runtime_cache_root: Some(temp.path().join("runtime")),
        })
        .expect("POSIX snapshot backend");
        let repository: Arc<dyn Repository> = Arc::new(RemoteManagedLayers {
            inner: backend.repository(),
        });
        let manager =
            VolumeManager::open_with_repository(temp.path().join("volumes/catalog"), repository)
                .await
                .expect("volume manager");
        let parent = published_volume(temp.path(), &manager).await;
        assert!(matches!(parent.backing_layers[0], OverlaybdLayerRef::Managed(_)));
        let seed = manager
            .create(
                "seed-ro".to_owned(),
                VolumeMode::ReadOnly,
                Some(parent.id.clone()),
                None,
                1024,
            )
            .await
            .unwrap();
        manager.reserve(&seed.id, "sandbox").await.unwrap();
        let mounted = manager.materialize_backing(&seed.id).await.unwrap();
        let config =
            overlaybd::config::load_image_config(mounted.backing_image_config.as_ref().unwrap())
                .unwrap();
        assert!(config.lowers[0].file.is_empty());
        assert_eq!(config.lowers[0].repo_blob_url, REMOTE_MANAGED_LAYERS_URL);

        let captured = manager.snapshot_volume_state(&seed.id).await.unwrap();

        assert_eq!(captured.backing_layers, parent.backing_layers);
        let stored = manager.get(&seed.id).await.unwrap();
        assert_eq!(stored.backing_layers, parent.backing_layers);
        assert_eq!(stored.status, VolumeStatus::Ready);
        assert_eq!(stored.read_only_mounts, vec!["sandbox".to_owned()]);
    }

    /// Counts catalog round trips so tests can assert which operations write
    /// the shared catalog. A write is a put, a delete, or an applied update.
    struct CountingRepository {
        inner: Arc<dyn Repository>,
        reads: AtomicUsize,
        writes: AtomicUsize,
    }

    impl CountingRepository {
        fn new(inner: Arc<dyn Repository>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                reads: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
            })
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }

        fn writes(&self) -> usize {
            self.writes.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Repository for CountingRepository {
        fn managed_layers_repo_blob_url(&self) -> Option<String> {
            self.inner.managed_layers_repo_blob_url()
        }

        async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
            self.inner.put_blob(key, body).await
        }

        fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
            self.inner.locate_blob(key)
        }

        fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
            self.inner.staging_file()
        }

        async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_document(key).await
        }

        async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.inner.put_document(key, bytes).await
        }

        async fn update_document(
            &self,
            key: DocumentKey,
            update: DocumentUpdate,
        ) -> RepositoryResult<Option<Vec<u8>>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let wrote = Arc::new(AtomicBool::new(false));
            let last_change = Arc::clone(&wrote);
            let observed: DocumentUpdate = Box::new(move |current| {
                let change = update(current)?;
                last_change.store(!matches!(change, DocumentChange::Keep), Ordering::SeqCst);
                Ok(change)
            });
            let result = self.inner.update_document(key, observed).await;
            if result.is_ok() && wrote.load(Ordering::SeqCst) {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
            result
        }

        async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete_document(key).await
        }

        async fn list_documents(
            &self,
            collection: DocumentCollection,
            after: Option<&str>,
            limit: usize,
        ) -> RepositoryResult<Vec<String>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.list_documents(collection, after, limit).await
        }
    }

    fn limits_with_read_only_lease_idle(secs: u64) -> VolumeLimits {
        VolumeLimits {
            read_only_node_lease_idle_secs: secs,
            ..VolumeLimits::default()
        }
    }

    /// A POSIX repository shared by every simulated node, wrapped for counting.
    fn counting_repository(root: &Path) -> Arc<CountingRepository> {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("cache")),
            runtime_cache_root: Some(root.join("runtime")),
        })
        .expect("POSIX snapshot backend");
        CountingRepository::new(backend.repository())
    }

    async fn node_manager(
        node_root: &Path,
        repository: &Arc<CountingRepository>,
        idle_secs: u64,
    ) -> VolumeManager {
        VolumeManager::open_with_repository_and_limits(
            node_root.join("volumes/catalog"),
            Arc::clone(repository) as Arc<dyn Repository>,
            limits_with_read_only_lease_idle(idle_secs),
        )
        .await
        .expect("volume manager")
    }

    /// Mirrors `POST /sandboxes` from a snapshot with one read-only volume:
    /// resolve the restore volume, reserve it under a pending owner,
    /// materialize it, then rebind the reservation to the sandbox.
    async fn restore_read_only_mount(
        manager: &VolumeManager,
        size_mb: u64,
        layers: Vec<OverlaybdLayerRef>,
        sandbox_id: String,
    ) -> String {
        let volume = manager
            .read_only_volume_for_snapshot(size_mb, layers)
            .await
            .expect("resolve snapshot read-only volume")
            .expect("derived name is not taken by another volume");
        let pending = format!("pending-{sandbox_id}");
        let volume = manager.get_for_mount(&volume.id).await.unwrap();
        manager.reserve_mount(&volume, &pending).await.unwrap();
        let mounted = manager.materialize_backing(&volume.id).await.unwrap();
        assert!(mounted.backing_image_config.unwrap().exists());
        manager
            .replace_owner_for(&pending, Some(&sandbox_id), std::slice::from_ref(&volume.id))
            .await
            .unwrap();
        volume.id
    }

    async fn restore_concurrently(
        manager: &VolumeManager,
        size_mb: u64,
        layers: &[OverlaybdLayerRef],
        sandbox_ids: &[String],
    ) -> Vec<String> {
        let tasks = sandbox_ids
            .iter()
            .cloned()
            .map(|sandbox_id| {
                let manager = manager.clone();
                let layers = layers.to_vec();
                tokio::spawn(async move {
                    restore_read_only_mount(&manager, size_mb, layers, sandbox_id).await
                })
            })
            .collect::<Vec<_>>();
        let mut volume_ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            volume_ids.push(task.await.unwrap());
        }
        volume_ids
    }

    fn sandbox_ids(prefix: &str, count: usize) -> Vec<String> {
        (0..count).map(|index| format!("{prefix}-{index}")).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_read_only_snapshot_restores_write_the_catalog_once_per_cluster_and_node() {
        const SANDBOXES: usize = 32;
        let temp = tempfile::tempdir().unwrap();
        let repository = counting_repository(temp.path());
        let node_a = node_manager(&temp.path().join("node-a"), &repository, 300).await;
        let parent = published_volume(temp.path(), &node_a).await;
        let layers = parent.backing_layers.clone();

        let writes_before = repository.writes();
        let volume_ids =
            restore_concurrently(&node_a, parent.size_mb, &layers, &sandbox_ids("cold", SANDBOXES))
                .await;
        let cold_writes = repository.writes() - writes_before;
        assert!(volume_ids.iter().all(|id| id == &volume_ids[0]));
        let volume_id = volume_ids[0].clone();
        // One record plus its name, and one node lease, for all 32 creates.
        assert_eq!(cold_writes, 3, "cold restores wrote {cold_writes} catalog documents");

        let restored = node_a.get(&volume_id).await.unwrap();
        assert_eq!(restored.mode, VolumeMode::ReadOnly);
        assert_eq!(restored.backing_layers, layers);
        let node_a_owner = node_a.read_only_mounts.node_owner().to_owned();
        assert_eq!(restored.read_only_mounts, vec![node_a_owner.clone()]);

        // Warm creates on the leased node touch no catalog document at all.
        let (reads_before, writes_before) = (repository.reads(), repository.writes());
        let warm = sandbox_ids("warm", SANDBOXES);
        let warm_ids = restore_concurrently(&node_a, parent.size_mb, &layers, &warm).await;
        assert!(warm_ids.iter().all(|id| id == &volume_id));
        assert_eq!(repository.writes(), writes_before);
        assert_eq!(repository.reads(), reads_before);

        // Releasing every sandbox writes nothing; the idle node lease remains.
        let writes_before = repository.writes();
        for owner in sandbox_ids("cold", SANDBOXES).iter().chain(&warm) {
            node_a
                .replace_owner_for(owner, None, std::slice::from_ref(&volume_id))
                .await
                .unwrap();
        }
        assert_eq!(repository.writes(), writes_before);

        // A second node shares the record and writes only its own lease.
        let node_b = node_manager(&temp.path().join("node-b"), &repository, 300).await;
        let writes_before = repository.writes();
        let node_b_ids =
            restore_concurrently(&node_b, parent.size_mb, &layers, &sandbox_ids("b", SANDBOXES))
                .await;
        assert!(node_b_ids.iter().all(|id| id == &volume_id));
        assert_eq!(repository.writes() - writes_before, 1);

        // Node A's idle lease still blocks a delete from node B.
        for owner in sandbox_ids("b", SANDBOXES) {
            node_b
                .replace_owner_for(&owner, None, std::slice::from_ref(&volume_id))
                .await
                .unwrap();
        }
        assert_eq!(
            node_b.delete(&volume_id).await.unwrap_err(),
            VolumeError::Reserved(node_a_owner)
        );
        assert!(matches!(
            node_a.delete(&volume_id).await.unwrap_err(),
            VolumeError::Reserved(owner) if owner == node_b.read_only_mounts.node_owner()
        ));
        // Stands in for node A's idle timer; node B then drops its own idle
        // lease as part of a local delete.
        node_a
            .read_only_mounts
            .release_for_delete(&volume_id)
            .await
            .unwrap();
        node_b.delete(&volume_id).await.unwrap();
        assert_eq!(
            node_a.get(&volume_id).await.unwrap_err(),
            VolumeError::NotFound(volume_id)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_cold_restores_on_two_nodes_reuse_the_record_that_won_the_name() {
        const SANDBOXES_PER_NODE: usize = 16;
        let temp = tempfile::tempdir().unwrap();
        let repository = counting_repository(temp.path());
        let node_a = node_manager(&temp.path().join("node-a"), &repository, 300).await;
        let node_b = node_manager(&temp.path().join("node-b"), &repository, 300).await;
        let parent = published_volume(temp.path(), &node_a).await;
        let layers = parent.backing_layers.clone();

        let writes_before = repository.writes();
        let a_ids = sandbox_ids("a", SANDBOXES_PER_NODE);
        let b_ids = sandbox_ids("b", SANDBOXES_PER_NODE);
        let (on_a, on_b) = tokio::join!(
            restore_concurrently(&node_a, parent.size_mb, &layers, &a_ids),
            restore_concurrently(&node_b, parent.size_mb, &layers, &b_ids),
        );
        let volume_id = on_a[0].clone();
        assert!(on_a.iter().chain(&on_b).all(|id| id == &volume_id));
        // At most: the winner's record and name, a losing node's rolled-back
        // record, and one lease per node.
        let writes = repository.writes() - writes_before;
        assert!(writes <= 6, "cold restores wrote {writes} catalog documents");
        let mut leases = node_a.get(&volume_id).await.unwrap().read_only_mounts;
        leases.sort();
        let mut expected = vec![
            node_a.read_only_mounts.node_owner().to_owned(),
            node_b.read_only_mounts.node_owner().to_owned(),
        ];
        expected.sort();
        assert_eq!(leases, expected);
    }

    #[tokio::test]
    async fn read_only_mount_ledger_survives_restart_and_drops_stale_owners() {
        let temp = tempfile::tempdir().unwrap();
        let repository = counting_repository(temp.path());
        let node_root = temp.path().join("node");
        let manager = node_manager(&node_root, &repository, 300).await;
        let parent = published_volume(temp.path(), &manager).await;
        let volume = manager
            .read_only_volume_for_snapshot(parent.size_mb, parent.backing_layers.clone())
            .await
            .unwrap()
            .unwrap();
        for owner in ["live-sandbox", "stale-sandbox", "pending-create"] {
            manager.reserve_mount(&volume, owner).await.unwrap();
        }
        let node_owner = manager.read_only_mounts.node_owner().to_owned();
        drop(manager);

        // Restart with immediate idle release.
        let restarted = node_manager(&node_root, &repository, 0).await;
        assert_eq!(restarted.read_only_mounts.node_owner(), node_owner);
        restarted
            .reconcile_read_only_mounts(&HashMap::from([
                ("live-sandbox".to_owned(), vec![volume.id.clone()]),
                ("stale-sandbox".to_owned(), vec!["vol_other".to_owned()]),
            ]))
            .await;
        // The live owner keeps the volume mounted; the lease is reused as-is.
        assert_eq!(
            restarted.delete(&volume.id).await.unwrap_err(),
            VolumeError::Reserved("live-sandbox".to_owned())
        );
        let record = restarted.get_for_mount(&volume.id).await.unwrap();
        let writes_before = repository.writes();
        restarted.reserve_mount(&record, "next-sandbox").await.unwrap();
        assert_eq!(repository.writes(), writes_before);
        // Stale owners were dropped, so releasing the live ones frees the lease.
        assert!(!restarted
            .read_only_mounts
            .replace_owner(&volume.id, "stale-sandbox", None)
            .await
            .unwrap());
        restarted
            .replace_owner_for("live-sandbox", None, std::slice::from_ref(&volume.id))
            .await
            .unwrap();
        assert_eq!(
            restarted.get(&volume.id).await.unwrap().read_only_mounts,
            vec![node_owner]
        );
        restarted
            .replace_owner_for("next-sandbox", None, std::slice::from_ref(&volume.id))
            .await
            .unwrap();
        assert!(restarted
            .get(&volume.id)
            .await
            .unwrap()
            .read_only_mounts
            .is_empty());
        assert!(!node_root
            .join("volumes/read-only-mounts")
            .join(format!("{}.json", volume.id))
            .exists());
        let other_node = node_manager(&temp.path().join("other"), &repository, 300).await;
        other_node.delete(&volume.id).await.unwrap();
    }

    #[tokio::test]
    async fn read_only_mounts_reserved_by_earlier_releases_are_released_through_the_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        let seed = manager
            .create(
                "seed".to_owned(),
                VolumeMode::ReadOnly,
                Some(parent.id.clone()),
                None,
                1024,
            )
            .await
            .unwrap();
        // `reserve` keeps the per-owner catalog lease used before node leases.
        manager.reserve(&seed.id, "legacy-sandbox").await.unwrap();
        assert_eq!(
            manager.get(&seed.id).await.unwrap().read_only_mounts,
            vec!["legacy-sandbox".to_owned()]
        );
        manager
            .replace_owner_for("legacy-sandbox", None, std::slice::from_ref(&seed.id))
            .await
            .unwrap();
        assert!(manager
            .get(&seed.id)
            .await
            .unwrap()
            .read_only_mounts
            .is_empty());
    }

    #[tokio::test]
    async fn exclusive_mount_reservation_is_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let volume = published_volume(temp.path(), &manager).await;
        manager.reserve_mount(&volume, "first").await.unwrap();
        let record = manager.get(&volume.id).await.unwrap();
        assert_eq!(record.reserved_by_sandbox_id.as_deref(), Some("first"));
        assert!(record.read_only_mounts.is_empty());
        assert_eq!(
            manager.reserve_mount(&record, "second").await.unwrap_err(),
            VolumeError::Reserved("first".to_owned())
        );
        manager
            .replace_owner_for("first", None, std::slice::from_ref(&volume.id))
            .await
            .unwrap();
        assert!(manager
            .get(&volume.id)
            .await
            .unwrap()
            .reserved_by_sandbox_id
            .is_none());
    }

    #[tokio::test]
    async fn read_only_snapshot_volume_name_taken_by_another_volume_falls_back() {
        let temp = tempfile::tempdir().unwrap();
        let manager = manager_at(temp.path()).await;
        let parent = published_volume(temp.path(), &manager).await;
        let name =
            super::snapshot_read_only_volume_name(parent.size_mb, &parent.backing_layers).unwrap();
        manager
            .create_from_snapshot(
                name,
                VolumeMode::ReadOnly,
                parent.size_mb,
                vec![OverlaybdLayerRef::Managed(ManagedLayer {
                    digest: "sha256:other".to_owned(),
                    size: 1,
                    uuid: None,
                })],
            )
            .await
            .unwrap();
        assert!(manager
            .read_only_volume_for_snapshot(parent.size_mb, parent.backing_layers.clone())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn create_from_snapshot_preserves_read_only_mode() {
        let temp = tempfile::tempdir().expect("temporary volume repository");
        let manager = manager_at(temp.path()).await;

        let restored = manager
            .create_from_snapshot(
                "restored-read-only".to_owned(),
                VolumeMode::ReadOnly,
                1024,
                vec![OverlaybdLayerRef::Managed(ManagedLayer {
                    digest: "sha256:abc".to_owned(),
                    size: 4096,
                    uuid: None,
                })],
            )
            .await
            .expect("restore volume snapshot");

        assert_eq!(restored.mode, VolumeMode::ReadOnly);
        assert_eq!(
            manager
                .get(&restored.id)
                .await
                .expect("persisted volume")
                .mode,
            VolumeMode::ReadOnly
        );
    }
}
