//! Durable identity bound to one WAL root.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::write_protocol::{ClusterId, NodeId, ResolvedIdentity, WriterEpoch, WriterId, WriterSlot};
use uuid::Uuid;

const IDENTITY_FILE: &str = "writer-identity.json";
const IDENTITY_VERSION: u16 = 1;
const MAX_IDENTITY_FILE_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Default)]
pub struct WalIdentityConfig {
    pub cluster_id: Option<ClusterId>,
    pub node_id: Option<NodeId>,
    pub writer_slot: Option<WriterSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityFile {
    version: u16,
    cluster_id: ClusterId,
    writer_slot: WriterSlot,
    writer_id: WriterId,
    epoch: WriterEpoch,
}

/// Mutable identity state serialized under the WAL root.
#[derive(Debug)]
pub(super) struct WalIdentity {
    root: PathBuf,
    resolved: ResolvedIdentity,
}

impl WalIdentity {
    pub(super) fn open(root: &Path, configured: &WalIdentityConfig) -> TeoDBResult<Self> {
        validate_configured_identity(configured)?;
        let path = root.join(IDENTITY_FILE);
        let node_id = configured
            .node_id
            .clone()
            .unwrap_or(NodeId::new("standalone")?);

        let mut persisted = if path.exists() {
            read_identity(&path)?
        } else {
            ensure_uninitialized_root(root)?;
            let cluster_id = configured
                .cluster_id
                .unwrap_or_else(|| ClusterId::from_uuid(Uuid::now_v7()));
            let writer_slot = configured
                .writer_slot
                .clone()
                .unwrap_or(WriterSlot::new("standalone")?);
            let identity = IdentityFile {
                version: IDENTITY_VERSION,
                cluster_id,
                writer_id: WriterId::derive(cluster_id, &writer_slot),
                writer_slot,
                epoch: WriterEpoch::ZERO,
            };
            persist_identity(root, &identity)?;
            identity
        };

        validate_identity(&persisted, configured)?;
        persisted.epoch = persisted.epoch.checked_next()?;
        persist_identity(root, &persisted)?;

        Ok(Self {
            root: root.to_path_buf(),
            resolved: ResolvedIdentity {
                cluster_id: persisted.cluster_id,
                node_id,
                writer_slot: persisted.writer_slot,
                writer_id: persisted.writer_id,
                writer_epoch: persisted.epoch,
            },
        })
    }

    pub(super) fn resolved(&self) -> ResolvedIdentity {
        self.resolved.clone()
    }

    /// Ensure the local epoch is strictly greater than an epoch observed in
    /// this writer's authoritative catalog checkpoint.
    pub(super) fn observe_epoch_and_bump(&mut self, observed: WriterEpoch) -> TeoDBResult<ResolvedIdentity> {
        if self.resolved.writer_epoch > observed {
            return Ok(self.resolved());
        }

        let epoch = observed.checked_next()?;
        let persisted = IdentityFile {
            version: IDENTITY_VERSION,
            cluster_id: self.resolved.cluster_id,
            writer_slot: self.resolved.writer_slot.clone(),
            writer_id: self.resolved.writer_id,
            epoch,
        };
        persist_identity(&self.root, &persisted)?;
        self.resolved.writer_epoch = epoch;
        Ok(self.resolved())
    }
}

fn read_identity(path: &Path) -> TeoDBResult<IdentityFile> {
    let metadata = std::fs::metadata(path).map_err(|error| TeoDBError::wal_source("stat writer identity", error))?;
    if metadata.len() == 0 {
        return Err(TeoDBError::wal("writer identity file is empty"));
    }
    if metadata.len() > MAX_IDENTITY_FILE_BYTES {
        return Err(TeoDBError::wal(format!(
            "writer identity file exceeds {MAX_IDENTITY_FILE_BYTES} bytes"
        )));
    }

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| TeoDBError::wal_source("read writer identity", error))?;
    let identity: IdentityFile =
        serde_json::from_slice(&bytes).map_err(|error| TeoDBError::wal_source("parse writer identity", error))?;
    if identity.version != IDENTITY_VERSION {
        return Err(TeoDBError::wal(format!(
            "unsupported writer identity version {}",
            identity.version
        )));
    }
    if identity.cluster_id.as_uuid().is_nil() {
        return Err(TeoDBError::wal("writer identity contains a nil cluster UUID"));
    }
    if identity.writer_id.as_uuid().is_nil() {
        return Err(TeoDBError::wal("writer identity contains a nil writer UUID"));
    }
    WriterSlot::new(identity.writer_slot.as_str())
        .map_err(|error| TeoDBError::wal(format!("writer identity contains an invalid writer slot: {error}")))?;
    let derived = WriterId::derive(identity.cluster_id, &identity.writer_slot);
    if identity.writer_id != derived {
        return Err(TeoDBError::wal(format!(
            "writer identity is internally inconsistent: persisted {}, derived {}",
            identity.writer_id, derived
        )));
    }
    Ok(identity)
}

fn validate_configured_identity(configured: &WalIdentityConfig) -> TeoDBResult<()> {
    if configured
        .cluster_id
        .is_some_and(|cluster_id| cluster_id.as_uuid().is_nil())
    {
        return Err(TeoDBError::Config("configured cluster UUID must not be nil".into()));
    }
    if let Some(node_id) = &configured.node_id {
        NodeId::new(node_id.as_str())?;
    }
    if let Some(writer_slot) = &configured.writer_slot {
        WriterSlot::new(writer_slot.as_str())?;
    }
    Ok(())
}

fn validate_identity(persisted: &IdentityFile, configured: &WalIdentityConfig) -> TeoDBResult<()> {
    if let Some(cluster_id) = configured.cluster_id
        && cluster_id != persisted.cluster_id
    {
        return Err(TeoDBError::Config(format!(
            "WAL cluster identity mismatch: configured {cluster_id}, persisted {}",
            persisted.cluster_id
        )));
    }
    if let Some(writer_slot) = &configured.writer_slot
        && writer_slot != &persisted.writer_slot
    {
        return Err(TeoDBError::Config(format!(
            "WAL writer slot mismatch: configured {writer_slot}, persisted {}",
            persisted.writer_slot
        )));
    }
    Ok(())
}

fn ensure_uninitialized_root(root: &Path) -> TeoDBResult<()> {
    let mut durable_files = Vec::new();
    for entry in std::fs::read_dir(root)
        .map_err(|error| TeoDBError::wal_source("scan WAL root before identity creation", error))?
    {
        let entry = entry.map_err(|error| TeoDBError::wal_source("read WAL root entry", error))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_segment = name.ends_with(".wal");
        let is_checkpoint = name == "committed.json";
        if (is_segment || is_checkpoint)
            && entry
                .metadata()
                .map_err(|error| TeoDBError::wal_source("stat WAL file before identity creation", error))?
                .len()
                > 0
        {
            durable_files.push(name.into_owned());
        }
    }
    if durable_files.is_empty() {
        return Ok(());
    }
    durable_files.sort();
    Err(TeoDBError::wal(format!(
        "writer identity is missing while durable WAL state exists (files: {})",
        durable_files.join(", ")
    )))
}

fn persist_identity(root: &Path, identity: &IdentityFile) -> TeoDBResult<()> {
    let path = root.join(IDENTITY_FILE);
    let tmp_path = root.join(format!(".{IDENTITY_FILE}.{}.tmp", Uuid::now_v7()));
    let bytes = serde_json::to_vec_pretty(identity)
        .map_err(|error| TeoDBError::wal_source("serialize writer identity", error))?;

    let write_result = (|| -> TeoDBResult<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|error| TeoDBError::wal_source("create writer identity temp file", error))?;
        file.write_all(&bytes)
            .map_err(|error| TeoDBError::wal_source("write writer identity temp file", error))?;
        file.sync_all()
            .map_err(|error| TeoDBError::wal_source("fsync writer identity temp file", error))?;
        std::fs::rename(&tmp_path, &path).map_err(|error| TeoDBError::wal_source("rename writer identity", error))?;
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| TeoDBError::wal_source("fsync WAL root after identity rename", error))?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured(slot: &str) -> WalIdentityConfig {
        WalIdentityConfig {
            cluster_id: Some(ClusterId::from_uuid(
                Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            )),
            node_id: Some(NodeId::new("node-0").unwrap()),
            writer_slot: Some(WriterSlot::new(slot).unwrap()),
        }
    }

    #[test]
    fn mw_t12_restart_preserves_writer_and_advances_epoch() {
        let root = tempfile::tempdir().unwrap();
        let first = WalIdentity::open(root.path(), &configured("slot-0")).unwrap();
        let first = first.resolved();
        let second = WalIdentity::open(root.path(), &configured("slot-0")).unwrap();
        let second = second.resolved();
        assert_eq!(first.writer_id, second.writer_id);
        assert_eq!(second.writer_epoch.get(), first.writer_epoch.get() + 1);
    }

    #[test]
    fn mw_t13_configured_slot_mismatch_is_fatal() {
        let root = tempfile::tempdir().unwrap();
        WalIdentity::open(root.path(), &configured("slot-a")).unwrap();
        let error = WalIdentity::open(root.path(), &configured("slot-b")).unwrap_err();
        assert!(error.to_string().contains("writer slot mismatch"));
    }

    #[test]
    fn nil_cluster_identity_is_rejected_before_persistence() {
        let root = tempfile::tempdir().unwrap();
        let mut config = configured("slot-0");
        config.cluster_id = Some(ClusterId::from_uuid(Uuid::nil()));
        assert!(WalIdentity::open(root.path(), &config).is_err());
        assert!(!root.path().join(IDENTITY_FILE).exists());
    }

    #[test]
    fn observed_catalog_epoch_is_exceeded_durably() {
        let root = tempfile::tempdir().unwrap();
        let mut identity = WalIdentity::open(root.path(), &configured("slot-0")).unwrap();
        let bumped = identity
            .observe_epoch_and_bump(WriterEpoch::new(41))
            .unwrap();
        assert_eq!(bumped.writer_epoch, WriterEpoch::new(42));
        let restarted = WalIdentity::open(root.path(), &configured("slot-0"))
            .unwrap()
            .resolved();
        assert_eq!(restarted.writer_epoch, WriterEpoch::new(43));
    }

    #[test]
    fn corrupt_and_identityless_state_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(IDENTITY_FILE), b"{").unwrap();
        assert!(WalIdentity::open(root.path(), &configured("slot-0")).is_err());

        let identityless = tempfile::tempdir().unwrap();
        std::fs::write(
            identityless
                .path()
                .join("00000000000000000001.wal"),
            b"state",
        )
        .unwrap();
        let error = WalIdentity::open(identityless.path(), &configured("slot-0")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("writer identity is missing")
        );
    }
}
