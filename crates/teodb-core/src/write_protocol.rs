//! Multi-writer append protocol identities and metadata codecs.
//!
//! Generation numbers are writer-local. A durable write position is therefore
//! identified by `(table_uuid, writer_id, generation)`, while a logical flush
//! is identified by an exact [`CommitId`].

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{TeoDBError, TeoDBResult};
use crate::ident::{Generation, TableIdent};

pub const WRITE_PROTOCOL_VERSION: u16 = 1;
pub const WRITER_CHECKPOINT_PREFIX: &str = "teodb.writer.v1.";
pub const MAX_WRITER_CHECKPOINT_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_IDENTITY_COMPONENT_BYTES: usize = 128;

pub const COMMIT_ID_PROPERTY: &str = "teodb.commit.id";
pub const WRITER_ID_PROPERTY: &str = "teodb.writer.id";
pub const WRITER_EPOCH_PROPERTY: &str = "teodb.writer.epoch";
pub const GENERATION_MIN_PROPERTY: &str = "teodb.generation.min";
pub const GENERATION_MAX_PROPERTY: &str = "teodb.generation.max";
pub const TABLE_UUID_PROPERTY: &str = "teodb.table.uuid";
pub const PROTOCOL_VERSION_PROPERTY: &str = "teodb.protocol.version";

macro_rules! uuid_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            pub const fn into_uuid(self) -> Uuid {
                self.0
            }

            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_newtype!(ClusterId);
uuid_newtype!(WriterId);
uuid_newtype!(CommitId);

impl WriterId {
    /// Derive a stable writer identity from a cluster UUID and deployment slot.
    pub fn derive(cluster_id: ClusterId, writer_slot: &WriterSlot) -> Self {
        Self(Uuid::new_v5(cluster_id.as_uuid(), writer_slot.as_str().as_bytes()))
    }
}

impl CommitId {
    pub fn now_v7() -> Self {
        Self(Uuid::now_v7())
    }
}

macro_rules! string_newtype {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> TeoDBResult<Self> {
                let value = value.into();
                validate_identity_component($field, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

string_newtype!(NodeId, "cluster.node_id");
string_newtype!(WriterSlot, "cluster.writer_slot");

fn validate_identity_component(field: &str, value: &str) -> TeoDBResult<()> {
    if value.is_empty() {
        return Err(TeoDBError::Config(format!("{field} must not be empty")));
    }
    if value.len() > MAX_IDENTITY_COMPONENT_BYTES {
        return Err(TeoDBError::Config(format!(
            "{field} exceeds {MAX_IDENTITY_COMPONENT_BYTES} bytes"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(TeoDBError::Config(format!(
            "{field} may contain only ASCII letters, digits, '.', '_' and '-'"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WriterEpoch(u64);

impl WriterEpoch {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> TeoDBResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| TeoDBError::Config("writer epoch exhausted".into()))
    }
}

impl fmt::Display for WriterEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub writer_slot: WriterSlot,
    pub writer_id: WriterId,
    pub writer_epoch: WriterEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationRange {
    pub lo: Generation,
    pub hi: Generation,
}

impl GenerationRange {
    pub fn new(lo: Generation, hi: Generation) -> TeoDBResult<Self> {
        if lo == 0 {
            return Err(TeoDBError::InvalidArgument {
                field: "generation_lo".into(),
                message: "generation ranges start at 1".into(),
            });
        }
        if lo > hi {
            return Err(TeoDBError::InvalidArgument {
                field: "generation_range".into(),
                message: format!("lower bound {lo} exceeds upper bound {hi}"),
            });
        }
        Ok(Self { lo, hi })
    }

    pub const fn contains(self, generation: Generation) -> bool {
        generation >= self.lo && generation <= self.hi
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritePosition {
    pub table_uuid: Uuid,
    pub writer_id: WriterId,
    pub generation: Generation,
}

/// Incarnation-aware key for local WAL checkpoints. A WAL root belongs to one
/// writer, so the writer ID is deliberately not repeated here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalTableKey {
    pub table_uuid: Uuid,
    pub ident: TableIdent,
}

impl WalTableKey {
    pub fn new(table_uuid: Uuid, ident: TableIdent) -> Self {
        Self { table_uuid, ident }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendCommitIdentity {
    pub writer_id: WriterId,
    pub writer_epoch: WriterEpoch,
    pub commit_id: CommitId,
    pub generations: GenerationRange,
}

impl AppendCommitIdentity {
    pub fn validate(&self, table: &TableIdent, table_uuid: Uuid) -> TeoDBResult<()> {
        let invalid = if table_uuid.is_nil() {
            Some("table UUID is nil")
        } else if self.writer_id.as_uuid().is_nil() {
            Some("writer ID is nil")
        } else if self.writer_epoch == WriterEpoch::ZERO {
            Some("writer epoch is zero")
        } else if self.commit_id.as_uuid().is_nil() {
            Some("commit ID is nil")
        } else if self.generations.lo == 0 || self.generations.lo > self.generations.hi {
            Some("generation range is invalid")
        } else {
            None
        };
        if let Some(message) = invalid {
            return Err(TeoDBError::WriteProtocol {
                table: table.clone(),
                message: message.into(),
            });
        }
        Ok(())
    }
}

/// The canonical snapshot-summary representation of an exact append identity.
///
/// Both commit publication and exact status resolution consume this function
/// so a property-key change cannot make an already-published append invisible
/// to idempotency checks.
pub fn append_snapshot_identity_properties(
    table_uuid: Uuid,
    identity: &AppendCommitIdentity,
) -> [(&'static str, String); 7] {
    [
        (COMMIT_ID_PROPERTY, identity.commit_id.to_string()),
        (WRITER_ID_PROPERTY, identity.writer_id.to_string()),
        (WRITER_EPOCH_PROPERTY, identity.writer_epoch.to_string()),
        (GENERATION_MIN_PROPERTY, identity.generations.lo.to_string()),
        (GENERATION_MAX_PROPERTY, identity.generations.hi.to_string()),
        (TABLE_UUID_PROPERTY, table_uuid.to_string()),
        (PROTOCOL_VERSION_PROPERTY, WRITE_PROTOCOL_VERSION.to_string()),
    ]
}

/// Match an Iceberg snapshot against an exact append identity.
///
/// A different commit ID is an ordinary non-match. Reusing the requested
/// commit ID with any other tuple component is metadata corruption.
pub fn snapshot_matches_append_identity(
    table: &TableIdent,
    table_uuid: Uuid,
    identity: &AppendCommitIdentity,
    snapshot_id: i64,
    properties: &HashMap<String, String>,
) -> TeoDBResult<bool> {
    let commit_id = identity.commit_id.to_string();
    if properties.get(COMMIT_ID_PROPERTY) != Some(&commit_id) {
        return Ok(false);
    }

    for (key, expected) in append_snapshot_identity_properties(table_uuid, identity) {
        if properties.get(key) != Some(&expected) {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!(
                    "snapshot {} reuses commit ID {} with mismatched {key}",
                    snapshot_id, identity.commit_id
                ),
            });
        }
    }
    Ok(true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterCheckpoint {
    pub version: u16,
    pub epoch: WriterEpoch,
    pub generation: Generation,
    pub commit_id: CommitId,
    pub committed_at_ms: i64,
}

impl WriterCheckpoint {
    pub fn new(epoch: WriterEpoch, generation: Generation, commit_id: CommitId, committed_at_ms: i64) -> Self {
        Self {
            version: WRITE_PROTOCOL_VERSION,
            epoch,
            generation,
            commit_id,
            committed_at_ms,
        }
    }

    pub fn encode(&self) -> TeoDBResult<String> {
        serde_json::to_string(self)
            .map_err(|error| TeoDBError::Internal(format!("serialize writer checkpoint: {error}")))
    }

    pub fn decode(table: &TableIdent, writer_id: WriterId, value: &str) -> TeoDBResult<Self> {
        if value.len() > MAX_WRITER_CHECKPOINT_VALUE_BYTES {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint for {writer_id} exceeds {MAX_WRITER_CHECKPOINT_VALUE_BYTES} bytes"),
            });
        }
        let checkpoint: Self = serde_json::from_str(value).map_err(|error| TeoDBError::MetadataCorruption {
            table: table.clone(),
            message: format!("malformed writer checkpoint for {writer_id}: {error}"),
        })?;
        if checkpoint.version != WRITE_PROTOCOL_VERSION {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!(
                    "unsupported writer checkpoint version {} for {writer_id}",
                    checkpoint.version
                ),
            });
        }
        if checkpoint.epoch == WriterEpoch::ZERO {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint for {writer_id} has zero epoch"),
            });
        }
        if checkpoint.generation == 0 {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint for {writer_id} has zero generation"),
            });
        }
        if checkpoint.commit_id.as_uuid().is_nil() {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint for {writer_id} has a nil commit ID"),
            });
        }
        if checkpoint.committed_at_ms < 0 {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint for {writer_id} has a negative timestamp"),
            });
        }
        Ok(checkpoint)
    }
}

pub fn writer_checkpoint_key(writer_id: WriterId) -> String {
    format!("{WRITER_CHECKPOINT_PREFIX}{writer_id}")
}

pub fn parse_writer_checkpoint(
    table: &TableIdent,
    properties: &HashMap<String, String>,
    writer_id: WriterId,
) -> TeoDBResult<Option<WriterCheckpoint>> {
    properties
        .get(&writer_checkpoint_key(writer_id))
        .map(|value| WriterCheckpoint::decode(table, writer_id, value))
        .transpose()
}

/// Validate every writer-registry entry, including entries belonging to other
/// writers. A malformed foreign checkpoint is still table metadata
/// corruption and must not be ignored by a local writer.
pub fn validate_writer_checkpoints(table: &TableIdent, properties: &HashMap<String, String>) -> TeoDBResult<usize> {
    let mut count = 0;
    for (key, value) in properties
        .iter()
        .filter(|(key, _)| key.starts_with(WRITER_CHECKPOINT_PREFIX))
    {
        count += 1;
        let suffix = key
            .strip_prefix(WRITER_CHECKPOINT_PREFIX)
            .expect("prefix was filtered");
        let writer_uuid = Uuid::parse_str(suffix).map_err(|error| TeoDBError::MetadataCorruption {
            table: table.clone(),
            message: format!("malformed writer checkpoint key '{key}': {error}"),
        })?;
        if writer_uuid.is_nil() || suffix != writer_uuid.to_string() {
            return Err(TeoDBError::MetadataCorruption {
                table: table.clone(),
                message: format!("writer checkpoint key '{key}' is not a canonical, non-nil writer ID"),
            });
        }
        WriterCheckpoint::decode(table, WriterId::from_uuid(writer_uuid), value)?;
    }
    Ok(count)
}

pub fn writer_checkpoint_count(properties: &HashMap<String, String>) -> usize {
    properties
        .keys()
        .filter(|key| key.starts_with(WRITER_CHECKPOINT_PREFIX))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_id_is_stable_and_slot_scoped() {
        let cluster = ClusterId::from_uuid(Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap());
        let a = WriterSlot::new("data-node-0").unwrap();
        let b = WriterSlot::new("data-node-1").unwrap();
        assert_eq!(WriterId::derive(cluster, &a), WriterId::derive(cluster, &a));
        assert_ne!(WriterId::derive(cluster, &a), WriterId::derive(cluster, &b));
        assert_eq!(
            WriterId::derive(cluster, &a)
                .into_uuid()
                .get_version_num(),
            5
        );
    }

    #[test]
    fn identity_components_reject_path_syntax() {
        assert!(WriterSlot::new("../other").is_err());
        assert!(NodeId::new("node/one").is_err());
        assert!(WriterSlot::new("data-node_0.example").is_ok());
    }

    #[test]
    fn generation_range_is_validated() {
        assert!(GenerationRange::new(0, 1).is_err());
        assert!(GenerationRange::new(2, 1).is_err());
        assert_eq!(GenerationRange::new(2, 4).unwrap(), GenerationRange { lo: 2, hi: 4 });
    }

    #[test]
    fn writer_checkpoint_roundtrips() {
        let table = TableIdent::new("analytics", "events");
        let writer_id = WriterId::from_uuid(Uuid::now_v7());
        let checkpoint = WriterCheckpoint::new(WriterEpoch::new(7), 42, CommitId::now_v7(), 1_785_360_000_000);
        assert_eq!(
            WriterCheckpoint::decode(&table, writer_id, &checkpoint.encode().unwrap()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn writer_checkpoint_rejects_semantically_invalid_values() {
        let table = TableIdent::new("analytics", "events");
        let writer_id = WriterId::from_uuid(Uuid::now_v7());
        let commit_id = CommitId::now_v7();
        for value in [
            serde_json::json!({
                "version": WRITE_PROTOCOL_VERSION,
                "epoch": 0,
                "generation": 1,
                "commit_id": commit_id,
                "committed_at_ms": 1
            }),
            serde_json::json!({
                "version": WRITE_PROTOCOL_VERSION,
                "epoch": 1,
                "generation": 0,
                "commit_id": commit_id,
                "committed_at_ms": 1
            }),
            serde_json::json!({
                "version": WRITE_PROTOCOL_VERSION,
                "epoch": 1,
                "generation": 1,
                "commit_id": Uuid::nil(),
                "committed_at_ms": 1
            }),
            serde_json::json!({
                "version": WRITE_PROTOCOL_VERSION,
                "epoch": 1,
                "generation": 1,
                "commit_id": commit_id,
                "committed_at_ms": -1
            }),
        ] {
            assert!(WriterCheckpoint::decode(&table, writer_id, &value.to_string(),).is_err());
        }
    }
}
