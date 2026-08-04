use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::catalog::CommitAppend;
use teodb_core::write_protocol::{
    COMMIT_ID_PROPERTY, GENERATION_MAX_PROPERTY, GENERATION_MIN_PROPERTY, PROTOCOL_VERSION_PROPERTY,
    TABLE_UUID_PROPERTY, WRITER_EPOCH_PROPERTY, WRITER_ID_PROPERTY, WriterCheckpoint,
    append_snapshot_identity_properties, writer_checkpoint_key,
};

pub(super) const ENGINE_NAME: &str = "engine-name";
pub(super) const ENGINE_VERSION: &str = "engine-version";

const RESERVED_KEYS: [&str; 9] = [
    COMMIT_ID_PROPERTY,
    WRITER_ID_PROPERTY,
    WRITER_EPOCH_PROPERTY,
    GENERATION_MIN_PROPERTY,
    GENERATION_MAX_PROPERTY,
    TABLE_UUID_PROPERTY,
    PROTOCOL_VERSION_PROPERTY,
    ENGINE_NAME,
    ENGINE_VERSION,
];

pub(super) fn snapshot_properties(request: &CommitAppend) -> TeoDBResult<HashMap<String, String>> {
    for key in request.properties.keys() {
        if RESERVED_KEYS.contains(&key.as_str()) || key.starts_with("teodb.") {
            return Err(TeoDBError::InvalidArgument {
                field: "properties".into(),
                message: format!("reserved snapshot property '{key}' cannot be overridden"),
            });
        }
    }

    let mut properties = request.properties.clone();
    properties.extend(
        append_snapshot_identity_properties(request.table_uuid, &request.identity)
            .map(|(key, value)| (key.into(), value)),
    );
    properties.extend([
        (ENGINE_NAME.into(), "teodb".into()),
        (ENGINE_VERSION.into(), env!("CARGO_PKG_VERSION").into()),
    ]);
    Ok(properties)
}

pub(super) fn checkpoint_property(request: &CommitAppend) -> TeoDBResult<(String, String)> {
    let committed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| TeoDBError::Internal(format!("system clock before UNIX epoch: {error}")))?
        .as_millis()
        .try_into()
        .map_err(|_| TeoDBError::Internal("commit timestamp exceeds i64".into()))?;
    let checkpoint = WriterCheckpoint::new(
        request.identity.writer_epoch,
        request.identity.generations.hi,
        request.identity.commit_id,
        committed_at_ms,
    );
    Ok((writer_checkpoint_key(request.identity.writer_id), checkpoint.encode()?))
}
