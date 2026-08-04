use ahash::AHashMap;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::Generation;
use teodb_core::write_protocol::WalTableKey;

use super::WalManager;

impl WalManager {
    pub(super) async fn acquire_lease(lease_path: &std::path::Path) -> TeoDBResult<std::fs::File> {
        let lease_path = lease_path.to_path_buf();
        tokio::task::spawn_blocking(move || Self::acquire_lease_blocking(&lease_path))
            .await
            .map_err(|e| TeoDBError::wal(format!("WAL lease task failed: {e}")))?
    }

    fn acquire_lease_blocking(lease_path: &std::path::Path) -> TeoDBResult<std::fs::File> {
        use fs2::FileExt;
        use std::io::Write;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Do not truncate on open: another live process may hold this lease.
            // We only overwrite (set_len(0) + write) after acquiring the lock.
            .truncate(false)
            .open(lease_path)
            .map_err(|e| TeoDBError::wal_source(format!("failed to open WAL lease file: {e}"), e))?;

        if let Err(error) = file.try_lock_exclusive() {
            return Err(if error.kind() == std::io::ErrorKind::WouldBlock {
                TeoDBError::wal("WAL directory is locked by another process")
            } else {
                TeoDBError::wal_source(format!("failed to lock WAL lease file: {error}"), error)
            });
        }

        let meta = serde_json::json!({
            "pid": std::process::id(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "lock": "flock",
        });
        let payload = meta.to_string();
        file.set_len(0)
            .map_err(|e| TeoDBError::wal_source(format!("failed to truncate WAL lease file: {e}"), e))?;
        file.write_all(payload.as_bytes())
            .map_err(|e| TeoDBError::wal_source(format!("failed to write WAL lease file: {e}"), e))?;
        file.sync_all()
            .map_err(|e| TeoDBError::wal_source(format!("failed to fsync WAL lease file: {e}"), e))?;
        Ok(file)
    }

    pub(super) fn unlock_lease(file: &std::fs::File) -> std::io::Result<()> {
        fs2::FileExt::unlock(file)
    }

    pub(super) async fn scan_next_seq(root: &std::path::Path) -> TeoDBResult<u64> {
        let mut max_seq: u64 = 0;
        let mut entries = tokio::fs::read_dir(root)
            .await
            .map_err(|e| TeoDBError::wal_source(format!("readdir: {e}"), e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| TeoDBError::wal_source(format!("readdir: {e}"), e))?
        {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(seq) = Self::parse_seq(&name_str) {
                max_seq = max_seq.max(seq);
            }
        }
        Ok(max_seq + 1)
    }

    pub(super) fn parse_seq(filename: &str) -> Option<u64> {
        filename
            .strip_suffix(".wal")
            .and_then(|s| s.parse::<u64>().ok())
    }

    const COMMITTED_CHECKPOINT_FILE: &'static str = "committed.json";

    /// Load the incarnation-aware committed-generation checkpoint from disk.
    /// Corrupt state fails closed; silently dropping a cutoff could replay
    /// already committed data.
    pub(super) async fn load_committed_checkpoint(
        root: &std::path::Path,
    ) -> TeoDBResult<AHashMap<WalTableKey, Generation>> {
        let path = root.join(Self::COMMITTED_CHECKPOINT_FILE);
        let data = match tokio::fs::read_to_string(&path).await {
            Ok(d) => d,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AHashMap::new());
            }
            Err(error) => {
                return Err(TeoDBError::wal_source("read committed checkpoint", error));
            }
        };

        const CHECKPOINT_VERSION: u16 = 1;

        #[derive(serde::Deserialize)]
        struct Entry {
            namespace: String,
            name: String,
            table_uuid: uuid::Uuid,
            generation: Generation,
        }

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Checkpoint {
            version: u16,
            entries: Vec<Entry>,
        }

        let checkpoint: Checkpoint = serde_json::from_str(&data)
            .map_err(|error| TeoDBError::wal_source("committed checkpoint is corrupt", error))?;
        if checkpoint.version != CHECKPOINT_VERSION {
            return Err(TeoDBError::wal(format!(
                "unsupported committed checkpoint version {}",
                checkpoint.version
            )));
        }

        let mut committed = AHashMap::with_capacity(checkpoint.entries.len());
        for entry in checkpoint.entries {
            if entry.table_uuid.is_nil() {
                return Err(TeoDBError::wal("committed checkpoint contains a nil table UUID"));
            }
            if entry.namespace.is_empty() || entry.name.is_empty() {
                return Err(TeoDBError::wal(
                    "committed checkpoint contains an empty table identifier",
                ));
            }
            let key = WalTableKey::new(
                entry.table_uuid,
                teodb_core::ident::TableIdent::new(entry.namespace, entry.name),
            );
            if committed.insert(key, entry.generation).is_some() {
                return Err(TeoDBError::wal(
                    "committed checkpoint contains a duplicate table incarnation",
                ));
            }
        }
        Ok(committed)
    }

    /// Persist committed generations to disk atomically (temp + rename).
    pub(super) async fn persist_committed_checkpoint(
        root: &std::path::Path,
        committed: &AHashMap<WalTableKey, Generation>,
    ) -> TeoDBResult<()> {
        const CHECKPOINT_VERSION: u16 = 1;

        #[derive(serde::Serialize)]
        struct Entry<'a> {
            namespace: &'a str,
            name: &'a str,
            table_uuid: uuid::Uuid,
            generation: Generation,
        }

        let mut entries: Vec<Entry<'_>> = committed
            .iter()
            .map(|(key, &committed_gen)| Entry {
                namespace: &key.ident.namespace,
                name: &key.ident.name,
                table_uuid: key.table_uuid,
                generation: committed_gen,
            })
            .collect();
        entries.sort_unstable_by(|left, right| {
            (left.namespace, left.name, left.table_uuid.as_bytes()).cmp(&(
                right.namespace,
                right.name,
                right.table_uuid.as_bytes(),
            ))
        });

        #[derive(serde::Serialize)]
        struct Checkpoint<'a> {
            version: u16,
            entries: Vec<Entry<'a>>,
        }

        let json = serde_json::to_string_pretty(&Checkpoint {
            version: CHECKPOINT_VERSION,
            entries,
        })
        .map_err(|e| TeoDBError::wal_source(format!("serialize checkpoint: {e}"), e))?;

        let path = root.join(Self::COMMITTED_CHECKPOINT_FILE);
        let tmp_path = path.with_extension("json.tmp");

        tokio::fs::write(&tmp_path, &json)
            .await
            .map_err(|e| TeoDBError::wal_source(format!("write checkpoint: {e}"), e))?;

        // fsync the temp file before rename for durability.
        let file = tokio::fs::File::open(&tmp_path)
            .await
            .map_err(|e| TeoDBError::wal_source(format!("open checkpoint for fsync: {e}"), e))?;
        file.sync_all()
            .await
            .map_err(|e| TeoDBError::wal_source(format!("fsync checkpoint: {e}"), e))?;

        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| TeoDBError::wal_source(format!("rename checkpoint: {e}"), e))?;

        let directory = tokio::fs::File::open(root)
            .await
            .map_err(|e| TeoDBError::wal_source("open WAL root for checkpoint fsync", e))?;
        directory
            .sync_all()
            .await
            .map_err(|e| TeoDBError::wal_source("fsync WAL root after checkpoint rename", e))?;

        Ok(())
    }
}
