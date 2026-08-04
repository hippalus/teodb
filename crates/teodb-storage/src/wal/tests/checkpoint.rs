use super::*;

#[tokio::test]
async fn seed_committed_overwrites_stale_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let table = TableIdent::new("test", "events");
    let key = table_key(&table);

    let wal = WalManager::open(cfg).await.unwrap();
    wal.mark_committed(key.clone(), 7).await;
    wal.mark_committed(key.clone(), 3).await;
    assert_eq!(wal.committed_generation(&key).await, Some(7));
    wal.seed_committed(key.clone(), 3).await;
    assert_eq!(wal.committed_generation(&key).await, Some(3));
}

#[tokio::test]
async fn committed_checkpoint_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    let record = sample_record();
    let key = record.header.table_key().unwrap();
    wal.append(&record).await.unwrap();
    wal.mark_committed(key.clone(), 1).await;

    let checkpoint_path = dir.path().join("committed.json");
    assert!(checkpoint_path.exists(), "committed.json should be created");

    let committed = wal.committed_generation(&key).await;
    assert_eq!(committed, Some(1));
    wal.release_lease().await;
    drop(wal);

    let wal2 = WalManager::open(cfg).await.unwrap();
    let committed2 = wal2.committed_generation(&key).await;
    assert_eq!(committed2, Some(1), "checkpoint should survive restart");

    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(records.is_empty(), "committed record should not replay");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_checkpoint_persistence_never_regresses() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let key = table_key(&TableIdent::new("test", "events"));
    let wal = std::sync::Arc::new(WalManager::open(cfg.clone()).await.unwrap());
    let tasks = (1..=32)
        .rev()
        .map(|generation| {
            let wal = wal.clone();
            let key = key.clone();
            tokio::spawn(async move {
                wal.mark_committed(key, generation).await;
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(wal.committed_generation(&key).await, Some(32));
    wal.release_lease().await;
    drop(wal);

    let reopened = WalManager::open(cfg).await.unwrap();
    assert_eq!(
        reopened.committed_generation(&key).await,
        Some(32),
        "durable checkpoint must retain the highest concurrent cutoff"
    );
}

#[tokio::test]
async fn malformed_incarnation_checkpoint_fails_startup() {
    for invalid_entries in [
        serde_json::json!([{
            "namespace": "test",
            "name": "events",
            "table_uuid": uuid::Uuid::nil(),
            "generation": 1
        }]),
        serde_json::json!([
            {
                "namespace": "test",
                "name": "events",
                "table_uuid": uuid::Uuid::from_u128(1),
                "generation": 1
            },
            {
                "namespace": "test",
                "name": "events",
                "table_uuid": uuid::Uuid::from_u128(1),
                "generation": 2
            }
        ]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = WalConfig {
            root_dir: dir.path().to_path_buf(),
            fsync_on_append: false,
            ..Default::default()
        };
        let wal = WalManager::open(cfg.clone()).await.unwrap();
        wal.release_lease().await;
        drop(wal);
        std::fs::write(
            dir.path().join("committed.json"),
            serde_json::json!({
                "version": 1,
                "entries": invalid_entries,
            })
            .to_string(),
        )
        .unwrap();
        assert!(WalManager::open(cfg).await.is_err());
    }
}
