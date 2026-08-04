use std::sync::Arc;

use super::*;

#[tokio::test]
async fn append_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    let record = sample_record();
    wal.append(&record).await.unwrap();
    drop(wal);

    let wal2 = WalManager::open(cfg).await.unwrap();
    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].header.table, TableIdent::new("test", "events"));
    assert_eq!(records[0].batch.num_rows(), 3);
}

#[tokio::test]
async fn open_rejects_concurrent_wal_owner() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    let err = match WalManager::open(cfg.clone()).await {
        Ok(_) => panic!("concurrent WAL open unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("locked"),
        "unexpected concurrent open error: {err}"
    );

    drop(wal);
    let reopened = WalManager::open(cfg).await.unwrap();
    drop(reopened);
}

#[tokio::test]
async fn check_capacity_backpressure() {
    let dir = tempfile::tempdir().unwrap();

    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        soft_watermark_bytes: 512,
        hard_cap_bytes: 2048,
        ..Default::default()
    };
    let wal = WalManager::open(cfg).await.unwrap();
    let dummy_path = dir.path().join("dummy.wal");
    tokio::fs::write(&dummy_path, vec![0u8; 1024])
        .await
        .unwrap();
    let ok = wal.check_capacity().await.unwrap();
    assert!(!ok, "should signal backpressure (above soft watermark)");
    drop(wal);

    let cfg2 = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        soft_watermark_bytes: 256,
        hard_cap_bytes: 512,
        ..Default::default()
    };
    let wal2 = WalManager::open(cfg2).await.unwrap();
    assert!(
        wal2.check_capacity().await.is_err(),
        "should reject writes above hard cap"
    );
    drop(wal2);

    let cfg3 = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        soft_watermark_bytes: 4096,
        hard_cap_bytes: 8192,
        ..Default::default()
    };
    let wal3 = WalManager::open(cfg3).await.unwrap();
    let ok3 = wal3.check_capacity().await.unwrap();
    assert!(ok3, "should be under soft watermark");
}

#[tokio::test]
async fn disk_usage_includes_prepared_sidecar_directory() {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalManager::open(WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    })
    .await
    .unwrap();
    let before = wal.disk_usage_bytes().await.unwrap();
    let nested = dir.path().join("prepared");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("fixture.bin"), vec![0u8; 4096]).unwrap();

    let after = wal.disk_usage_bytes().await.unwrap();
    assert!(after >= before + 4096);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_all_replayed_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 64 * 1024,
        fsync_on_append: true,
        ..Default::default()
    };

    let wal = Arc::new(WalManager::open(cfg.clone()).await.unwrap());
    let tasks: Vec<_> = (0..16u64)
        .map(|t| {
            let wal = wal.clone();
            tokio::spawn(async move {
                for i in 0..16u64 {
                    wal.append(&record_with_generation(t * 16 + i + 1))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    wal.release_lease().await;
    drop(wal);

    let wal2 = WalManager::open(cfg).await.unwrap();
    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(records.len(), 256);

    let mut generations: Vec<Generation> = records
        .iter()
        .map(|r| r.header.generation)
        .collect();
    generations.sort_unstable();
    generations.dedup();
    assert_eq!(generations.len(), 256, "every append replayed exactly once");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_failure_fails_waiters_and_writer_recovers() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1,
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = Arc::new(WalManager::open(cfg).await.unwrap());
    wal.append(&record_with_generation(1))
        .await
        .unwrap();

    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

    let records: Vec<WalRecord> = (2..=5).map(record_with_generation).collect();
    let (r1, r2, r3, r4) = tokio::join!(
        wal.append(&records[0]),
        wal.append(&records[1]),
        wal.append(&records[2]),
        wal.append(&records[3]),
    );
    for result in [r1, r2, r3, r4] {
        assert!(result.is_err(), "append into read-only WAL dir must fail");
    }

    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    wal.append(&record_with_generation(6))
        .await
        .unwrap();

    let records = wal
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut generations: Vec<Generation> = records
        .iter()
        .map(|r| r.header.generation)
        .collect();
    generations.sort_unstable();
    assert_eq!(generations, vec![1, 6], "only acked appends are durable");
}
