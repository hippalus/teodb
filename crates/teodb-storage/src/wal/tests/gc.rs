use super::*;

#[tokio::test]
async fn mark_committed_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        max_segment_bytes: 1024 * 1024,
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg).await.unwrap();
    let record = sample_record();
    let key = record.header.table_key().unwrap();
    wal.append(&record).await.unwrap();
    wal.rotate().await.unwrap();

    wal.mark_committed(key, 1).await;

    let deleted = wal.gc().await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn gc_refuses_corrupt_segment() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg).await.unwrap();
    wal.append(&record_with_generation(1))
        .await
        .unwrap();
    wal.append(&record_with_generation(2))
        .await
        .unwrap();
    wal.rotate().await.unwrap();

    corrupt_second_frame(&only_segment(dir.path()));

    wal.mark_committed(table_key(&TableIdent::new("test", "events")), 2)
        .await;
    let deleted = wal.gc().await.unwrap();
    assert_eq!(deleted, 0, "corrupt segment must never be GC'd");
    assert!(only_segment(dir.path()).exists());
}

#[tokio::test]
async fn gc_keeps_tombstone_while_earlier_records_retained() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let dropped = TableIdent::new("test", "dropped");
    let other = TableIdent::new("test", "other");

    let wal = WalManager::open(cfg).await.unwrap();
    wal.append(&record_for_table(&dropped, 1))
        .await
        .unwrap();
    wal.append(&record_for_table(&other, 1))
        .await
        .unwrap();
    wal.rotate().await.unwrap();
    wal.append_drop_tombstone(&dropped).await.unwrap();
    wal.rotate().await.unwrap();

    assert_eq!(wal.gc().await.unwrap(), 0);

    wal.mark_committed(table_key(&other), 1).await;
    assert_eq!(wal.gc().await.unwrap(), 2);

    assert!(
        wal.prepare_replay()
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn gc_reclaims_voided_records_and_their_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let table = TableIdent::new("test", "events");

    let wal = WalManager::open(cfg).await.unwrap();
    wal.append(&record_for_table(&table, 1))
        .await
        .unwrap();
    wal.append(&record_for_table(&table, 2))
        .await
        .unwrap();
    wal.rotate().await.unwrap();
    wal.append_drop_tombstone(&table).await.unwrap();
    wal.rotate().await.unwrap();

    assert_eq!(wal.gc().await.unwrap(), 2);
    assert!(
        wal.prepare_replay()
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .is_empty()
    );
}
