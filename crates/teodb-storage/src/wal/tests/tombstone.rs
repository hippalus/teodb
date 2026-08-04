use super::*;

#[tokio::test]
async fn tombstone_voids_earlier_records_but_not_later_ones() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let table = TableIdent::new("test", "events");

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    for generation in 1..=3 {
        wal.append(&record_for_table(&table, generation))
            .await
            .unwrap();
    }
    wal.append_drop_tombstone(&table).await.unwrap();
    wal.append(&record_for_table(&table, 1))
        .await
        .unwrap();
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
    assert_eq!(records.len(), 1, "only the post-tombstone record survives");
    assert_eq!(records[0].header.generation, 1);
}

#[tokio::test]
async fn tombstone_voids_records_in_earlier_segments() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let table = TableIdent::new("test", "events");

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    wal.append(&record_for_table(&table, 1))
        .await
        .unwrap();
    wal.rotate().await.unwrap();
    wal.append_drop_tombstone(&table).await.unwrap();
    wal.release_lease().await;
    drop(wal);

    let wal2 = WalManager::open(cfg).await.unwrap();
    assert!(
        wal2.prepare_replay()
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn tombstone_clears_committed_cutoff_for_recreated_table() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let table = TableIdent::new("test", "events");

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    let old_record = record_for_table(&table, 5);
    let old_key = old_record.header.table_key().unwrap();
    wal.append(&old_record).await.unwrap();
    wal.mark_committed(old_key.clone(), 5).await;
    wal.append_drop_tombstone(&table).await.unwrap();
    assert_eq!(wal.committed_generation(&old_key).await, None);

    let mut first_new = record_for_table(&table, 1);
    first_new.header.table_uuid = Some(uuid::Uuid::from_u128(2));
    let new_key = first_new.header.table_key().unwrap();
    let mut second_new = record_for_table(&table, 2);
    second_new.header.table_uuid = Some(uuid::Uuid::from_u128(2));
    wal.append(&first_new).await.unwrap();
    wal.append(&second_new).await.unwrap();
    wal.release_lease().await;
    drop(wal);

    let wal2 = WalManager::open(cfg).await.unwrap();
    assert_eq!(
        wal2.committed_generation(&new_key).await,
        None,
        "cutoff reset survives restart"
    );
    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let generations: Vec<Generation> = records
        .iter()
        .map(|r| r.header.generation)
        .collect();
    assert_eq!(generations, vec![1, 2], "new incarnation's records replay");
}
