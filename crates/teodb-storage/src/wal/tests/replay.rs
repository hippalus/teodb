use super::*;

#[tokio::test]
async fn replay_plan_orders_out_of_order_appends_by_generation_then_batch_id() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg).await.unwrap();
    let mut generation_2 = record_with_generation(2);
    generation_2.header.batch_id = uuid::Uuid::from_u128(2);
    let mut generation_1_later_batch = record_with_generation(1);
    generation_1_later_batch.header.batch_id = uuid::Uuid::from_u128(11);
    let mut generation_1_earlier_batch = record_with_generation(1);
    generation_1_earlier_batch.header.batch_id = uuid::Uuid::from_u128(10);

    // Generation reservation happens before the asynchronous WAL append, so
    // this is a valid durable order for concurrent requests to the same table.
    wal.append(&generation_2).await.unwrap();
    wal.append(&generation_1_later_batch)
        .await
        .unwrap();
    wal.append(&generation_1_earlier_batch)
        .await
        .unwrap();

    let mut plan = wal.prepare_replay_all().await.unwrap();
    let mut order = Vec::new();
    while let Some(record) = plan.next_record().await.unwrap() {
        order.push((record.header.generation, record.header.batch_id));
    }
    assert_eq!(
        order,
        vec![
            (1, uuid::Uuid::from_u128(10)),
            (1, uuid::Uuid::from_u128(11)),
            (2, uuid::Uuid::from_u128(2)),
        ],
        "bounded replay must preserve canonical generation/batch ordering"
    );
}

#[tokio::test]
async fn replay_order_respects_tombstone_incarnations_and_stable_key_ties() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };
    let events = TableIdent::new("test", "events");
    let audit = TableIdent::new("test", "audit");
    let wal = WalManager::open(cfg).await.unwrap();

    let mut old_incarnation = record_for_table(&events, 1);
    old_incarnation.header.batch_id = uuid::Uuid::from_u128(1);
    wal.append(&old_incarnation).await.unwrap();
    wal.append_drop_tombstone(&events).await.unwrap();

    let middle_uuid = uuid::Uuid::from_u128(2);
    let mut middle_incarnation = record_for_table(&events, 1);
    middle_incarnation.header.table_uuid = Some(middle_uuid);
    middle_incarnation.header.batch_id = uuid::Uuid::from_u128(2);
    wal.append(&middle_incarnation).await.unwrap();
    wal.append_drop_tombstone(&events).await.unwrap();

    let new_uuid = uuid::Uuid::from_u128(3);
    let tied_batch_id = uuid::Uuid::from_u128(10);
    let mut generation_2 = record_for_table(&events, 2);
    generation_2.header.table_uuid = Some(new_uuid);
    generation_2.header.batch_id = uuid::Uuid::from_u128(20);
    let mut generation_1 = record_for_table(&events, 1);
    generation_1.header.table_uuid = Some(new_uuid);
    generation_1.header.batch_id = tied_batch_id;
    let mut tied_other_table = record_for_table(&audit, 1);
    tied_other_table.header.batch_id = tied_batch_id;

    wal.append(&generation_2).await.unwrap();
    wal.append(&generation_1).await.unwrap();
    wal.append(&tied_other_table).await.unwrap();

    let records = wal
        .prepare_replay_all()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let order: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.header.table.clone(),
                record.header.table_uuid,
                record.header.generation,
                record.header.batch_id,
            )
        })
        .collect();
    assert_eq!(
        order,
        vec![
            (events.clone(), Some(new_uuid), 1, tied_batch_id),
            (audit, Some(uuid::Uuid::from_u128(1)), 1, tied_batch_id),
            (events, Some(new_uuid), 2, uuid::Uuid::from_u128(20),),
        ],
        "the tombstoned UUID must stay absent and exact sort-key ties must retain physical order"
    );
}

#[tokio::test]
async fn replay_plan_keeps_one_live_decoded_record() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        max_segment_bytes: 1024,
        ..Default::default()
    };

    let wal = WalManager::open(cfg).await.unwrap();
    for generation in 1..=64 {
        wal.append(&record_with_generation(generation))
            .await
            .unwrap();
    }

    let mut plan = wal.prepare_replay_all().await.unwrap();
    assert_eq!(plan.record_count(), 64);
    let mut generations = Vec::new();
    while let Some(record) = plan.next_record().await.unwrap() {
        generations.push(record.header.generation);
    }
    assert_eq!(generations, (1..=64).collect::<Vec<_>>());
    assert_eq!(
        plan.peak_live_decoded_records(),
        1,
        "the iterator layer must release each decoded record before decoding the next"
    );

    assert_eq!(generations.len(), 64);
}

#[tokio::test]
async fn replay_plan_rejects_a_segment_snapshot_change() {
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
    let mut plan = wal.prepare_replay_all().await.unwrap();

    wal.append(&record_with_generation(2))
        .await
        .unwrap();
    let error = plan
        .next_record()
        .await
        .expect_err("a changed segment must not be replayed from a stale plan");
    assert!(
        error.to_string().contains("snapshot mismatch"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn replay_fail_mode_errors_on_mid_segment_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    for generation in 1..=3 {
        wal.append(&record_with_generation(generation))
            .await
            .unwrap();
    }
    wal.release_lease().await;
    drop(wal);

    corrupt_second_frame(&only_segment(dir.path()));

    let wal2 = WalManager::open(cfg).await.unwrap();
    let err = match wal2.prepare_replay().await {
        Ok(_) => panic!("corrupt WAL unexpectedly produced a replay plan"),
        Err(error) => error,
    };
    assert!(err.to_string().contains("corrupt WAL frame"), "unexpected error: {err}");
}

#[tokio::test]
async fn replay_salvage_mode_quarantines_and_keeps_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        recovery_mode: WalRecoveryMode::Salvage,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    for generation in 1..=3 {
        wal.append(&record_with_generation(generation))
            .await
            .unwrap();
    }
    wal.release_lease().await;
    drop(wal);

    corrupt_second_frame(&only_segment(dir.path()));

    let wal2 = WalManager::open(cfg).await.unwrap();
    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(records.len(), 1, "only the frame before the corruption survives");
    assert_eq!(records[0].header.generation, 1);

    let names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names.iter().any(|n| n.ends_with(".wal")),
        "corrupt segment must not stay live: {names:?}"
    );
    assert!(
        names.iter().any(|n| n.ends_with(".wal.corrupt")),
        "quarantined segment expected: {names:?}"
    );

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
async fn replay_tolerates_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        root_dir: dir.path().to_path_buf(),
        fsync_on_append: false,
        ..Default::default()
    };

    let wal = WalManager::open(cfg.clone()).await.unwrap();
    wal.append(&record_with_generation(1))
        .await
        .unwrap();
    wal.append(&record_with_generation(2))
        .await
        .unwrap();
    wal.release_lease().await;
    drop(wal);

    let path = only_segment(dir.path());
    let mut data = std::fs::read(&path).unwrap();
    let first_len = match segment::decode_frame(&data) {
        FrameDecode::Complete(_, consumed) => consumed,
        other => panic!("expected complete first frame, got {other:?}"),
    };
    data.truncate(first_len + (data.len() - first_len) / 2);
    std::fs::write(&path, &data).unwrap();

    let wal2 = WalManager::open(cfg).await.unwrap();
    let records = wal2
        .prepare_replay()
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].header.generation, 1);
}
