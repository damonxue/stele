//! Postgres round-trip tests.
//!
//! These are ignored by default and require `STELE_TEST_DATABASE_URL`, for
//! example:
//!
//! ```text
//! STELE_TEST_DATABASE_URL=postgres://user:pass@localhost/stele \
//!   cargo test -p stele-sync -- --ignored
//! ```

use stele_sync::interval::{BlockRange, BlockRangeSet};
use stele_sync::{SyncError, SyncStore};
use stele_types::{
    Address, BlockData, BlockRef, CallType, Log, LogRef, ReceiptRef, Topic, Trace, TraceOutcome,
    TraceRef, TransactionRef, B256,
};

fn database_url() -> Option<String> {
    std::env::var("STELE_TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

fn block_data() -> BlockData {
    let block_number = 100;
    let from_a = Address([0xaa; 20]);
    let from_b = Address([0xbb; 20]);
    let to_a = Address([0xcc; 20]);
    BlockData {
        block: Some(BlockRef {
            number: block_number,
            hash: B256([0x11; 32]),
            parent_hash: B256([0x22; 32]),
            timestamp: 1_700_000_000,
        }),
        transactions: vec![
            TransactionRef {
                block_number,
                transaction_index: 0,
                hash: B256([0x01; 32]),
                from: from_a,
                to: Some(to_a),
            },
            TransactionRef {
                block_number,
                transaction_index: 1,
                hash: B256([0x02; 32]),
                from: from_b,
                to: None,
            },
        ],
        receipts: vec![
            ReceiptRef {
                block_number,
                transaction_index: 0,
                from: from_a,
                to: Some(to_a),
                success: true,
            },
            ReceiptRef {
                block_number,
                transaction_index: 1,
                from: from_b,
                to: None,
                success: false,
            },
        ],
        traces: vec![
            TraceRef {
                block_number,
                transaction_index: 0,
                trace_address: Vec::new(),
                trace: Trace {
                    from: from_a,
                    to: Some(to_a),
                    call_type: CallType::Call,
                    input: vec![0x12, 0x34],
                    value: Some([0u8; 32]),
                    output: Some(vec![0x56]),
                    outcome: TraceOutcome::Successful,
                },
            },
            TraceRef {
                block_number,
                transaction_index: 0,
                trace_address: vec![1, 0],
                trace: Trace {
                    from: to_a,
                    to: None,
                    call_type: CallType::Create,
                    input: Vec::new(),
                    value: None,
                    output: None,
                    outcome: TraceOutcome::Reverted,
                },
            },
        ],
        logs: vec![
            LogRef {
                transaction_index: 0,
                log_index: 0,
                log: Log {
                    block_number,
                    address: to_a,
                    topics: vec![Topic([0x09; 32])],
                    data: vec![0xde, 0xad],
                },
            },
            LogRef {
                transaction_index: 1,
                log_index: 1,
                log: Log {
                    block_number,
                    address: from_b,
                    topics: Vec::new(),
                    data: Vec::new(),
                },
            },
        ],
    }
}

fn set(ranges: &[(u64, u64)]) -> BlockRangeSet {
    BlockRangeSet::new(
        ranges
            .iter()
            .map(|&(start, end)| BlockRange::new(start, end).unwrap())
            .collect(),
    )
}

#[tokio::test]
#[ignore = "requires STELE_TEST_DATABASE_URL"]
async fn block_data_intervals_and_factory_addresses_round_trip() {
    let Some(url) = database_url() else {
        eprintln!("STELE_TEST_DATABASE_URL is not set; skipping");
        return;
    };
    let schema = format!("stele_sync_test_{}", std::process::id());
    let store = SyncStore::connect(&url, &schema).await.expect("connect");

    // `initialize` is idempotent.
    store.initialize().await.expect("initialize");
    store.initialize().await.expect("initialize again");

    let data = block_data();
    store.insert_block_data(7, &data).await.expect("insert");
    store
        .insert_block_data(7, &data)
        .await
        .expect("replayed insert is a no-op");

    // Intervals merge into the existing set.
    store
        .insert_intervals(7, "log_7_any_0xaa", &set(&[(10, 20)]))
        .await
        .expect("insert intervals");
    store
        .insert_intervals(7, "log_7_any_0xaa", &set(&[(21, 30), (5, 9)]))
        .await
        .expect("insert intervals");
    assert_eq!(
        store.get_intervals(7, "log_7_any_0xaa").await.unwrap(),
        set(&[(5, 30)])
    );
    assert!(store.get_intervals(7, "missing").await.unwrap().is_empty());

    // The earliest creation block wins for a repeated address.
    let child = Address([0x77; 20]);
    store
        .append_factory_addresses(7, "factory_log_7_parent", &[(child, 50)])
        .await
        .expect("append factory addresses");
    store
        .append_factory_addresses(7, "factory_log_7_parent", &[(child, 40), (child, 60)])
        .await
        .expect("append factory addresses");
    store
        .append_factory_addresses(7, "factory_log_7_parent", &[])
        .await
        .expect("empty append");
    assert_eq!(
        store
            .factory_addresses(7, "factory_log_7_parent")
            .await
            .unwrap(),
        vec![(child, 40)]
    );

    cleanup(&url, &schema).await;
}

#[tokio::test]
#[ignore = "requires STELE_TEST_DATABASE_URL"]
async fn invalid_schema_names_are_rejected_before_connecting() {
    assert!(matches!(
        SyncStore::connect("postgres://localhost/stele", "bad schema").await,
        Err(SyncError::InvalidSchema(_))
    ));
}

async fn cleanup(url: &str, schema: &str) {
    let Ok(pool) = sqlx::PgPool::connect(url).await else {
        return;
    };
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}
