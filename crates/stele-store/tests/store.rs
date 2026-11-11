use stele_checkpoint::Checkpoint;
use stele_schema::{ColumnDef, ColumnType, ScalarType, SchemaDef, TableDef};
use stele_store::{CellValue, MemoryStore, PostgresStore, Row, Store, StoreError, TableRef};
use stele_types::Address;

fn checkpoint(block: u64) -> Checkpoint {
    Checkpoint {
        block_timestamp: block,
        chain_id: 1,
        block_number: block,
        ..Checkpoint::ZERO
    }
}

fn account_table(schema: &str) -> TableDef {
    TableDef {
        schema: schema.to_owned(),
        name: "account".to_owned(),
        columns: vec![
            ColumnDef::new("owner", ScalarType::Text, false),
            ColumnDef::new("id", ScalarType::BigInt, false),
            ColumnDef::new("balance", ScalarType::EVM_INTEGER, false),
            ColumnDef::new("active", ScalarType::Boolean, false),
            ColumnDef::new("tags", ColumnType::Array(ScalarType::Text), true),
        ],
        primary_key: vec!["owner".to_owned(), "id".to_owned()],
        indexes: Vec::new(),
    }
}

fn metrics_table() -> TableDef {
    TableDef {
        schema: "app".to_owned(),
        name: "metric".to_owned(),
        columns: vec![
            ColumnDef::new("owner", ScalarType::Text, false),
            ColumnDef::new("epoch", ScalarType::BigInt, false),
            ColumnDef::new("small", ScalarType::SmallInt, true),
            ColumnDef::new("ratio", ScalarType::Real, true),
            ColumnDef::new("precise", ScalarType::DoublePrecision, true),
            ColumnDef::new("blob", ScalarType::Bytes, true),
            ColumnDef::new("seen_at", ScalarType::Timestamp, true),
            ColumnDef::new("amounts", ColumnType::Array(ScalarType::EVM_INTEGER), true),
        ],
        primary_key: vec!["owner".to_owned(), "epoch".to_owned()],
        indexes: vec![stele_schema::IndexDef::new(
            "metric_owner_idx",
            vec!["owner".to_owned()],
            false,
        )],
    }
}

fn schema() -> SchemaDef {
    SchemaDef {
        tables: vec![
            account_table("app"),
            account_table("audit"),
            metrics_table(),
        ],
        enums: Vec::new(),
        views: Vec::new(),
    }
}

fn table(schema: &str) -> TableRef {
    TableRef::new(schema, "account")
}

fn row(pairs: &[(&str, CellValue)]) -> Row {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn key(owner: &str, id: i64) -> Row {
    row(&[
        ("owner", CellValue::Text(owner.to_owned())),
        ("id", CellValue::Int(id)),
    ])
}

fn account(balance: i64, active: bool) -> Row {
    row(&[
        ("balance", CellValue::Numeric(balance.to_string())),
        ("active", CellValue::Bool(active)),
        (
            "tags",
            CellValue::Array(vec![CellValue::Text("example".to_owned())]),
        ),
    ])
}

fn balance(row: &Row) -> Option<&CellValue> {
    row.get("balance")
}

#[tokio::test]
async fn memory_insert_update_upsert_delete_and_cache() {
    let store = MemoryStore::new(schema());
    let app = table("app");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    assert!(batch
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .is_none());
    batch
        .insert(&app, &key("alice", 1), &account(10, true))
        .await
        .expect("insert");

    let cached = batch
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .expect("cached row");
    assert_eq!(balance(&cached), Some(&CellValue::Numeric("10".to_owned())));
    assert_eq!(
        cached.get("owner"),
        Some(&CellValue::Text("alice".to_owned()))
    );

    // Uncommitted rows stay invisible outside the batch.
    assert!(store
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .is_none());
    assert!(store.progress(1).await.expect("progress").is_none());

    batch.commit().await.expect("commit");
    assert_eq!(
        store.progress(1).await.expect("progress"),
        Some(stele_store::Progress {
            checkpoint: checkpoint(1),
            finalized: Checkpoint::ZERO,
        })
    );
    let committed = store
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .expect("committed row");
    assert_eq!(committed.get("active"), Some(&CellValue::Bool(true)));

    let batch = store.begin(1, checkpoint(2)).await.expect("begin");
    batch
        .update(
            &app,
            &key("alice", 1),
            &row(&[("balance", CellValue::Numeric("7".to_owned()))]),
        )
        .await
        .expect("update");
    batch
        .upsert(&app, &key("bob", 2), &account(1, false))
        .await
        .expect("upsert");
    batch.delete(&app, &key("alice", 1)).await.expect("delete");
    assert!(batch
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .is_none());
    batch.commit().await.expect("commit");

    assert!(store
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .is_none());
    let bob = store
        .find(&app, &key("bob", 2))
        .await
        .expect("find")
        .expect("bob");
    assert_eq!(
        bob.get("balance"),
        Some(&CellValue::Numeric("1".to_owned()))
    );
}

#[tokio::test]
async fn memory_rollback_discards_writes() {
    let store = MemoryStore::new(schema());
    let app = table("app");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    batch
        .insert(&app, &key("alice", 1), &account(10, true))
        .await
        .expect("insert");
    batch.rollback().await.expect("rollback");

    assert!(store.progress(1).await.expect("progress").is_none());
    assert!(store
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .is_none());

    // The checkpoint is still available because nothing advanced.
    let batch = store.begin(1, checkpoint(1)).await.expect("begin again");
    batch.commit().await.expect("commit");
}

#[tokio::test]
async fn memory_revert_restores_each_schema_in_reverse_order() {
    let store = MemoryStore::new(schema());
    let app = table("app");
    let audit = table("audit");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    batch
        .insert(&app, &key("alice", 1), &account(10, true))
        .await
        .expect("insert app");
    batch
        .insert(&audit, &key("alice", 1), &account(100, false))
        .await
        .expect("insert audit");
    batch.commit().await.expect("commit");

    let batch = store.begin(1, checkpoint(2)).await.expect("begin");
    batch
        .update(&app, &key("alice", 1), &account(20, true))
        .await
        .expect("update app");
    batch
        .delete(&audit, &key("alice", 1))
        .await
        .expect("delete audit");
    batch
        .insert(&app, &key("bob", 2), &account(5, true))
        .await
        .expect("insert bob");
    batch.commit().await.expect("commit");

    let batch = store.begin(1, checkpoint(3)).await.expect("begin");
    batch
        .update(&app, &key("bob", 2), &account(6, true))
        .await
        .expect("update bob");
    batch.commit().await.expect("commit");

    store.revert(1, &checkpoint(1)).await.expect("revert");

    let alice = store
        .find(&app, &key("alice", 1))
        .await
        .expect("find")
        .expect("alice restored");
    assert_eq!(balance(&alice), Some(&CellValue::Numeric("10".to_owned())));
    let audited = store
        .find(&audit, &key("alice", 1))
        .await
        .expect("find")
        .expect("same-named table restored");
    assert_eq!(
        balance(&audited),
        Some(&CellValue::Numeric("100".to_owned()))
    );
    assert!(store
        .find(&app, &key("bob", 2))
        .await
        .expect("find")
        .is_none());
    assert_eq!(
        store
            .progress(1)
            .await
            .expect("progress")
            .map(|p| p.checkpoint),
        Some(checkpoint(1))
    );

    // Checkpoints after the target were removed.
    assert!(matches!(
        store.revert(1, &checkpoint(2)).await,
        Err(StoreError::UnknownCheckpoint)
    ));
    // Reverting to the current checkpoint is a no-op but still valid.
    store.revert(1, &checkpoint(1)).await.expect("revert again");
}

#[tokio::test]
async fn memory_finalize_bounds_revert() {
    let store = MemoryStore::new(schema());
    let app = table("app");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    batch
        .insert(&app, &key("alice", 1), &account(10, true))
        .await
        .expect("insert");
    batch.commit().await.expect("commit");

    let batch = store.begin(1, checkpoint(2)).await.expect("begin");
    batch
        .update(&app, &key("alice", 1), &account(20, true))
        .await
        .expect("update");
    batch.commit().await.expect("commit");

    store.finalize(1, &checkpoint(1)).await.expect("finalize");
    let progress = store
        .progress(1)
        .await
        .expect("progress")
        .expect("progress");
    assert_eq!(progress.finalized, checkpoint(1));
    assert_eq!(progress.checkpoint, checkpoint(2));

    assert!(matches!(
        store.revert(1, &Checkpoint::ZERO).await,
        Err(StoreError::BeyondFinalized)
    ));
    assert!(matches!(
        store.finalize(1, &checkpoint(3)).await,
        Err(StoreError::UnknownCheckpoint)
    ));
    store
        .revert(1, &checkpoint(1))
        .await
        .expect("revert to floor");
    assert_eq!(
        store
            .find(&app, &key("alice", 1))
            .await
            .expect("find")
            .as_ref()
            .and_then(balance),
        Some(&CellValue::Numeric("10".to_owned()))
    );
}

#[tokio::test]
async fn memory_composite_key_isolates_rows() {
    let store = MemoryStore::new(schema());
    let app = table("app");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    batch
        .insert(&app, &key("alice", 1), &account(1, true))
        .await
        .expect("insert");
    batch
        .insert(&app, &key("alice", 2), &account(2, true))
        .await
        .expect("insert");
    batch
        .insert(&app, &key("bob", 1), &account(3, true))
        .await
        .expect("insert");
    batch.commit().await.expect("commit");

    let batch = store.begin(1, checkpoint(2)).await.expect("begin");
    batch
        .delete(&app, &key("alice", 2))
        .await
        .expect("delete one");
    batch.commit().await.expect("commit");

    assert_eq!(
        store
            .find(&app, &key("alice", 1))
            .await
            .expect("find")
            .as_ref()
            .and_then(balance),
        Some(&CellValue::Numeric("1".to_owned()))
    );
    assert!(store
        .find(&app, &key("alice", 2))
        .await
        .expect("find")
        .is_none());
    assert_eq!(
        store
            .find(&app, &key("bob", 1))
            .await
            .expect("find")
            .as_ref()
            .and_then(balance),
        Some(&CellValue::Numeric("3".to_owned()))
    );
}

#[tokio::test]
async fn memory_rejects_non_advancing_checkpoints() {
    let store = MemoryStore::new(schema());
    let app = table("app");

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    // The in-memory store has no progress row to lock, so a second batch may
    // begin at the same checkpoint; its commit is rejected instead.
    let competing = store
        .begin(1, checkpoint(1))
        .await
        .expect("concurrent begin");
    batch.commit().await.expect("commit");
    assert!(matches!(
        competing.commit().await,
        Err(StoreError::NonAdvancingCheckpoint)
    ));
    assert!(matches!(
        store.begin(1, checkpoint(1)).await,
        Err(StoreError::NonAdvancingCheckpoint)
    ));
    store
        .begin(1, checkpoint(2))
        .await
        .expect("advancing begin")
        .rollback()
        .await
        .expect("rollback");

    // Logical errors do not poison the batch.
    let batch = store.begin(1, checkpoint(2)).await.expect("begin");
    batch
        .insert(&app, &key("alice", 1), &account(1, true))
        .await
        .expect("insert");
    assert!(matches!(
        batch
            .insert(&app, &key("alice", 1), &account(2, true))
            .await,
        Err(StoreError::AlreadyExists { .. })
    ));
    assert!(matches!(
        batch
            .update(&app, &key("carol", 9), &account(1, true))
            .await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        batch.delete(&app, &key("carol", 9)).await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(matches!(
        batch
            .find(&TableRef::new("app", "missing"), &key("alice", 1))
            .await,
        Err(StoreError::UnknownTable { .. })
    ));
    batch
        .update(&app, &key("alice", 1), &account(3, true))
        .await
        .expect("update after errors");
    batch.commit().await.expect("commit");
}

#[tokio::test]
async fn memory_rejects_unsafe_raw_sql() {
    let store = MemoryStore::new(schema());

    assert!(matches!(
        store.query("SELECT 1; SELECT 2", &[]).await,
        Err(StoreError::MultipleStatements)
    ));
    assert!(matches!(
        store.query("DROP TABLE app.account", &[]).await,
        Err(StoreError::ForbiddenStatement(_))
    ));
    assert!(matches!(
        store.query("   ", &[]).await,
        Err(StoreError::EmptyStatement)
    ));
    assert!(matches!(
        store.query("SELECT 1", &[]).await,
        Err(StoreError::Unsupported(_))
    ));

    let batch = store.begin(1, checkpoint(1)).await.expect("begin");
    assert!(matches!(
        batch.raw("SELECT 1; SELECT 2", &[]).await,
        Err(StoreError::MultipleStatements)
    ));
    assert!(matches!(
        batch.raw("CREATE TABLE app.other (id BIGINT)", &[]).await,
        Err(StoreError::ForbiddenStatement(_))
    ));
    batch.rollback().await.expect("rollback");
}

fn factory_address(value: u8) -> Address {
    Address([value; 20])
}

#[tokio::test]
async fn memory_factory_addresses_append_is_idempotent_and_keeps_the_min_block() {
    let store = MemoryStore::new(schema());
    assert!(
        store
            .factory_addresses(1, "Pool")
            .await
            .expect("read")
            .is_empty(),
        "an unseen factory has no addresses"
    );

    store
        .append_factory_addresses(1, "Pool", &[])
        .await
        .expect("an empty append is a no-op");

    let first = factory_address(0xa0);
    let second = factory_address(0xb0);
    store
        .append_factory_addresses(1, "Pool", &[(first, 5), (second, 7)])
        .await
        .expect("append");
    // Re-appending keeps the earliest block, so a restart that rediscovers an
    // address does not move it forward. Duplicates inside one call collapse to
    // the same result.
    store
        .append_factory_addresses(1, "Pool", &[(first, 9), (second, 3), (first, 6)])
        .await
        .expect("append again");

    assert_eq!(
        store.factory_addresses(1, "Pool").await.expect("read"),
        [(second, 3), (first, 5)]
    );
    assert!(
        store
            .factory_addresses(1, "Other")
            .await
            .expect("read")
            .is_empty(),
        "factories are independent"
    );
    assert!(
        store
            .factory_addresses(2, "Pool")
            .await
            .expect("read")
            .is_empty(),
        "chains are independent"
    );
}

#[tokio::test]
async fn memory_factory_addresses_are_ordered_by_block_then_address() {
    let store = MemoryStore::new(schema());
    let low = factory_address(0x01);
    let high = factory_address(0x02);
    let later = factory_address(0x03);

    store
        .append_factory_addresses(1, "Pool", &[(later, 4), (high, 1), (low, 1)])
        .await
        .expect("append");

    assert_eq!(
        store.factory_addresses(1, "Pool").await.expect("read"),
        [(low, 1), (high, 1), (later, 4)]
    );
}

#[tokio::test]
async fn memory_remove_factory_addresses_after_keeps_the_ancestor() {
    let store = MemoryStore::new(schema());
    let kept = factory_address(0xa0);
    let boundary = factory_address(0xb0);
    let removed = factory_address(0xc0);
    store
        .append_factory_addresses(1, "Pool", &[(kept, 1), (boundary, 2), (removed, 3)])
        .await
        .expect("append");

    store
        .remove_factory_addresses_after(1, "Pool", 2)
        .await
        .expect("remove after the ancestor");
    assert_eq!(
        store.factory_addresses(1, "Pool").await.expect("read"),
        [(kept, 1), (boundary, 2)],
        "addresses at or below the ancestor stay"
    );

    store
        .remove_factory_addresses_after(1, "Pool", 0)
        .await
        .expect("remove everything");
    assert!(store
        .factory_addresses(1, "Pool")
        .await
        .expect("read")
        .is_empty());

    // Removing from an unknown factory is not an error.
    store
        .remove_factory_addresses_after(1, "Missing", 2)
        .await
        .expect("missing factory is a no-op");
}

/// Run against a disposable database:
///
/// ```text
/// STELE_TEST_DATABASE_URL=postgres://... cargo test -p stele-store -- --ignored
/// ```
///
/// The test creates the `app` and `audit` schemas and rows in them. Never point
/// it at an application database.
#[tokio::test]
#[ignore = "requires STELE_TEST_DATABASE_URL"]
async fn postgres_round_trip_revert_and_raw_sql() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("STELE_TEST_DATABASE_URL")?;
    let store = PostgresStore::connect(&url).await?;
    store.initialize(&schema()).await?;

    let app = table("app");
    let audit = table("audit");
    let chain = 1_000_000
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64
            % 1_000_000;
    let owner = format!("test-{chain}");

    let first = checkpoint(1);
    let second = checkpoint(2);
    let third = checkpoint(3);

    let batch = store.begin(chain, first).await?;
    assert!(batch.find(&app, &key(&owner, 1)).await?.is_none());
    batch
        .insert(&app, &key(&owner, 1), &account(10, true))
        .await?;
    batch
        .insert(&audit, &key(&owner, 1), &account(100, false))
        .await?;
    assert_eq!(
        batch
            .find(&app, &key(&owner, 1))
            .await?
            .as_ref()
            .and_then(balance),
        Some(&CellValue::Numeric("10".to_owned()))
    );
    assert!(store.find(&app, &key(&owner, 1)).await?.is_none());
    batch.commit().await?;
    assert_eq!(
        store
            .progress(chain)
            .await?
            .map(|progress| progress.checkpoint),
        Some(first)
    );

    let batch = store.begin(chain, second).await?;
    batch
        .update(&app, &key(&owner, 1), &account(20, true))
        .await?;
    batch.delete(&audit, &key(&owner, 1)).await?;
    batch
        .insert(&app, &key(&owner, 2), &account(5, true))
        .await?;
    batch.commit().await?;

    let batch = store.begin(chain, third).await?;
    batch
        .update(&app, &key(&owner, 2), &account(6, true))
        .await?;
    batch.commit().await?;

    // Numeric and array results keep their exact value through raw SQL.
    let rows = store
        .query(
            "SELECT $1::numeric AS amount, $2::int4[] AS numbers, $3::jsonb AS document",
            &[
                CellValue::Numeric("123456789012345678901234567890".to_owned()),
                CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]),
                CellValue::Json(serde_json::json!({ "a": 1 })),
            ],
        )
        .await?;
    assert_eq!(rows.len(), 1);
    let result = rows.first().expect("one row");
    assert_eq!(
        result.get("amount"),
        Some(&CellValue::Numeric(
            "123456789012345678901234567890".to_owned()
        ))
    );
    assert_eq!(
        result.get("numbers"),
        Some(&CellValue::Array(vec![
            CellValue::Int(1),
            CellValue::Int(2)
        ]))
    );
    assert_eq!(
        result.get("document"),
        Some(&CellValue::Json(serde_json::json!({ "a": 1 })))
    );

    store.revert(chain, &first).await?;
    let restored = store
        .find(&app, &key(&owner, 1))
        .await?
        .expect("restored row");
    assert_eq!(
        balance(&restored),
        Some(&CellValue::Numeric("10".to_owned()))
    );
    assert!(store.find(&audit, &key(&owner, 1)).await?.is_some());
    assert!(store.find(&app, &key(&owner, 2)).await?.is_none());
    assert!(matches!(
        store.revert(chain, &second).await,
        Err(StoreError::UnknownCheckpoint)
    ));

    store.finalize(chain, &first).await?;
    assert!(matches!(
        store.revert(chain, &Checkpoint::ZERO).await,
        Err(StoreError::BeyondFinalized)
    ));

    assert!(matches!(
        store.query("SELECT 1; SELECT 2", &[]).await,
        Err(StoreError::MultipleStatements)
    ));
    assert!(matches!(
        store.query("DROP TABLE app.account", &[]).await,
        Err(StoreError::ForbiddenStatement(_))
    ));

    // Raw reads inside a batch see the batch's own cached writes.
    let batch = store.begin(chain, checkpoint(4)).await?;
    batch
        .insert(&app, &key(&owner, 3), &account(1, true))
        .await?;
    let rows = batch
        .raw(
            "SELECT count(*) AS total FROM app.account WHERE owner = $1",
            &[CellValue::Text(owner.clone())],
        )
        .await?;
    assert_eq!(
        rows.first().and_then(|row| row.get("total")),
        Some(&CellValue::Int(2))
    );
    batch.rollback().await?;
    assert!(store.find(&app, &key(&owner, 3)).await?.is_none());

    // A rejected batch statement aborts the batch rather than the commit.
    let batch = store.begin(chain, checkpoint(5)).await?;
    assert!(batch
        .raw("SELECT missing_column FROM app.account", &[])
        .await
        .is_err());
    assert!(matches!(
        batch.commit().await,
        Err(StoreError::BatchUnavailable)
    ));

    // Clean up the rows this test created.
    let cleanup = store
        .begin(
            chain,
            Checkpoint {
                block_timestamp: 9_999_999_999,
                ..third
            },
        )
        .await?;
    let _ = cleanup.delete(&app, &key(&owner, 1)).await;
    let _ = cleanup.delete(&app, &key(&owner, 2)).await;
    let _ = cleanup.delete(&audit, &key(&owner, 1)).await;
    cleanup.commit().await?;
    Ok(())
}

/// Exercise every supported column type through insert, find, undo and a
/// second idempotent `initialize`. Requires `STELE_TEST_DATABASE_URL`.
#[tokio::test]
#[ignore = "requires STELE_TEST_DATABASE_URL"]
async fn postgres_type_round_trip_and_idempotent_initialize(
) -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("STELE_TEST_DATABASE_URL")?;
    let store = PostgresStore::connect(&url).await?;
    store.initialize(&schema()).await?;
    // Missing objects are created; existing objects and rows are retained.
    store.initialize(&schema()).await?;

    let metric = TableRef::new("app", "metric");
    let chain = 2_000_000
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64
            % 1_000_000;
    let owner = format!("metric-{chain}");
    let first = checkpoint(1);
    let second = checkpoint(2);
    let key = row(&[
        ("owner", CellValue::Text(owner.clone())),
        ("epoch", CellValue::Int(7)),
    ]);
    let big = "123456789012345678901234567890".to_owned();
    let original = row(&[
        ("small", CellValue::Int(-7)),
        ("ratio", CellValue::Float(1.5)),
        ("precise", CellValue::Float(2.25)),
        ("blob", CellValue::Bytes(vec![0, 1, 255])),
        (
            "seen_at",
            CellValue::Text("2024-05-06T07:08:09.123456Z".to_owned()),
        ),
        (
            "amounts",
            CellValue::Array(vec![CellValue::Numeric(big.clone()), CellValue::Null]),
        ),
    ]);

    let batch = store.begin(chain, first).await?;
    batch.insert(&metric, &key, &original).await?;
    batch.commit().await?;

    let stored = store.find(&metric, &key).await?.expect("stored metric");
    assert_eq!(stored.get("small"), Some(&CellValue::Int(-7)));
    assert_eq!(stored.get("ratio"), Some(&CellValue::Float(1.5)));
    assert_eq!(stored.get("precise"), Some(&CellValue::Float(2.25)));
    assert_eq!(stored.get("blob"), Some(&CellValue::Bytes(vec![0, 1, 255])));
    assert_eq!(
        stored.get("seen_at"),
        Some(&CellValue::Text("2024-05-06T07:08:09.123456Z".to_owned()))
    );
    assert_eq!(
        stored.get("amounts"),
        Some(&CellValue::Array(vec![
            CellValue::Numeric(big.clone()),
            CellValue::Null,
        ]))
    );

    let batch = store.begin(chain, second).await?;
    batch
        .update(
            &metric,
            &key,
            &row(&[
                ("small", CellValue::Int(9)),
                ("blob", CellValue::Bytes(vec![9])),
                (
                    "amounts",
                    CellValue::Array(vec![CellValue::Numeric("1".to_owned())]),
                ),
            ]),
        )
        .await?;
    batch.commit().await?;

    store.revert(chain, &first).await?;
    let restored = store.find(&metric, &key).await?.expect("restored metric");
    assert_eq!(restored.get("small"), Some(&CellValue::Int(-7)));
    assert_eq!(
        restored.get("blob"),
        Some(&CellValue::Bytes(vec![0, 1, 255]))
    );
    assert_eq!(
        restored.get("amounts"),
        Some(&CellValue::Array(vec![
            CellValue::Numeric(big),
            CellValue::Null,
        ]))
    );

    let cleanup = store
        .begin(
            chain,
            Checkpoint {
                block_timestamp: 9_999_999_999,
                ..second
            },
        )
        .await?;
    let _ = cleanup.delete(&metric, &key).await;
    cleanup.commit().await?;
    Ok(())
}

/// Exercise factory address persistence against a database. Requires
/// `STELE_TEST_DATABASE_URL`; each run uses its own chain id and factory id,
/// so no cleanup outside the table is needed.
#[tokio::test]
#[ignore = "requires STELE_TEST_DATABASE_URL"]
async fn postgres_factory_addresses_append_and_remove() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("STELE_TEST_DATABASE_URL")?;
    let store = PostgresStore::connect(&url).await?;
    store.initialize(&schema()).await?;

    let chain = 3_000_000
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64
            % 1_000_000;
    let factory = format!("Pool-{chain}");
    let first = factory_address(0xa0);
    let second = factory_address(0xb0);

    assert!(store.factory_addresses(chain, &factory).await?.is_empty());
    store
        .append_factory_addresses(chain, &factory, &[(first, 5), (second, 7)])
        .await?;
    // Repeated addresses, across calls and inside one call, keep the minimum.
    store
        .append_factory_addresses(chain, &factory, &[(first, 9), (second, 3), (first, 6)])
        .await?;
    assert_eq!(
        store.factory_addresses(chain, &factory).await?,
        [(second, 3), (first, 5)]
    );

    store
        .remove_factory_addresses_after(chain, &factory, 3)
        .await?;
    assert_eq!(
        store.factory_addresses(chain, &factory).await?,
        [(second, 3)],
        "only addresses above the ancestor are deleted"
    );

    store
        .remove_factory_addresses_after(chain, &factory, 0)
        .await?;
    assert!(store.factory_addresses(chain, &factory).await?.is_empty());
    Ok(())
}
