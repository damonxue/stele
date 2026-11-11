//! PostgreSQL-backed indexing store.
//!
//! User tables are written with dynamic SQL against the schema supplied to
//! [`PostgresStore::initialize`]. Internal state lives in the fixed
//! `stele_store` schema:
//!
//! ```text
//! stele_store.progress    chain_id PK, checkpoint, finalized
//! stele_store.checkpoints chain_id, checkpoint PK
//! stele_store.undo        id PK, chain_id, checkpoint, schema_name,
//!                          table_name, row_key jsonb, before_value jsonb
//! stele_store.factories   chain_id, factory_id, address, block_number,
//!                          PK (chain_id, factory_id, address)
//! ```
//!
//! `initialize` creates missing tables and never drops or rewrites existing
//! data.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row as _, Transaction};
use stele_checkpoint::Checkpoint;
use stele_schema::{quote_identifier, SchemaDef, TableDef};
use stele_types::Address;

use crate::cache::{check_presence, next_row, BatchCache, Mutation};
use crate::sql::{
    decode_row, delete_statement, encode_row, execute_statement, fetch_raw, fetch_rows, lookup,
    primary_key_row, row_key_string, schema_tables, select_statement, upsert_statement,
    validate_statement, validate_values, Statement, TableId,
};
use crate::{CellValue, IndexingStore, Progress, Row, Store, StoreBatch, StoreError, TableRef};

/// Internal objects created by [`PostgresStore::initialize`].
const INTERNAL_STATEMENTS: &[&str] = &[
    "CREATE SCHEMA IF NOT EXISTS stele_store",
    "CREATE TABLE IF NOT EXISTS stele_store.progress (\
        chain_id BIGINT PRIMARY KEY, \
        checkpoint TEXT COLLATE \"C\" NOT NULL, \
        finalized TEXT COLLATE \"C\" NOT NULL)",
    "CREATE TABLE IF NOT EXISTS stele_store.checkpoints (\
        chain_id BIGINT NOT NULL, \
        checkpoint TEXT COLLATE \"C\" NOT NULL, \
        PRIMARY KEY (chain_id, checkpoint))",
    "CREATE TABLE IF NOT EXISTS stele_store.undo (\
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
        chain_id BIGINT NOT NULL, \
        checkpoint TEXT COLLATE \"C\" NOT NULL, \
        schema_name TEXT NOT NULL, \
        table_name TEXT NOT NULL, \
        row_key JSONB NOT NULL, \
        before_value JSONB)",
    "CREATE INDEX IF NOT EXISTS stele_store_undo_lookup_idx \
        ON stele_store.undo (chain_id, checkpoint, id)",
    "CREATE TABLE IF NOT EXISTS stele_store.factories (\
        chain_id BIGINT NOT NULL, \
        factory_id TEXT NOT NULL, \
        address TEXT COLLATE \"C\" NOT NULL, \
        block_number BIGINT NOT NULL, \
        PRIMARY KEY (chain_id, factory_id, address))",
];

type Tables = Arc<RwLock<BTreeMap<TableId, TableDef>>>;

/// A store backed by PostgreSQL.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    tables: Tables,
}

impl PostgresStore {
    /// Connect to `url`.
    ///
    /// The store has no table metadata until [`PostgresStore::initialize`]
    /// succeeds, so reads and batches return [`StoreError::UnknownTable`]
    /// before that point.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().connect(url).await?;
        tracing::debug!("connected to postgres");
        Ok(Self::from_pool(pool))
    }

    /// Build a store on an existing pool. Call [`PostgresStore::initialize`]
    /// before using it.
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            tables: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Create the internal tables and any missing application tables, then
    /// register `schema` for subsequent operations.
    ///
    /// Existing tables and rows are left untouched. Concurrent initialization
    /// of the same database is serialized with a transaction advisory lock.
    pub async fn initialize(&self, schema: &SchemaDef) -> Result<(), StoreError> {
        let tables = schema_tables(schema)?;
        let mut transaction = self.pool.begin().await?;
        // Serialize concurrent initializers: CREATE ... IF NOT EXISTS is not
        // atomic against a competing CREATE in another session.
        sqlx::query("SELECT pg_advisory_xact_lock(1886351972)")
            .execute(&mut *transaction)
            .await?;
        for statement in INTERNAL_STATEMENTS {
            sqlx::query(statement).execute(&mut *transaction).await?;
        }
        let mut schemas: Vec<String> = Vec::new();
        for table in &schema.tables {
            if !schemas.contains(&table.schema) {
                schemas.push(table.schema.clone());
            }
        }
        for name in schemas {
            let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_identifier(&name)?);
            sqlx::query(&sql).execute(&mut *transaction).await?;
        }
        for statement in schema.create_tables_sql()? {
            let body =
                statement
                    .strip_prefix("CREATE TABLE ")
                    .ok_or(StoreError::InvalidStatement(
                        "unexpected CREATE TABLE statement",
                    ))?;
            let sql = format!("CREATE TABLE IF NOT EXISTS {body}");
            sqlx::query(&sql).execute(&mut *transaction).await?;
        }
        for table in &schema.tables {
            for statement in table.create_index_sqls()? {
                // The schema crate renders plain CREATE INDEX statements;
                // make them idempotent so existing indexes are retained.
                let sql = statement
                    .replacen("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
                    .replacen(
                        "CREATE UNIQUE INDEX ",
                        "CREATE UNIQUE INDEX IF NOT EXISTS ",
                        1,
                    );
                sqlx::query(&sql).execute(&mut *transaction).await?;
            }
        }
        // Views live in the same schema as the tables. Their SQL is already
        // schema-qualified by callers; the view name itself is not.
        let view_schema = schema
            .tables
            .first()
            .map(|table| table.schema.clone())
            .unwrap_or_else(|| "public".to_owned());
        for view in &schema.views {
            let sql = format!(
                "CREATE OR REPLACE VIEW {}.{} AS {}",
                quote_identifier(&view_schema)?,
                quote_identifier(&view.name)?,
                view.sql.trim_end_matches(';')
            );
            sqlx::query(&sql).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        let mut registered = self.tables.write().map_err(|_| StoreError::Poisoned)?;
        *registered = tables;
        tracing::info!(
            tables = schema.tables.len(),
            views = schema.views.len(),
            enums = schema.enums.len(),
            "database schema initialized"
        );
        Ok(())
    }

    fn table_def(&self, table: &TableRef) -> Result<TableDef, StoreError> {
        let registered = self.tables.read().map_err(|_| StoreError::Poisoned)?;
        registered
            .get(&TableId::from_ref(table))
            .cloned()
            .ok_or_else(|| StoreError::UnknownTable {
                schema: table.schema.clone(),
                name: table.name.clone(),
            })
    }

    async fn begin_batch(
        &self,
        chain_id: u64,
        checkpoint: Checkpoint,
    ) -> Result<Box<dyn StoreBatch>, StoreError> {
        let chain_id = chain_key(chain_id)?;
        let mut transaction = self.pool.begin().await?;
        let zero = Checkpoint::ZERO.encode()?;
        sqlx::query(
            "INSERT INTO stele_store.progress (chain_id, checkpoint, finalized) \
             VALUES ($1, $2, $2) ON CONFLICT (chain_id) DO NOTHING",
        )
        .bind(chain_id)
        .bind(&zero)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT checkpoint FROM stele_store.progress WHERE chain_id = $1 FOR UPDATE",
        )
        .bind(chain_id)
        .fetch_one(&mut *transaction)
        .await?;
        let current: Checkpoint = row.try_get::<String, _>("checkpoint")?.parse()?;
        if checkpoint <= current {
            return Err(StoreError::NonAdvancingCheckpoint);
        }
        tracing::trace!(
            chain_id,
            checkpoint = ?checkpoint,
            current = ?current,
            "opened store batch"
        );
        Ok(Box::new(PostgresBatch {
            tables: Arc::clone(&self.tables),
            chain_id,
            checkpoint,
            state: Mutex::new(BatchState {
                transaction: Some(transaction),
                cache: BatchCache::default(),
            }),
        }))
    }

    async fn lock_progress(
        &self,
        transaction: &mut Transaction<'static, Postgres>,
        chain_id: i64,
    ) -> Result<(Checkpoint, Checkpoint), StoreError> {
        let row = sqlx::query(
            "SELECT checkpoint, finalized FROM stele_store.progress \
             WHERE chain_id = $1 FOR UPDATE",
        )
        .bind(chain_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(StoreError::UnknownCheckpoint)?;
        Ok((
            row.try_get::<String, _>("checkpoint")?.parse()?,
            row.try_get::<String, _>("finalized")?.parse()?,
        ))
    }

    async fn validate_target(
        &self,
        transaction: &mut Transaction<'static, Postgres>,
        chain_id: i64,
        target: &Checkpoint,
        current: &Checkpoint,
        finalized: &Checkpoint,
        target_text: &str,
    ) -> Result<(), StoreError> {
        if target < finalized {
            return Err(StoreError::BeyondFinalized);
        }
        let retained: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM stele_store.checkpoints \
             WHERE chain_id = $1 AND checkpoint = $2)",
        )
        .bind(chain_id)
        .bind(target_text)
        .fetch_one(&mut **transaction)
        .await?;
        if !retained || target > current {
            return Err(StoreError::UnknownCheckpoint);
        }
        Ok(())
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn begin(
        &self,
        chain_id: u64,
        checkpoint: Checkpoint,
    ) -> Result<Box<dyn StoreBatch>, StoreError> {
        self.begin_batch(chain_id, checkpoint).await
    }

    async fn progress(&self, chain_id: u64) -> Result<Option<Progress>, StoreError> {
        let chain_id = chain_key(chain_id)?;
        let row = sqlx::query(
            "SELECT checkpoint, finalized FROM stele_store.progress WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some(row) => Ok(Some(Progress {
                checkpoint: row.try_get::<String, _>("checkpoint")?.parse()?,
                finalized: row.try_get::<String, _>("finalized")?.parse()?,
            })),
        }
    }

    async fn revert(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError> {
        let chain_id = chain_key(chain_id)?;
        let target_text = target.encode()?;
        let mut transaction = self.pool.begin().await?;
        let (current, finalized) = self.lock_progress(&mut transaction, chain_id).await?;
        self.validate_target(
            &mut transaction,
            chain_id,
            target,
            &current,
            &finalized,
            &target_text,
        )
        .await?;

        let entries = sqlx::query(
            "SELECT schema_name, table_name, row_key, before_value FROM stele_store.undo \
             WHERE chain_id = $1 AND checkpoint > $2 ORDER BY id DESC",
        )
        .bind(chain_id)
        .bind(&target_text)
        .fetch_all(&mut *transaction)
        .await?;

        let statements = {
            let tables = self.tables.read().map_err(|_| StoreError::Poisoned)?;
            let mut statements = Vec::with_capacity(entries.len());
            for entry in &entries {
                let schema: String = entry.try_get("schema_name")?;
                let name: String = entry.try_get("table_name")?;
                let table = TableId {
                    schema: schema.clone(),
                    name: name.clone(),
                };
                let def = lookup(&tables, &table)?;
                let before: Option<sqlx::types::Json<serde_json::Value>> =
                    entry.try_get("before_value")?;
                match before {
                    Some(before) => {
                        let row: Row = decode_row(&before.0)?;
                        statements.push(upsert_statement(def, &row)?);
                    }
                    None => {
                        let key: sqlx::types::Json<serde_json::Value> = entry.try_get("row_key")?;
                        let key: Row = serde_json::from_value(key.0)?;
                        statements.push(delete_statement(def, &key)?);
                    }
                }
            }
            statements
        };
        for statement in &statements {
            execute_statement(&mut *transaction, statement).await?;
        }

        sqlx::query("DELETE FROM stele_store.undo WHERE chain_id = $1 AND checkpoint > $2")
            .bind(chain_id)
            .bind(&target_text)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM stele_store.checkpoints WHERE chain_id = $1 AND checkpoint > $2")
            .bind(chain_id)
            .bind(&target_text)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE stele_store.progress SET checkpoint = $1 WHERE chain_id = $2")
            .bind(&target_text)
            .bind(chain_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn finalize(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError> {
        let chain_id = chain_key(chain_id)?;
        let target_text = target.encode()?;
        let mut transaction = self.pool.begin().await?;
        let (current, finalized) = self.lock_progress(&mut transaction, chain_id).await?;
        self.validate_target(
            &mut transaction,
            chain_id,
            target,
            &current,
            &finalized,
            &target_text,
        )
        .await?;
        sqlx::query("DELETE FROM stele_store.undo WHERE chain_id = $1 AND checkpoint <= $2")
            .bind(chain_id)
            .bind(&target_text)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM stele_store.checkpoints WHERE chain_id = $1 AND checkpoint < $2")
            .bind(chain_id)
            .bind(&target_text)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE stele_store.progress SET finalized = $1 WHERE chain_id = $2")
            .bind(&target_text)
            .bind(chain_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn query(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError> {
        validate_statement(sql)?;
        fetch_raw(&self.pool, sql, params).await
    }

    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError> {
        let def = self.table_def(table)?;
        let primary_key = primary_key_row(&def, key)?;
        let statement = select_statement(&def, &primary_key)?;
        Ok(fetch_rows(&self.pool, &statement).await?.into_iter().next())
    }

    async fn factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
    ) -> Result<Vec<(Address, u64)>, StoreError> {
        let chain_id = chain_key(chain_id)?;
        let rows = sqlx::query(
            "SELECT address, block_number FROM stele_store.factories \
             WHERE chain_id = $1 AND factory_id = $2 ORDER BY block_number, address",
        )
        .bind(chain_id)
        .bind(factory_id)
        .fetch_all(&self.pool)
        .await?;
        let mut addresses = Vec::with_capacity(rows.len());
        for row in rows {
            let encoded: String = row.try_get("address")?;
            let address = Address::from_hex(&encoded).map_err(|error| {
                StoreError::Decode(format!("invalid factory address {encoded:?}: {error}"))
            })?;
            let block: i64 = row.try_get("block_number")?;
            let block = u64::try_from(block).map_err(|_| {
                StoreError::Decode(format!("negative factory block number {block}"))
            })?;
            addresses.push((address, block));
        }
        Ok(addresses)
    }

    async fn append_factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
        values: &[(Address, u64)],
    ) -> Result<(), StoreError> {
        if values.is_empty() {
            return Ok(());
        }
        let chain_id = chain_key(chain_id)?;
        let mut addresses = Vec::with_capacity(values.len());
        let mut blocks = Vec::with_capacity(values.len());
        for (address, block) in values {
            addresses.push(address.to_hex());
            blocks.push(block_key(*block)?);
        }
        // `LEAST` keeps the earliest creation block, so re-appending the same
        // discovery after a restart is a no-op for the stored row. Duplicate
        // addresses in one call are collapsed first, because PostgreSQL rejects
        // an ON CONFLICT DO UPDATE that would affect the same row twice.
        sqlx::query(
            "INSERT INTO stele_store.factories AS existing \
                 (chain_id, factory_id, address, block_number) \
             SELECT $1::bigint, $2::text, pending.address, MIN(pending.block_number) \
             FROM unnest($3::text[], $4::bigint[]) AS pending(address, block_number) \
             GROUP BY pending.address \
             ON CONFLICT (chain_id, factory_id, address) \
             DO UPDATE SET block_number = LEAST(existing.block_number, EXCLUDED.block_number)",
        )
        .bind(chain_id)
        .bind(factory_id)
        .bind(addresses)
        .bind(blocks)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn remove_factory_addresses_after(
        &self,
        chain_id: u64,
        factory_id: &str,
        block_number: u64,
    ) -> Result<(), StoreError> {
        let chain_id = chain_key(chain_id)?;
        let block_number = block_key(block_number)?;
        sqlx::query(
            "DELETE FROM stele_store.factories \
             WHERE chain_id = $1 AND factory_id = $2 AND block_number > $3",
        )
        .bind(chain_id)
        .bind(factory_id)
        .bind(block_number)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn chain_key(chain_id: u64) -> Result<i64, StoreError> {
    i64::try_from(chain_id).map_err(|_| StoreError::InvalidChainId(chain_id))
}

fn block_key(block_number: u64) -> Result<i64, StoreError> {
    i64::try_from(block_number).map_err(|_| StoreError::InvalidBlockNumber(block_number))
}

struct BatchState {
    transaction: Option<Transaction<'static, Postgres>>,
    cache: BatchCache,
}

/// One uncommitted batch. The transaction holds the chain's progress row lock
/// until the batch commits, rolls back or is dropped.
struct PostgresBatch {
    tables: Tables,
    chain_id: i64,
    checkpoint: Checkpoint,
    state: Mutex<BatchState>,
}

impl PostgresBatch {
    fn take_transaction(&self) -> Result<Transaction<'static, Postgres>, StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        state.transaction.take().ok_or(StoreError::BatchUnavailable)
    }

    fn restore_transaction(
        &self,
        transaction: Transaction<'static, Postgres>,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        state.transaction = Some(transaction);
        Ok(())
    }

    async fn load(&self, def: &TableDef, key: &Row) -> Result<Option<Row>, StoreError> {
        let statement = select_statement(def, key)?;
        let mut transaction = self.take_transaction()?;
        match fetch_rows(&mut *transaction, &statement).await {
            Ok(rows) => {
                self.restore_transaction(transaction)?;
                Ok(rows.into_iter().next())
            }
            Err(error) => {
                // The statement aborted the transaction; dropping it rolls
                // back and releases the progress lock.
                drop(transaction);
                Err(error)
            }
        }
    }

    async fn mutate(
        &self,
        table: &TableRef,
        key: &Row,
        value: Option<&Row>,
        mode: Mutation,
    ) -> Result<(), StoreError> {
        let table_id = TableId::from_ref(table);
        let def = {
            let tables = self.tables.read().map_err(|_| StoreError::Poisoned)?;
            lookup(&tables, &table_id)?.clone()
        };
        let primary_key = primary_key_row(&def, key)?;
        let row_key = row_key_string(&primary_key)?;
        if let Some(value) = value {
            validate_values(&def, value)?;
        }
        let cached = {
            let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
            state.cache.state(&table_id, &row_key)
        };
        let current = match cached {
            Some(current) => current,
            None => self.load(&def, &primary_key).await?,
        };
        check_presence(&def, mode, current.is_some())?;
        let next = next_row(&def, &primary_key, current.as_ref(), value, mode);
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        state
            .cache
            .record(table_id, row_key, primary_key, current, next);
        Ok(())
    }

    /// Statements that bring the transaction up to the cached state.
    fn pending_statements(&self) -> Result<Vec<Statement>, StoreError> {
        let tables = self.tables.read().map_err(|_| StoreError::Poisoned)?;
        let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        cache_statements(&tables, &state.cache)
    }

    async fn commit_batch(self: Box<Self>) -> Result<(), StoreError> {
        let this = *self;
        let mut state = this.state.into_inner().map_err(|_| StoreError::Poisoned)?;
        let mut transaction = state
            .transaction
            .take()
            .ok_or(StoreError::BatchUnavailable)?;
        let checkpoint = this.checkpoint.encode()?;

        let (statements, undo) = {
            let tables = this.tables.read().map_err(|_| StoreError::Poisoned)?;
            let statements = cache_statements(&tables, &state.cache)?;
            let undo = undo_records(&state.cache)?;
            (statements, undo)
        };
        for statement in &statements {
            execute_statement(&mut *transaction, statement).await?;
        }
        for entry in &undo {
            sqlx::query(
                "INSERT INTO stele_store.undo \
                 (chain_id, checkpoint, schema_name, table_name, row_key, before_value) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(this.chain_id)
            .bind(&checkpoint)
            .bind(&entry.schema)
            .bind(&entry.table)
            .bind(sqlx::types::Json(entry.row_key.clone()))
            .bind(entry.before.clone().map(sqlx::types::Json))
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "INSERT INTO stele_store.checkpoints (chain_id, checkpoint) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
        )
        .bind(this.chain_id)
        .bind(&checkpoint)
        .execute(&mut *transaction)
        .await?;
        let advanced = sqlx::query(
            "UPDATE stele_store.progress SET checkpoint = $1 \
             WHERE chain_id = $2 AND checkpoint < $1",
        )
        .bind(&checkpoint)
        .bind(this.chain_id)
        .execute(&mut *transaction)
        .await?;
        if advanced.rows_affected() != 1 {
            return Err(StoreError::NonAdvancingCheckpoint);
        }
        transaction.commit().await?;
        tracing::debug!(
            chain_id = this.chain_id,
            checkpoint = %checkpoint,
            statements = statements.len(),
            undo_records = undo.len(),
            "committed store batch"
        );
        Ok(())
    }
}

struct UndoRecord {
    schema: String,
    table: String,
    row_key: serde_json::Value,
    before: Option<serde_json::Value>,
}

fn cache_statements(
    tables: &BTreeMap<TableId, TableDef>,
    cache: &BatchCache,
) -> Result<Vec<Statement>, StoreError> {
    let mut statements = Vec::new();
    for (table_id, rows) in cache.tables() {
        let def = lookup(tables, table_id)?;
        for row in rows.values() {
            statements.push(match &row.value {
                Some(value) => upsert_statement(def, value)?,
                None => delete_statement(def, &row.key)?,
            });
        }
    }
    Ok(statements)
}

fn undo_records(cache: &BatchCache) -> Result<Vec<UndoRecord>, StoreError> {
    let mut records = Vec::with_capacity(cache.undo().len());
    for entry in cache.undo() {
        records.push(UndoRecord {
            schema: entry.table.schema.clone(),
            table: entry.table.name.clone(),
            row_key: serde_json::to_value(&entry.key)?,
            before: entry.before.as_ref().map(encode_row),
        });
    }
    Ok(records)
}

#[async_trait]
impl IndexingStore for PostgresBatch {
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError> {
        let table_id = TableId::from_ref(table);
        let def = {
            let tables = self.tables.read().map_err(|_| StoreError::Poisoned)?;
            lookup(&tables, &table_id)?.clone()
        };
        let primary_key = primary_key_row(&def, key)?;
        let row_key = row_key_string(&primary_key)?;
        {
            let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
            if let Some(cached) = state.cache.state(&table_id, &row_key) {
                return Ok(cached);
            }
        }
        self.load(&def, &primary_key).await
    }

    async fn insert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Insert).await
    }

    async fn update(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Update).await
    }

    async fn upsert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Upsert).await
    }

    async fn delete(&self, table: &TableRef, key: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, None, Mutation::Delete).await
    }

    async fn raw(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError> {
        validate_statement(sql)?;
        // Apply cached rows first so the statement observes this batch's
        // writes. The rows stay cached, so commit applies them again.
        let statements = self.pending_statements()?;
        let mut transaction = self.take_transaction()?;
        let result = async {
            for statement in &statements {
                execute_statement(&mut *transaction, statement).await?;
            }
            fetch_raw(&mut *transaction, sql, params).await
        }
        .await;
        if result.is_ok() {
            self.restore_transaction(transaction)?;
        } else {
            // A rejected statement aborts the transaction. Drop it so the
            // batch reports BatchUnavailable instead of failing at commit.
            drop(transaction);
        }
        result
    }
}

#[async_trait]
impl StoreBatch for PostgresBatch {
    async fn commit(self: Box<Self>) -> Result<(), StoreError> {
        self.commit_batch().await
    }

    async fn rollback(self: Box<Self>) -> Result<(), StoreError> {
        let this = *self;
        let mut state = this.state.into_inner().map_err(|_| StoreError::Poisoned)?;
        if let Some(transaction) = state.transaction.take() {
            transaction.rollback().await?;
        }
        tracing::debug!(
            chain_id = this.chain_id,
            checkpoint = ?this.checkpoint,
            "rolled back store batch"
        );
        Ok(())
    }
}
