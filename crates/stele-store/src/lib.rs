//! Durable indexing storage for stele.
//!
//! Two implementations share one contract:
//!
//! - [`PostgresStore`] writes rows with dynamic SQL against the tables declared
//!   by a [`stele_schema::SchemaDef`]. Progress, committed checkpoints and
//!   undo images live in the fixed `stele_store` schema.
//! - [`MemoryStore`] implements the same semantics without a database, for
//!   tests and for callers that do not need durability.
//!
//! Writes are grouped into a [`StoreBatch`]. A batch reads through an
//! in-memory write cache and records the before image of every touched row
//! once per checkpoint. [`StoreBatch::commit`] applies the cache, writes undo
//! rows, inserts the checkpoint and advances progress in a single
//! transaction; [`StoreBatch::rollback`] discards the cache. [`Store::revert`]
//! replays undo rows behind a retained checkpoint, and [`Store::finalize`]
//! discards undo rows that can no longer be reverted.
//!
//! Factory child addresses live outside user tables and outside the undo log:
//! [`Store::factory_addresses`], [`Store::append_factory_addresses`] and
//! [`Store::remove_factory_addresses_after`] persist them so a restarted indexer
//! still recognizes children discovered in earlier blocks, which backfill never
//! replays. Reorg cleanup of those addresses is the caller's responsibility.
//!
//! Values are bound with explicit SQL casts derived from the schema, and every
//! identifier passes through [`stele_schema::quote_identifier`]. Raw SQL is
//! accepted as a single statement without DDL and with bound parameters only.

mod cache;
mod memory;
mod postgres;
mod sql;

use std::collections::BTreeMap;

use async_trait::async_trait;
use thiserror::Error;

pub use memory::MemoryStore;
pub use postgres::PostgresStore;
pub use stele_checkpoint::Checkpoint;
pub use stele_types::CellValue;

/// A column value keyed by column name.
pub type Row = BTreeMap<String, CellValue>;

/// A schema-qualified table name.
///
/// `TableRef` is only a name. The column layout and primary key come from the
/// [`stele_schema::SchemaDef`] passed to
/// [`PostgresStore::initialize`] or [`MemoryStore::new`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableRef {
    /// PostgreSQL schema name.
    pub schema: String,
    /// Table name inside [`TableRef::schema`].
    pub name: String,
}

impl TableRef {
    /// Build a table reference. Names are validated when they are used in SQL.
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

/// Indexing position of one chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Progress {
    /// Last checkpoint committed by [`StoreBatch::commit`].
    pub checkpoint: Checkpoint,
    /// Oldest checkpoint that can still be reverted.
    pub finalized: Checkpoint,
}

/// Row operations available inside a batch.
///
/// Implementations cache writes in memory and read their own uncommitted
/// changes, so a batch observes the state produced by its earlier calls.
/// `key` holds the primary-key columns; extra columns in `key` are ignored.
/// `value` holds the columns to write. Primary-key columns found in `value`
/// are replaced with the values from `key`. Columns present in `value`
/// replace the current value; columns absent from `value` keep their current
/// value (or take the database default on insert).
#[async_trait]
pub trait IndexingStore: Send + Sync {
    /// Return the current row for `key`, including uncommitted batch writes.
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError>;

    /// Insert a new row. Fails with [`StoreError::AlreadyExists`] if the row
    /// already exists in the committed state or earlier in the same batch.
    async fn insert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;

    /// Update an existing row. Fails with [`StoreError::NotFound`] if the row
    /// does not exist.
    async fn update(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;

    /// Insert or update a row without checking whether it exists.
    async fn upsert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;

    /// Delete an existing row. Fails with [`StoreError::NotFound`] if the row
    /// does not exist.
    async fn delete(&self, table: &TableRef, key: &Row) -> Result<(), StoreError>;

    /// Run one parameterized SQL statement.
    ///
    /// The statement must be a single statement, must not be DDL, and must
    /// pass values as parameters rather than interpolate them. [`MemoryStore`]
    /// cannot execute SQL and returns [`StoreError::Unsupported`] after
    /// validating the statement.
    ///
    /// In a PostgreSQL batch the write cache is applied before the statement
    /// runs, so the statement observes this batch's writes. Statements that
    /// modify rows directly are not recorded in the undo log; use
    /// `insert`/`update`/`upsert`/`delete` for data that must be reversible.
    /// Numeric and array parameters are sent as text, so cast them in the
    /// statement (`$1::numeric`, `$1::int4[]`) when the column type matters.
    async fn raw(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError>;
}

/// A batch of changes that commit or roll back together.
///
/// Dropping a batch without committing discards the cache. Both methods
/// consume the batch, so a batch can be finished exactly once.
#[async_trait]
pub trait StoreBatch: IndexingStore {
    /// Apply the cache, write undo rows, insert the checkpoint, advance
    /// progress and commit, all in one database transaction.
    async fn commit(self: Box<Self>) -> Result<(), StoreError>;

    /// Discard all cached writes.
    async fn rollback(self: Box<Self>) -> Result<(), StoreError>;
}

/// Progress, revert and read operations for one store.
#[async_trait]
pub trait Store: Send + Sync {
    /// Start a batch at `checkpoint`.
    ///
    /// The store locks the progress row of `chain_id` for the lifetime of the
    /// batch. New chains start at [`Checkpoint::ZERO`]. A checkpoint that does
    /// not advance beyond the current progress is rejected with
    /// [`StoreError::NonAdvancingCheckpoint`].
    async fn begin(
        &self,
        chain_id: u64,
        checkpoint: Checkpoint,
    ) -> Result<Box<dyn StoreBatch>, StoreError>;

    /// Return the committed progress of `chain_id`, or `None` if the chain has
    /// never begun a batch.
    async fn progress(&self, chain_id: u64) -> Result<Option<Progress>, StoreError>;

    /// Undo every committed checkpoint after `target` and make `target` the
    /// current progress.
    ///
    /// `target` must be a retained committed checkpoint that is not older than
    /// the finalized checkpoint. Errors are [`StoreError::UnknownCheckpoint`]
    /// or [`StoreError::BeyondFinalized`].
    async fn revert(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError>;

    /// Discard undo rows at or before `target` and move the finality floor.
    ///
    /// `target` must be a retained committed checkpoint that is not older than
    /// the current finalized checkpoint.
    async fn finalize(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError>;

    /// Run one parameterized read statement.
    ///
    /// The same single-statement and no-DDL rules as [`IndexingStore::raw`]
    /// apply. Results are decoded by column type. Numeric and array parameters
    /// are sent as text and should be cast in the statement when needed.
    async fn query(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError>;

    /// Read a committed row, ignoring uncommitted batches.
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError>;

    /// Return the persisted child addresses of `factory_id` on `chain_id`.
    ///
    /// Each entry pairs an address with the earliest block that created it.
    /// Results are ordered by block number, then by address, so callers see a
    /// deterministic sequence. Factories that were never seen return an empty
    /// vector rather than [`StoreError::NotFound`].
    async fn factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
    ) -> Result<Vec<(stele_types::Address, u64)>, StoreError>;

    /// Persist child addresses discovered for `factory_id` on `chain_id`.
    ///
    /// The operation is idempotent: appending an address that already exists
    /// keeps the smaller of the two block numbers, matching the factory
    /// registries used by indexing engines, where the earliest creation block
    /// wins. Appending an empty slice does nothing.
    async fn append_factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
        values: &[(stele_types::Address, u64)],
    ) -> Result<(), StoreError>;

    /// Delete every persisted address of `factory_id` created after
    /// `block_number`.
    ///
    /// Used after a reorg: addresses at or below the common ancestor stay
    /// valid, deeper ones belong to abandoned blocks.
    async fn remove_factory_addresses_after(
        &self,
        chain_id: u64,
        factory_id: &str,
        block_number: u64,
    ) -> Result<(), StoreError>;
}

/// Errors returned by every store operation.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The table is not part of the schema given to `initialize` or `new`.
    #[error("table {schema:?}.{name:?} is not part of the initialized schema")]
    UnknownTable { schema: String, name: String },
    /// A supplied row names a column that the table does not have.
    #[error("column {column:?} does not exist in table {table:?}")]
    UnknownColumn { table: String, column: String },
    /// A key row omits one of the primary-key columns.
    #[error("primary key column {column:?} is missing from the key of table {table:?}")]
    MissingKeyColumn { table: String, column: String },
    /// A [`CellValue`] cannot be stored in the target column.
    #[error("value of type {found} cannot be stored in {expected} column {column:?}")]
    TypeMismatch {
        column: String,
        expected: &'static str,
        found: &'static str,
    },
    /// `insert` found an existing row.
    #[error("row already exists in {table:?}")]
    AlreadyExists { table: String },
    /// `update` or `delete` found no row.
    #[error("row does not exist in {table:?}")]
    NotFound { table: String },
    /// The chain id does not fit in a PostgreSQL `bigint`.
    #[error("chain id {0} does not fit in a PostgreSQL bigint")]
    InvalidChainId(u64),
    /// A block number does not fit in a PostgreSQL `bigint`.
    #[error("block number {0} does not fit in a PostgreSQL bigint")]
    InvalidBlockNumber(u64),
    /// `begin` received a checkpoint that does not advance progress.
    #[error("checkpoint must advance beyond current progress")]
    NonAdvancingCheckpoint,
    /// A revert or finalize target is not a retained committed checkpoint.
    #[error("target is not a retained committed checkpoint")]
    UnknownCheckpoint,
    /// A revert or finalize target precedes the finality floor.
    #[error("target precedes the finalized checkpoint")]
    BeyondFinalized,
    /// A batch lost its transaction, usually because a statement was cancelled
    /// or the database rejected an earlier statement.
    #[error("store batch is no longer usable after a failed or cancelled statement")]
    BatchUnavailable,
    /// The SQL text contains more than one statement.
    #[error("SQL must contain at most one statement")]
    MultipleStatements,
    /// The SQL statement begins with a keyword that is not allowed.
    #[error("SQL statement {0:?} is not allowed")]
    ForbiddenStatement(String),
    /// The SQL text is empty or contains only comments.
    #[error("empty SQL statement")]
    EmptyStatement,
    /// The SQL text cannot be scanned safely.
    #[error("invalid SQL statement: {0}")]
    InvalidStatement(&'static str),
    /// The database returned a type that has no [`CellValue`] representation.
    #[error("column type {0} is not supported")]
    UnsupportedType(String),
    /// A column value returned by the database has an unexpected encoding.
    #[error("cannot decode column value: {0}")]
    Decode(String),
    /// The operation needs a database this store does not have.
    #[error("{0} is not available on the in-memory store")]
    Unsupported(&'static str),
    /// The schema definition is invalid.
    #[error("schema definition is invalid: {0}")]
    Schema(#[from] stele_schema::SchemaError),
    /// A checkpoint could not be encoded.
    #[error("checkpoint encoding is invalid: {0}")]
    Checkpoint(#[from] stele_checkpoint::CheckpointError),
    /// JSON conversion failed.
    #[error("JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    /// The database reported an error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// A lock that protects store state was poisoned by a panic.
    #[error("store state lock is poisoned")]
    Poisoned,
}
