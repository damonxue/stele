//! In-memory store with the same batch, undo and progress semantics as
//! [`crate::PostgresStore`].
//!
//! Rows, undo images, committed checkpoints and progress live in process
//! memory. Raw SQL is validated but cannot be executed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use stele_checkpoint::Checkpoint;
use stele_schema::{SchemaDef, TableDef};
use stele_types::Address;

use crate::cache::{check_presence, next_row, BatchCache, Mutation};
use crate::sql::{
    lookup, primary_key_row, row_key_string, validate_statement, validate_values, TableId,
};
use crate::{CellValue, IndexingStore, Progress, Row, Store, StoreBatch, StoreError, TableRef};

/// A store that keeps all state in process memory.
///
/// The schema is used the same way as with [`crate::PostgresStore`]: it
/// supplies the primary keys and column types that `insert`, `find` and the
/// undo log need. Invalid schemas are reported when a table is used.
#[derive(Clone)]
pub struct MemoryStore {
    tables: Arc<BTreeMap<TableId, TableDef>>,
    state: Arc<Mutex<MemoryState>>,
}

impl MemoryStore {
    /// Create an empty store for `schema`.
    pub fn new(schema: SchemaDef) -> Self {
        let mut tables = BTreeMap::new();
        for table in schema.tables {
            tables.insert(
                TableId {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                },
                table,
            );
        }
        Self {
            tables: Arc::new(tables),
            state: Arc::new(Mutex::new(MemoryState::default())),
        }
    }
}

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<TableId, BTreeMap<String, Row>>,
    /// Factory child addresses keyed by chain and factory id, then by address.
    ///
    /// These live outside `rows` so they are not part of the undo log; a reorg
    /// cleans them through [`Store::remove_factory_addresses_after`].
    factories: BTreeMap<(u64, String), BTreeMap<Address, u64>>,
    undo: BTreeMap<u64, Vec<MemoryUndo>>,
    checkpoints: BTreeMap<u64, BTreeSet<Checkpoint>>,
    progress: BTreeMap<u64, Progress>,
}

struct MemoryUndo {
    checkpoint: Checkpoint,
    table: TableId,
    row_key: String,
    before: Option<Row>,
}

#[async_trait]
impl Store for MemoryStore {
    async fn begin(
        &self,
        chain_id: u64,
        checkpoint: Checkpoint,
    ) -> Result<Box<dyn StoreBatch>, StoreError> {
        let current = {
            let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
            state
                .progress
                .get(&chain_id)
                .map(|progress| progress.checkpoint)
                .unwrap_or(Checkpoint::ZERO)
        };
        if checkpoint <= current {
            return Err(StoreError::NonAdvancingCheckpoint);
        }
        Ok(Box::new(MemoryBatch {
            tables: Arc::clone(&self.tables),
            state: Arc::clone(&self.state),
            chain_id,
            checkpoint,
            cache: Mutex::new(BatchCache::default()),
        }))
    }

    async fn progress(&self, chain_id: u64) -> Result<Option<Progress>, StoreError> {
        let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        Ok(state.progress.get(&chain_id).copied())
    }

    async fn revert(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        let progress = state
            .progress
            .get(&chain_id)
            .copied()
            .ok_or(StoreError::UnknownCheckpoint)?;
        if target < &progress.finalized {
            return Err(StoreError::BeyondFinalized);
        }
        let retained = state
            .checkpoints
            .get(&chain_id)
            .is_some_and(|checkpoints| checkpoints.contains(target));
        if !retained {
            return Err(StoreError::UnknownCheckpoint);
        }

        let entries = state.undo.remove(&chain_id).unwrap_or_default();
        let mut kept = Vec::with_capacity(entries.len());
        for entry in entries.into_iter().rev() {
            if entry.checkpoint > *target {
                let rows = state.rows.entry(entry.table).or_default();
                match entry.before {
                    Some(row) => {
                        rows.insert(entry.row_key, row);
                    }
                    None => {
                        rows.remove(&entry.row_key);
                    }
                }
            } else {
                kept.push(entry);
            }
        }
        kept.reverse();
        if !kept.is_empty() {
            state.undo.insert(chain_id, kept);
        }

        if let Some(checkpoints) = state.checkpoints.get_mut(&chain_id) {
            while let Some(last) = checkpoints.pop_last() {
                if last <= *target {
                    checkpoints.insert(last);
                    break;
                }
            }
        }
        if let Some(progress) = state.progress.get_mut(&chain_id) {
            progress.checkpoint = *target;
        }
        Ok(())
    }

    async fn finalize(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        let progress = state
            .progress
            .get(&chain_id)
            .copied()
            .ok_or(StoreError::UnknownCheckpoint)?;
        if target < &progress.finalized {
            return Err(StoreError::BeyondFinalized);
        }
        let retained = state
            .checkpoints
            .get(&chain_id)
            .is_some_and(|checkpoints| checkpoints.contains(target));
        if !retained {
            return Err(StoreError::UnknownCheckpoint);
        }

        if let Some(entries) = state.undo.get_mut(&chain_id) {
            entries.retain(|entry| entry.checkpoint > *target);
        }
        if let Some(checkpoints) = state.checkpoints.get_mut(&chain_id) {
            let retained = checkpoints.split_off(target);
            *checkpoints = retained;
        }
        if let Some(progress) = state.progress.get_mut(&chain_id) {
            progress.finalized = *target;
        }
        Ok(())
    }

    async fn query(&self, sql: &str, _params: &[CellValue]) -> Result<Vec<Row>, StoreError> {
        validate_statement(sql)?;
        Err(StoreError::Unsupported("raw SQL"))
    }

    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError> {
        let table_id = TableId::from_ref(table);
        let def = lookup(&self.tables, &table_id)?;
        let primary_key = primary_key_row(def, key)?;
        let row_key = row_key_string(&primary_key)?;
        let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        Ok(state
            .rows
            .get(&table_id)
            .and_then(|rows| rows.get(&row_key))
            .cloned())
    }

    async fn factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
    ) -> Result<Vec<(Address, u64)>, StoreError> {
        let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        let mut addresses: Vec<(Address, u64)> = state
            .factories
            .get(&(chain_id, factory_id.to_owned()))
            .map(|entries| {
                entries
                    .iter()
                    .map(|(address, block)| (*address, *block))
                    .collect()
            })
            .unwrap_or_default();
        addresses.sort_unstable_by_key(|(address, block)| (*block, *address));
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
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        let entries = state
            .factories
            .entry((chain_id, factory_id.to_owned()))
            .or_default();
        for (address, block) in values {
            entries
                .entry(*address)
                .and_modify(|existing| *existing = (*existing).min(*block))
                .or_insert(*block);
        }
        Ok(())
    }

    async fn remove_factory_addresses_after(
        &self,
        chain_id: u64,
        factory_id: &str,
        block_number: u64,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        let key = (chain_id, factory_id.to_owned());
        let mut empty = false;
        if let Some(entries) = state.factories.get_mut(&key) {
            entries.retain(|_, block| *block <= block_number);
            empty = entries.is_empty();
        }
        if empty {
            state.factories.remove(&key);
        }
        Ok(())
    }
}

struct MemoryBatch {
    tables: Arc<BTreeMap<TableId, TableDef>>,
    state: Arc<Mutex<MemoryState>>,
    chain_id: u64,
    checkpoint: Checkpoint,
    cache: Mutex<BatchCache>,
}

impl MemoryBatch {
    fn mutate(
        &self,
        table: &TableRef,
        key: &Row,
        value: Option<&Row>,
        mode: Mutation,
    ) -> Result<(), StoreError> {
        let table_id = TableId::from_ref(table);
        let def = lookup(&self.tables, &table_id)?;
        let primary_key = primary_key_row(def, key)?;
        let row_key = row_key_string(&primary_key)?;
        if let Some(value) = value {
            validate_values(def, value)?;
        }
        let cached = {
            let cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
            cache.state(&table_id, &row_key)
        };
        let current = match cached {
            Some(current) => current,
            None => {
                let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
                state
                    .rows
                    .get(&table_id)
                    .and_then(|rows| rows.get(&row_key))
                    .cloned()
            }
        };
        check_presence(def, mode, current.is_some())?;
        let next = next_row(def, &primary_key, current.as_ref(), value, mode);
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        cache.record(table_id, row_key, primary_key, current, next);
        Ok(())
    }
}

#[async_trait]
impl IndexingStore for MemoryBatch {
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError> {
        let table_id = TableId::from_ref(table);
        let def = lookup(&self.tables, &table_id)?;
        let primary_key = primary_key_row(def, key)?;
        let row_key = row_key_string(&primary_key)?;
        {
            let cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
            if let Some(cached) = cache.state(&table_id, &row_key) {
                return Ok(cached);
            }
        }
        let state = self.state.lock().map_err(|_| StoreError::Poisoned)?;
        Ok(state
            .rows
            .get(&table_id)
            .and_then(|rows| rows.get(&row_key))
            .cloned())
    }

    async fn insert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Insert)
    }

    async fn update(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Update)
    }

    async fn upsert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, Some(value), Mutation::Upsert)
    }

    async fn delete(&self, table: &TableRef, key: &Row) -> Result<(), StoreError> {
        self.mutate(table, key, None, Mutation::Delete)
    }

    async fn raw(&self, sql: &str, _params: &[CellValue]) -> Result<Vec<Row>, StoreError> {
        validate_statement(sql)?;
        Err(StoreError::Unsupported("raw SQL"))
    }
}

#[async_trait]
impl StoreBatch for MemoryBatch {
    async fn commit(self: Box<Self>) -> Result<(), StoreError> {
        let this = *self;
        let cache = this.cache.into_inner().map_err(|_| StoreError::Poisoned)?;
        let mut state = this.state.lock().map_err(|_| StoreError::Poisoned)?;
        let current = state
            .progress
            .get(&this.chain_id)
            .map(|progress| progress.checkpoint)
            .unwrap_or(Checkpoint::ZERO);
        if this.checkpoint <= current {
            return Err(StoreError::NonAdvancingCheckpoint);
        }

        let (tables, undo) = cache.into_parts();
        for (table_id, rows) in tables {
            let target = state.rows.entry(table_id).or_default();
            for (row_key, row) in rows {
                match row.value {
                    Some(value) => {
                        target.insert(row_key, value);
                    }
                    None => {
                        target.remove(&row_key);
                    }
                }
            }
        }
        for entry in undo {
            state
                .undo
                .entry(this.chain_id)
                .or_default()
                .push(MemoryUndo {
                    checkpoint: this.checkpoint,
                    table: entry.table,
                    row_key: row_key_string(&entry.key)?,
                    before: entry.before,
                });
        }
        state
            .checkpoints
            .entry(this.chain_id)
            .or_default()
            .insert(this.checkpoint);
        state
            .progress
            .entry(this.chain_id)
            .or_insert(Progress {
                checkpoint: Checkpoint::ZERO,
                finalized: Checkpoint::ZERO,
            })
            .checkpoint = this.checkpoint;
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), StoreError> {
        let this = *self;
        let _cache = this.cache.into_inner().map_err(|_| StoreError::Poisoned)?;
        Ok(())
    }
}
