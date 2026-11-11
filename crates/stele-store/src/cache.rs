//! Per-batch write cache shared by the PostgreSQL and in-memory stores.
//!
//! The cache holds the final in-batch state of every touched row plus one
//! before image per row. The before image is the committed state at the time
//! of the first mutation, which is exactly what [`crate::Store::revert`] needs.

use std::collections::BTreeMap;

use stele_schema::TableDef;

use crate::sql::TableId;
use crate::{Row, StoreError};

/// How a mutation changes a cached row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mutation {
    Insert,
    Update,
    Upsert,
    Delete,
}

/// Current in-batch state of one row.
pub(crate) struct CachedRow {
    /// Primary-key columns and their values.
    pub(crate) key: Row,
    /// Row after all mutations in this batch; `None` marks a deletion.
    pub(crate) value: Option<Row>,
}

/// Before image recorded for one row in one checkpoint.
pub(crate) struct UndoEntry {
    pub(crate) table: TableId,
    pub(crate) key: Row,
    /// Committed row before the first mutation; `None` means the row did not
    /// exist.
    pub(crate) before: Option<Row>,
}

/// Rows written by a batch, keyed by table and canonical primary-key string.
#[derive(Default)]
pub(crate) struct BatchCache {
    tables: BTreeMap<TableId, BTreeMap<String, CachedRow>>,
    undo: Vec<UndoEntry>,
}

impl BatchCache {
    /// Return the cached state of one row.
    ///
    /// The outer `Option` reports whether the row was touched at all, so
    /// `Some(None)` means "deleted in this batch" and `None` means "read the
    /// committed state".
    pub(crate) fn state(&self, table: &TableId, row_key: &str) -> Option<Option<Row>> {
        self.tables
            .get(table)?
            .get(row_key)
            .map(|row| row.value.clone())
    }

    /// Record the final state of a row.
    ///
    /// The before image is stored only for the first mutation. Later
    /// mutations of the same row in the same batch reuse it.
    pub(crate) fn record(
        &mut self,
        table: TableId,
        row_key: String,
        key: Row,
        before: Option<Row>,
        value: Option<Row>,
    ) {
        let rows = self.tables.entry(table.clone()).or_default();
        if let Some(cached) = rows.get_mut(&row_key) {
            cached.value = value;
            return;
        }
        let undo_key = key.clone();
        rows.insert(row_key, CachedRow { key, value });
        self.undo.push(UndoEntry {
            table,
            key: undo_key,
            before,
        });
    }

    /// Iterate the cached rows grouped by table.
    pub(crate) fn tables(&self) -> impl Iterator<Item = (&TableId, &BTreeMap<String, CachedRow>)> {
        self.tables.iter()
    }

    /// Before images in the order they were first recorded.
    pub(crate) fn undo(&self) -> &[UndoEntry] {
        &self.undo
    }

    /// Consume the cache and return its rows and undo entries.
    pub(crate) fn into_parts(
        self,
    ) -> (
        BTreeMap<TableId, BTreeMap<String, CachedRow>>,
        Vec<UndoEntry>,
    ) {
        (self.tables, self.undo)
    }
}

/// Apply a mutation to the current row.
///
/// Deletes produce `None`. Other mutations start from the current row (or an
/// empty row for inserts), overlay the supplied values and then restore the
/// primary-key columns from `key` so the key always wins.
pub(crate) fn next_row(
    def: &TableDef,
    key: &Row,
    current: Option<&Row>,
    value: Option<&Row>,
    mode: Mutation,
) -> Option<Row> {
    if mode == Mutation::Delete {
        return None;
    }
    let mut next = if mode == Mutation::Insert {
        Row::new()
    } else {
        current.cloned().unwrap_or_default()
    };
    if let Some(value) = value {
        for (name, cell) in value {
            next.insert(name.clone(), cell.clone());
        }
    }
    for name in &def.primary_key {
        if let Some(cell) = key.get(name) {
            next.insert(name.clone(), cell.clone());
        }
    }
    Some(next)
}

/// Reject `insert` of an existing row and `update`/`delete` of a missing row.
pub(crate) fn check_presence(
    def: &TableDef,
    mode: Mutation,
    exists: bool,
) -> Result<(), StoreError> {
    match (mode, exists) {
        (Mutation::Insert, true) => Err(StoreError::AlreadyExists {
            table: def.name.clone(),
        }),
        (Mutation::Update | Mutation::Delete, false) => Err(StoreError::NotFound {
            table: def.name.clone(),
        }),
        _ => Ok(()),
    }
}
