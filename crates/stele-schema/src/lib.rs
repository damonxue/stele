//! Explicit table definitions and PostgreSQL DDL.
//!
//! Definitions are validated before SQL is returned. Identifiers are quoted,
//! not interpreted as SQL. The 63-byte identifier limit assumes a standard
//! PostgreSQL build; the crate does not inspect a live server.
//!
//! Decimal declarations support precision 1..=1000 and scale 0..=precision.
//! Other PostgreSQL numeric declarations, including negative scales and scales
//! larger than precision, are intentionally outside this API.
//!
//! Enum labels are rendered as SQL string literals with embedded single quotes
//! doubled. View SQL is copied verbatim (apart from trimming whitespace and a
//! trailing semicolon) and is therefore trusted input, not a sandboxed value.
//! Enum, view and index names are quoted like every other identifier.
//!
//! This crate does not encode values or connect to a database. It does not
//! create schemas, inspect existing objects or distinguish a rename from a
//! drop plus an add; see [`SchemaDef::diff`].

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

/// A supported scalar PostgreSQL column type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScalarType {
    Text,
    Integer,
    SmallInt,
    BigInt,
    Boolean,
    Real,
    DoublePrecision,
    Jsonb,
    Bytes,
    Timestamp,
    /// An exact decimal with the supported precision and scale described above.
    Numeric {
        precision: u16,
        scale: u16,
    },
}

impl ScalarType {
    /// Storage type specified by the design for both U256 and I256.
    pub const EVM_INTEGER: Self = Self::Numeric {
        precision: 78,
        scale: 0,
    };

    fn sql(&self) -> Result<String, SchemaError> {
        Ok(match self {
            Self::Text => "TEXT".into(),
            Self::Integer => "INTEGER".into(),
            Self::SmallInt => "SMALLINT".into(),
            Self::BigInt => "BIGINT".into(),
            Self::Boolean => "BOOLEAN".into(),
            Self::Real => "REAL".into(),
            Self::DoublePrecision => "DOUBLE PRECISION".into(),
            Self::Jsonb => "JSONB".into(),
            Self::Bytes => "BYTEA".into(),
            Self::Timestamp => "TIMESTAMPTZ".into(),
            Self::Numeric { precision, scale } => {
                if !(1..=1000).contains(precision) || scale > precision {
                    return Err(SchemaError::InvalidNumeric {
                        precision: *precision,
                        scale: *scale,
                    });
                }
                format!("NUMERIC({precision},{scale})")
            }
        })
    }
}

/// A scalar or a one-dimensional array of supported scalars.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Scalar(ScalarType),
    Array(ScalarType),
}

impl From<ScalarType> for ColumnType {
    fn from(value: ScalarType) -> Self {
        Self::Scalar(value)
    }
}

impl ColumnType {
    fn sql(&self) -> Result<String, SchemaError> {
        match self {
            Self::Scalar(ty) => ty.sql(),
            Self::Array(ty) => Ok(format!("{}[]", ty.sql()?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(
        name: impl Into<String>,
        column_type: impl Into<ColumnType>,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
            nullable,
        }
    }
}

/// A secondary index on one or more columns of a table.
///
/// Index names are unique per schema, matching PostgreSQL. Use
/// [`TableDef::create_index_sqls`] to render a table's indexes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    /// Column names, in index order.
    pub columns: Vec<String>,
    pub unique: bool,
}

impl IndexDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>, unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            unique,
        }
    }
}

/// A read-only view. `sql` is the body of the `CREATE VIEW` statement.
///
/// The name is an unqualified identifier resolved through the connection's
/// `search_path`; the SQL itself is trusted input and is not sanitized.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewDef {
    pub name: String,
    pub sql: String,
}

impl ViewDef {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
        }
    }
}

/// A PostgreSQL enum type.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnumDef {
    pub name: String,
    pub values: Vec<String>,
}

impl EnumDef {
    pub fn new(name: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            name: name.into(),
            values,
        }
    }
}

/// A table with an explicit, nonempty single-column or composite primary key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableDef {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Column names, in primary-key order.
    pub primary_key: Vec<String>,
    /// Secondary indexes, emitted after the table exists.
    pub indexes: Vec<IndexDef>,
}

impl TableDef {
    pub fn validate(&self) -> Result<(), SchemaError> {
        validate_identifier(&self.schema)?;
        validate_identifier(&self.name)?;
        if self.columns.is_empty() {
            return Err(SchemaError::NoColumns);
        }

        let mut columns = HashSet::new();
        for column in &self.columns {
            validate_identifier(&column.name)?;
            if !columns.insert(column.name.as_str()) {
                return Err(SchemaError::DuplicateColumn(column.name.clone()));
            }
            column.column_type.sql()?;
        }
        if self.primary_key.is_empty() {
            return Err(SchemaError::NoPrimaryKey);
        }
        let mut primary_key = HashSet::new();
        for name in &self.primary_key {
            validate_identifier(name)?;
            if !primary_key.insert(name.as_str()) {
                return Err(SchemaError::DuplicatePrimaryKeyColumn(name.clone()));
            }
            let column = self.columns.iter().find(|column| column.name == *name);
            match column {
                None => return Err(SchemaError::UnknownPrimaryKeyColumn(name.clone())),
                Some(column) if column.nullable => {
                    return Err(SchemaError::NullablePrimaryKeyColumn(name.clone()));
                }
                Some(_) => {}
            }
        }

        let mut index_names = HashSet::new();
        for index in &self.indexes {
            validate_identifier(&index.name)?;
            if !index_names.insert(index.name.as_str()) {
                return Err(SchemaError::DuplicateIndex {
                    schema: self.schema.clone(),
                    name: index.name.clone(),
                });
            }
            if index.columns.is_empty() {
                return Err(SchemaError::NoIndexColumns(index.name.clone()));
            }
            for column in &index.columns {
                validate_identifier(column)?;
                if !columns.contains(column.as_str()) {
                    return Err(SchemaError::UnknownIndexColumn {
                        index: index.name.clone(),
                        column: column.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Render a statement for an existing PostgreSQL schema.
    pub fn create_table_sql(&self) -> Result<String, SchemaError> {
        self.validate()?;
        let mut parts = Vec::with_capacity(self.columns.len() + 1);
        for column in &self.columns {
            let nullable = if column.nullable { "" } else { " NOT NULL" };
            parts.push(format!(
                "  {} {}{}",
                quote_identifier(&column.name)?,
                column.column_type.sql()?,
                nullable
            ));
        }
        let keys = self
            .primary_key
            .iter()
            .map(|name| quote_identifier(name))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        parts.push(format!("  PRIMARY KEY ({keys})"));
        Ok(format!(
            "CREATE TABLE {}.{} (\n{}\n);",
            quote_identifier(&self.schema)?,
            quote_identifier(&self.name)?,
            parts.join(",\n")
        ))
    }

    /// Render one `CREATE INDEX` statement per index, in declaration order.
    pub fn create_index_sqls(&self) -> Result<Vec<String>, SchemaError> {
        self.validate()?;
        self.indexes
            .iter()
            .map(|index| create_index_sql(self, index))
            .collect()
    }
}

/// A collection of explicitly qualified tables, enum types and views.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaDef {
    pub tables: Vec<TableDef>,
    pub enums: Vec<EnumDef>,
    pub views: Vec<ViewDef>,
}

impl SchemaDef {
    pub fn validate(&self) -> Result<(), SchemaError> {
        let mut tables = HashSet::new();
        let mut indexes = HashSet::new();
        for table in &self.tables {
            table.validate()?;
            if !tables.insert((table.schema.as_str(), table.name.as_str())) {
                return Err(SchemaError::DuplicateTable {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                });
            }
            for index in &table.indexes {
                if !indexes.insert((table.schema.as_str(), index.name.as_str())) {
                    return Err(SchemaError::DuplicateIndex {
                        schema: table.schema.clone(),
                        name: index.name.clone(),
                    });
                }
            }
        }

        let mut enums = HashSet::new();
        for def in &self.enums {
            validate_enum(def)?;
            if !enums.insert(def.name.as_str()) {
                return Err(SchemaError::DuplicateEnum(def.name.clone()));
            }
        }

        let mut views = HashSet::new();
        for def in &self.views {
            validate_view(def)?;
            if !views.insert(def.name.as_str()) {
                return Err(SchemaError::DuplicateView(def.name.clone()));
            }
        }
        Ok(())
    }

    /// Validate the complete collection, then render statements in table order.
    pub fn create_tables_sql(&self) -> Result<Vec<String>, SchemaError> {
        self.validate()?;
        self.tables.iter().map(TableDef::create_table_sql).collect()
    }

    /// Render DDL for a fresh schema in dependency-safe order: enum types,
    /// tables, indexes, then views.
    ///
    /// Unlike [`Self::create_tables_sql`], this includes every object in the
    /// collection. Statements are not wrapped in a transaction.
    pub fn create_statements(&self) -> Result<Vec<String>, SchemaError> {
        self.validate()?;
        let mut statements = Vec::new();
        for def in &self.enums {
            statements.push(create_enum_sql(def)?);
        }
        for table in &self.tables {
            statements.push(table.create_table_sql()?);
        }
        for table in &self.tables {
            statements.extend(table.create_index_sqls()?);
        }
        for def in &self.views {
            statements.push(create_view_sql(def)?);
        }
        Ok(statements)
    }

    /// Render statements that migrate `previous` to `self`.
    ///
    /// Added tables, columns, indexes and views produce `CREATE`; removed
    /// objects produce `DROP`; column type and nullability changes produce
    /// `ALTER COLUMN`. Changes that this API cannot express safely, such as a
    /// different primary key or a different set of enum labels, return
    /// [`SchemaError::UnsupportedChange`] instead of being ignored.
    ///
    /// PostgreSQL cannot distinguish a rename from a drop plus an add without
    /// explicit metadata, which this API does not carry. A renamed table or
    /// column therefore appears as `DROP` plus `CREATE`; review the statements
    /// before applying them when data loss matters.
    pub fn diff(&self, previous: &SchemaDef) -> Result<Vec<String>, SchemaError> {
        self.validate()?;
        previous.validate()?;

        let mut statements = Vec::new();

        // Drop removed views first so they cannot block table changes.
        for view in &previous.views {
            if !self.views.iter().any(|current| current.name == view.name) {
                statements.push(format!("DROP VIEW {};", quote_identifier(&view.name)?));
            }
        }

        // Create new enum types before any table that might reference them.
        for def in &self.enums {
            match previous.enums.iter().find(|old| old.name == def.name) {
                None => statements.push(create_enum_sql(def)?),
                Some(old) if old.values != def.values => {
                    return Err(SchemaError::UnsupportedChange(format!(
                        "enum {:?} changes its labels from {:?} to {:?}; PostgreSQL cannot \
                         rewrite an existing enum in place",
                        def.name, old.values, def.values
                    )));
                }
                Some(_) => {}
            }
        }

        for table in &self.tables {
            if !previous.tables.iter().any(|old| same_table(old, table)) {
                statements.push(table.create_table_sql()?);
                statements.extend(table.create_index_sqls()?);
            }
        }

        for table in &self.tables {
            if let Some(old) = previous.tables.iter().find(|old| same_table(old, table)) {
                statements.extend(diff_table(old, table)?);
            }
        }

        for table in &previous.tables {
            if !self.tables.iter().any(|current| same_table(current, table)) {
                statements.push(format!(
                    "DROP TABLE {}.{};",
                    quote_identifier(&table.schema)?,
                    quote_identifier(&table.name)?
                ));
            }
        }

        // Views may depend on tables and indexes created above.
        for def in &self.views {
            match previous.views.iter().find(|old| old.name == def.name) {
                None => statements.push(create_view_sql(def)?),
                Some(old) => {
                    if normalized_view_sql(&old.sql) != normalized_view_sql(&def.sql) {
                        statements.push(format!(
                            "CREATE OR REPLACE VIEW {} AS {};",
                            quote_identifier(&def.name)?,
                            normalized_view_sql(&def.sql)
                        ));
                    }
                }
            }
        }

        // Drop removed enum types last, after tables that might use them.
        for def in &previous.enums {
            if !self.enums.iter().any(|current| current.name == def.name) {
                statements.push(format!("DROP TYPE {};", quote_identifier(&def.name)?));
            }
        }

        Ok(statements)
    }
}

fn same_table(left: &TableDef, right: &TableDef) -> bool {
    left.schema == right.schema && left.name == right.name
}

fn diff_table(previous: &TableDef, current: &TableDef) -> Result<Vec<String>, SchemaError> {
    if previous.primary_key != current.primary_key {
        return Err(SchemaError::UnsupportedChange(format!(
            "primary key of {}.{} changes from {:?} to {:?}; recreate the table or perform a \
             manual migration",
            current.schema, current.name, previous.primary_key, current.primary_key
        )));
    }

    let table = format!(
        "{}.{}",
        quote_identifier(&current.schema)?,
        quote_identifier(&current.name)?
    );
    let mut statements = Vec::new();

    for index in &previous.indexes {
        if current.indexes.iter().find(|new| new.name == index.name) != Some(index) {
            statements.push(format!(
                "DROP INDEX {}.{};",
                quote_identifier(&current.schema)?,
                quote_identifier(&index.name)?
            ));
        }
    }

    for column in &previous.columns {
        if !current.columns.iter().any(|new| new.name == column.name) {
            statements.push(format!(
                "ALTER TABLE {table} DROP COLUMN {};",
                quote_identifier(&column.name)?
            ));
        }
    }

    for column in &current.columns {
        let Some(old) = previous.columns.iter().find(|old| old.name == column.name) else {
            continue;
        };
        if old.column_type != column.column_type {
            statements.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {} TYPE {};",
                quote_identifier(&column.name)?,
                column.column_type.sql()?
            ));
        }
        if old.nullable != column.nullable {
            let action = if column.nullable {
                "DROP NOT NULL"
            } else {
                "SET NOT NULL"
            };
            statements.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {} {action};",
                quote_identifier(&column.name)?
            ));
        }
    }

    for column in &current.columns {
        if !previous.columns.iter().any(|old| old.name == column.name) {
            let nullable = if column.nullable { "" } else { " NOT NULL" };
            statements.push(format!(
                "ALTER TABLE {table} ADD COLUMN {} {}{nullable};",
                quote_identifier(&column.name)?,
                column.column_type.sql()?
            ));
        }
    }

    for index in &current.indexes {
        if previous.indexes.iter().find(|old| old.name == index.name) != Some(index) {
            statements.push(create_index_sql(current, index)?);
        }
    }

    Ok(statements)
}

fn create_index_sql(table: &TableDef, index: &IndexDef) -> Result<String, SchemaError> {
    validate_identifier(&index.name)?;
    if index.columns.is_empty() {
        return Err(SchemaError::NoIndexColumns(index.name.clone()));
    }
    let unique = if index.unique { "UNIQUE " } else { "" };
    let columns = index
        .columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "CREATE {unique}INDEX {} ON {}.{} ({columns});",
        quote_identifier(&index.name)?,
        quote_identifier(&table.schema)?,
        quote_identifier(&table.name)?
    ))
}

fn create_enum_sql(def: &EnumDef) -> Result<String, SchemaError> {
    validate_enum(def)?;
    let values = def
        .values
        .iter()
        .map(|value| quote_string_literal(value))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "CREATE TYPE {} AS ENUM ({values});",
        quote_identifier(&def.name)?
    ))
}

fn create_view_sql(def: &ViewDef) -> Result<String, SchemaError> {
    validate_view(def)?;
    Ok(format!(
        "CREATE VIEW {} AS {};",
        quote_identifier(&def.name)?,
        normalized_view_sql(&def.sql)
    ))
}

fn validate_enum(def: &EnumDef) -> Result<(), SchemaError> {
    validate_identifier(&def.name)?;
    if def.values.is_empty() {
        return Err(SchemaError::NoEnumValues(def.name.clone()));
    }
    let mut values = HashSet::new();
    for value in &def.values {
        if value.contains('\0') {
            return Err(SchemaError::InvalidEnumValue(value.clone()));
        }
        if !values.insert(value.as_str()) {
            return Err(SchemaError::DuplicateEnumValue {
                enum_name: def.name.clone(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn validate_view(def: &ViewDef) -> Result<(), SchemaError> {
    validate_identifier(&def.name)?;
    if normalized_view_sql(&def.sql).is_empty() || def.sql.contains('\0') {
        return Err(SchemaError::InvalidViewSql(def.name.clone()));
    }
    Ok(())
}

/// Trim surrounding whitespace and a trailing semicolon so generated
/// statements do not end in `;;`.
fn normalized_view_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

/// Render a PostgreSQL string literal. Only the single quote is special with
/// `standard_conforming_strings` (the default since PostgreSQL 9.1).
fn quote_string_literal(value: &str) -> Result<String, SchemaError> {
    if value.contains('\0') {
        return Err(SchemaError::InvalidEnumValue(value.to_owned()));
    }
    Ok(format!("'{}'", value.replace('\'', "''")))
}

/// Quote one PostgreSQL identifier, rejecting invalid or truncating names.
pub fn quote_identifier(identifier: &str) -> Result<String, SchemaError> {
    validate_identifier(identifier)?;
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

fn validate_identifier(identifier: &str) -> Result<(), SchemaError> {
    if identifier.is_empty() || identifier.len() > 63 || identifier.contains('\0') {
        return Err(SchemaError::InvalidIdentifier(identifier.to_owned()));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaError {
    InvalidIdentifier(String),
    InvalidNumeric { precision: u16, scale: u16 },
    NoColumns,
    NoPrimaryKey,
    DuplicateColumn(String),
    DuplicatePrimaryKeyColumn(String),
    UnknownPrimaryKeyColumn(String),
    NullablePrimaryKeyColumn(String),
    DuplicateTable { schema: String, name: String },
    DuplicateIndex { schema: String, name: String },
    NoIndexColumns(String),
    UnknownIndexColumn { index: String, column: String },
    DuplicateEnum(String),
    NoEnumValues(String),
    DuplicateEnumValue { enum_name: String, value: String },
    InvalidEnumValue(String),
    DuplicateView(String),
    InvalidViewSql(String),
    UnsupportedChange(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier(name) => write!(f, "invalid PostgreSQL identifier: {name:?}"),
            Self::InvalidNumeric { precision, scale } => {
                write!(
                    f,
                    "unsupported numeric precision {precision} and scale {scale}"
                )
            }
            Self::NoColumns => write!(f, "table has no columns"),
            Self::NoPrimaryKey => write!(f, "table has no primary key"),
            Self::DuplicateColumn(name) => write!(f, "duplicate column: {name:?}"),
            Self::DuplicatePrimaryKeyColumn(name) => {
                write!(f, "duplicate primary key column: {name:?}")
            }
            Self::UnknownPrimaryKeyColumn(name) => {
                write!(f, "unknown primary key column: {name:?}")
            }
            Self::NullablePrimaryKeyColumn(name) => {
                write!(f, "primary key column is nullable: {name:?}")
            }
            Self::DuplicateTable { schema, name } => {
                write!(f, "duplicate table: {schema:?}.{name:?}")
            }
            Self::DuplicateIndex { schema, name } => {
                write!(f, "duplicate index: {schema:?}.{name:?}")
            }
            Self::NoIndexColumns(name) => write!(f, "index has no columns: {name:?}"),
            Self::UnknownIndexColumn { index, column } => {
                write!(f, "index {index:?} references unknown column {column:?}")
            }
            Self::DuplicateEnum(name) => write!(f, "duplicate enum: {name:?}"),
            Self::NoEnumValues(name) => write!(f, "enum has no labels: {name:?}"),
            Self::DuplicateEnumValue { enum_name, value } => {
                write!(f, "enum {enum_name:?} repeats label {value:?}")
            }
            Self::InvalidEnumValue(value) => {
                write!(f, "enum label contains a NUL byte: {value:?}")
            }
            Self::DuplicateView(name) => write!(f, "duplicate view: {name:?}"),
            Self::InvalidViewSql(name) => write!(f, "view has empty or invalid SQL: {name:?}"),
            Self::UnsupportedChange(message) => write!(f, "unsupported schema change: {message}"),
        }
    }
}

impl Error for SchemaError {}
