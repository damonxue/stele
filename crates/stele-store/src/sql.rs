//! SQL construction, statement validation and value conversion.
//!
//! Every identifier is quoted with [`stele_schema::quote_identifier`]. Values
//! bound to user tables are sent as text and cast to the column type declared
//! by the schema, e.g. `$1::int4` or `$1::numeric[]`; PostgreSQL parses and
//! range-checks the literal during execution.

use std::collections::BTreeMap;

use sqlx::postgres::PgArguments;
use sqlx::postgres::{PgRow, PgValueFormat, PgValueRef};
use sqlx::query::Query;
use sqlx::{Column as _, Executor, Postgres, Row as _, TypeInfo as _, ValueRef as _};
use stele_schema::{quote_identifier, ColumnDef, ColumnType, ScalarType, SchemaDef, TableDef};

use crate::{CellValue, Row, StoreError, TableRef};

/// PostgreSQL type OIDs used by [`CellValue`] decoding.
const BOOL_OID: u32 = 16;
const BYTEA_OID: u32 = 17;
const CHAR_OID: u32 = 18;
const NAME_OID: u32 = 19;
const INT8_OID: u32 = 20;
const INT2_OID: u32 = 21;
const INT4_OID: u32 = 23;
const TEXT_OID: u32 = 25;
const JSON_OID: u32 = 114;
const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;
const BPCHAR_OID: u32 = 1042;
const VARCHAR_OID: u32 = 1043;
const NUMERIC_OID: u32 = 1700;
const TIMESTAMP_OID: u32 = 1114;
const TIMESTAMPTZ_OID: u32 = 1184;
const JSONB_OID: u32 = 3802;

const BOOL_ARRAY_OID: u32 = 1000;
const BYTEA_ARRAY_OID: u32 = 1001;
const CHAR_ARRAY_OID: u32 = 1002;
const NAME_ARRAY_OID: u32 = 1003;
const INT2_ARRAY_OID: u32 = 1005;
const INT4_ARRAY_OID: u32 = 1007;
const TEXT_ARRAY_OID: u32 = 1009;
const BPCHAR_ARRAY_OID: u32 = 1014;
const VARCHAR_ARRAY_OID: u32 = 1015;
const INT8_ARRAY_OID: u32 = 1016;
const FLOAT4_ARRAY_OID: u32 = 1021;
const FLOAT8_ARRAY_OID: u32 = 1022;
const JSON_ARRAY_OID: u32 = 199;
const NUMERIC_ARRAY_OID: u32 = 1231;
const TIMESTAMP_ARRAY_OID: u32 = 1115;
const TIMESTAMPTZ_ARRAY_OID: u32 = 1185;
const JSONB_ARRAY_OID: u32 = 3807;

/// Keywords that are never accepted by [`crate::Store::query`] or
/// [`crate::IndexingStore::raw`].
const FORBIDDEN_KEYWORDS: &[&str] = &[
    "ABORT",
    "ALTER",
    "ANALYZE",
    "BEGIN",
    "CALL",
    "CLOSE",
    "CLUSTER",
    "COMMENT",
    "COMMIT",
    "COPY",
    "CREATE",
    "DEALLOCATE",
    "DECLARE",
    "DISCARD",
    "DO",
    "DROP",
    "END",
    "EXECUTE",
    "FETCH",
    "GRANT",
    "LISTEN",
    "LOCK",
    "MOVE",
    "NOTIFY",
    "PREPARE",
    "REASSIGN",
    "REFRESH",
    "REINDEX",
    "RELEASE",
    "RESET",
    "REVOKE",
    "ROLLBACK",
    "SAVEPOINT",
    "SECURITY",
    "SET",
    "START",
    "TRUNCATE",
    "UNLISTEN",
    "VACUUM",
];

/// Identifies one logical table, including schemas that reuse a table name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TableId {
    pub(crate) schema: String,
    pub(crate) name: String,
}

impl TableId {
    pub(crate) fn from_ref(table: &TableRef) -> Self {
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
        }
    }
}

/// A statement with its parameters already encoded as text literals.
pub(crate) struct Statement {
    pub(crate) sql: String,
    pub(crate) params: Vec<Option<String>>,
}

/// Validate the schema and index its tables by qualified name.
pub(crate) fn schema_tables(schema: &SchemaDef) -> Result<BTreeMap<TableId, TableDef>, StoreError> {
    schema.validate()?;
    let mut tables = BTreeMap::new();
    for table in &schema.tables {
        tables.insert(
            TableId {
                schema: table.schema.clone(),
                name: table.name.clone(),
            },
            table.clone(),
        );
    }
    Ok(tables)
}

/// Look up a table definition, reporting [`StoreError::UnknownTable`] when it
/// was not part of the initialized schema.
pub(crate) fn lookup<'a>(
    tables: &'a BTreeMap<TableId, TableDef>,
    table: &TableId,
) -> Result<&'a TableDef, StoreError> {
    tables.get(table).ok_or_else(|| StoreError::UnknownTable {
        schema: table.schema.clone(),
        name: table.name.clone(),
    })
}

/// Extract the primary-key columns named by `key`.
pub(crate) fn primary_key_row(def: &TableDef, key: &Row) -> Result<Row, StoreError> {
    let mut primary_key = Row::new();
    for name in &def.primary_key {
        let value = key.get(name).ok_or_else(|| StoreError::MissingKeyColumn {
            table: def.name.clone(),
            column: name.clone(),
        })?;
        primary_key.insert(name.clone(), value.clone());
    }
    Ok(primary_key)
}

/// Canonical string form of a primary key.
///
/// [`Row`] is a [`BTreeMap`], so the JSON object orders its keys and two keys
/// with the same values always serialize identically.
pub(crate) fn row_key_string(key: &Row) -> Result<String, StoreError> {
    Ok(serde_json::to_string(key)?)
}

/// Reject unknown columns and values that cannot be encoded for their column.
pub(crate) fn validate_values(def: &TableDef, value: &Row) -> Result<(), StoreError> {
    for name in value.keys() {
        if !def.columns.iter().any(|column| column.name == *name) {
            return Err(StoreError::UnknownColumn {
                table: def.name.clone(),
                column: name.clone(),
            });
        }
    }
    for column in &def.columns {
        if let Some(cell) = value.get(&column.name) {
            encode_value(column, cell)?;
        }
    }
    Ok(())
}

/// Serialize a full row image for the undo table.
///
/// [`CellValue`] is tagged because its plain JSON form is lossy: a JSON string
/// and a text value look identical, and non-finite floats cannot be encoded as
/// JSON numbers. Undo replay must restore the exact value, so each cell is
/// written as `{"type": ..., "value": ...}`.
pub(crate) fn encode_row(image: &Row) -> serde_json::Value {
    let mut object = serde_json::Map::with_capacity(image.len());
    for (name, value) in image {
        object.insert(name.clone(), encode_cell(value));
    }
    serde_json::Value::Object(object)
}

/// Inverse of [`encode_row`].
pub(crate) fn decode_row(value: &serde_json::Value) -> Result<Row, StoreError> {
    let object = value
        .as_object()
        .ok_or_else(|| StoreError::Decode("row image must be a JSON object".to_owned()))?;
    let mut row = Row::new();
    for (name, cell) in object {
        row.insert(name.clone(), decode_cell(cell)?);
    }
    Ok(row)
}

fn encode_cell(value: &CellValue) -> serde_json::Value {
    use serde_json::json;
    match value {
        CellValue::Null => json!({ "type": "null" }),
        CellValue::Bool(flag) => json!({ "type": "bool", "value": flag }),
        CellValue::Int(number) => json!({ "type": "int", "value": number }),
        CellValue::Float(number) => json!({ "type": "float", "value": float_text(*number) }),
        CellValue::Numeric(text) => json!({ "type": "numeric", "value": text }),
        CellValue::Text(text) => json!({ "type": "text", "value": text }),
        CellValue::Bytes(bytes) => json!({ "type": "bytes", "value": bytes_hex(bytes) }),
        CellValue::Json(document) => json!({ "type": "json", "value": document }),
        CellValue::Array(items) => json!({
            "type": "array",
            "value": items.iter().map(encode_cell).collect::<Vec<_>>(),
        }),
    }
}

fn decode_cell(value: &serde_json::Value) -> Result<CellValue, StoreError> {
    let object = value
        .as_object()
        .ok_or_else(|| StoreError::Decode("cell image must be a JSON object".to_owned()))?;
    let tag = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| StoreError::Decode("cell image is missing its type".to_owned()))?;
    let field = || {
        object
            .get("value")
            .ok_or_else(|| StoreError::Decode(format!("cell image {tag:?} is missing its value")))
    };
    match tag {
        "null" => Ok(CellValue::Null),
        "bool" => field()?
            .as_bool()
            .map(CellValue::Bool)
            .ok_or_else(|| StoreError::Decode("boolean cell image is invalid".to_owned())),
        "int" => field()?
            .as_i64()
            .map(CellValue::Int)
            .ok_or_else(|| StoreError::Decode("integer cell image is invalid".to_owned())),
        "float" => {
            let text = field()?
                .as_str()
                .ok_or_else(|| StoreError::Decode("float cell image is invalid".to_owned()))?;
            text.parse()
                .map(CellValue::Float)
                .map_err(|_| StoreError::Decode("float cell image is invalid".to_owned()))
        }
        "numeric" => Ok(CellValue::Numeric(
            field()?
                .as_str()
                .ok_or_else(|| StoreError::Decode("numeric cell image is invalid".to_owned()))?
                .to_owned(),
        )),
        "text" => Ok(CellValue::Text(
            field()?
                .as_str()
                .ok_or_else(|| StoreError::Decode("text cell image is invalid".to_owned()))?
                .to_owned(),
        )),
        "bytes" => {
            let text = field()?
                .as_str()
                .ok_or_else(|| StoreError::Decode("bytes cell image is invalid".to_owned()))?;
            Ok(CellValue::Bytes(parse_bytea(text)?))
        }
        "json" => Ok(CellValue::Json(field()?.clone())),
        "array" => {
            let items = field()?
                .as_array()
                .ok_or_else(|| StoreError::Decode("array cell image is invalid".to_owned()))?;
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(decode_cell(item)?);
            }
            Ok(CellValue::Array(values))
        }
        other => Err(StoreError::Decode(format!(
            "unknown cell image type {other:?}"
        ))),
    }
}

/// Encode one value for its column, returning `None` for SQL `NULL`.
pub(crate) fn encode_value(
    column: &ColumnDef,
    value: &CellValue,
) -> Result<Option<String>, StoreError> {
    let mismatch = || StoreError::TypeMismatch {
        column: column.name.clone(),
        expected: column_type_name(&column.column_type),
        found: cell_name(value),
    };
    if value.is_null() {
        return Ok(None);
    }
    match (&column.column_type, value) {
        (ColumnType::Scalar(ty), value) => scalar_text(ty, value).map(Some).ok_or_else(mismatch),
        (ColumnType::Array(element), CellValue::Array(items)) => {
            let mut encoded = Vec::with_capacity(items.len());
            for item in items {
                if item.is_null() {
                    encoded.push("NULL".to_owned());
                    continue;
                }
                let text = scalar_text(element, item).ok_or_else(mismatch)?;
                encoded.push(quote_array_element(&text));
            }
            Ok(Some(format!("{{{}}}", encoded.join(","))))
        }
        _ => Err(mismatch()),
    }
}

fn scalar_text(ty: &ScalarType, value: &CellValue) -> Option<String> {
    use CellValue as Value;
    match (ty, value) {
        (ScalarType::Text, Value::Text(text) | Value::Numeric(text)) => Some(text.clone()),
        (ScalarType::Integer | ScalarType::SmallInt | ScalarType::BigInt, Value::Int(number)) => {
            Some(number.to_string())
        }
        (ScalarType::Integer | ScalarType::SmallInt | ScalarType::BigInt, Value::Numeric(text)) => {
            Some(text.clone())
        }
        (ScalarType::Boolean, Value::Bool(flag)) => Some(flag.to_string()),
        (ScalarType::Bytes, Value::Bytes(bytes)) => Some(bytes_hex(bytes)),
        (ScalarType::Timestamp, Value::Text(text) | Value::Numeric(text)) => Some(text.clone()),
        (
            ScalarType::Real | ScalarType::DoublePrecision | ScalarType::Numeric { .. },
            Value::Float(number),
        ) => Some(float_text(*number)),
        (
            ScalarType::Real | ScalarType::DoublePrecision | ScalarType::Numeric { .. },
            Value::Int(number),
        ) => Some(number.to_string()),
        (
            ScalarType::Real | ScalarType::DoublePrecision | ScalarType::Numeric { .. },
            Value::Numeric(text),
        ) => Some(text.clone()),
        (ScalarType::Jsonb, Value::Json(json)) => serde_json::to_string(json).ok(),
        (ScalarType::Jsonb, Value::Text(text) | Value::Numeric(text)) => Some(text.clone()),
        _ => None,
    }
}

fn float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() && value.is_sign_positive() {
        "Infinity".to_owned()
    } else if value.is_infinite() {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn quote_array_element(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        if character == '"' || character == '\\' {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

fn column_cast(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Scalar(ty) => scalar_cast(ty).to_owned(),
        ColumnType::Array(ty) => format!("{}[]", scalar_cast(ty)),
    }
}

fn scalar_cast(ty: &ScalarType) -> &'static str {
    match ty {
        ScalarType::Text => "text",
        ScalarType::Integer => "int4",
        ScalarType::SmallInt => "int2",
        ScalarType::BigInt => "int8",
        ScalarType::Boolean => "bool",
        ScalarType::Real => "float4",
        ScalarType::DoublePrecision => "float8",
        ScalarType::Jsonb => "jsonb",
        ScalarType::Bytes => "bytea",
        ScalarType::Timestamp => "timestamptz",
        ScalarType::Numeric { .. } => "numeric",
    }
}

fn column_type_name(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Scalar(ty) => scalar_cast(ty),
        ColumnType::Array(_) => "array",
    }
}

fn cell_name(value: &CellValue) -> &'static str {
    match value {
        CellValue::Null => "null",
        CellValue::Bool(_) => "boolean",
        CellValue::Int(_) => "integer",
        CellValue::Float(_) => "float",
        CellValue::Numeric(_) => "numeric",
        CellValue::Text(_) => "text",
        CellValue::Bytes(_) => "bytes",
        CellValue::Json(_) => "json",
        CellValue::Array(_) => "array",
    }
}

fn qualified_name(schema: &str, name: &str) -> Result<String, StoreError> {
    Ok(format!(
        "{}.{}",
        quote_identifier(schema)?,
        quote_identifier(name)?
    ))
}

fn key_clause(def: &TableDef, key: &Row) -> Result<(Vec<String>, Vec<Option<String>>), StoreError> {
    let mut clauses = Vec::with_capacity(def.primary_key.len());
    let mut params = Vec::with_capacity(def.primary_key.len());
    for name in &def.primary_key {
        let column = def
            .columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| StoreError::UnknownColumn {
                table: def.name.clone(),
                column: name.clone(),
            })?;
        let value = key.get(name).ok_or_else(|| StoreError::MissingKeyColumn {
            table: def.name.clone(),
            column: name.clone(),
        })?;
        params.push(encode_value(column, value)?);
        clauses.push(format!(
            "{} = ${}::{}",
            quote_identifier(name)?,
            params.len(),
            column_cast(&column.column_type)
        ));
    }
    Ok((clauses, params))
}

/// Build an idempotent insert for the columns present in `row`.
pub(crate) fn upsert_statement(def: &TableDef, row: &Row) -> Result<Statement, StoreError> {
    let qualified = qualified_name(&def.schema, &def.name)?;
    let mut columns: Vec<&ColumnDef> = Vec::with_capacity(row.len());
    let mut params: Vec<Option<String>> = Vec::with_capacity(row.len());
    for column in &def.columns {
        if let Some(value) = row.get(&column.name) {
            params.push(encode_value(column, value)?);
            columns.push(column);
        }
    }
    for name in row.keys() {
        if !def.columns.iter().any(|column| column.name == *name) {
            return Err(StoreError::UnknownColumn {
                table: def.name.clone(),
                column: name.clone(),
            });
        }
    }
    for name in &def.primary_key {
        if !row.contains_key(name) {
            return Err(StoreError::MissingKeyColumn {
                table: def.name.clone(),
                column: name.clone(),
            });
        }
    }

    let mut names = Vec::with_capacity(columns.len());
    let mut placeholders = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        names.push(quote_identifier(&column.name)?);
        placeholders.push(format!(
            "${}::{}",
            index + 1,
            column_cast(&column.column_type)
        ));
    }
    let mut keys = Vec::with_capacity(def.primary_key.len());
    for name in &def.primary_key {
        keys.push(quote_identifier(name)?);
    }
    let mut updates = Vec::new();
    for column in &columns {
        if def.primary_key.contains(&column.name) {
            continue;
        }
        let name = quote_identifier(&column.name)?;
        updates.push(format!("{name} = EXCLUDED.{name}"));
    }
    let conflict = if updates.is_empty() {
        format!("ON CONFLICT ({}) DO NOTHING", keys.join(", "))
    } else {
        format!(
            "ON CONFLICT ({}) DO UPDATE SET {}",
            keys.join(", "),
            updates.join(", ")
        )
    };
    Ok(Statement {
        sql: format!(
            "INSERT INTO {qualified} ({}) VALUES ({}) {conflict}",
            names.join(", "),
            placeholders.join(", ")
        ),
        params,
    })
}

/// Build a delete addressed by primary key.
pub(crate) fn delete_statement(def: &TableDef, key: &Row) -> Result<Statement, StoreError> {
    let qualified = qualified_name(&def.schema, &def.name)?;
    let (clauses, params) = key_clause(def, key)?;
    Ok(Statement {
        sql: format!("DELETE FROM {qualified} WHERE {}", clauses.join(" AND ")),
        params,
    })
}

/// Build a single-row select addressed by primary key.
pub(crate) fn select_statement(def: &TableDef, key: &Row) -> Result<Statement, StoreError> {
    let qualified = qualified_name(&def.schema, &def.name)?;
    let (clauses, params) = key_clause(def, key)?;
    Ok(Statement {
        sql: format!("SELECT * FROM {qualified} WHERE {}", clauses.join(" AND ")),
        params,
    })
}

/// Check that `sql` is one statement and does not begin with a forbidden
/// keyword.
///
/// Quoted strings, quoted identifiers, dollar-quoted bodies and comments are
/// skipped while looking for statement terminators. The scanner is
/// deliberately conservative: anything it cannot classify is rejected.
pub(crate) fn validate_statement(sql: &str) -> Result<(), StoreError> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut keyword: Option<String> = None;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_quoted(bytes, index, b'\'')?,
            b'"' => index = skip_quoted(bytes, index, b'"')?,
            b'-' if bytes.get(index + 1) == Some(&b'-') => index = skip_line_comment(bytes, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index)?
            }
            b'$' => index = skip_dollar_quoted(bytes, index)?,
            b';' => {
                if !is_trivia(&bytes[index + 1..]) {
                    return Err(StoreError::MultipleStatements);
                }
                break;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                if keyword.is_some() {
                    index += 1;
                    continue;
                }
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric()
                        || bytes[index] == b'_'
                        || bytes[index] == b'$')
                {
                    index += 1;
                }
                keyword = Some(sql[start..index].to_ascii_uppercase());
            }
            _ => index += 1,
        }
    }
    let keyword = keyword.ok_or(StoreError::EmptyStatement)?;
    if FORBIDDEN_KEYWORDS.contains(&keyword.as_str()) {
        return Err(StoreError::ForbiddenStatement(keyword));
    }
    Ok(())
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> Result<usize, StoreError> {
    index += 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(StoreError::InvalidStatement("unterminated quoted literal"))
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> Result<usize, StoreError> {
    index += 2;
    let mut depth = 1usize;
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
            index += 2;
        } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Ok(index);
            }
        } else {
            index += 1;
        }
    }
    Err(StoreError::InvalidStatement("unterminated block comment"))
}

fn skip_dollar_quoted(bytes: &[u8], index: usize) -> Result<usize, StoreError> {
    let mut tag_end = index + 1;
    while tag_end < bytes.len()
        && (bytes[tag_end].is_ascii_alphanumeric() || bytes[tag_end] == b'_')
    {
        tag_end += 1;
    }
    // `$1` is a parameter placeholder, not a tag: tags cannot start with a
    // digit and must be terminated by `$`.
    let is_tag = tag_end < bytes.len()
        && bytes[tag_end] == b'$'
        && (tag_end == index + 1 || !bytes[index + 1].is_ascii_digit());
    if !is_tag {
        return Ok(index + 1);
    }
    let tag = &bytes[index..=tag_end];
    let mut search = tag_end + 1;
    while search + tag.len() <= bytes.len() {
        if &bytes[search..search + tag.len()] == tag {
            return Ok(search + tag.len());
        }
        search += 1;
    }
    Err(StoreError::InvalidStatement(
        "unterminated dollar-quoted literal",
    ))
}

fn is_trivia(bytes: &[u8]) -> bool {
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_whitespace() => index += 1,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                let mut depth = 1usize;
                while index < bytes.len() && depth > 0 {
                    if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                        depth += 1;
                        index += 2;
                    } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                if depth > 0 {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Execute a prepared statement with text parameters.
pub(crate) async fn execute_statement<'e, E>(
    executor: E,
    statement: &Statement,
) -> Result<u64, StoreError>
where
    E: Executor<'e, Database = Postgres>,
{
    let mut query = sqlx::query(&statement.sql);
    for param in &statement.params {
        query = query.bind(param.clone());
    }
    Ok(query.execute(executor).await?.rows_affected())
}

/// Execute a prepared select and decode every returned row.
pub(crate) async fn fetch_rows<'e, E>(
    executor: E,
    statement: &Statement,
) -> Result<Vec<Row>, StoreError>
where
    E: Executor<'e, Database = Postgres>,
{
    let mut query = sqlx::query(&statement.sql);
    for param in &statement.params {
        query = query.bind(param.clone());
    }
    let rows = query.fetch_all(executor).await?;
    rows.iter().map(pg_row_to_row).collect()
}

/// Execute caller-supplied SQL with [`CellValue`] parameters.
pub(crate) async fn fetch_raw<'e, E>(
    executor: E,
    sql: &str,
    params: &[CellValue],
) -> Result<Vec<Row>, StoreError>
where
    E: Executor<'e, Database = Postgres>,
{
    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_cell(query, param);
    }
    let rows = query.fetch_all(executor).await?;
    rows.iter().map(pg_row_to_row).collect()
}

fn bind_cell<'q>(
    query: Query<'q, Postgres, PgArguments>,
    value: &CellValue,
) -> Query<'q, Postgres, PgArguments> {
    match value {
        // PostgreSQL resolves untyped NULL parameters from context; a text
        // null works for comparisons and explicit casts.
        CellValue::Null => query.bind(None::<String>),
        CellValue::Bool(flag) => query.bind(*flag),
        CellValue::Int(number) => query.bind(*number),
        CellValue::Float(number) => query.bind(*number),
        CellValue::Numeric(text) => query.bind(text.clone()),
        CellValue::Text(text) => query.bind(text.clone()),
        CellValue::Bytes(bytes) => query.bind(bytes.clone()),
        CellValue::Json(json) => query.bind(sqlx::types::Json(json.clone())),
        CellValue::Array(items) => query.bind(raw_array_literal(items)),
    }
}

fn raw_array_literal(items: &[CellValue]) -> String {
    let mut encoded = Vec::with_capacity(items.len());
    for item in items {
        if item.is_null() {
            encoded.push("NULL".to_owned());
            continue;
        }
        let text = match item {
            CellValue::Json(json) => serde_json::to_string(json).unwrap_or_default(),
            CellValue::Text(text) | CellValue::Numeric(text) => text.clone(),
            CellValue::Bool(flag) => flag.to_string(),
            CellValue::Int(number) => number.to_string(),
            CellValue::Float(number) => float_text(*number),
            CellValue::Bytes(bytes) => bytes_hex(bytes),
            CellValue::Array(_) | CellValue::Null => continue,
        };
        encoded.push(quote_array_element(&text));
    }
    format!("{{{}}}", encoded.join(","))
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("\\x");
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Convert a database row into a [`Row`], decoding each column by its type.
pub(crate) fn pg_row_to_row(row: &PgRow) -> Result<Row, StoreError> {
    let mut result = Row::new();
    for (index, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(index)?;
        result.insert(
            column.name().to_owned(),
            decode_value(raw, column.type_info().name())?,
        );
    }
    Ok(result)
}

fn decode_value(raw: PgValueRef<'_>, name: &str) -> Result<CellValue, StoreError> {
    if raw.is_null() {
        return Ok(CellValue::Null);
    }
    let type_info = raw.type_info();
    let oid = type_info
        .oid()
        .map(|oid| oid.0)
        .ok_or_else(|| StoreError::UnsupportedType(name.to_owned()))?;
    let bytes = raw
        .as_bytes()
        .map_err(|error| StoreError::Decode(error.to_string()))?;
    match raw.format() {
        PgValueFormat::Binary => decode_binary(oid, name, bytes),
        PgValueFormat::Text => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| StoreError::Decode("value is not valid UTF-8".to_owned()))?;
            decode_text(oid, name, text)
        }
    }
}

fn decode_binary(oid: u32, name: &str, bytes: &[u8]) -> Result<CellValue, StoreError> {
    if array_element_oid(oid).is_some() {
        return decode_binary_array(bytes);
    }
    Ok(match oid {
        BOOL_OID => CellValue::Bool(match bytes {
            [0] => false,
            [1] => true,
            _ => {
                return Err(StoreError::Decode("invalid boolean".to_owned()));
            }
        }),
        INT2_OID => CellValue::Int(i64::from(i16::from_be_bytes(fixed(bytes)?))),
        INT4_OID => CellValue::Int(i64::from(i32::from_be_bytes(fixed(bytes)?))),
        INT8_OID => CellValue::Int(i64::from_be_bytes(fixed(bytes)?)),
        FLOAT4_OID => {
            CellValue::Float(f64::from(f32::from_bits(u32::from_be_bytes(fixed(bytes)?))))
        }
        FLOAT8_OID => CellValue::Float(f64::from_bits(u64::from_be_bytes(fixed(bytes)?))),
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | CHAR_OID => CellValue::Text(utf8(bytes)?),
        BYTEA_OID => CellValue::Bytes(bytes.to_vec()),
        JSON_OID => CellValue::Json(serde_json::from_slice(bytes)?),
        JSONB_OID => {
            let (version, payload) = bytes
                .split_first()
                .ok_or_else(|| StoreError::Decode("empty jsonb value".to_owned()))?;
            if *version != 1 {
                return Err(StoreError::Decode(format!(
                    "unsupported jsonb version {version}"
                )));
            }
            CellValue::Json(serde_json::from_slice(payload)?)
        }
        NUMERIC_OID => CellValue::Numeric(format_numeric(bytes)?),
        TIMESTAMP_OID => {
            CellValue::Text(format_timestamp(i64::from_be_bytes(fixed(bytes)?), false)?)
        }
        TIMESTAMPTZ_OID => {
            CellValue::Text(format_timestamp(i64::from_be_bytes(fixed(bytes)?), true)?)
        }
        _ => return Err(StoreError::UnsupportedType(name.to_owned())),
    })
}

fn decode_binary_array(bytes: &[u8]) -> Result<CellValue, StoreError> {
    let mut cursor = bytes;
    let dimensions = read_i32(&mut cursor)?;
    let _flags = read_i32(&mut cursor)?;
    let element_oid = read_u32(&mut cursor)?;
    if dimensions == 0 {
        return Ok(CellValue::Array(Vec::new()));
    }
    if dimensions != 1 {
        return Err(StoreError::Decode(format!(
            "{dimensions}-dimensional arrays are not supported"
        )));
    }
    let _length = read_i32(&mut cursor)?;
    let _lower_bound = read_i32(&mut cursor)?;
    let mut items = Vec::new();
    while !cursor.is_empty() {
        let length = read_i32(&mut cursor)?;
        if length < 0 {
            items.push(CellValue::Null);
            continue;
        }
        let length = usize::try_from(length)
            .map_err(|_| StoreError::Decode("invalid array element length".to_owned()))?;
        let element = take(&mut cursor, length)?;
        items.push(decode_binary_scalar(element_oid, element)?);
    }
    Ok(CellValue::Array(items))
}

fn decode_binary_scalar(oid: u32, bytes: &[u8]) -> Result<CellValue, StoreError> {
    match oid {
        BOOL_OID => match bytes {
            [0] => Ok(CellValue::Bool(false)),
            [1] => Ok(CellValue::Bool(true)),
            _ => Err(StoreError::Decode("invalid boolean".to_owned())),
        },
        INT2_OID => Ok(CellValue::Int(i64::from(i16::from_be_bytes(fixed(bytes)?)))),
        INT4_OID => Ok(CellValue::Int(i64::from(i32::from_be_bytes(fixed(bytes)?)))),
        INT8_OID => Ok(CellValue::Int(i64::from_be_bytes(fixed(bytes)?))),
        FLOAT4_OID => Ok(CellValue::Float(f64::from(f32::from_bits(
            u32::from_be_bytes(fixed(bytes)?),
        )))),
        FLOAT8_OID => Ok(CellValue::Float(f64::from_bits(u64::from_be_bytes(fixed(
            bytes,
        )?)))),
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | CHAR_OID => {
            Ok(CellValue::Text(utf8(bytes)?))
        }
        BYTEA_OID => Ok(CellValue::Bytes(bytes.to_vec())),
        JSON_OID => Ok(CellValue::Json(serde_json::from_slice(bytes)?)),
        JSONB_OID => {
            let (version, payload) = bytes
                .split_first()
                .ok_or_else(|| StoreError::Decode("empty jsonb value".to_owned()))?;
            if *version != 1 {
                return Err(StoreError::Decode(format!(
                    "unsupported jsonb version {version}"
                )));
            }
            Ok(CellValue::Json(serde_json::from_slice(payload)?))
        }
        NUMERIC_OID => Ok(CellValue::Numeric(format_numeric(bytes)?)),
        TIMESTAMP_OID => Ok(CellValue::Text(format_timestamp(
            i64::from_be_bytes(fixed(bytes)?),
            false,
        )?)),
        TIMESTAMPTZ_OID => Ok(CellValue::Text(format_timestamp(
            i64::from_be_bytes(fixed(bytes)?),
            true,
        )?)),
        _ => Err(StoreError::UnsupportedType(type_name(oid))),
    }
}

fn decode_text(oid: u32, name: &str, text: &str) -> Result<CellValue, StoreError> {
    Ok(match oid {
        BOOL_OID => match text {
            "t" | "true" => CellValue::Bool(true),
            "f" | "false" => CellValue::Bool(false),
            _ => return Err(StoreError::Decode("invalid boolean".to_owned())),
        },
        INT2_OID | INT4_OID | INT8_OID => CellValue::Int(
            text.parse()
                .map_err(|_| StoreError::Decode("invalid integer".to_owned()))?,
        ),
        FLOAT4_OID | FLOAT8_OID => CellValue::Float(
            text.parse()
                .map_err(|_| StoreError::Decode("invalid float".to_owned()))?,
        ),
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | CHAR_OID => {
            CellValue::Text(text.to_owned())
        }
        BYTEA_OID => CellValue::Bytes(parse_bytea(text)?),
        JSON_OID | JSONB_OID => CellValue::Json(serde_json::from_str(text)?),
        NUMERIC_OID => CellValue::Numeric(text.to_owned()),
        TIMESTAMP_OID | TIMESTAMPTZ_OID => CellValue::Text(text.to_owned()),
        _ => {
            let element_oid = array_element_oid(oid)
                .ok_or_else(|| StoreError::UnsupportedType(name.to_owned()))?;
            let mut items = Vec::new();
            for element in parse_array_literal(text)? {
                match element {
                    None => items.push(CellValue::Null),
                    Some(element) => {
                        items.push(decode_text(element_oid, &type_name(element_oid), &element)?);
                    }
                }
            }
            CellValue::Array(items)
        }
    })
}

fn parse_bytea(text: &str) -> Result<Vec<u8>, StoreError> {
    let digits = text
        .strip_prefix("\\x")
        .ok_or_else(|| StoreError::Decode("unsupported bytea text format".to_owned()))?;
    if digits.len() % 2 != 0 {
        return Err(StoreError::Decode("invalid bytea hex length".to_owned()));
    }
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for index in (0..digits.len()).step_by(2) {
        let byte = u8::from_str_radix(&digits[index..index + 2], 16)
            .map_err(|_| StoreError::Decode("invalid bytea hex digit".to_owned()))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn array_element_oid(oid: u32) -> Option<u32> {
    match oid {
        BOOL_ARRAY_OID => Some(BOOL_OID),
        BYTEA_ARRAY_OID => Some(BYTEA_OID),
        CHAR_ARRAY_OID => Some(CHAR_OID),
        NAME_ARRAY_OID => Some(NAME_OID),
        INT2_ARRAY_OID => Some(INT2_OID),
        INT4_ARRAY_OID => Some(INT4_OID),
        TEXT_ARRAY_OID => Some(TEXT_OID),
        BPCHAR_ARRAY_OID => Some(BPCHAR_OID),
        VARCHAR_ARRAY_OID => Some(VARCHAR_OID),
        INT8_ARRAY_OID => Some(INT8_OID),
        FLOAT4_ARRAY_OID => Some(FLOAT4_OID),
        FLOAT8_ARRAY_OID => Some(FLOAT8_OID),
        NUMERIC_ARRAY_OID => Some(NUMERIC_OID),
        JSON_ARRAY_OID => Some(JSON_OID),
        TIMESTAMP_ARRAY_OID => Some(TIMESTAMP_OID),
        TIMESTAMPTZ_ARRAY_OID => Some(TIMESTAMPTZ_OID),
        JSONB_ARRAY_OID => Some(JSONB_OID),
        _ => None,
    }
}

fn type_name(oid: u32) -> String {
    match oid {
        BOOL_OID => "boolean".to_owned(),
        BYTEA_OID => "bytea".to_owned(),
        CHAR_OID => "\"char\"".to_owned(),
        NAME_OID => "name".to_owned(),
        INT8_OID => "bigint".to_owned(),
        INT2_OID => "smallint".to_owned(),
        INT4_OID => "integer".to_owned(),
        TEXT_OID => "text".to_owned(),
        JSON_OID => "json".to_owned(),
        FLOAT4_OID => "real".to_owned(),
        FLOAT8_OID => "double precision".to_owned(),
        BPCHAR_OID => "character".to_owned(),
        VARCHAR_OID => "character varying".to_owned(),
        NUMERIC_OID => "numeric".to_owned(),
        TIMESTAMP_OID => "timestamp".to_owned(),
        TIMESTAMPTZ_OID => "timestamptz".to_owned(),
        JSONB_OID => "jsonb".to_owned(),
        _ => format!("oid {oid}"),
    }
}

/// Parse the textual PostgreSQL array format into its unquoted elements.
///
/// `NULL` (unquoted) becomes `None`. Quoted elements preserve commas, braces
/// and escaped quotes. Multi-dimensional literals are rejected.
pub(crate) fn parse_array_literal(text: &str) -> Result<Vec<Option<String>>, StoreError> {
    let inner = text
        .strip_prefix('{')
        .and_then(|text| text.strip_suffix('}'))
        .ok_or_else(|| StoreError::Decode("array literal must be enclosed in braces".to_owned()))?;
    let mut characters = inner.chars().peekable();
    let mut items = Vec::new();
    loop {
        while matches!(characters.peek(), Some(character) if character.is_whitespace()) {
            characters.next();
        }
        match characters.peek() {
            None => break,
            Some('"') => {
                characters.next();
                let mut element = String::new();
                let mut closed = false;
                while let Some(character) = characters.next() {
                    match character {
                        '\\' => {
                            let escaped = characters.next().ok_or_else(|| {
                                StoreError::Decode("dangling array escape".to_owned())
                            })?;
                            element.push(escaped);
                        }
                        '"' => {
                            closed = true;
                            break;
                        }
                        _ => element.push(character),
                    }
                }
                if !closed {
                    return Err(StoreError::Decode("unterminated array element".to_owned()));
                }
                items.push(Some(element));
            }
            Some(_) => {
                let mut element = String::new();
                while let Some(character) = characters.peek() {
                    if *character == ',' || *character == '}' {
                        break;
                    }
                    element.push(*character);
                    characters.next();
                }
                let element = element.trim();
                if element.is_empty() {
                    return Err(StoreError::Decode("empty array element".to_owned()));
                }
                if element.eq_ignore_ascii_case("NULL") {
                    items.push(None);
                } else {
                    items.push(Some(element.to_owned()));
                }
            }
        }
        while matches!(characters.peek(), Some(character) if character.is_whitespace()) {
            characters.next();
        }
        match characters.next() {
            None => break,
            Some(',') => continue,
            Some(_) => {
                return Err(StoreError::Decode(
                    "unexpected character in array literal".to_owned(),
                ));
            }
        }
    }
    Ok(items)
}

/// Format PostgreSQL's microsecond epoch (2000-01-01T00:00:00Z) as ISO 8601.
///
/// This avoids a date-library dependency for the one representation the store
/// needs. Timestamps outside the supported range are rejected rather than
/// rendered incorrectly.
fn format_timestamp(micros: i64, with_timezone: bool) -> Result<String, StoreError> {
    const MICROS_PER_SECOND: i64 = 1_000_000;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MICROS_PER_DAY: i64 = MICROS_PER_SECOND * SECONDS_PER_DAY;
    const EPOCH_DAYS: i64 = 10_957; // 1970-01-01 to 2000-01-01

    let days = micros.div_euclid(MICROS_PER_DAY);
    let remainder = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days + EPOCH_DAYS)?;
    let seconds = remainder / MICROS_PER_SECOND;
    let nanos_in_second = remainder % MICROS_PER_SECOND;
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    let second = seconds % 60;
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos_in_second:06}{}",
        if with_timezone { "Z" } else { "" }
    ))
}

/// Convert days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> Result<(i64, u32, u32), StoreError> {
    let shifted = days
        .checked_add(719_468)
        .ok_or_else(|| StoreError::Decode("timestamp is out of range".to_owned()))?;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    };
    let year = year + if month <= 2 { 1 } else { 0 };
    let month = u32::try_from(month)
        .map_err(|_| StoreError::Decode("timestamp is out of range".to_owned()))?;
    let day = u32::try_from(day)
        .map_err(|_| StoreError::Decode("timestamp is out of range".to_owned()))?;
    Ok((year, month, day))
}

fn format_numeric(bytes: &[u8]) -> Result<String, StoreError> {
    let mut cursor = bytes;
    let digit_count = i32::from(read_i16(&mut cursor)?);
    let weight = i32::from(read_i16(&mut cursor)?);
    let sign = read_u16(&mut cursor)?;
    let scale = i32::from(read_i16(&mut cursor)?);
    if digit_count < 0 || scale < 0 {
        return Err(StoreError::Decode("invalid numeric header".to_owned()));
    }
    let mut digits = Vec::with_capacity(digit_count as usize);
    for _ in 0..digit_count {
        digits.push(read_i16(&mut cursor)?);
    }
    match sign {
        0xC000 => return Ok("NaN".to_owned()),
        0xD000 => return Ok("Infinity".to_owned()),
        0xF000 => return Ok("-Infinity".to_owned()),
        0x0000 | 0x4000 => {}
        _ => return Err(StoreError::Decode("invalid numeric sign".to_owned())),
    }
    let mut output = String::new();
    if sign == 0x4000 {
        output.push('-');
    }
    if weight < 0 {
        output.push('0');
    } else {
        for group in 0..=weight {
            let digit = digits.get(group as usize).copied().unwrap_or(0);
            if !(0..=9999).contains(&digit) {
                return Err(StoreError::Decode("invalid numeric digit".to_owned()));
            }
            if group == 0 {
                output.push_str(&digit.to_string());
            } else {
                use std::fmt::Write as _;
                let _ = write!(output, "{digit:04}");
            }
        }
    }
    if scale > 0 {
        let mut fraction = String::new();
        if weight < -1 {
            for _ in 0..(-weight - 1) {
                fraction.push_str("0000");
            }
        }
        let start = if weight < 0 {
            0
        } else {
            usize::try_from(weight + 1).map_err(|_| {
                StoreError::Decode("numeric weight does not fit in memory".to_owned())
            })?
        };
        for digit in digits.iter().skip(start) {
            if !(0..=9999).contains(digit) {
                return Err(StoreError::Decode("invalid numeric digit".to_owned()));
            }
            use std::fmt::Write as _;
            let _ = write!(fraction, "{digit:04}");
        }
        let scale = usize::try_from(scale)
            .map_err(|_| StoreError::Decode("numeric scale does not fit in memory".to_owned()))?;
        while fraction.len() < scale {
            fraction.push('0');
        }
        fraction.truncate(scale);
        output.push('.');
        output.push_str(&fraction);
    }
    Ok(output)
}

fn take<'a>(cursor: &mut &'a [u8], length: usize) -> Result<&'a [u8], StoreError> {
    if cursor.len() < length {
        return Err(StoreError::Decode("truncated value".to_owned()));
    }
    let (head, tail) = cursor.split_at(length);
    *cursor = tail;
    Ok(head)
}

fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], StoreError> {
    bytes
        .try_into()
        .map_err(|_| StoreError::Decode(format!("expected {N} bytes, found {}", bytes.len())))
}

fn read_i16(cursor: &mut &[u8]) -> Result<i16, StoreError> {
    Ok(i16::from_be_bytes(fixed(take(cursor, 2)?)?))
}

fn read_u16(cursor: &mut &[u8]) -> Result<u16, StoreError> {
    Ok(u16::from_be_bytes(fixed(take(cursor, 2)?)?))
}

fn read_i32(cursor: &mut &[u8]) -> Result<i32, StoreError> {
    Ok(i32::from_be_bytes(fixed(take(cursor, 4)?)?))
}

fn read_u32(cursor: &mut &[u8]) -> Result<u32, StoreError> {
    Ok(u32::from_be_bytes(fixed(take(cursor, 4)?)?))
}

fn utf8(bytes: &[u8]) -> Result<String, StoreError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| StoreError::Decode("value is not valid UTF-8".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_schema::ColumnDef;

    fn open_statement(sql: &str) -> Result<(), StoreError> {
        validate_statement(sql)
    }

    #[test]
    fn accepts_single_statements_and_skips_literals_and_comments() {
        assert!(open_statement("SELECT 1").is_ok());
        assert!(open_statement("SELECT 1;").is_ok());
        assert!(open_statement("SELECT ';'").is_ok());
        assert!(open_statement("SELECT 1 /* ; */ ;").is_ok());
        assert!(open_statement("-- comment\nSELECT 1").is_ok());
        assert!(open_statement("SELECT $$;$$").is_ok());
        assert!(open_statement("SELECT \"a;b\" FROM t").is_ok());
    }

    #[test]
    fn rejects_multiple_statements_and_ddl() {
        assert!(matches!(
            open_statement("SELECT 1; SELECT 2"),
            Err(StoreError::MultipleStatements)
        ));
        assert!(matches!(
            open_statement("SELECT 1; -- trailing\nSELECT 2"),
            Err(StoreError::MultipleStatements)
        ));
        assert!(matches!(
            open_statement("DROP TABLE app.account"),
            Err(StoreError::ForbiddenStatement(keyword)) if keyword == "DROP"
        ));
        assert!(matches!(
            open_statement("  -- leading comment\ncreate table t (id int)"),
            Err(StoreError::ForbiddenStatement(keyword)) if keyword == "CREATE"
        ));
        assert!(matches!(
            open_statement("TRUNCATE app.account"),
            Err(StoreError::ForbiddenStatement(_))
        ));
        assert!(matches!(
            open_statement("   /* only a comment */ "),
            Err(StoreError::EmptyStatement)
        ));
        assert!(matches!(
            open_statement("SELECT 'unterminated"),
            Err(StoreError::InvalidStatement(_))
        ));
    }

    #[test]
    fn array_literals_round_trip() {
        let items = vec![
            Some("plain".to_owned()),
            Some("with,comma".to_owned()),
            Some("with\"quote".to_owned()),
            Some("with\\slash".to_owned()),
            None,
            Some("NULL".to_owned()),
            Some("snowman ☃".to_owned()),
        ];
        let mut literal = String::from("{");
        for (index, item) in items.iter().enumerate() {
            if index > 0 {
                literal.push(',');
            }
            match item {
                None => literal.push_str("NULL"),
                Some(item) => literal.push_str(&quote_array_element(item)),
            }
        }
        literal.push('}');
        assert_eq!(parse_array_literal(&literal).expect("parse"), items);
        assert_eq!(
            parse_array_literal("{}").expect("empty array"),
            Vec::<Option<String>>::new()
        );
    }

    #[test]
    fn encodes_scalars_and_arrays_for_their_column_types() {
        let text = ColumnDef::new("label", ScalarType::Text, false);
        assert_eq!(
            encode_value(&text, &CellValue::Text("a".into())).expect("text"),
            Some("a".to_owned())
        );
        assert!(matches!(
            encode_value(&text, &CellValue::Int(1)),
            Err(StoreError::TypeMismatch { .. })
        ));

        let numeric = ColumnDef::new("balance", ScalarType::EVM_INTEGER, false);
        assert_eq!(
            encode_value(&numeric, &CellValue::Numeric("123".into())).expect("numeric"),
            Some("123".to_owned())
        );

        let tags = ColumnDef::new("tags", ColumnType::Array(ScalarType::Text), true);
        assert_eq!(
            encode_value(
                &tags,
                &CellValue::Array(vec![
                    CellValue::Text("a,b".into()),
                    CellValue::Null,
                    CellValue::Text("c".into()),
                ]),
            )
            .expect("array"),
            Some("{\"a,b\",NULL,\"c\"}".to_owned())
        );
    }

    #[test]
    fn row_images_round_trip_exactly() {
        let image = row(&[
            ("flag", CellValue::Bool(true)),
            ("count", CellValue::Int(7)),
            ("rate", CellValue::Float(f64::INFINITY)),
            ("balance", CellValue::Numeric("1".to_owned())),
            ("label", CellValue::Text("1".to_owned())),
            ("payload", CellValue::Json(serde_json::json!("text"))),
            ("blob", CellValue::Bytes(vec![0, 255, 16])),
            ("missing", CellValue::Null),
            (
                "tags",
                CellValue::Array(vec![CellValue::Text("a".to_owned()), CellValue::Null]),
            ),
        ]);
        let encoded = encode_row(&image);
        assert_eq!(decode_row(&encoded).expect("decode"), image);
    }

    fn row(pairs: &[(&str, CellValue)]) -> Row {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect()
    }

    #[test]
    fn timestamps_are_rendered_as_iso_8601() {
        assert_eq!(
            format_timestamp(0, true).expect("postgres epoch"),
            "2000-01-01T00:00:00.000000Z"
        );
        assert_eq!(
            format_timestamp(-946_684_800_000_000, true).expect("unix epoch"),
            "1970-01-01T00:00:00.000000Z"
        );
        assert_eq!(
            format_timestamp(1_234_567, false).expect("fraction"),
            "2000-01-01T00:00:01.234567"
        );
    }

    #[test]
    fn numeric_binary_is_rendered_exactly() {
        fn numeric_bytes(
            ndigits: i16,
            weight: i16,
            sign: u16,
            dscale: i16,
            digits: &[i16],
        ) -> Vec<u8> {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&ndigits.to_be_bytes());
            bytes.extend_from_slice(&weight.to_be_bytes());
            bytes.extend_from_slice(&sign.to_be_bytes());
            bytes.extend_from_slice(&dscale.to_be_bytes());
            for digit in digits {
                bytes.extend_from_slice(&digit.to_be_bytes());
            }
            bytes
        }

        assert_eq!(
            format_numeric(&numeric_bytes(3, 1, 0x0000, 4, &[1, 2345, 6789])).expect("decimal"),
            "12345.6789"
        );
        assert_eq!(
            format_numeric(&numeric_bytes(1, -2, 0x0000, 5, &[1000])).expect("small"),
            "0.00001"
        );
        assert_eq!(
            format_numeric(&numeric_bytes(2, 0, 0x4000, 1, &[1, 5000])).expect("negative"),
            "-1.5"
        );
        assert_eq!(
            format_numeric(&numeric_bytes(1, 1, 0x0000, 0, &[1])).expect("padded"),
            "10000"
        );
        assert_eq!(
            format_numeric(&numeric_bytes(0, 0, 0xC000, 0, &[])).expect("nan"),
            "NaN"
        );
    }
}
