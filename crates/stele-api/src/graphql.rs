//! Dynamic GraphQL endpoint.
//!
//! The schema is generated at startup from [`ApiConfig::custom_schema`] or, by
//! default, from the tables found in `information_schema` for the configured
//! schema. Every table becomes a query field:
//!
//! ```graphql
//! {
//!   holder(limit: 10, offset: 0, orderBy: "balance_desc", where: "{\"active\":true}") {
//!     items
//!     total
//!   }
//! }
//! ```
//!
//! `items` is a list of `Row`, a JSON scalar, so a table does not need a
//! compile-time Rust type. `total` is the number of rows matching `where`
//! before `limit`/`offset` are applied; GraphQL `Int` is 32-bit, so larger
//! totals saturate at `i32::MAX`.
//!
//! Filters and sorts are parsed into [`TableQuery`] and rendered with
//! identifiers quoted by [`stele_schema::quote_identifier`]. Every identifier
//! must name a column known to the schema; values are always bound as
//! parameters. `where` values compare the column's text form, except for JSONB
//! columns, which compare as `jsonb`; a `null` value means `IS NULL`. Errors
//! returned to clients never contain SQL text, schema names, or database error
//! details.
//!
//! `_tables` lists the query fields that are available, which is also what keeps
//! the schema valid when no table is exposed.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ObjectAccessor, Scalar, Schema, TypeRef,
};
use async_graphql::Error as GqlError;
use async_graphql::Value as GqlValue;
use async_graphql_axum::GraphQLRequest;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;
use sqlx::{postgres::PgPool, Row as _};
use stele_schema::{quote_identifier, ColumnType, ScalarType, TableDef};
use thiserror::Error;

use crate::{begin_read_only, metrics, ApiConfig, ApiError, ApiState};

/// Name of the JSON scalar that carries one row.
const ROW_SCALAR: &str = "Row";
/// Name of the connection-like wrapper type shared by every table.
const CONNECTION_TYPE: &str = "SteleConnection";
/// Rows returned when `limit` is omitted.
const DEFAULT_LIMIT: i64 = 100;

/// Why the GraphQL schema could not be built.
///
/// Messages are intentionally stable and free of connection details so they can
/// be surfaced to operators and clients unchanged.
#[derive(Debug, Error)]
pub enum GraphqlInitError {
    /// [`ApiConfig::graphql`] is false.
    #[error("GraphQL is disabled")]
    Disabled,
    /// Table metadata could not be read from the database.
    #[error("table metadata is unavailable")]
    MetadataUnavailable,
    /// The generated schema was rejected by `async-graphql`.
    #[error("the GraphQL schema could not be built")]
    InvalidSchema,
}

/// How a discovered column behaves in SQL filters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnKind {
    /// Compared through its text representation.
    Scalar,
    /// Compared as `jsonb`, so JSON whitespace and key order do not matter.
    Json,
    /// Not filterable through `where`; SQL arrays need element-wise operators.
    Array,
}

#[derive(Clone, Debug)]
struct ColumnInfo {
    name: String,
    kind: ColumnKind,
}

/// One exposed table: where it lives, which columns it has, and its query field.
#[derive(Clone, Debug)]
struct TableInfo {
    schema: String,
    name: String,
    columns: Vec<ColumnInfo>,
    field: String,
}

impl TableInfo {
    fn from_def(def: &TableDef) -> Self {
        Self {
            schema: def.schema.clone(),
            name: def.name.clone(),
            columns: def
                .columns
                .iter()
                .map(|column| ColumnInfo {
                    name: column.name.clone(),
                    kind: match &column.column_type {
                        ColumnType::Array(_) => ColumnKind::Array,
                        ColumnType::Scalar(ScalarType::Jsonb) => ColumnKind::Json,
                        ColumnType::Scalar(_) => ColumnKind::Scalar,
                    },
                })
                .collect(),
            field: String::new(),
        }
    }

    fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|column| column.name == name)
    }
}

/// Ordering requested through `orderBy`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Order {
    column: String,
    descending: bool,
}

/// One equality filter requested through `where`.
#[derive(Clone, Debug, Eq, PartialEq)]
enum FilterValue {
    /// Compare `"column"::text` to this text value.
    Text(String),
    /// Compare `"column"` to this JSON document.
    Json(String),
    /// Emit `IS NULL` and bind nothing.
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Filter {
    column: String,
    value: FilterValue,
}

/// A validated, fully bound table query.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TableQuery {
    limit: i64,
    offset: i64,
    order: Option<Order>,
    filters: Vec<Filter>,
}

/// Raw GraphQL arguments before validation.
#[derive(Clone, Debug, Default)]
struct RawArgs {
    limit: Option<i64>,
    offset: Option<i64>,
    order_by: Option<String>,
    filter: Option<String>,
}

/// Rejections produced while validating `orderBy` and `where`.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
enum QueryError {
    #[error("unknown column {0:?}")]
    UnknownColumn(String),
    #[error("limit must be a positive integer")]
    InvalidLimit,
    #[error("offset must be zero or a positive integer")]
    InvalidOffset,
    #[error("where must be a JSON object of equality filters")]
    InvalidWhere,
    #[error("column {0:?} cannot be filtered")]
    UnsupportedFilter(String),
    #[error("the schema contains an identifier that cannot be quoted")]
    InvalidIdentifier,
}

/// A rendered statement pair plus the parameters bound to both.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Prepared {
    rows_sql: String,
    total_sql: String,
    filter_params: Vec<Option<String>>,
}

/// Data behind one connection field.
struct ConnectionData {
    items: Vec<GqlValue>,
    total: i64,
}

/// Everything a table resolver needs.
struct TableSource {
    info: Arc<TableInfo>,
    pool: PgPool,
    config: Arc<ApiConfig>,
}

/// Build the GraphQL schema for `config`.
///
/// With [`ApiConfig::custom_schema`] the definition is trusted and no database
/// round-trip happens. Otherwise tables and columns are read from
/// `information_schema`. [`crate::router`] starts this in the background at
/// startup, and [`crate::ApiState::prepare_graphql`] runs it eagerly.
pub(crate) async fn build_schema(
    pool: &PgPool,
    config: &Arc<ApiConfig>,
) -> Result<Schema, GraphqlInitError> {
    let mut tables = match &config.custom_schema {
        Some(schema) => schema
            .tables
            .iter()
            .map(TableInfo::from_def)
            .collect::<Vec<_>>(),
        None => discover_tables(pool, &config.schema).await?,
    };
    assign_field_names(&mut tables);

    let table_fields = tables
        .iter()
        .map(|table| table.field.clone())
        .collect::<Vec<_>>();

    let mut query = Object::new("Query")
        .description("Read-only queries over the indexed tables.")
        .field(
            Field::new(
                "_tables",
                TypeRef::named_nn_list_nn(TypeRef::STRING),
                move |_ctx| {
                    let names = table_fields.clone();
                    FieldFuture::new(async move {
                        Ok(Some(GqlValue::List(
                            names.into_iter().map(GqlValue::String).collect(),
                        )))
                    })
                },
            )
            .description("Query fields exposed by this API."),
        );

    for table in tables {
        let field = table.field.clone();
        let source = Arc::new(TableSource {
            info: Arc::new(table),
            pool: pool.clone(),
            config: config.clone(),
        });
        query = query.field(table_field(field, source));
    }

    let row = Scalar::new(ROW_SCALAR).description("An arbitrary row encoded as a JSON object.");
    Schema::build("Query", None, None)
        .register(row)
        .register(connection_object())
        .register(query)
        .finish()
        .map_err(|error| {
            tracing::error!(error_kind = "graphql_schema", error = %error, "GraphQL schema is invalid");
            GraphqlInitError::InvalidSchema
        })
}

fn connection_object() -> Object {
    Object::new(CONNECTION_TYPE)
        .description("A page of rows and the total number of matching rows.")
        .field(Field::new(
            "items",
            TypeRef::named_nn_list_nn(ROW_SCALAR),
            |ctx| {
                FieldFuture::new(async move {
                    let data = ctx.parent_value.try_downcast_ref::<ConnectionData>()?;
                    Ok(Some(FieldValue::list(
                        data.items.iter().cloned().map(FieldValue::value),
                    )))
                })
            },
        ))
        .field(Field::new(
            "total",
            TypeRef::named_nn(TypeRef::INT),
            |ctx| {
                FieldFuture::new(async move {
                    let data = ctx.parent_value.try_downcast_ref::<ConnectionData>()?;
                    let total = i32::try_from(data.total).unwrap_or(i32::MAX);
                    Ok(Some(FieldValue::value(total)))
                })
            },
        ))
}

fn table_field(field: String, source: Arc<TableSource>) -> Field {
    Field::new(field, TypeRef::named_nn(CONNECTION_TYPE), move |ctx| {
        let source = Arc::clone(&source);
        FieldFuture::new(async move {
            let args = RawArgs {
                limit: optional_i64(&ctx.args, "limit")?,
                offset: optional_i64(&ctx.args, "offset")?,
                order_by: optional_string(&ctx.args, "orderBy")?,
                filter: optional_string(&ctx.args, "where")?,
            };
            let query = plan_query(&source.info, args, source.config.max_rows)
                .map_err(|error| GqlError::new(error.to_string()))?;
            let page = execute(&source, &query).await?;
            Ok(Some(FieldValue::owned_any(ConnectionData {
                items: page.items,
                total: page.total,
            })))
        })
    })
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("orderBy", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("where", TypeRef::named(TypeRef::STRING)))
}

fn optional_i64(args: &ObjectAccessor<'_>, name: &str) -> Result<Option<i64>, GqlError> {
    match args.get(name) {
        None => Ok(None),
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value
            .i64()
            .map(Some)
            .map_err(|_| GqlError::new(format!("{name} must be an integer"))),
    }
}

fn optional_string(args: &ObjectAccessor<'_>, name: &str) -> Result<Option<String>, GqlError> {
    match args.get(name) {
        None => Ok(None),
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value
            .string()
            .map(|value| Some(value.to_owned()))
            .map_err(|_| GqlError::new(format!("{name} must be a string"))),
    }
}

/// Read tables and columns for `schema` from `information_schema`.
async fn discover_tables(pool: &PgPool, schema: &str) -> Result<Vec<TableInfo>, GraphqlInitError> {
    let rows = sqlx::query(
        "SELECT c.table_name, c.column_name, c.data_type, c.udt_name \
         FROM information_schema.columns c \
         JOIN information_schema.tables t \
           ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
         WHERE c.table_schema = $1 AND t.table_type = 'BASE TABLE' \
         ORDER BY c.table_name, c.ordinal_position",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        if let Some(db) = error.as_database_error() {
            tracing::warn!(
                error_kind = "graphql_metadata",
                sqlstate = ?db.code(),
                "GraphQL table discovery failed"
            );
        } else {
            tracing::warn!(
                error_kind = "graphql_metadata",
                "GraphQL table discovery failed"
            );
        }
        GraphqlInitError::MetadataUnavailable
    })?;

    let mut tables: Vec<TableInfo> = Vec::new();
    for row in rows {
        let table_name: String = row
            .try_get("table_name")
            .map_err(|_| GraphqlInitError::MetadataUnavailable)?;
        let column_name: String = row
            .try_get("column_name")
            .map_err(|_| GraphqlInitError::MetadataUnavailable)?;
        let data_type: String = row
            .try_get("data_type")
            .map_err(|_| GraphqlInitError::MetadataUnavailable)?;
        let udt_name: String = row
            .try_get("udt_name")
            .map_err(|_| GraphqlInitError::MetadataUnavailable)?;
        let kind = match discovered_column_type(&data_type, &udt_name) {
            ColumnType::Array(_) => ColumnKind::Array,
            ColumnType::Scalar(ScalarType::Jsonb) => ColumnKind::Json,
            ColumnType::Scalar(_) => ColumnKind::Scalar,
        };
        let column = ColumnInfo {
            name: column_name,
            kind,
        };
        match tables.last_mut() {
            Some(table) if table.name == table_name => table.columns.push(column),
            _ => tables.push(TableInfo {
                schema: schema.to_owned(),
                name: table_name,
                columns: vec![column],
                field: String::new(),
            }),
        }
    }
    Ok(tables)
}

/// Best-effort mapping from `information_schema` type names to schema scalars.
///
/// The mapping only describes the column for operators; rows are always
/// returned as JSON, so an unknown type degrades to [`ScalarType::Text`].
fn discovered_column_type(data_type: &str, udt_name: &str) -> ColumnType {
    if data_type == "ARRAY" {
        return ColumnType::Array(base_scalar(data_type, udt_name.trim_start_matches('_')));
    }
    ColumnType::Scalar(base_scalar(data_type, udt_name))
}

fn base_scalar(data_type: &str, udt_name: &str) -> ScalarType {
    match udt_name {
        "int2" => ScalarType::SmallInt,
        "int4" => ScalarType::Integer,
        "int8" => ScalarType::BigInt,
        "bool" => ScalarType::Boolean,
        "float4" => ScalarType::Real,
        "float8" => ScalarType::DoublePrecision,
        "json" | "jsonb" => ScalarType::Jsonb,
        "bytea" => ScalarType::Bytes,
        "timestamptz" | "timestamp" => ScalarType::Timestamp,
        "numeric" => ScalarType::EVM_INTEGER,
        "text" | "varchar" | "bpchar" | "uuid" | "name" => ScalarType::Text,
        _ => match data_type {
            "smallint" => ScalarType::SmallInt,
            "integer" => ScalarType::Integer,
            "bigint" => ScalarType::BigInt,
            "boolean" => ScalarType::Boolean,
            "real" => ScalarType::Real,
            "double precision" => ScalarType::DoublePrecision,
            "numeric" | "decimal" => ScalarType::EVM_INTEGER,
            "json" | "jsonb" => ScalarType::Jsonb,
            "bytea" => ScalarType::Bytes,
            "timestamp with time zone" | "timestamp without time zone" => ScalarType::Timestamp,
            _ => ScalarType::Text,
        },
    }
}

/// Give every table a unique, valid GraphQL field name.
fn assign_field_names(tables: &mut [TableInfo]) {
    let mut taken = BTreeSet::new();
    for table in tables.iter_mut() {
        table.field = graphql_field_name(&table.name, &mut taken);
    }
}

fn graphql_field_name(raw: &str, taken: &mut BTreeSet<String>) -> String {
    let mut name = String::with_capacity(raw.len());
    for character in raw.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            name.push(character);
        } else {
            name.push('_');
        }
    }
    if name.is_empty() {
        name.push_str("table");
    }
    if name.starts_with(|character: char| character.is_ascii_digit()) {
        name.insert(0, '_');
    }
    if name.starts_with("__") {
        name.insert(0, '_');
    }
    let base = name.clone();
    let mut suffix = 2;
    while taken.contains(&name) {
        name = format!("{base}_{suffix}");
        suffix += 1;
    }
    taken.insert(name.clone());
    name
}

fn plan_query(table: &TableInfo, args: RawArgs, max_rows: usize) -> Result<TableQuery, QueryError> {
    let max_rows = i64::try_from(max_rows).unwrap_or(i64::MAX);
    let limit = match args.limit {
        None => DEFAULT_LIMIT.min(max_rows),
        Some(limit) if limit < 1 => return Err(QueryError::InvalidLimit),
        Some(limit) => limit.min(max_rows),
    };
    let offset = match args.offset {
        None => 0,
        Some(offset) if offset < 0 => return Err(QueryError::InvalidOffset),
        Some(offset) => offset,
    };
    Ok(TableQuery {
        limit,
        offset,
        order: parse_order(table, args.order_by.as_deref())?,
        filters: parse_where(table, args.filter.as_deref())?,
    })
}

/// Parse `orderBy`: a column name with an optional `_desc` suffix.
///
/// An exact column match wins, so a column that literally ends in `_desc` can
/// still be sorted ascending.
fn parse_order(table: &TableInfo, raw: Option<&str>) -> Result<Option<Order>, QueryError> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if let Some(column) = table.column(raw) {
        return Ok(Some(Order {
            column: column.name.clone(),
            descending: false,
        }));
    }
    if let Some(base) = raw.strip_suffix("_desc") {
        if let Some(column) = table.column(base) {
            return Ok(Some(Order {
                column: column.name.clone(),
                descending: true,
            }));
        }
        return Err(QueryError::UnknownColumn(base.to_owned()));
    }
    Err(QueryError::UnknownColumn(raw.to_owned()))
}

/// Parse `where`: a JSON object whose keys are columns and whose values are
/// scalars. `null` becomes `IS NULL`; JSONB columns accept any JSON value.
fn parse_where(table: &TableInfo, raw: Option<&str>) -> Result<Vec<Filter>, QueryError> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };
    let value: Value = serde_json::from_str(raw).map_err(|_| QueryError::InvalidWhere)?;
    let object = value.as_object().ok_or(QueryError::InvalidWhere)?;
    let mut filters = Vec::with_capacity(object.len());
    for (name, value) in object {
        let column = table
            .column(name)
            .ok_or_else(|| QueryError::UnknownColumn(name.clone()))?;
        filters.push(Filter {
            column: column.name.clone(),
            value: filter_value(column, value)?,
        });
    }
    Ok(filters)
}

fn filter_value(column: &ColumnInfo, value: &Value) -> Result<FilterValue, QueryError> {
    match column.kind {
        ColumnKind::Array => return Err(QueryError::UnsupportedFilter(column.name.clone())),
        ColumnKind::Json => {
            return match value {
                Value::Null => Ok(FilterValue::Null),
                value => serde_json::to_string(value)
                    .map(FilterValue::Json)
                    .map_err(|_| QueryError::InvalidWhere),
            };
        }
        ColumnKind::Scalar => {}
    }
    match value {
        Value::Null => Ok(FilterValue::Null),
        Value::String(text) => Ok(FilterValue::Text(text.clone())),
        Value::Number(number) => Ok(FilterValue::Text(number.to_string())),
        Value::Bool(flag) => Ok(FilterValue::Text(flag.to_string())),
        Value::Array(_) | Value::Object(_) => Err(QueryError::InvalidWhere),
    }
}

/// Render the row and count statements for `query`.
///
/// Identifiers come from [`TableInfo`], never from the request text, and are
/// quoted before use. Scalar filters compare the text representation of the
/// column so one text parameter works for every scalar type; JSONB filters
/// compare as `jsonb`.
fn prepare(table: &TableInfo, query: &TableQuery) -> Result<Prepared, QueryError> {
    let table_ref = format!(
        "{}.{}",
        quote_identifier(&table.schema).map_err(|_| QueryError::InvalidIdentifier)?,
        quote_identifier(&table.name).map_err(|_| QueryError::InvalidIdentifier)?
    );
    let columns = table
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name).map_err(|_| QueryError::InvalidIdentifier))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    let mut filter_params = Vec::with_capacity(query.filters.len());
    let mut clauses = Vec::with_capacity(query.filters.len());
    for filter in &query.filters {
        let identifier =
            quote_identifier(&filter.column).map_err(|_| QueryError::InvalidIdentifier)?;
        match &filter.value {
            FilterValue::Null => clauses.push(format!("{identifier} IS NULL")),
            FilterValue::Json(document) => {
                filter_params.push(Some(document.clone()));
                clauses.push(format!("{identifier} = ${}::jsonb", filter_params.len()));
            }
            FilterValue::Text(text) => {
                filter_params.push(Some(text.clone()));
                clauses.push(format!("{identifier}::text = ${}", filter_params.len()));
            }
        }
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    let order_sql = match &query.order {
        Some(order) => {
            let identifier =
                quote_identifier(&order.column).map_err(|_| QueryError::InvalidIdentifier)?;
            if order.descending {
                format!(" ORDER BY {identifier} DESC")
            } else {
                format!(" ORDER BY {identifier}")
            }
        }
        None => String::new(),
    };

    let limit_index = filter_params.len() + 1;
    let offset_index = filter_params.len() + 2;
    Ok(Prepared {
        rows_sql: format!(
            "SELECT pg_catalog.row_to_json(_stele_row) AS row FROM \
             (SELECT {columns} FROM {table_ref}{where_sql}{order_sql} \
             LIMIT ${limit_index} OFFSET ${offset_index}) AS _stele_row"
        ),
        total_sql: format!("SELECT pg_catalog.count(*) AS total FROM {table_ref}{where_sql}"),
        filter_params,
    })
}

struct RowPage {
    items: Vec<GqlValue>,
    total: i64,
}

async fn execute(source: &TableSource, query: &TableQuery) -> Result<RowPage, GqlError> {
    let prepared =
        prepare(&source.info, query).map_err(|error| GqlError::new(error.to_string()))?;
    metrics::record_query();
    let mut tx = begin_read_only(&source.pool, &source.config)
        .await
        .map_err(|error| {
            tracing::warn!(
                error_kind = "graphql_query",
                status = error.status.as_u16(),
                "GraphQL query could not start"
            );
            GqlError::new("Query execution failed")
        })?;

    let mut total_statement = sqlx::query_scalar::<_, i64>(&prepared.total_sql);
    for param in &prepared.filter_params {
        total_statement = total_statement.bind(param.clone());
    }
    let total = total_statement
        .fetch_one(&mut *tx)
        .await
        .map_err(query_error)?;

    let mut rows_statement = sqlx::query(&prepared.rows_sql);
    for param in &prepared.filter_params {
        rows_statement = rows_statement.bind(param.clone());
    }
    let rows = rows_statement
        .bind(query.limit)
        .bind(query.offset)
        .fetch_all(&mut *tx)
        .await
        .map_err(query_error)?;

    let mut items = Vec::with_capacity(rows.len());
    let mut bytes = 0usize;
    for row in rows {
        let value: Value = row
            .try_get("row")
            .map_err(|_| GqlError::new("Could not decode a row"))?;
        bytes = bytes.saturating_add(
            serde_json::to_vec(&value)
                .map_err(|_| GqlError::new("Could not encode a row"))?
                .len(),
        );
        if bytes > source.config.max_result_bytes {
            return Err(GqlError::new("Result exceeds the configured size limit"));
        }
        items.push(json_to_graphql(&value)?);
    }
    tx.commit().await.map_err(query_error)?;
    Ok(RowPage { items, total })
}

fn query_error(error: sqlx::Error) -> GqlError {
    if let Some(db) = error.as_database_error() {
        tracing::warn!(
            error_kind = "graphql_query",
            sqlstate = ?db.code(),
            "GraphQL query failed"
        );
    } else {
        tracing::warn!(error_kind = "graphql_query", "GraphQL query failed");
    }
    GqlError::new("Query execution failed")
}

fn json_to_graphql(value: &Value) -> Result<GqlValue, GqlError> {
    serde_json::from_value(value.clone()).map_err(|_| GqlError::new("Could not encode a row"))
}

/// Rejection for a malformed GraphQL request body.
///
/// The `async-graphql-axum` extractor's default rejection renders a debug
/// representation of the parse error; this type reuses the API's structured
/// error shape instead.
pub(crate) struct GraphqlRejection(async_graphql::ParseRequestError);

impl From<async_graphql::ParseRequestError> for GraphqlRejection {
    fn from(error: async_graphql::ParseRequestError) -> Self {
        Self(error)
    }
}

impl GraphqlRejection {
    fn api_error(&self) -> ApiError {
        let status = if matches!(&self.0, async_graphql::ParseRequestError::PayloadTooLarge) {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        };
        ApiError::new(status, "invalid_request", "Expected a JSON GraphQL request")
    }
}

impl IntoResponse for GraphqlRejection {
    fn into_response(self) -> Response {
        self.api_error().into_response()
    }
}

/// `POST /graphql` handler.
///
/// Accepts `{ "query": "...", "variables": { ... } }` and returns
/// `{ "data": ..., "errors": ... }`. GraphQL-level failures use HTTP 200 with
/// an `errors` array, matching GraphQL-over-HTTP behaviour; extraction and
/// schema failures use normal HTTP status codes.
pub(crate) async fn graphql(
    State(state): State<ApiState>,
    request: Result<GraphQLRequest<GraphqlRejection>, GraphqlRejection>,
) -> Result<Json<Value>, ApiError> {
    let request = request
        .map_err(|rejection| rejection.api_error())?
        .into_inner();
    let schema = state.graphql_schema().await.map_err(|error| match error {
        GraphqlInitError::MetadataUnavailable => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "graphql_unavailable",
            "GraphQL metadata is unavailable",
        ),
        GraphqlInitError::Disabled => ApiError::new(
            StatusCode::NOT_FOUND,
            "graphql_disabled",
            "GraphQL is disabled",
        ),
        GraphqlInitError::InvalidSchema => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "graphql_schema",
            "GraphQL schema is unavailable",
        ),
    })?;

    let response = schema.execute(request).await;
    if !response.errors.is_empty() {
        metrics::record_error();
    }
    let data = serde_json::to_value(&response.data).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            "Could not serialize the response",
        )
    })?;
    let errors = serde_json::to_value(&response.errors).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            "Could not serialize the response",
        )
    })?;
    Ok(Json(serde_json::json!({
        "data": data,
        "errors": errors,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> TableInfo {
        TableInfo {
            schema: "public".into(),
            name: "holder".into(),
            columns: vec![
                ColumnInfo {
                    name: "address".into(),
                    kind: ColumnKind::Scalar,
                },
                ColumnInfo {
                    name: "balance".into(),
                    kind: ColumnKind::Scalar,
                },
                ColumnInfo {
                    name: "metadata".into(),
                    kind: ColumnKind::Json,
                },
                ColumnInfo {
                    name: "tags".into(),
                    kind: ColumnKind::Array,
                },
            ],
            field: "holder".into(),
        }
    }

    fn query(args: RawArgs) -> TableQuery {
        plan_query(&table(), args, 1_000).expect("query should validate")
    }

    #[test]
    fn renders_quoted_identifiers_and_bound_parameters() {
        let prepared = prepare(
            &table(),
            &TableQuery {
                limit: 10,
                offset: 5,
                order: Some(Order {
                    column: "balance".into(),
                    descending: true,
                }),
                filters: vec![Filter {
                    column: "address".into(),
                    value: FilterValue::Text("0xabc".into()),
                }],
            },
        )
        .expect("statement should render");

        assert_eq!(
            prepared.rows_sql,
            "SELECT pg_catalog.row_to_json(_stele_row) AS row FROM \
             (SELECT \"address\", \"balance\", \"metadata\", \"tags\" FROM \"public\".\"holder\" \
             WHERE \"address\"::text = $1 ORDER BY \"balance\" DESC LIMIT $2 OFFSET $3) \
             AS _stele_row"
        );
        assert_eq!(
            prepared.total_sql,
            "SELECT pg_catalog.count(*) AS total FROM \"public\".\"holder\" WHERE \"address\"::text = $1"
        );
        assert_eq!(prepared.filter_params, vec![Some("0xabc".to_owned())]);
    }

    #[test]
    fn json_null_and_array_filters_are_handled() {
        let prepared = prepare(
            &table(),
            &TableQuery {
                limit: 1,
                offset: 0,
                order: None,
                filters: vec![
                    Filter {
                        column: "metadata".into(),
                        value: FilterValue::Json(r#"{"a":1}"#.into()),
                    },
                    Filter {
                        column: "balance".into(),
                        value: FilterValue::Null,
                    },
                ],
            },
        )
        .expect("statement should render");
        assert!(prepared.rows_sql.contains("\"metadata\" = $1::jsonb"));
        assert!(prepared.rows_sql.contains("\"balance\" IS NULL"));
        assert_eq!(prepared.filter_params, vec![Some(r#"{"a":1}"#.to_owned())]);

        let error = parse_where(&table(), Some(r#"{"tags":["a"]}"#));
        assert_eq!(error, Err(QueryError::UnsupportedFilter("tags".to_owned())));
    }

    #[test]
    fn order_by_accepts_columns_and_desc_suffix() {
        assert_eq!(parse_order(&table(), None), Ok(None));
        assert_eq!(
            parse_order(&table(), Some("balance")),
            Ok(Some(Order {
                column: "balance".into(),
                descending: false,
            }))
        );
        assert_eq!(
            parse_order(&table(), Some("balance_desc")),
            Ok(Some(Order {
                column: "balance".into(),
                descending: true,
            }))
        );
        assert_eq!(
            parse_order(&table(), Some("missing")),
            Err(QueryError::UnknownColumn("missing".into()))
        );
        assert_eq!(
            parse_order(&table(), Some("missing_desc")),
            Err(QueryError::UnknownColumn("missing".into()))
        );
    }

    #[test]
    fn where_rejects_unknown_columns_and_non_objects() {
        assert_eq!(
            parse_where(&table(), Some(r#"{"nope":"x"}"#)),
            Err(QueryError::UnknownColumn("nope".into()))
        );
        assert_eq!(
            parse_where(&table(), Some("[]")),
            Err(QueryError::InvalidWhere)
        );
        assert_eq!(
            parse_where(&table(), Some(r#"{"address":{"nested":true}}"#)),
            Err(QueryError::InvalidWhere)
        );
        assert_eq!(parse_where(&table(), None), Ok(Vec::new()));
    }

    #[test]
    fn limits_are_clamped_and_negatives_rejected() {
        assert_eq!(query(RawArgs::default()).limit, DEFAULT_LIMIT);
        assert_eq!(
            query(RawArgs {
                limit: Some(10_000),
                ..RawArgs::default()
            })
            .limit,
            1_000
        );
        assert_eq!(
            plan_query(
                &table(),
                RawArgs {
                    limit: Some(0),
                    ..RawArgs::default()
                },
                10
            ),
            Err(QueryError::InvalidLimit)
        );
        assert_eq!(
            plan_query(
                &table(),
                RawArgs {
                    offset: Some(-1),
                    ..RawArgs::default()
                },
                10
            ),
            Err(QueryError::InvalidOffset)
        );
    }

    #[test]
    fn field_names_are_unique_and_valid() {
        let mut taken = BTreeSet::new();
        assert_eq!(graphql_field_name("holder", &mut taken), "holder");
        assert_eq!(graphql_field_name("holder", &mut taken), "holder_2");
        assert_eq!(graphql_field_name("2fa-codes", &mut taken), "_2fa_codes");
        assert_eq!(graphql_field_name("__meta", &mut taken), "___meta");
        assert_eq!(graphql_field_name("", &mut taken), "table");
    }

    #[tokio::test]
    async fn connection_fields_serialize_rows_and_totals() {
        let row = Scalar::new(ROW_SCALAR);
        let connection = connection_object();
        let query = Object::new("Query").field(Field::new(
            "holder",
            TypeRef::named_nn(CONNECTION_TYPE),
            |_ctx| {
                FieldFuture::new(async move {
                    let item = json_to_graphql(&serde_json::json!({
                        "address": "0xa",
                        "balance": 12,
                        "tags": ["x"],
                        "extra": null,
                    }))?;
                    Ok(Some(FieldValue::owned_any(ConnectionData {
                        items: vec![item],
                        total: 1,
                    })))
                })
            },
        ));
        let schema = Schema::build("Query", None, None)
            .register(row)
            .register(connection)
            .register(query)
            .finish()
            .expect("schema should build");

        let response = schema.execute("{ holder { items total } }").await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data,
            async_graphql::value!({
                "holder": {
                    "items": [{
                        "address": "0xa",
                        "balance": 12,
                        "tags": ["x"],
                        "extra": null,
                    }],
                    "total": 1,
                }
            })
        );
    }

    #[test]
    fn discovered_types_cover_scalars_and_arrays() {
        assert_eq!(
            discovered_column_type("text", "text"),
            ColumnType::Scalar(ScalarType::Text)
        );
        assert_eq!(
            discovered_column_type("bigint", "int8"),
            ColumnType::Scalar(ScalarType::BigInt)
        );
        assert_eq!(
            discovered_column_type("numeric", "numeric"),
            ColumnType::Scalar(ScalarType::EVM_INTEGER)
        );
        assert_eq!(
            discovered_column_type("boolean", "bool"),
            ColumnType::Scalar(ScalarType::Boolean)
        );
        assert_eq!(
            discovered_column_type("jsonb", "jsonb"),
            ColumnType::Scalar(ScalarType::Jsonb)
        );
        assert_eq!(
            discovered_column_type("bytea", "bytea"),
            ColumnType::Scalar(ScalarType::Bytes)
        );
        assert_eq!(
            discovered_column_type("double precision", "float8"),
            ColumnType::Scalar(ScalarType::DoublePrecision)
        );
        assert_eq!(
            discovered_column_type("ARRAY", "_int8"),
            ColumnType::Array(ScalarType::BigInt)
        );
        assert_eq!(
            discovered_column_type("custom", "custom"),
            ColumnType::Scalar(ScalarType::Text)
        );
    }
}
