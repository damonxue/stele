//! Read-only HTTP query foundation.
//!
//! The caller supplies a dedicated, least-privilege PostgreSQL pool and a trusted
//! schema: untrusted users must not be able to create objects in that schema.
//! A read-only transaction is not a sandbox for arbitrary database functions,
//! operators, casts, views, or extensions. The database role must not own the
//! exposed objects or have privileges beyond the intended data. Authentication,
//! TLS, rate limiting, and binding the listener are the caller's responsibility.
//!
//! Routes:
//!
//! | Method | Path | Purpose |
//! |---|---|---|
//! | POST | `/sql` | One validated, parameterized SELECT. |
//! | GET | `/sql/live` | Server-sent events; re-runs a SELECT and emits changes. |
//! | POST | `/graphql` | Dynamic GraphQL over the configured tables. |
//! | GET | `/status` | Per-chain indexing progress. |
//! | GET | `/metrics` | Process counters in Prometheus text format. |
//! | GET | `/health` | Process and database liveness. |
//! | GET | `/ready` | Readiness, including backfill completion. |

mod graphql;
mod live;
pub mod metrics;
mod status;

pub use graphql::GraphqlInitError;

use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgPool, Postgres},
    Row, Transaction,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const MAX_SQL_BYTES: usize = 5_000;

/// Resource limits, feature switches, and schema metadata for the HTTP API.
///
/// `graphql`, `live_interval_ms`, and `custom_schema` are additive; their
/// defaults keep the endpoints of earlier releases working unchanged.
#[derive(Clone, Debug)]
pub struct ApiConfig {
    /// Schema exposed by `/sql` and, unless overridden, by `/graphql`.
    pub schema: String,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_concurrent_queries: usize,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
    pub work_mem_kb: u32,
    pub ready_after_backfill: bool,
    /// Expose `POST /graphql`. Defaults to `true`.
    pub graphql: bool,
    /// Re-execution interval for `GET /sql/live`. Must be non-zero.
    pub live_interval_ms: u64,
    /// Tables exposed by `/graphql`.
    ///
    /// When `None`, tables are discovered at startup from
    /// `information_schema` for [`ApiConfig::schema`]. When `Some`, the
    /// definition is used as-is so the API can start before the database is
    /// reachable.
    pub custom_schema: Option<stele_schema::SchemaDef>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            schema: "public".into(),
            max_rows: 100_000,
            max_result_bytes: 8 * 1024 * 1024,
            max_concurrent_queries: 10,
            statement_timeout: Duration::from_millis(500),
            lock_timeout: Duration::from_millis(500),
            work_mem_kb: 4 * 1024,
            ready_after_backfill: true,
            graphql: true,
            live_interval_ms: 1_000,
            custom_schema: None,
        }
    }
}

/// Shared state. Readiness starts false and is driven by the embedding runtime.
#[derive(Clone)]
pub struct ApiState {
    pool: PgPool,
    config: Arc<ApiConfig>,
    ready: Arc<AtomicBool>,
    permits: Arc<Semaphore>,
    graphql: Arc<tokio::sync::OnceCell<async_graphql::dynamic::Schema>>,
}

impl ApiState {
    /// Build state from a pool and configuration.
    ///
    /// Returns a stable, non-sensitive message when the configuration is
    /// unusable. The GraphQL schema is warmed by [`router`], can be built
    /// eagerly with [`ApiState::prepare_graphql`], and is otherwise built on
    /// the first `/graphql` request.
    pub fn new(pool: PgPool, config: ApiConfig) -> Result<Self, &'static str> {
        if config.schema.is_empty()
            || config.schema.len() > 63
            || config.schema.contains('\0')
            || config.schema == "pg_catalog"
            || config.schema == "information_schema"
            || config.schema.starts_with("pg_temp")
            || config.schema.starts_with("pg_toast")
        {
            return Err("invalid schema");
        }
        if config.max_rows == 0
            || config.max_rows >= i64::MAX as usize
            || config.max_result_bytes == 0
            || config.max_concurrent_queries == 0
            || config.max_concurrent_queries > Semaphore::MAX_PERMITS
            || config.work_mem_kb == 0
            || config.statement_timeout.as_millis() == 0
            || config.lock_timeout.as_millis() == 0
            || config.statement_timeout.as_millis() > i32::MAX as u128
            || config.lock_timeout.as_millis() > i32::MAX as u128
            || config.live_interval_ms == 0
        {
            return Err("invalid resource limits");
        }
        let permits = Arc::new(Semaphore::new(config.max_concurrent_queries));
        Ok(Self {
            pool,
            config: Arc::new(config),
            ready: Arc::new(AtomicBool::new(false)),
            permits,
            graphql: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    /// Mark historical indexing as complete (or not) for `/ready`.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    /// Build the GraphQL schema now instead of on the first request.
    ///
    /// Table discovery needs the database; calling this during startup keeps
    /// the first `/graphql` request from paying for it. The result is cached,
    /// so a failure can be retried by calling again.
    pub async fn prepare_graphql(&self) -> Result<(), GraphqlInitError> {
        self.graphql_schema().await.map(|_| ())
    }

    async fn graphql_schema(&self) -> Result<&async_graphql::dynamic::Schema, GraphqlInitError> {
        if !self.config.graphql {
            return Err(GraphqlInitError::Disabled);
        }
        self.graphql
            .get_or_try_init(|| graphql::build_schema(&self.pool, &self.config))
            .await
    }
}

/// Create a mergeable router; no HTTP listener or authentication is supplied.
///
/// `POST /graphql` is registered only when [`ApiConfig::graphql`] is true, and
/// its schema is warmed in the background when called inside a Tokio runtime.
/// The same semaphore bounds concurrent `/sql`, `/graphql`, and `/sql/live`
/// work.
pub fn router(state: ApiState) -> Router {
    // Warm the GraphQL schema as soon as a runtime is available so table
    // discovery happens at startup rather than on the first request. The
    // schema is built once and retried on demand, so a failure here is not
    // fatal; `/graphql` reports it when it matters.
    if state.config.graphql {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let state = state.clone();
            handle.spawn(async move {
                if let Err(error) = state.prepare_graphql().await {
                    tracing::debug!(
                        error_kind = "graphql_init",
                        error = %error,
                        "GraphQL schema was not ready at startup"
                    );
                }
            });
        }
    }
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/status", get(status::status))
        .route("/metrics", get(metrics::metrics))
        .route("/sql", post(sql))
        .route("/sql/live", get(live::sql_live));
    if state.config.graphql {
        router = router.route("/graphql", post(graphql::graphql));
    }
    router
        .layer(DefaultBodyLimit::max(64 * 1024))
        // `DefaultBodyLimit` only covers extractors that opt in; this layer also
        // bounds bodies read by third-party extractors such as GraphQL.
        .layer(tower_http::limit::RequestBodyLimitLayer::new(64 * 1024))
        .with_state(state)
}

/// Add a CORS layer that only allows the given origins.
///
/// Requires the `cors` feature. CORS is off by default because the API has no
/// built-in authentication; a permissive policy would expose the read-only
/// endpoints to any page the browser already trusts.
#[cfg(feature = "cors")]
pub fn cors_layer(origins: Vec<axum::http::HeaderValue>) -> tower_http::cors::CorsLayer {
    use axum::http::{header, Method};
    tower_http::cors::CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE, header::ACCEPT])
}

/// Validate a conservative PostgreSQL SELECT subset and return deparsed SQL.
///
/// Unsupported constructs fail closed. Parameters are text or null; use
/// `$1::text::numeric` (for example) when comparing to a non-text column.
pub fn validate_sql(sql: &str) -> Result<String, &'static str> {
    if sql.is_empty() || sql.len() > MAX_SQL_BYTES || sql.contains('\0') {
        return Err("SQL must be between 1 and 5000 bytes");
    }
    let parsed = pg_query::parse(sql).map_err(|_| "Invalid SQL syntax")?;
    if parsed.protobuf.stmts.len() != 1 {
        return Err("Exactly one SELECT statement is required");
    }
    use pg_query::protobuf::node::Node;
    if !matches!(
        parsed.protobuf.stmts[0]
            .stmt
            .as_ref()
            .and_then(|n| n.node.as_ref()),
        Some(Node::SelectStmt(_))
    ) {
        return Err("Only SELECT is supported");
    }
    let tree = serde_json::to_value(&parsed.protobuf).map_err(|_| "Invalid SQL")?;
    validate_tree(&tree)?;
    pg_query::deparse(&parsed.protobuf).map_err(|_| "Invalid SQL")
}

fn validate_tree(value: &Value) -> Result<(), &'static str> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_tree(value)?;
            }
        }
        Value::Object(object) => {
            // Protobuf Node wrappers contain only `node`; other AST structs
            // (notably SortBy) can also have a field with that name.
            if let Some(node) = object.get("node").filter(|_| object.len() == 1) {
                let tagged = node.as_object().ok_or("Unsupported SQL node")?;
                if tagged.len() != 1 {
                    return Err("Unsupported SQL node");
                }
                let (kind, body) = tagged.iter().next().ok_or("Unsupported SQL node")?;
                match kind.as_str() {
                    "SelectStmt" => {
                        if !body["into_clause"].is_null()
                            || !body["with_clause"].is_null()
                            || !body["larg"].is_null()
                            || !body["rarg"].is_null()
                            || body["locking_clause"]
                                .as_array()
                                .is_none_or(|a| !a.is_empty())
                            || body["values_lists"]
                                .as_array()
                                .is_none_or(|a| !a.is_empty())
                        {
                            return Err("SELECT INTO, locking, CTEs, VALUES and set operations are unsupported");
                        }
                    }
                    "RangeVar" => {
                        if body["schemaname"].as_str() != Some("")
                            || body["catalogname"].as_str() != Some("")
                            || body["relname"]
                                .as_str()
                                .is_none_or(|n| n.starts_with("pg_"))
                        {
                            return Err("Qualified and system relations are unsupported");
                        }
                    }
                    "ColumnRef" => {
                        if body["fields"]
                            .as_array()
                            .is_none_or(|fields| fields.len() > 2)
                        {
                            return Err("Schema-qualified columns are unsupported");
                        }
                    }
                    "SortBy" => {
                        if body["use_op"]
                            .as_array()
                            .is_none_or(|operators| !operators.is_empty())
                        {
                            return Err("ORDER BY USING is not supported");
                        }
                    }
                    "FuncCall" => {
                        let names = body["funcname"].as_array().ok_or("Unsupported function")?;
                        let name = names.first().and_then(node_string);
                        if names.len() != 1
                            || !matches!(
                                name,
                                Some(
                                    "count"
                                        | "sum"
                                        | "avg"
                                        | "min"
                                        | "max"
                                        | "lower"
                                        | "upper"
                                        | "length"
                                        | "abs"
                                        | "round"
                                        | "coalesce"
                                )
                            )
                        {
                            return Err("Function is not allowed");
                        }
                    }
                    "AExpr" => {
                        let names = body["name"].as_array().ok_or("Unsupported operator")?;
                        if !names.is_empty()
                            && (names.len() != 1
                                || !matches!(
                                    names.first().and_then(node_string),
                                    Some(
                                        "=" | "<>"
                                            | "!="
                                            | "<"
                                            | ">"
                                            | "<="
                                            | ">="
                                            | "+"
                                            | "-"
                                            | "*"
                                            | "/"
                                            | "%"
                                            | "~~"
                                            | "!~~"
                                            | "~~*"
                                            | "!~~*"
                                    )
                                ))
                        {
                            return Err("Operator is not allowed");
                        }
                    }
                    "TypeCast" => {
                        let names = body["type_name"]["names"]
                            .as_array()
                            .ok_or("Unsupported type")?;
                        let parts: Option<Vec<_>> = names.iter().map(node_string).collect();
                        let parts = parts.ok_or("Unsupported type")?;
                        let name = match parts.as_slice() {
                            [name] => *name,
                            ["pg_catalog", name] => *name,
                            _ => return Err("Qualified type is not allowed"),
                        };
                        if !matches!(
                            name,
                            "text"
                                | "varchar"
                                | "bpchar"
                                | "int2"
                                | "int4"
                                | "int8"
                                | "numeric"
                                | "float4"
                                | "float8"
                                | "bool"
                                | "date"
                                | "timestamp"
                                | "timestamptz"
                                | "uuid"
                        ) {
                            return Err("Type is not allowed");
                        }
                    }
                    "ResTarget" | "AConst" | "String" | "Integer" | "Float" | "Boolean"
                    | "AStar" | "ParamRef" | "BoolExpr" | "NullTest" | "JoinExpr"
                    | "RangeSubselect" | "List" | "CoalesceExpr" | "CaseExpr" | "CaseWhen" => {}
                    _ => return Err("SQL construct is not supported"),
                }
            }
            for child in object.values() {
                validate_tree(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn node_string(value: &Value) -> Option<&str> {
    value.get("node")?.get("String")?.get("sval")?.as_str()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Option<String>>,
}

#[derive(Debug, Serialize)]
pub struct SqlResponse {
    pub rows: Vec<Value>,
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        metrics::record_error();
        tracing::warn!(
            error_kind = self.code,
            status = self.status.as_u16(),
            "API request failed"
        );
        (
            self.status,
            Json(json!({ "error": { "code": self.code, "message": self.message } })),
        )
            .into_response()
    }
}

async fn health(State(state): State<ApiState>) -> Result<StatusCode, ApiError> {
    match tokio::time::timeout(
        state.config.statement_timeout,
        sqlx::query("SELECT 1").execute(&state.pool),
    )
    .await
    {
        Ok(Ok(_)) => Ok(StatusCode::OK),
        _ => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "Database unavailable",
        )),
    }
}

async fn ready(State(state): State<ApiState>) -> Result<StatusCode, ApiError> {
    if !state.config.ready_after_backfill || state.ready.load(Ordering::Acquire) {
        Ok(StatusCode::OK)
    } else {
        Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "Historical indexing is not complete",
        ))
    }
}

async fn sql(
    State(state): State<ApiState>,
    request: Result<Json<SqlRequest>, JsonRejection>,
) -> Result<Json<SqlResponse>, ApiError> {
    let Json(request) = request.map_err(|rejection| {
        let status = if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        };
        ApiError::new(status, "invalid_request", "Expected a JSON SQL request")
    })?;
    let query = validate_sql(&request.sql)
        .map_err(|message| ApiError::new(StatusCode::BAD_REQUEST, "invalid_sql", message))?;
    let _permit = state.permits.try_acquire().map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "query_capacity",
            "Query capacity exceeded",
        )
    })?;
    execute_query(&state, &query, &request.params)
        .await
        .map(Json)
}

pub(crate) fn database_error(error: sqlx::Error) -> ApiError {
    if let Some(db) = error.as_database_error() {
        if db.code().as_deref() == Some("57014") {
            return ApiError::new(
                StatusCode::REQUEST_TIMEOUT,
                "query_timeout",
                "Query timed out",
            );
        }
        tracing::error!(error_kind = "database_error", sqlstate = ?db.code(), "SQL query failed");
    } else {
        tracing::error!(error_kind = "database_error", "SQL query failed");
    }
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "database_error",
        "Database query failed",
    )
}

/// Start a read-only transaction with the configured timeouts and
/// `search_path`.
///
/// Shared by `/sql`, `/sql/live`, and `/graphql` so every read runs under the
/// same isolation and resource limits.
pub(crate) async fn begin_read_only(
    pool: &PgPool,
    config: &ApiConfig,
) -> Result<Transaction<'static, Postgres>, ApiError> {
    let mut tx = pool.begin().await.map_err(database_error)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
    // set_config handles schema names as values; quote_ident makes search_path one identifier.
    sqlx::query("SELECT pg_catalog.set_config('search_path', 'pg_catalog,' || pg_catalog.quote_ident($1) || ',pg_temp', true), pg_catalog.set_config('statement_timeout', $2, true), pg_catalog.set_config('lock_timeout', $3, true), pg_catalog.set_config('work_mem', $4, true)")
        .bind(&config.schema)
        .bind(format!("{}ms", config.statement_timeout.as_millis()))
        .bind(format!("{}ms", config.lock_timeout.as_millis()))
        .bind(format!("{}kB", config.work_mem_kb))
        .execute(&mut *tx).await.map_err(database_error)?;
    Ok(tx)
}

pub(crate) async fn execute_query(
    state: &ApiState,
    query: &str,
    params: &[Option<String>],
) -> Result<SqlResponse, ApiError> {
    metrics::record_query();
    let mut tx = begin_read_only(&state.pool, &state.config).await?;

    let wrapped = format!(
        "SELECT pg_catalog.row_to_json(_stele_row) AS row FROM ({query}) AS _stele_row LIMIT {}",
        state.config.max_rows.saturating_add(1)
    );
    let mut statement = sqlx::query(&wrapped);
    for param in params {
        statement = statement.bind(param.clone());
    }
    let mut cursor = statement.fetch(&mut *tx);
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    use futures_util::TryStreamExt;
    while let Some(row) = cursor.try_next().await.map_err(database_error)? {
        if rows.len() == state.config.max_rows {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "row_limit",
                "Query exceeded the row limit",
            ));
        }
        let value: Value = row.try_get("row").map_err(database_error)?;
        bytes = bytes.saturating_add(
            serde_json::to_vec(&value)
                .map_err(|_| {
                    ApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "serialization_error",
                        "Could not serialize result",
                    )
                })?
                .len(),
        );
        if bytes > state.config.max_result_bytes {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "result_limit",
                "Query exceeded the result size limit",
            ));
        }
        rows.push(value);
    }
    drop(cursor);
    tx.commit().await.map_err(database_error)?;
    Ok(SqlResponse { rows })
}
