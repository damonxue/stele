use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use stele_api::{router, validate_sql, ApiConfig, ApiState};
use stele_schema::{ColumnDef, ScalarType, SchemaDef, TableDef};
use tower::ServiceExt;

fn state(config: ApiConfig) -> ApiState {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    ApiState::new(pool, config).unwrap()
}

async fn request(app: axum::Router, method: &str, path: &str, body: &str) -> (StatusCode, String) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn test_schema() -> SchemaDef {
    SchemaDef {
        tables: vec![TableDef {
            schema: "public".into(),
            name: "holder".into(),
            columns: vec![
                ColumnDef::new("address", ScalarType::Text, false),
                ColumnDef::new("balance", ScalarType::BigInt, false),
            ],
            primary_key: vec!["address".into()],
            indexes: Vec::new(),
        }],
        enums: Vec::new(),
        views: Vec::new(),
    }
}

#[test]
fn validates_select_and_rejects_attack_shapes() {
    for sql in [
        "SELECT address, balance FROM holder WHERE balance > $1::text::numeric ORDER BY balance DESC LIMIT 50",
        "SELECT count(*) FROM holder",
        "SELECT h.address FROM holder h JOIN other o ON h.address = o.address",
        "SELECT 1; -- trailing comment",
        "SELECT address FROM holder ORDER BY balance ASC, address DESC",
    ] {
        assert!(validate_sql(sql).is_ok(), "{sql}: {:?}", validate_sql(sql));
    }
    for sql in [
        "",
        "DELETE FROM holder",
        "SELECT 1; DELETE FROM holder",
        "SELECT * FROM public.holder",
        "SELECT * FROM pg_catalog.pg_class",
        "SELECT * FROM pg_class",
        "SELECT pg_read_file('/etc/passwd')",
        "SELECT pg_advisory_lock(1)",
        "SELECT public.my_function()",
        "SELECT 1 INTO new_table",
        "SELECT * FROM holder FOR UPDATE",
        "WITH x AS (DELETE FROM holder RETURNING *) SELECT * FROM x",
        "SELECT 1::regclass",
        "SELECT 1 OPERATOR(pg_catalog.+) 2",
        "SELECT address FROM holder ORDER BY pg_advisory_lock(1)",
        "SELECT address FROM holder ORDER BY address USING <",
        "SELECT address FROM holder ORDER BY address USING OPERATOR(public.<)",
        "SELECT address FROM holder ORDER BY (SELECT pg_read_file('/etc/passwd'))",
    ] {
        assert!(validate_sql(sql).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn probes_and_errors_are_honest() {
    let shared = state(ApiConfig::default());
    let app = router(shared.clone());
    assert_eq!(
        request(app.clone(), "GET", "/ready", "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    shared.set_ready(true);
    assert_eq!(
        request(app.clone(), "GET", "/ready", "").await.0,
        StatusCode::OK
    );
    shared.set_ready(false);
    assert_eq!(
        request(app.clone(), "GET", "/health", "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let (status, body) = request(
        app.clone(),
        "POST",
        "/sql",
        r#"{"sql":"SELECT pg_read_file('secret')"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_sql"));
    assert!(!body.contains("secret"));
    assert_eq!(
        request(app.clone(), "POST", "/sql", "{").await.0,
        StatusCode::BAD_REQUEST
    );
    // `/sql/live` is GET-only; the old POST form is gone.
    assert_eq!(
        request(app.clone(), "POST", "/sql/live", "{}").await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        request(app.clone(), "POST", "/graphql", "{").await.0,
        StatusCode::BAD_REQUEST
    );

    let config = ApiConfig {
        ready_after_backfill: false,
        ..Default::default()
    };
    assert_eq!(
        request(router(state(config)), "GET", "/ready", "").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn graphql_can_be_disabled() {
    let config = ApiConfig {
        graphql: false,
        ..Default::default()
    };
    assert_eq!(
        request(router(state(config)), "POST", "/graphql", "{}")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn live_query_rejects_missing_and_invalid_input() {
    let app = router(state(ApiConfig::default()));
    let (status, body) = request(app.clone(), "GET", "/sql/live", "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_request"));

    let (status, body) = request(
        app.clone(),
        "GET",
        "/sql/live?sql=DELETE%20FROM%20holder",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_sql"));

    let (status, body) = request(
        app.clone(),
        "GET",
        "/sql/live?sql=SELECT%201&params=%7B%7D",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_params"));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/sql/live?sql=SELECT%201")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
}

#[tokio::test]
async fn metrics_expose_prometheus_text() {
    let app = router(state(ApiConfig::default()));
    let (status, body) = request(app, "GET", "/metrics", "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("# HELP stele_api_queries_total"));
    assert!(body.contains("# TYPE stele_api_errors_total counter"));
    assert!(body.contains("stele_reorg_total "));
}

#[tokio::test]
async fn status_reports_database_failures_without_details() {
    let app = router(state(ApiConfig::default()));
    let (status, body) = request(app, "GET", "/status", "").await;
    assert!(!status.is_success());
    assert!(body.contains("\"error\""));
    assert!(!body.contains("postgres://"));
    assert!(!body.contains("unused"));
}

#[tokio::test]
async fn graphql_uses_the_configured_schema_without_the_database() {
    let config = ApiConfig {
        custom_schema: Some(test_schema()),
        ..Default::default()
    };
    let app = router(state(config));

    let (status, body) = request(
        app.clone(),
        "POST",
        "/graphql",
        &json!({ "query": "{ __type(name: \"Row\") { name kind } }" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["data"]["__type"]["name"], "Row");
    assert_eq!(body["errors"], json!([]));

    let (status, body) = request(
        app.clone(),
        "POST",
        "/graphql",
        &json!({ "query": "{ _tables }" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["data"]["_tables"], json!(["holder"]));

    // Unknown columns are rejected before the database is touched.
    let (status, body) = request(
        app.clone(),
        "POST",
        "/graphql",
        &json!({ "query": "{ holder(orderBy: \"missing\") { total } }" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert!(body["errors"][0]["message"]
        .as_str()
        .is_some_and(|message| message.contains("unknown column")));

    // A valid query fails at execution because the test pool is unreachable,
    // and the error never includes SQL or connection details.
    let (status, body) = request(
        app.clone(),
        "POST",
        "/graphql",
        &json!({ "query": "{ holder(limit: 1, where: \"{\\\"address\\\":\\\"0x1\\\"}\") { items total } }" })
            .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("SELECT"));
    assert!(!body.contains("postgres://"));
    let body: Value = serde_json::from_str(&body).unwrap();
    assert!(body["errors"][0]["message"]
        .as_str()
        .is_some_and(|message| message.contains("Query execution failed")));
}

#[tokio::test]
async fn graphql_reports_missing_metadata() {
    let shared = state(ApiConfig::default());
    let error = shared
        .prepare_graphql()
        .await
        .expect_err("an unreachable database cannot supply metadata");
    assert!(matches!(
        error,
        stele_api::GraphqlInitError::MetadataUnavailable
    ));
    let (status, body) = request(
        router(shared),
        "POST",
        "/graphql",
        &json!({ "query": "{ _tables }" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("graphql_unavailable"));
    assert!(!body.contains("postgres://"));
}

#[tokio::test]
async fn rejects_invalid_configuration() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    for config in [
        ApiConfig {
            schema: "pg_catalog".into(),
            ..Default::default()
        },
        ApiConfig {
            max_rows: 0,
            ..Default::default()
        },
        ApiConfig {
            max_concurrent_queries: 0,
            ..Default::default()
        },
        ApiConfig {
            statement_timeout: std::time::Duration::ZERO,
            ..Default::default()
        },
        ApiConfig {
            live_interval_ms: 0,
            ..Default::default()
        },
    ] {
        assert!(ApiState::new(pool.clone(), config).is_err());
    }
}
