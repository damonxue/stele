//! Run only against a disposable database:
//! STELE_API_TEST_DATABASE_URL=... cargo test --test postgres -- --ignored

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use stele_api::{router, ApiConfig, ApiState};
use tower::ServiceExt;

async fn call(app: axum::Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let json = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, json)
}

#[tokio::test]
#[ignore = "requires an explicitly configured disposable PostgreSQL database"]
async fn executes_read_only_queries_and_enforces_limits() {
    let url = std::env::var("STELE_API_TEST_DATABASE_URL")
        .expect("set STELE_API_TEST_DATABASE_URL to a disposable database");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("stele_api_test_{}", std::process::id());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".holder (name text, balance numeric)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".holder VALUES ('a', 1), ('b', 2), ('c', 3)"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let config = ApiConfig {
        schema: schema.clone(),
        max_rows: 2,
        ..Default::default()
    };
    let shared = ApiState::new(pool.clone(), config).unwrap();
    let app = router(shared.clone());
    assert_eq!(
        call(app.clone(), "GET", "/health", Value::Null).await.0,
        StatusCode::OK
    );
    let (status, result) = call(app.clone(), "POST", "/sql", json!({
        "sql": "SELECT name, balance FROM holder WHERE balance > $1::text::numeric ORDER BY balance",
        "params": ["1"]
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert_eq!(result["rows"][0]["name"], "b");

    let (status, result) = call(
        app.clone(),
        "POST",
        "/sql",
        json!({"sql": "SELECT * FROM holder"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["error"]["code"], "row_limit");
    let (status, result) = call(
        app.clone(),
        "POST",
        "/sql",
        json!({"sql": "SELECT * FROM missing_sensitive_table"}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!result.to_string().contains("missing_sensitive_table"));
    let (status, _) = call(app, "POST", "/sql", json!({"sql": "DELETE FROM holder"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
