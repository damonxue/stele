//! Indexing progress exposed at `GET /status`.
//!
//! Progress is read from `stele_store.progress`, the table written by
//! `stele-store` alongside committed checkpoints. Reads run under the same
//! statement timeout as queries. When the table does not exist (an API-only
//! deployment against a database that has never indexed anything), the endpoint
//! returns an empty list instead of failing.
//!
//! The response exposes chain ids, encoded checkpoints, and decoded block
//! numbers. It never includes connection strings, RPC URLs, or table layouts.

use std::str::FromStr;

use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use sqlx::Row as _;
use stele_checkpoint::Checkpoint;
use stele_store::Progress;

use crate::{database_error, metrics, ApiError, ApiState};

/// `GET /status` handler.
pub(crate) async fn status(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let exists = tokio::time::timeout(
        state.config.statement_timeout,
        sqlx::query_scalar::<_, bool>(
            "SELECT pg_catalog.to_regclass('stele_store.progress') IS NOT NULL",
        )
        .fetch_one(&state.pool),
    )
    .await;

    let exists = match exists {
        Ok(Ok(exists)) => exists,
        Ok(Err(error)) => return Err(database_error(error)),
        Err(_) => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                "Database unavailable",
            ))
        }
    };
    if !exists {
        return Ok(Json(json!({ "chains": [] })));
    }

    let rows = tokio::time::timeout(
        state.config.statement_timeout,
        sqlx::query(
            "SELECT chain_id, checkpoint, finalized \
             FROM stele_store.progress ORDER BY chain_id",
        )
        .fetch_all(&state.pool),
    )
    .await;

    let rows = match rows {
        Ok(Ok(rows)) => rows,
        Ok(Err(error)) => return Err(database_error(error)),
        Err(_) => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                "Database unavailable",
            ))
        }
    };

    let mut chains = Vec::with_capacity(rows.len());
    for row in rows {
        let chain_id: i64 = row.try_get("chain_id").map_err(|_| decode_error())?;
        let encoded_checkpoint: String = row.try_get("checkpoint").map_err(|_| decode_error())?;
        let encoded_finalized: String = row.try_get("finalized").map_err(|_| decode_error())?;
        let checkpoint = Checkpoint::from_str(&encoded_checkpoint).ok();
        let finalized = Checkpoint::from_str(&encoded_finalized).ok();
        let block_number = checkpoint.map(|checkpoint| checkpoint.block_number);
        let finalized_block_number = finalized.map(|checkpoint| checkpoint.block_number);
        // Using the store's `Progress` keeps the decoded pair tied to the shape
        // the indexer commits.
        let progress = match (checkpoint, finalized) {
            (Some(checkpoint), Some(finalized)) => Some(Progress {
                checkpoint,
                finalized,
            }),
            _ => None,
        };
        if let (Some(chain), Some(progress)) = (u64::try_from(chain_id).ok(), progress) {
            metrics::set_last_indexed_block(chain, progress.checkpoint.block_number);
        }
        chains.push(json!({
            "chain_id": chain_id,
            "checkpoint": encoded_checkpoint,
            "finalized": encoded_finalized,
            "block_number": block_number,
            "finalized_block_number": finalized_block_number,
        }));
    }
    Ok(Json(json!({ "chains": chains })))
}

fn decode_error() -> ApiError {
    tracing::warn!(
        error_kind = "status_decode",
        "Progress row could not be decoded"
    );
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "database_error",
        "Database query failed",
    )
}
