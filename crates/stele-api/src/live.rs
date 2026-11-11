//! Live SQL over server-sent events.
//!
//! `GET /sql/live?sql=...&params=[...]` validates the statement exactly like
//! `/sql`, then re-runs it on [`ApiConfig::live_interval_ms`] and pushes
//! `data: {"rows":[...]}` only when the serialized result changes. Results are
//! compared through [`content_hash`]; no diff is emitted for an unchanged
//! result, so a subscriber only sees writes that affect its query.
//!
//! Each open stream holds one permit from the shared semaphore, so streams and
//! one-shot queries share the same concurrency budget.

use std::{convert::Infallible, time::Duration};

use axum::{
    extract::{rejection::QueryRejection, Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{interval, MissedTickBehavior};

use crate::{execute_query, validate_sql, ApiError, ApiState};

/// Query parameters for `GET /sql/live`.
#[derive(Debug, Deserialize)]
pub(crate) struct LiveParams {
    sql: String,
    /// JSON array of scalars, matching `SqlRequest::params`.
    #[serde(default)]
    params: Option<String>,
}

struct LiveState {
    api: ApiState,
    query: String,
    params: Vec<Option<String>>,
    ticker: tokio::time::Interval,
    /// Hash of the last payload sent, `None` before the first result.
    previous: Option<u64>,
    /// Whether the previous attempt failed, so errors are reported once.
    failed: bool,
    /// Released when the stream is dropped.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// `GET /sql/live` handler.
pub(crate) async fn sql_live(
    State(state): State<ApiState>,
    params: Result<Query<LiveParams>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let Query(params) = params.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Expected sql and optional params query parameters",
        )
    })?;
    let query = validate_sql(&params.sql)
        .map_err(|message| ApiError::new(StatusCode::BAD_REQUEST, "invalid_sql", message))?;
    let bound = parse_live_params(params.params.as_deref())
        .map_err(|message| ApiError::new(StatusCode::BAD_REQUEST, "invalid_params", message))?;
    let permit = state.permits.clone().try_acquire_owned().map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "query_capacity",
            "Query capacity exceeded",
        )
    })?;

    let mut ticker = interval(Duration::from_millis(state.config.live_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let stream = futures_util::stream::unfold(
        LiveState {
            api: state,
            query,
            params: bound,
            ticker,
            previous: None,
            failed: false,
            _permit: permit,
        },
        |mut state| async move {
            loop {
                state.ticker.tick().await;
                match execute_query(&state.api, &state.query, &state.params).await {
                    Ok(response) => {
                        state.failed = false;
                        let payload = match serde_json::to_string(&response) {
                            Ok(payload) => payload,
                            Err(_) => {
                                tracing::warn!(
                                    error_kind = "live_serialize",
                                    "Live query result could not be serialized"
                                );
                                continue;
                            }
                        };
                        let hash = content_hash(payload.as_bytes());
                        if result_changed(state.previous, hash) {
                            state.previous = Some(hash);
                            return Some((Ok(Event::default().data(payload)), state));
                        }
                    }
                    Err(error) => {
                        if !state.failed {
                            state.failed = true;
                            crate::metrics::record_error();
                            tracing::warn!(
                                error_kind = "live_query",
                                status = error.status.as_u16(),
                                "Live query failed"
                            );
                            return Some((
                                Ok(Event::default().event("error").data(
                                    r#"{"error":{"code":"database_error","message":"Query execution failed"}}"#,
                                )),
                                state,
                            ));
                        }
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// Parse the `params` query parameter.
///
/// Accepts a JSON array of strings, numbers, booleans, or nulls and converts
/// each element to the text form that `/sql` accepts. Missing or empty input
/// means "no parameters".
pub(crate) fn parse_live_params(raw: Option<&str>) -> Result<Vec<Option<String>>, &'static str> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };
    let value: Value =
        serde_json::from_str(raw).map_err(|_| "params must be a JSON array of scalars")?;
    let array = value
        .as_array()
        .ok_or("params must be a JSON array of scalars")?;
    let mut params = Vec::with_capacity(array.len());
    for item in array {
        params.push(match item {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            Value::Array(_) | Value::Object(_) => {
                return Err("params must be a JSON array of scalars")
            }
        });
    }
    Ok(params)
}

/// Whether a newly serialized result should be sent.
///
/// The first result always differs; afterwards only a different hash does.
pub(crate) fn result_changed(previous: Option<u64>, current: u64) -> bool {
    previous != Some(current)
}

/// Stable FNV-1a hash of a serialized result.
///
/// The hash only needs to be stable inside one process: it decides whether two
/// consecutive executions produced the same payload, not whether two processes
/// agree. FNV-1a avoids a cryptographic dependency on the hot path.
pub(crate) fn content_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_result_is_always_sent_and_repeats_are_not() {
        let first = content_hash(br#"{"rows":[]}"#);
        assert!(result_changed(None, first));
        assert!(!result_changed(Some(first), first));
        assert!(result_changed(
            Some(first),
            content_hash(br#"{"rows":[1]}"#)
        ));
    }

    #[test]
    fn hashing_is_deterministic_and_order_sensitive() {
        assert_eq!(content_hash(b"abc"), content_hash(b"abc"));
        assert_ne!(content_hash(b"abc"), content_hash(b"acb"));
        assert_eq!(content_hash(b""), 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn params_accept_scalars_and_reject_containers() {
        assert_eq!(parse_live_params(None), Ok(Vec::new()));
        assert_eq!(parse_live_params(Some("")), Ok(Vec::new()));
        assert_eq!(
            parse_live_params(Some(r#"["a", 1, true, null]"#)),
            Ok(vec![
                Some("a".to_owned()),
                Some("1".to_owned()),
                Some("true".to_owned()),
                None
            ])
        );
        assert!(parse_live_params(Some(r#"{"a":1}"#)).is_err());
        assert!(parse_live_params(Some(r#"[[1]]"#)).is_err());
        assert!(parse_live_params(Some("not json")).is_err());
    }
}
