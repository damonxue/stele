//! In-memory JSON-RPC transport for tests.
//!
//! Production callers use [`crate::client::HttpTransport`]. This module exists
//! so block assembly, log chunking, retry behaviour and response validation can
//! be tested without a network, and so downstream crates can exercise their own
//! sync logic against canned chain data.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::client::RpcTransport;
use crate::error::SyncError;

/// One request observed by [`MockTransport`].
#[derive(Clone, Debug, PartialEq)]
pub struct MockCall {
    /// Endpoint the request was sent to.
    pub url: String,
    /// JSON-RPC id chosen by the client.
    pub id: Value,
    /// JSON-RPC method.
    pub method: String,
    /// Positional parameters.
    pub params: Value,
}

type Handler = dyn Fn(&str, &MockCall) -> Result<Value, SyncError> + Send + Sync;

/// Records requests and answers them from a closure.
///
/// [`MockTransport::new`] wraps the closure's return value in a valid JSON-RPC
/// envelope. [`MockTransport::new_raw`] returns the closure's value verbatim,
/// which allows tests to inject malformed envelopes or provider error objects.
pub struct MockTransport {
    handler: Box<Handler>,
    wrap: bool,
    calls: Mutex<Vec<MockCall>>,
}

impl MockTransport {
    /// Creates a transport whose handler returns the JSON-RPC `result` member.
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&str, &MockCall) -> Result<Value, SyncError> + Send + Sync + 'static,
    {
        Self {
            handler: Box::new(handler),
            wrap: true,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Creates a transport whose handler returns the whole response body.
    pub fn new_raw<F>(handler: F) -> Self
    where
        F: Fn(&str, &MockCall) -> Result<Value, SyncError> + Send + Sync + 'static,
    {
        Self {
            handler: Box::new(handler),
            wrap: false,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Requests observed so far, in order.
    pub fn calls(&self) -> Vec<MockCall> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[async_trait]
impl RpcTransport for MockTransport {
    async fn post(&self, url: &str, body: &str) -> Result<Value, SyncError> {
        let request: Value = serde_json::from_str(body).map_err(|error| {
            SyncError::InvalidResponse(format!("mock received invalid JSON: {error}"))
        })?;
        let call = MockCall {
            url: url.to_string(),
            id: request.get("id").cloned().unwrap_or(Value::Null),
            method: request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            params: request.get("params").cloned().unwrap_or_else(|| json!([])),
        };
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(call.clone());
        let result = (self.handler)(url, &call)?;
        if self.wrap {
            Ok(json!({ "jsonrpc": "2.0", "id": call.id, "result": result }))
        } else {
            Ok(result)
        }
    }
}
