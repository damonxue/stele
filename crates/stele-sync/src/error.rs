//! Errors produced by the sync layer.

/// Failure of an RPC call or a cache-store operation.
///
/// Callers distinguish transient failures ([`SyncError::Transport`],
/// [`SyncError::Rpc`]) from responses that cannot be trusted
/// ([`SyncError::InvalidResponse`]) and from missing data
/// ([`SyncError::NotFound`]). Configuration mistakes are reported separately.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// The HTTP request or its response body failed at the transport level.
    #[error("transport failure: {0}")]
    Transport(String),
    /// The provider returned a JSON-RPC error object.
    #[error("rpc error {code}: {message}")]
    Rpc {
        /// Numeric JSON-RPC error code.
        code: i64,
        /// Provider-supplied message.
        message: String,
    },
    /// The provider responded with a shape that violates the JSON-RPC or a
    /// method-specific contract.
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    /// The RPC URL list was empty or contained a non-HTTP URL.
    #[error("invalid rpc url: {0}")]
    InvalidUrl(String),
    /// The provider returned `null` for a resource that was requested by a
    /// concrete identifier.
    #[error("not found: {0}")]
    NotFound(String),
    /// The database schema name is not a safe SQL identifier.
    #[error("invalid schema name: {0}")]
    InvalidSchema(String),
    /// The Postgres driver failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
