//! Sync building blocks for EVM JSON-RPC data retrieval.
//!
//! The crate is layered as follows:
//!
//! - [`rpc`] holds transport-independent JSON request plumbing and an in-memory
//!   request cache.
//! - [`interval`] holds block-range algebra and historical sync planning.
//! - [`fragment`] encodes fragment identifiers.
//! - [`client::RpcClient`] performs HTTP JSON-RPC calls, assembles
//!   [`stele_types::BlockData`] and applies endpoint failover with retries.
//! - [`store::SyncStore`] caches blocks, transactions, receipts, traces, logs,
//!   intervals and factory addresses in Postgres.
//!
//! The chain follower and indexing runtime live in `stele-core`.

pub mod client;
pub mod error;
pub mod fragment;
mod hex;
pub mod interval;
pub mod rpc;
pub mod store;
pub mod testing;

pub use client::{HttpTransport, RetryPolicy, RpcClient, RpcTransport};
pub use error::SyncError;
pub use store::SyncStore;
