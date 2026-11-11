//! Configuration parsing and validation for stele.
//!
//! The crate mirrors the TOML surface described in
//! `docs/10-rust-api-contract.md` and applies the documented defaults for
//! ordering, polling intervals, reorg windows, block ranges, and account
//! flags.
//!
//! ```
//! use stele_config::Config;
//!
//! let config = Config::from_toml_str(
//!     r#"
//! ordering = "multichain"
//!
//! [database]
//! url = "postgres://localhost/stele"
//!
//! [chains.mainnet]
//! id = 1
//! rpc = ["https://eth.llamarpc.com"]
//! "#,
//! )
//! .expect("configuration should parse");
//!
//! config.validate().expect("configuration should be valid");
//! assert_eq!(config.chains["mainnet"].polling_interval_ms, 1_000);
//! ```
//!
//! Parsing and validation are separate steps: [`Config::from_toml_str`] and
//! [`Config::from_file`] only check the document shape, while
//! [`Config::validate`] enforces cross-references such as unknown chain names,
//! duplicate chain IDs, and factory parent ABIs.
//!
//! [`Config::from_file`] additionally rewrites relative ABI paths so they are
//! resolved against the directory containing the configuration file, which
//! makes [`AbiSource::load_json`] independent of the process working
//! directory.

mod abi;
mod chain;
mod config;
mod contract;
mod error;
mod ordering;
mod serde_util;
mod source;

pub use abi::AbiSource;
pub use chain::{ChainConfig, DatabaseConfig, validate_chains};
pub use config::Config;
pub use contract::{ContractConfig, FactoryConfig, FactoryParameter};
pub use error::{ConfigError, ConfigSection};
pub use ordering::OrderingMode;
pub use source::{AccountConfig, BlockConfig};

/// Default RPC polling interval in milliseconds.
pub const DEFAULT_POLLING_INTERVAL_MS: u64 = 1_000;

/// Minimum accepted RPC polling interval in milliseconds.
pub const MIN_POLLING_INTERVAL_MS: u64 = 100;

/// Default reorg window in seconds.
pub const DEFAULT_REORG_WINDOW_SECONDS: u16 = 180;

/// Maximum accepted reorg window in seconds.
pub const MAX_REORG_WINDOW_SECONDS: u16 = 600;

/// Largest accepted chain ID: JavaScript's maximum
/// safe integer.
pub const MAX_CHAIN_ID: u64 = 9_007_199_254_740_991;

/// Default PostgreSQL schema for indexing tables.
pub const DEFAULT_DATABASE_SCHEMA: &str = "public";
