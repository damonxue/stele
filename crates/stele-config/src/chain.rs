//! Database and chain configuration.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::serde_util::deserialize_one_or_many;
use crate::{
    ConfigError, DEFAULT_DATABASE_SCHEMA, DEFAULT_POLLING_INTERVAL_MS,
    DEFAULT_REORG_WINDOW_SECONDS, MAX_CHAIN_ID, MAX_REORG_WINDOW_SECONDS, MIN_POLLING_INTERVAL_MS,
};

/// PostgreSQL connection settings.
///
/// `schema` defaults to `public`. `url` has no default; it is
/// rejected by [`DatabaseConfig::validate`] when blank.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection string.
    #[serde(default)]
    pub url: String,
    /// Schema that holds the indexing tables.
    #[serde(default = "default_database_schema")]
    pub schema: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            schema: DEFAULT_DATABASE_SCHEMA.to_owned(),
        }
    }
}

fn default_database_schema() -> String {
    DEFAULT_DATABASE_SCHEMA.to_owned()
}

impl DatabaseConfig {
    /// Rejects a blank connection string or schema.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.url.trim().is_empty() {
            return Err(ConfigError::EmptyDatabaseUrl);
        }
        if self.schema.trim().is_empty() {
            return Err(ConfigError::EmptyDatabaseSchema);
        }
        Ok(())
    }
}

/// Configuration for one chain.
///
/// TOML uses `polling_interval` and `reorg_window`; the struct fields carry the
/// `_ms`/`_seconds` units. Both spellings deserialize, and serialization emits
/// the TOML spelling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainConfig {
    /// Numeric EVM chain ID.
    pub id: u64,
    /// RPC endpoints, tried in order. A single string is also accepted.
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub rpc: Vec<String>,
    /// RPC polling interval in milliseconds. Defaults to `1000`.
    #[serde(
        rename = "polling_interval",
        alias = "polling_interval_ms",
        default = "default_polling_interval_ms"
    )]
    pub polling_interval_ms: u64,
    /// Reorg window in seconds. Defaults to `180`; the maximum is `600`.
    #[serde(
        rename = "reorg_window",
        alias = "reorg_window_seconds",
        default = "default_reorg_window_seconds"
    )]
    pub reorg_window_seconds: u16,
    /// Whether RPC responses are cached in the database. Defaults to `true`.
    #[serde(default = "default_cache_rpc_requests")]
    pub cache_rpc_requests: bool,
}

fn default_polling_interval_ms() -> u64 {
    DEFAULT_POLLING_INTERVAL_MS
}

fn default_reorg_window_seconds() -> u16 {
    DEFAULT_REORG_WINDOW_SECONDS
}

fn default_cache_rpc_requests() -> bool {
    true
}

impl ChainConfig {
    /// Creates a chain with the default settings and no RPC endpoints.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            rpc: Vec::new(),
            polling_interval_ms: DEFAULT_POLLING_INTERVAL_MS,
            reorg_window_seconds: DEFAULT_REORG_WINDOW_SECONDS,
            cache_rpc_requests: true,
        }
    }

    /// Validates the numeric chain ID, polling interval, and reorg window.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.id > MAX_CHAIN_ID {
            return Err(ConfigError::ChainIdTooLarge(self.id));
        }
        if self.polling_interval_ms < MIN_POLLING_INTERVAL_MS {
            return Err(ConfigError::PollingIntervalTooSmall(
                self.polling_interval_ms,
            ));
        }
        if self.reorg_window_seconds > MAX_REORG_WINDOW_SECONDS {
            return Err(ConfigError::ReorgWindowTooLarge(self.reorg_window_seconds));
        }
        Ok(())
    }
}

/// Validates every chain and rejects duplicate numeric chain IDs.
///
/// Duplicate detection is deterministic: entries are examined in `BTreeMap`
/// key order, so the error always reports the lexicographically smaller name
/// as `first`.
pub fn validate_chains(chains: &BTreeMap<String, ChainConfig>) -> Result<(), ConfigError> {
    let mut ids = BTreeMap::new();
    for (name, chain) in chains {
        chain.validate()?;
        if let Some(first) = ids.insert(chain.id, name) {
            return Err(ConfigError::DuplicateChainId {
                id: chain.id,
                first: first.clone(),
                second: name.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stele_defaults_and_boundaries() {
        let mut chain = ChainConfig::new(MAX_CHAIN_ID);
        assert_eq!(chain.reorg_window_seconds, 180);
        assert_eq!(chain.polling_interval_ms, 1_000);
        assert!(chain.cache_rpc_requests);
        assert!(chain.rpc.is_empty());
        chain.reorg_window_seconds = 0;
        chain.polling_interval_ms = 100;
        assert!(chain.validate().is_ok());
        chain.reorg_window_seconds = 600;
        assert!(chain.validate().is_ok());
        chain.reorg_window_seconds = 601;
        assert_eq!(chain.validate(), Err(ConfigError::ReorgWindowTooLarge(601)));
        chain.reorg_window_seconds = 180;
        chain.polling_interval_ms = 99;
        assert_eq!(
            chain.validate(),
            Err(ConfigError::PollingIntervalTooSmall(99))
        );
        chain.polling_interval_ms = 1_000;
        chain.id = MAX_CHAIN_ID + 1;
        assert_eq!(
            chain.validate(),
            Err(ConfigError::ChainIdTooLarge(MAX_CHAIN_ID + 1))
        );
    }

    #[test]
    fn duplicate_chain_ids_are_rejected() {
        let chains = BTreeMap::from([
            ("mainnet".to_owned(), ChainConfig::new(1)),
            ("other".to_owned(), ChainConfig::new(1)),
        ]);
        assert_eq!(
            validate_chains(&chains),
            Err(ConfigError::DuplicateChainId {
                id: 1,
                first: "mainnet".to_owned(),
                second: "other".to_owned(),
            })
        );
    }

    #[test]
    fn chain_defaults_apply_during_deserialization() {
        let chain: ChainConfig = toml::from_str("id = 1\nrpc = \"https://rpc.example\"").unwrap();
        assert_eq!(chain.rpc, vec!["https://rpc.example".to_owned()]);
        assert_eq!(chain.polling_interval_ms, DEFAULT_POLLING_INTERVAL_MS);
        assert_eq!(chain.reorg_window_seconds, DEFAULT_REORG_WINDOW_SECONDS);
        assert!(chain.cache_rpc_requests);
    }

    #[test]
    fn database_defaults_and_validation() {
        let database: DatabaseConfig =
            toml::from_str("url = \"postgres://localhost/stele\"").expect("schema should default");
        assert_eq!(database.schema, DEFAULT_DATABASE_SCHEMA);
        assert!(database.validate().is_ok());
        assert_eq!(
            DatabaseConfig::default().validate(),
            Err(ConfigError::EmptyDatabaseUrl)
        );
        assert_eq!(
            DatabaseConfig {
                url: "postgres://localhost/stele".to_owned(),
                schema: String::new(),
            }
            .validate(),
            Err(ConfigError::EmptyDatabaseSchema)
        );
    }
}
