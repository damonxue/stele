//! Error types returned while parsing and validating configuration.

use std::fmt;

use thiserror::Error;

/// The configuration section that referenced a chain or an address list.
///
/// Used by [ConfigError] so error messages can point at the offending section
/// without duplicating one error variant per section.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigSection {
    /// The `[contracts]` table.
    Contracts,
    /// The `[accounts]` table.
    Accounts,
    /// The `[blocks]` table.
    Blocks,
}

impl fmt::Display for ConfigSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Contracts => "contract",
            Self::Accounts => "account",
            Self::Blocks => "block interval",
        };
        f.write_str(name)
    }
}

/// Every failure that can occur while parsing or validating a configuration.
///
/// The variants that existed before the full config layer was added
/// ([`ConfigError::ChainIdTooLarge`], [`ConfigError::PollingIntervalTooSmall`],
/// [`ConfigError::ReorgWindowTooLarge`], and
/// [`ConfigError::DuplicateChainId`]) keep their original payloads and
/// messages.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ConfigError {
    /// A chain ID exceeds JavaScript's maximum safe integer.
    #[error("chain ID {0} exceeds 9007199254740991")]
    ChainIdTooLarge(u64),

    /// A polling interval is below the 100ms floor.
    #[error("polling interval {0}ms must be at least 100ms")]
    PollingIntervalTooSmall(u64),

    /// A reorg window exceeds the 600 second ceiling.
    #[error("reorg window {0}s exceeds 600s")]
    ReorgWindowTooLarge(u16),

    /// Two chains declare the same numeric chain ID.
    #[error("chains '{first}' and '{second}' use the same ID {id}")]
    DuplicateChainId {
        /// The duplicated chain ID.
        id: u64,
        /// The first chain name (in lexicographic order) that declared `id`.
        first: String,
        /// The second chain name that declared the same `id`.
        second: String,
    },

    /// The `ordering` value is not one of the supported ordering modes.
    #[error(
        "invalid ordering '{0}': expected 'omnichain', 'multichain', 'isolated', or 'experimental_isolated'"
    )]
    InvalidOrdering(String),

    /// The database connection string is missing or blank.
    #[error("database url must not be empty")]
    EmptyDatabaseUrl,

    /// The database schema is missing or blank.
    #[error("database schema must not be empty")]
    EmptyDatabaseSchema,

    /// The configuration does not declare any chain.
    #[error("configuration must define at least one chain")]
    EmptyChains,

    /// A contract, account, or block interval references an undefined chain.
    #[error("{section} '{name}' references unknown chain '{chain}'")]
    UnknownChain {
        /// The section that references the chain.
        section: ConfigSection,
        /// The contract, account, or block interval name.
        name: String,
        /// The chain name that does not exist in `chains`.
        chain: String,
    },

    /// A contract declares neither a static address nor a factory.
    #[error("contract '{contract}' must specify an address or a factory")]
    MissingContractAddress {
        /// The contract name.
        contract: String,
    },

    /// An address list is present but empty.
    #[error("{section} '{name}' must specify at least one address")]
    EmptyAddresses {
        /// The section that owns the address list.
        section: ConfigSection,
        /// The account name.
        name: String,
    },

    /// A factory references a parent contract that is not registered.
    #[error("factory on contract '{contract}' references unknown parent contract '{parent}'")]
    FactoryParentMissing {
        /// The contract that declares the factory.
        contract: String,
        /// The missing parent contract name.
        parent: String,
    },

    /// The parent ABI does not declare the factory event.
    #[error(
        "factory parent contract '{parent}' ABI does not contain event '{event}' (required by contract '{contract}')"
    )]
    FactoryEventMissing {
        /// The contract that declares the factory.
        contract: String,
        /// The parent contract whose ABI was inspected.
        parent: String,
        /// The event name that is missing from the parent ABI.
        event: String,
    },

    /// A low-level factory topic index is outside `1..=3`.
    #[error("factory parameter topic {0} is out of range; expected 1, 2, or 3")]
    InvalidFactoryTopic(u8),

    /// A nested factory parameter path is blank.
    #[error("factory parameter path must not be empty")]
    EmptyFactoryParameter,

    /// A block interval is zero.
    #[error("block interval for '{block}' must be greater than zero")]
    BlockIntervalZero {
        /// The block interval name.
        block: String,
    },

    /// An ABI file could not be read.
    #[error("failed to read ABI file '{path}': {message}")]
    AbiRead {
        /// The ABI path, as written in the configuration.
        path: String,
        /// The underlying I/O error message.
        message: String,
    },

    /// An ABI file did not contain valid JSON.
    #[error("failed to parse ABI file '{path}': {message}")]
    AbiParse {
        /// The ABI path, as written in the configuration.
        path: String,
        /// The underlying JSON error message.
        message: String,
    },

    /// A loaded ABI is neither an array of items nor an artifact object with an
    /// `abi` array.
    #[error("ABI '{abi}' must be a JSON array of ABI items or an object with an 'abi' array")]
    AbiInvalid {
        /// The contract name or ABI path that produced the value.
        abi: String,
    },

    /// The TOML document could not be deserialized.
    #[error("failed to parse TOML configuration: {message}")]
    TomlParse {
        /// The TOML parser error, including line and column information.
        message: String,
    },

    /// The configuration file could not be read.
    #[error("failed to read configuration file '{path}': {message}")]
    ReadFile {
        /// The configuration path, as passed to `Config::from_file`.
        path: String,
        /// The underlying I/O error message.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAX_CHAIN_ID;

    #[test]
    fn existing_messages_are_preserved() {
        assert_eq!(
            ConfigError::ChainIdTooLarge(MAX_CHAIN_ID + 1).to_string(),
            format!("chain ID {} exceeds {MAX_CHAIN_ID}", MAX_CHAIN_ID + 1)
        );
        assert_eq!(
            ConfigError::PollingIntervalTooSmall(99).to_string(),
            "polling interval 99ms must be at least 100ms"
        );
        assert_eq!(
            ConfigError::ReorgWindowTooLarge(601).to_string(),
            "reorg window 601s exceeds 600s"
        );
        assert_eq!(
            ConfigError::DuplicateChainId {
                id: 1,
                first: "a".to_owned(),
                second: "b".to_owned(),
            }
            .to_string(),
            "chains 'a' and 'b' use the same ID 1"
        );
    }

    #[test]
    fn sections_render_human_readable_names() {
        assert_eq!(ConfigSection::Contracts.to_string(), "contract");
        assert_eq!(ConfigSection::Accounts.to_string(), "account");
        assert_eq!(ConfigSection::Blocks.to_string(), "block interval");
    }
}
