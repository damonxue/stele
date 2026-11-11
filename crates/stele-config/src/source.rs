//! Account and block interval configuration.

use serde::{Deserialize, Serialize};

use stele_types::Address;

use crate::serde_util::deserialize_one_or_many;

/// Account indexing configuration.
///
/// Accounts index transactions and native transfers that involve the listed
/// addresses. Both flags default to `true`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountConfig {
    /// Name of a chain declared in `[chains]`.
    pub chain: String,
    /// Watched addresses. A single address string is also accepted.
    #[serde(deserialize_with = "deserialize_one_or_many")]
    pub address: Vec<Address>,
    /// First block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    /// Last block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_block: Option<u64>,
    /// Index transactions sent to or from the watched addresses.
    #[serde(default = "default_true")]
    pub include_transactions: bool,
    /// Index native transfers to or from the watched addresses.
    #[serde(default = "default_true")]
    pub include_transfers: bool,
}

fn default_true() -> bool {
    true
}

/// Block interval configuration.
///
/// The runtime invokes registered block handlers every `interval` blocks, with
/// `offset` shifting the phase of the schedule within the interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockConfig {
    /// Name of a chain declared in `[chains]`.
    pub chain: String,
    /// Interval in blocks. Must be greater than zero.
    pub interval: u64,
    /// Offset within the interval. Defaults to `0`.
    #[serde(default)]
    pub offset: u64,
    /// First block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    /// Last block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_block: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDRESS: &str = "0x0000000000000000000000000000000000000001";

    #[test]
    fn account_flags_default_to_true() {
        let account: AccountConfig =
            toml::from_str(&format!("chain = \"mainnet\"\naddress = \"{ADDRESS}\"\n")).unwrap();
        assert!(account.include_transactions);
        assert!(account.include_transfers);
        assert_eq!(account.address, vec![ADDRESS.parse().unwrap()]);
        assert_eq!(account.start_block, None);
        assert_eq!(account.end_block, None);
    }

    #[test]
    fn account_flags_can_be_disabled() {
        let account: AccountConfig = toml::from_str(&format!(
            "chain = \"mainnet\"\naddress = [\"{ADDRESS}\"]\ninclude_transactions = false\ninclude_transfers = false\n"
        ))
        .unwrap();
        assert!(!account.include_transactions);
        assert!(!account.include_transfers);
    }

    #[test]
    fn block_offset_defaults_to_zero() {
        let block: BlockConfig = toml::from_str("chain = \"mainnet\"\ninterval = 300\n").unwrap();
        assert_eq!(block.interval, 300);
        assert_eq!(block.offset, 0);
        assert_eq!(block.start_block, None);
        assert_eq!(block.end_block, None);
    }
}
