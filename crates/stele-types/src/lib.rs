//! Shared EVM data types used by every stele crate.
//!
//! Values are normalized before they enter the runtime: addresses, topics and
//! hashes are lowercase hex on the wire and fixed-width byte arrays in memory.
//! The crate deliberately has no network or database dependencies.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HexError {
    MissingPrefix,
    InvalidLength { expected: usize, actual: usize },
    InvalidCharacter,
}

impl fmt::Display for HexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrefix => write!(f, "hex value must start with 0x"),
            Self::InvalidLength { expected, actual } => {
                write!(f, "hex value must contain {expected} digits, got {actual}")
            }
            Self::InvalidCharacter => write!(f, "hex value contains a non-hex character"),
        }
    }
}

impl Error for HexError {}

fn decode_hex(value: &str, expected: usize) -> Result<Vec<u8>, HexError> {
    let digits = value.strip_prefix("0x").ok_or(HexError::MissingPrefix)?;
    if digits.len() != expected * 2 {
        return Err(HexError::InvalidLength {
            expected: expected * 2,
            actual: digits.len(),
        });
    }
    let mut bytes = Vec::with_capacity(expected);
    for index in (0..digits.len()).step_by(2) {
        let byte = u8::from_str_radix(&digits[index..index + 2], 16)
            .map_err(|_| HexError::InvalidCharacter)?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(2 + bytes.len() * 2);
    result.push_str("0x");
    for byte in bytes {
        use std::fmt::Write;
        write!(result, "{byte:02x}").expect("writing to a String cannot fail");
    }
    result
}

/// A 20-byte EVM address. Display and serde use lowercase `0x` hex, matching
/// normalized addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Address(pub [u8; 20]);

impl Address {
    pub const ZERO: Self = Self([0; 20]);

    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, HexError> {
        if bytes.len() != 20 {
            return Err(HexError::InvalidLength {
                expected: 40,
                actual: bytes.len() * 2,
            });
        }
        let mut address = [0; 20];
        address.copy_from_slice(bytes);
        Ok(Self(address))
    }

    pub fn from_hex(value: &str) -> Result<Self, HexError> {
        let bytes = decode_hex(value, 20)?;
        let mut address = [0; 20];
        address.copy_from_slice(&bytes);
        Ok(Self(address))
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        encode_hex(&self.0)
    }
}

impl FromStr for Address {
    type Err = HexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Address {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// A 32-byte log topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Topic(pub [u8; 32]);

impl Topic {
    pub const ZERO: Self = Self([0; 32]);

    pub fn from_hex(value: &str) -> Result<Self, HexError> {
        let bytes = decode_hex(value, 32)?;
        let mut topic = [0; 32];
        topic.copy_from_slice(&bytes);
        Ok(Self(topic))
    }

    pub fn to_hex(self) -> String {
        encode_hex(&self.0)
    }
}

impl FromStr for Topic {
    type Err = HexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Topic {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Topic {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// A 32-byte hash. Kept distinct from [Topic] so call sites cannot mix them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct B256(pub [u8; 32]);

impl B256 {
    pub const ZERO: Self = Self([0; 32]);

    pub fn from_hex(value: &str) -> Result<Self, HexError> {
        let bytes = decode_hex(value, 32)?;
        let mut hash = [0; 32];
        hash.copy_from_slice(&bytes);
        Ok(Self(hash))
    }

    pub fn to_hex(self) -> String {
        encode_hex(&self.0)
    }
}

impl FromStr for B256 {
    type Err = HexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl fmt::Display for B256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for B256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for B256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// A four-byte function selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Selector(pub [u8; 4]);

impl Selector {
    pub fn from_hex(value: &str) -> Result<Self, HexError> {
        let bytes = decode_hex(value, 4)?;
        Ok(Self([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn to_hex(self) -> String {
        encode_hex(&self.0)
    }
}

impl FromStr for Selector {
    type Err = HexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Selector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Selector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// A canonical block header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
}

/// The subset of a transaction required by filters and handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionRef {
    pub block_number: u64,
    pub transaction_index: u64,
    pub hash: B256,
    pub from: Address,
    pub to: Option<Address>,
}

/// The subset of a receipt required to decide transaction visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiptRef {
    pub block_number: u64,
    pub transaction_index: u64,
    pub from: Address,
    pub to: Option<Address>,
    pub success: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallType {
    Call,
    CallCode,
    DelegateCall,
    StaticCall,
    Create,
    Create2,
    SelfDestruct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceOutcome {
    Successful,
    Reverted,
    Unknown,
}

/// One normalized call frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trace {
    pub from: Address,
    pub to: Option<Address>,
    pub call_type: CallType,
    pub input: Vec<u8>,
    /// Full-width unsigned EVM value in big-endian bytes; `None` means null.
    pub value: Option<[u8; 32]>,
    pub output: Option<Vec<u8>>,
    pub outcome: TraceOutcome,
}

/// A call frame positioned inside a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRef {
    pub block_number: u64,
    pub transaction_index: u64,
    /// Position in the flattened call tree.
    pub trace_address: Vec<u32>,
    pub trace: Trace,
}

/// A normalized log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    pub block_number: u64,
    pub address: Address,
    pub topics: Vec<Topic>,
    pub data: Vec<u8>,
}

/// A log positioned inside a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRef {
    pub transaction_index: u64,
    pub log_index: u64,
    pub log: Log,
}

/// All fetched data for one block.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BlockData {
    pub block: Option<BlockRef>,
    pub transactions: Vec<TransactionRef>,
    pub receipts: Vec<ReceiptRef>,
    pub traces: Vec<TraceRef>,
    pub logs: Vec<LogRef>,
}

/// A database cell value shared by the store, schema and handler context.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CellValue {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Numeric(String),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Array(Vec<CellValue>),
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_rejects_malformed_values() {
        let address: Address = "0x000102030405060708090a0b0c0d0e0f10111213"
            .parse()
            .unwrap();
        assert_eq!(
            address.to_hex(),
            "0x000102030405060708090a0b0c0d0e0f10111213"
        );
        assert_eq!(
            "0x000102030405060708090a0b0c0d0e0f1011121".parse::<Address>(),
            Err(HexError::InvalidLength {
                expected: 40,
                actual: 39
            })
        );
        assert_eq!(
            "000102030405060708090a0b0c0d0e0f10111213".parse::<Address>(),
            Err(HexError::MissingPrefix)
        );
        assert_eq!(
            "0x000102030405060708090a0b0c0d0e0f1011121z".parse::<Address>(),
            Err(HexError::InvalidCharacter)
        );

        let topic = Topic::from_hex(&format!("0x{}", "ab".repeat(32))).unwrap();
        assert_eq!(topic.to_hex(), format!("0x{}", "ab".repeat(32)));
        assert_eq!(
            serde_json::to_string(&address).unwrap(),
            "\"0x000102030405060708090a0b0c0d0e0f10111213\""
        );
        assert_eq!(
            serde_json::from_str::<Address>("\"0x000102030405060708090a0b0c0d0e0f10111213\"")
                .unwrap(),
            address
        );
    }
}
