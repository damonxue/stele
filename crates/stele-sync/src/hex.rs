//! Strict decoding of JSON-RPC hex strings.
//!
//! JSON-RPC mixes two encodings that look alike: *quantities* (`0x` plus
//! minimal hex digits, no fixed width) and *data* (`0x` plus an even number of
//! digits). Addresses, hashes and topics are fixed-width data. Decoding here is
//! deliberately strict so a malformed provider response becomes
//! [`SyncError::InvalidResponse`] instead of silently wrong chain data.

use stele_types::{Address, Topic, B256};

use crate::error::SyncError;

fn invalid(message: impl Into<String>) -> SyncError {
    SyncError::InvalidResponse(message.into())
}

/// Strips the `0x` prefix and verifies that only hex digits remain.
fn digits(value: &str) -> Result<&str, SyncError> {
    let Some(rest) = value.strip_prefix("0x") else {
        return Err(invalid(format!("hex value {value:?} must start with 0x")));
    };
    if !rest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(format!(
            "hex value {value:?} contains a non-hex character"
        )));
    }
    Ok(rest)
}

/// Decodes a JSON-RPC quantity into a `u64`.
///
/// Rejects a missing prefix, empty digit sequences, non-hex characters and
/// values wider than 64 bits.
pub(crate) fn quantity_u64(value: &str) -> Result<u64, SyncError> {
    let digits = digits(value)?;
    if digits.is_empty() {
        return Err(invalid(format!("hex quantity {value:?} has no digits")));
    }
    if digits.len() > 16 {
        return Err(invalid(format!("hex quantity {value:?} exceeds 64 bits")));
    }
    u64::from_str_radix(digits, 16)
        .map_err(|_| invalid(format!("hex quantity {value:?} is out of range")))
}

/// Decodes a hex quantity into 32 big-endian bytes.
///
/// An empty payload (`"0x"`) is accepted as zero because several clients emit it
/// for zero values in `debug_trace` frames. Wider-than-32-byte values are
/// rejected rather than truncated.
pub(crate) fn quantity_u256(value: &str) -> Result<[u8; 32], SyncError> {
    let digits = digits(value)?;
    if digits.len() > 64 {
        return Err(invalid(format!("hex value {value:?} exceeds 32 bytes")));
    }
    let padded = if digits.len() % 2 == 1 {
        format!("0{digits}")
    } else {
        digits.to_string()
    };
    let mut bytes = [0u8; 32];
    let offset = bytes.len() - padded.len() / 2;
    for index in 0..padded.len() / 2 {
        bytes[offset + index] = u8::from_str_radix(&padded[2 * index..2 * index + 2], 16)
            .map_err(|_| invalid(format!("hex value {value:?} is not valid hex")))?;
    }
    Ok(bytes)
}

/// Decodes arbitrary hex data. Odd digit counts are rejected.
pub(crate) fn data(value: &str) -> Result<Vec<u8>, SyncError> {
    let digits = digits(value)?;
    if digits.len() % 2 != 0 {
        return Err(invalid(format!(
            "hex data {value:?} has an odd number of digits"
        )));
    }
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for index in (0..digits.len()).step_by(2) {
        bytes.push(
            u8::from_str_radix(&digits[index..index + 2], 16)
                .map_err(|_| invalid(format!("hex data {value:?} is not valid hex")))?,
        );
    }
    Ok(bytes)
}

/// Decodes a 20-byte address. Output is normalized to lowercase on display.
pub(crate) fn address(value: &str) -> Result<Address, SyncError> {
    Address::from_hex(value).map_err(|error| invalid(format!("invalid address {value:?}: {error}")))
}

/// Decodes a 32-byte hash.
pub(crate) fn hash(value: &str) -> Result<B256, SyncError> {
    B256::from_hex(value).map_err(|error| invalid(format!("invalid hash {value:?}: {error}")))
}

/// Decodes a 32-byte log topic.
pub(crate) fn topic(value: &str) -> Result<Topic, SyncError> {
    Topic::from_hex(value).map_err(|error| invalid(format!("invalid topic {value:?}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities_accept_minimal_hex_and_reject_malformed_input() {
        assert_eq!(quantity_u64("0x0").unwrap(), 0);
        assert_eq!(quantity_u64("0x1f4").unwrap(), 500);
        assert_eq!(quantity_u64("0x00ff").unwrap(), 255);
        assert_eq!(quantity_u64("0xffffffffffffffff").unwrap(), u64::MAX);

        for value in ["0x", "", "1f4", "0xzz", "0x10000000000000000", "0x 1"] {
            assert!(
                matches!(quantity_u64(value), Err(SyncError::InvalidResponse(_))),
                "{value:?} should be rejected"
            );
        }
    }

    #[test]
    fn u256_quantities_are_left_padded() {
        let mut expected = [0u8; 32];
        expected[31] = 1;
        assert_eq!(quantity_u256("0x1").unwrap(), expected);
        assert_eq!(quantity_u256("0x0").unwrap(), [0u8; 32]);
        assert_eq!(quantity_u256("0x").unwrap(), [0u8; 32]);

        let full = format!("0x{}", "ab".repeat(32));
        assert_eq!(quantity_u256(&full).unwrap(), [0xab; 32]);
        assert!(quantity_u256(&format!("0x{}", "ab".repeat(33))).is_err());
    }

    #[test]
    fn data_requires_even_digit_count() {
        assert_eq!(data("0x").unwrap(), Vec::<u8>::new());
        assert_eq!(data("0xdeadBEEF").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(data("0xabc").is_err());
        assert!(data("0xzz").is_err());
        assert!(data("dead").is_err());
    }

    #[test]
    fn fixed_width_values_are_normalized_to_lowercase() {
        let parsed = address("0x000102030405060708090A0B0C0D0E0F10111213").unwrap();
        assert_eq!(
            parsed.to_hex(),
            "0x000102030405060708090a0b0c0d0e0f10111213"
        );
        assert!(address("0x000102030405060708090a0b0c0d0e0f1011121").is_err());

        let hash_value = hash(&format!("0x{}", "AB".repeat(32))).unwrap();
        assert_eq!(hash_value.to_hex(), format!("0x{}", "ab".repeat(32)));
        assert!(hash(&format!("0x{}", "ab".repeat(31))).is_err());

        let topic_value = topic(&format!("0x{}", "CD".repeat(32))).unwrap();
        assert_eq!(topic_value.to_hex(), format!("0x{}", "cd".repeat(32)));
        assert!(topic("0x00").is_err());
    }
}
