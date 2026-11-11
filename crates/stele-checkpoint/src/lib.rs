//! Checkpoint encoding and ordering.

use std::{error::Error, fmt, str::FromStr};

pub const CHECKPOINT_LENGTH: usize = 75;
pub const MAX_TIMESTAMP: u64 = 9_999_999_999;
pub const MAX_COMPONENT: u64 = 9_999_999_999_999_999;

pub const TRANSACTION_EVENT: u8 = 2;
pub const LOG_EVENT: u8 = 5;
pub const BLOCK_EVENT: u8 = 5;
pub const TRACE_EVENT: u8 = 7;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Checkpoint {
    pub block_timestamp: u64,
    pub chain_id: u64,
    pub block_number: u64,
    pub transaction_index: u64,
    pub event_type: u8,
    pub event_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointError {
    ComponentOutOfRange(&'static str),
    InvalidEncoding,
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ComponentOutOfRange(name) => write!(f, "checkpoint {name} is out of range"),
            Self::InvalidEncoding => write!(f, "checkpoint must contain exactly 75 ASCII digits"),
        }
    }
}

impl Error for CheckpointError {}

impl Checkpoint {
    pub const ZERO: Self = Self {
        block_timestamp: 0,
        chain_id: 0,
        block_number: 0,
        transaction_index: 0,
        event_type: 0,
        event_index: 0,
    };

    pub const MAX: Self = Self {
        block_timestamp: MAX_TIMESTAMP,
        chain_id: MAX_COMPONENT,
        block_number: MAX_COMPONENT,
        transaction_index: MAX_COMPONENT,
        event_type: 9,
        event_index: MAX_COMPONENT,
    };

    pub fn encode(self) -> Result<String, CheckpointError> {
        if self.block_timestamp > MAX_TIMESTAMP {
            return Err(CheckpointError::ComponentOutOfRange("block_timestamp"));
        }
        for (name, value) in [
            ("chain_id", self.chain_id),
            ("block_number", self.block_number),
            ("transaction_index", self.transaction_index),
            ("event_index", self.event_index),
        ] {
            if value > MAX_COMPONENT {
                return Err(CheckpointError::ComponentOutOfRange(name));
            }
        }
        if self.event_type > 9 {
            return Err(CheckpointError::ComponentOutOfRange("event_type"));
        }

        Ok(format!(
            "{:010}{:016}{:016}{:016}{}{:016}",
            self.block_timestamp,
            self.chain_id,
            self.block_number,
            self.transaction_index,
            self.event_type,
            self.event_index
        ))
    }

    pub fn block_start(block_timestamp: u64, chain_id: u64, block_number: u64) -> Self {
        Self {
            block_timestamp,
            chain_id,
            block_number,
            ..Self::ZERO
        }
    }

    pub fn block_end(block_timestamp: u64, chain_id: u64, block_number: u64) -> Self {
        Self {
            block_timestamp,
            chain_id,
            block_number,
            ..Self::MAX
        }
    }
}

impl FromStr for Checkpoint {
    type Err = CheckpointError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != CHECKPOINT_LENGTH || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(CheckpointError::InvalidEncoding);
        }
        let part = |start, end| {
            value[start..end]
                .parse::<u64>()
                .map_err(|_| CheckpointError::InvalidEncoding)
        };
        Ok(Self {
            block_timestamp: part(0, 10)?,
            chain_id: part(10, 26)?,
            block_number: part(26, 42)?,
            transaction_index: part(42, 58)?,
            event_type: value.as_bytes()[58] - b'0',
            event_index: part(59, 75)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stele_fixture_round_trip() {
        let checkpoint = Checkpoint {
            block_timestamp: 1,
            chain_id: 1,
            block_number: 1,
            transaction_index: 1,
            event_type: 1,
            event_index: 1,
        };
        let expected = concat!(
            "0000000001",
            "0000000000000001",
            "0000000000000001",
            "0000000000000001",
            "1",
            "0000000000000001"
        );
        assert_eq!(checkpoint.encode().unwrap(), expected);
        assert_eq!(expected.parse::<Checkpoint>().unwrap(), checkpoint);
        assert_eq!(
            Checkpoint::MAX
                .encode()
                .unwrap()
                .parse::<Checkpoint>()
                .unwrap(),
            Checkpoint::MAX
        );
    }

    #[test]
    fn rejects_invalid_encoding_and_out_of_range_fields() {
        assert!("1".parse::<Checkpoint>().is_err());
        assert!("a".repeat(CHECKPOINT_LENGTH).parse::<Checkpoint>().is_err());
        assert!(
            format!("{}é", "0".repeat(CHECKPOINT_LENGTH - 2))
                .parse::<Checkpoint>()
                .is_err()
        );
        assert!(
            Checkpoint {
                chain_id: MAX_COMPONENT + 1,
                ..Checkpoint::ZERO
            }
            .encode()
            .is_err()
        );
        assert!(
            Checkpoint {
                event_type: 10,
                ..Checkpoint::ZERO
            }
            .encode()
            .is_err()
        );
        assert!(
            Checkpoint {
                block_timestamp: MAX_TIMESTAMP + 1,
                ..Checkpoint::ZERO
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn encoding_order_matches_checkpoint_order() {
        let values = [
            Checkpoint::ZERO,
            Checkpoint::block_start(1, 1, 1),
            Checkpoint {
                event_type: TRANSACTION_EVENT,
                ..Checkpoint::block_start(1, 1, 1)
            },
            Checkpoint {
                event_type: LOG_EVENT,
                ..Checkpoint::block_start(1, 1, 1)
            },
            Checkpoint {
                event_type: TRACE_EVENT,
                ..Checkpoint::block_start(1, 1, 1)
            },
            Checkpoint::block_end(1, 1, 1),
            Checkpoint::block_start(1, 2, 1),
            Checkpoint::block_start(2, 1, 1),
        ];
        for pair in values.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].encode().unwrap() < pair[1].encode().unwrap());
        }
    }

    #[test]
    fn block_bounds_contain_all_events_in_the_block() {
        let start = Checkpoint::block_start(123, 10, 456);
        let end = Checkpoint::block_end(123, 10, 456);
        let events = [
            Checkpoint {
                event_type: TRANSACTION_EVENT,
                ..start
            },
            Checkpoint {
                event_type: LOG_EVENT,
                transaction_index: 42,
                event_index: 2,
                ..start
            },
            Checkpoint {
                event_type: BLOCK_EVENT,
                transaction_index: MAX_COMPONENT,
                ..start
            },
        ];
        for event in events {
            assert!(start <= event && event <= end);
            assert_eq!(
                event.encode().unwrap().parse::<Checkpoint>().unwrap(),
                event
            );
        }
    }
}
