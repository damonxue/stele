//! Runtime primitives for the EVM event model.
//!
//! The crate is layered from data to execution:
//!
//! - [`events`], [`filter`], [`historical`], [`indexing`], [`progress`] and
//!   [`realtime`] hold the pure building blocks: event matching, checkpoint
//!   ordering, handler dispatch and local reorg tracking.
//! - [`decode`] turns matched ABI payloads into JSON.
//! - [`plan`] turns [`stele_config::Config`] into per-chain plans.
//! - [`handler`] stores user callbacks and dispatches them sequentially.
//! - [`engine`] fetches blocks, discovers factories, decodes events and drives
//!   handlers in store batches.

pub mod decode;
pub mod engine;
pub mod events;
pub mod filter;
pub mod handler;
pub mod historical;
pub mod indexing;
pub mod plan;
pub mod progress;
pub mod realtime;

pub use decode::{DecodeError, DecodeKind, DecodeSpec, DecodedValues};
pub use engine::{ChainError, ChainSource, Engine, EngineError, EngineOptions, Metrics};
pub use handler::{DecodedEvent, EventContext, HandlerError, HandlerFuture, HandlerSet};
pub use plan::{CallbackPlan, ChainPlan, FactoryPlan, PlanBuild, build_plans};

use std::{collections::HashMap, error::Error, fmt};
use stele_checkpoint::{
    BLOCK_EVENT, Checkpoint, CheckpointError, LOG_EVENT, MAX_COMPONENT, TRACE_EVENT,
    TRANSACTION_EVENT,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OrderingMode {
    Omnichain,
    #[default]
    Multichain,
    Isolated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Block {
    pub timestamp: u64,
    pub chain_id: u64,
    pub number: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventPosition {
    Transaction {
        transaction_index: u64,
    },
    Log {
        transaction_index: u64,
        log_index: u64,
    },
    Trace {
        transaction_index: u64,
        trace_index: u64,
    },
    Transfer {
        transaction_index: u64,
        trace_index: u64,
    },
    Block,
}

impl EventPosition {
    pub fn checkpoint(self, block: Block) -> Result<Checkpoint, CheckpointError> {
        let (transaction_index, event_type, event_index) = match self {
            Self::Transaction { transaction_index } => (transaction_index, TRANSACTION_EVENT, 0),
            Self::Log {
                transaction_index,
                log_index,
            } => (transaction_index, LOG_EVENT, log_index),
            Self::Trace {
                transaction_index,
                trace_index,
            }
            | Self::Transfer {
                transaction_index,
                trace_index,
            } => (transaction_index, TRACE_EVENT, trace_index),
            Self::Block => (MAX_COMPONENT, BLOCK_EVENT, 0),
        };
        let checkpoint = Checkpoint {
            block_timestamp: block.timestamp,
            chain_id: block.chain_id,
            block_number: block.number,
            transaction_index,
            event_type,
            event_index,
        };
        checkpoint.encode()?;
        Ok(checkpoint)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockInterval {
    start: u64,
    end: Option<u64>,
    interval: u64,
}

impl BlockInterval {
    pub fn new(start: u64, end: Option<u64>, interval: u64) -> Result<Self, &'static str> {
        if interval == 0 {
            return Err("block interval must be nonzero");
        }
        if end.is_some_and(|end| end < start) {
            return Err("end block must not precede start block");
        }
        Ok(Self {
            start,
            end,
            interval,
        })
    }

    pub fn matches(self, block_number: u64) -> bool {
        block_number >= self.start
            && self.end.is_none_or(|end| block_number <= end)
            && (block_number - self.start).is_multiple_of(self.interval)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DuplicateHandler(pub String);

impl fmt::Display for DuplicateHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "indexing function already registered for '{}'", self.0)
    }
}

impl Error for DuplicateHandler {}

/// Registration storage; invoking and validating handler signatures is the caller's responsibility.
#[derive(Debug)]
pub struct HandlerRegistry<H> {
    handlers: HashMap<String, H>,
}

impl<H> Default for HandlerRegistry<H> {
    fn default() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }
}

impl<H> HandlerRegistry<H> {
    pub fn register(&mut self, name: String, handler: H) -> Result<(), DuplicateHandler> {
        match self.handlers.entry(name) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(handler);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                Err(DuplicateHandler(entry.key().clone()))
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&H> {
        self.handlers.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_positions_match_stele_order() {
        let block = Block {
            timestamp: 100,
            chain_id: 1,
            number: 10,
        };
        let transaction = EventPosition::Transaction {
            transaction_index: 0,
        }
        .checkpoint(block)
        .unwrap();
        let log = EventPosition::Log {
            transaction_index: 0,
            log_index: 3,
        }
        .checkpoint(block)
        .unwrap();
        let trace = EventPosition::Trace {
            transaction_index: 0,
            trace_index: 0,
        }
        .checkpoint(block)
        .unwrap();
        let transfer = EventPosition::Transfer {
            transaction_index: 0,
            trace_index: 0,
        }
        .checkpoint(block)
        .unwrap();
        let next_transaction = EventPosition::Transaction {
            transaction_index: 1,
        }
        .checkpoint(block)
        .unwrap();
        let block_event = EventPosition::Block.checkpoint(block).unwrap();
        assert!(
            transaction < log
                && log < trace
                && trace < next_transaction
                && next_transaction < block_event
        );
        assert_eq!(trace, transfer);
    }

    #[test]
    fn block_intervals_are_anchored_and_inclusive() {
        let filter = BlockInterval::new(1000, Some(1020), 10).unwrap();
        assert!(!filter.matches(999));
        assert!(filter.matches(1000));
        assert!(!filter.matches(1001));
        assert!(filter.matches(1010));
        assert!(filter.matches(1020));
        assert!(!filter.matches(1030));
        assert!(BlockInterval::new(0, None, 0).is_err());
        assert!(BlockInterval::new(10, Some(9), 1).is_err());
    }

    #[test]
    fn duplicate_registration_preserves_first_handler() {
        let mut registry = HandlerRegistry::default();
        registry.register("Token:Transfer".to_owned(), 1).unwrap();
        assert_eq!(
            registry.register("Token:Transfer".to_owned(), 2),
            Err(DuplicateHandler("Token:Transfer".to_owned()))
        );
        assert_eq!(registry.get("Token:Transfer"), Some(&1));
    }
}
