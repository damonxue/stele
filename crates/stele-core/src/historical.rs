//! Ordered merging of independently fetched historical event batches.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
};
use stele_checkpoint::{Checkpoint, CheckpointError};

#[derive(Debug, PartialEq, Eq)]
pub struct HistoricalEvent<T> {
    pub checkpoint: Checkpoint,
    pub value: T,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HistoricalBatch<T> {
    /// All later batches for this chain must start after this checkpoint.
    pub checkpoint: Checkpoint,
    pub events: Vec<HistoricalEvent<T>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MergeError {
    NoChains,
    DuplicateChain(u64),
    UnknownChain(u64),
    ChainFinished(u64),
    InvalidCheckpoint(CheckpointError),
    NonAdvancingBatch(u64),
    EventOnWrongChain(u64),
    EventOutOfOrder(u64),
    EventBeyondBatch(u64),
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoChains => write!(f, "at least one chain is required"),
            Self::DuplicateChain(chain) => write!(f, "chain {chain} was registered more than once"),
            Self::UnknownChain(chain) => write!(f, "chain {chain} was not registered"),
            Self::ChainFinished(chain) => write!(f, "chain {chain} has already finished"),
            Self::InvalidCheckpoint(error) => write!(f, "invalid checkpoint: {error}"),
            Self::NonAdvancingBatch(chain) => {
                write!(f, "chain {chain} batch checkpoint did not advance")
            }
            Self::EventOnWrongChain(chain) => write!(f, "event does not belong to chain {chain}"),
            Self::EventOutOfOrder(chain) => write!(f, "chain {chain} events are out of order"),
            Self::EventBeyondBatch(chain) => {
                write!(f, "chain {chain} event is beyond its batch checkpoint")
            }
        }
    }
}

impl Error for MergeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidCheckpoint(error) => Some(error),
            _ => None,
        }
    }
}

struct ChainState<T> {
    checkpoint: Option<Checkpoint>,
    finished: bool,
    pending: VecDeque<HistoricalEvent<T>>,
}

/// Merges backfill batches using the minimum completed checkpoint across
/// unfinished chains. Fetching, crash recovery and live reorg handling remain
/// the caller's responsibility.
pub struct HistoricalMerge<T> {
    chains: BTreeMap<u64, ChainState<T>>,
}

impl<T> HistoricalMerge<T> {
    pub fn new(chains: impl IntoIterator<Item = u64>) -> Result<Self, MergeError> {
        let mut states = BTreeMap::new();
        for chain in chains {
            Checkpoint {
                chain_id: chain,
                ..Checkpoint::ZERO
            }
            .encode()
            .map_err(MergeError::InvalidCheckpoint)?;
            if states
                .insert(
                    chain,
                    ChainState {
                        checkpoint: None,
                        finished: false,
                        pending: VecDeque::new(),
                    },
                )
                .is_some()
            {
                return Err(MergeError::DuplicateChain(chain));
            }
        }
        if states.is_empty() {
            return Err(MergeError::NoChains);
        }
        Ok(Self { chains: states })
    }

    pub fn push(&mut self, chain: u64, batch: HistoricalBatch<T>) -> Result<(), MergeError> {
        let state = self
            .chains
            .get_mut(&chain)
            .ok_or(MergeError::UnknownChain(chain))?;
        if state.finished {
            return Err(MergeError::ChainFinished(chain));
        }
        batch
            .checkpoint
            .encode()
            .map_err(MergeError::InvalidCheckpoint)?;
        if state
            .checkpoint
            .is_some_and(|checkpoint| batch.checkpoint <= checkpoint)
        {
            return Err(MergeError::NonAdvancingBatch(chain));
        }
        let mut previous = None;
        for event in &batch.events {
            event
                .checkpoint
                .encode()
                .map_err(MergeError::InvalidCheckpoint)?;
            if event.checkpoint.chain_id != chain {
                return Err(MergeError::EventOnWrongChain(chain));
            }
            if event.checkpoint > batch.checkpoint {
                return Err(MergeError::EventBeyondBatch(chain));
            }
            // Equal checkpoints in one batch are valid, for example when
            // multiple callbacks match the same chain event.
            if state
                .checkpoint
                .is_some_and(|checkpoint| event.checkpoint <= checkpoint)
                || previous.is_some_and(|checkpoint| event.checkpoint < checkpoint)
            {
                return Err(MergeError::EventOutOfOrder(chain));
            }
            previous = Some(event.checkpoint);
        }
        state.checkpoint = Some(batch.checkpoint);
        state.pending.extend(batch.events);
        Ok(())
    }

    pub fn finish(&mut self, chain: u64) -> Result<(), MergeError> {
        let state = self
            .chains
            .get_mut(&chain)
            .ok_or(MergeError::UnknownChain(chain))?;
        if state.finished {
            return Err(MergeError::ChainFinished(chain));
        }
        state.finished = true;
        Ok(())
    }

    /// Return the events proven complete by every unfinished chain.
    pub fn drain_ready(&mut self) -> Vec<HistoricalEvent<T>> {
        let mut minimum = None;
        for state in self.chains.values().filter(|state| !state.finished) {
            let Some(checkpoint) = state.checkpoint else {
                return Vec::new();
            };
            minimum =
                Some(minimum.map_or(checkpoint, |current: Checkpoint| current.min(checkpoint)));
        }

        let mut ready = Vec::new();
        for state in self.chains.values_mut() {
            while state
                .pending
                .front()
                .is_some_and(|event| minimum.is_none_or(|bound| event.checkpoint <= bound))
            {
                ready.push(state.pending.pop_front().expect("front was present"));
            }
        }
        ready.sort_by_key(|event| event.checkpoint);
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(timestamp: u64, chain: u64, index: u64) -> Checkpoint {
        Checkpoint {
            block_timestamp: timestamp,
            chain_id: chain,
            block_number: timestamp,
            transaction_index: index,
            ..Checkpoint::ZERO
        }
    }

    fn event(timestamp: u64, chain: u64, index: u64, value: u8) -> HistoricalEvent<u8> {
        HistoricalEvent {
            checkpoint: cp(timestamp, chain, index),
            value,
        }
    }

    fn batch(checkpoint: Checkpoint, events: Vec<HistoricalEvent<u8>>) -> HistoricalBatch<u8> {
        HistoricalBatch { checkpoint, events }
    }

    #[test]
    fn waits_for_every_chain_and_releases_only_safe_events() {
        let mut merge = HistoricalMerge::new([1, 2]).unwrap();
        merge
            .push(
                1,
                batch(cp(20, 1, 9), vec![event(10, 1, 0, 1), event(20, 1, 0, 3)]),
            )
            .unwrap();
        assert!(merge.drain_ready().is_empty());
        merge
            .push(2, batch(cp(15, 2, 9), vec![event(12, 2, 0, 2)]))
            .unwrap();
        assert_eq!(
            merge
                .drain_ready()
                .into_iter()
                .map(|event| event.value)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        merge
            .push(2, batch(cp(25, 2, 9), vec![event(21, 2, 0, 4)]))
            .unwrap();
        assert_eq!(
            merge
                .drain_ready()
                .into_iter()
                .map(|event| event.value)
                .collect::<Vec<_>>(),
            [3]
        );
        merge.finish(1).unwrap();
        assert_eq!(
            merge
                .drain_ready()
                .into_iter()
                .map(|event| event.value)
                .collect::<Vec<_>>(),
            [4]
        );
    }

    #[test]
    fn empty_batches_and_finished_chains_advance_the_watermark() {
        let mut merge = HistoricalMerge::new([1, 2]).unwrap();
        merge
            .push(1, batch(cp(10, 1, 9), vec![event(10, 1, 0, 1)]))
            .unwrap();
        merge.push(2, batch(cp(9, 2, 9), vec![])).unwrap();
        assert!(merge.drain_ready().is_empty());
        merge.finish(2).unwrap();
        assert_eq!(merge.drain_ready()[0].value, 1);
        merge.finish(1).unwrap();
        assert!(merge.drain_ready().is_empty());
    }

    #[test]
    fn equal_event_checkpoints_preserve_callback_order() {
        let mut merge = HistoricalMerge::new([1]).unwrap();
        merge
            .push(
                1,
                batch(cp(10, 1, 9), vec![event(10, 1, 0, 1), event(10, 1, 0, 2)]),
            )
            .unwrap();
        assert_eq!(
            merge
                .drain_ready()
                .into_iter()
                .map(|event| event.value)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn rejects_bad_batches_without_mutating_progress() {
        let mut merge = HistoricalMerge::new([1]).unwrap();
        merge.push(1, batch(cp(10, 1, 9), vec![])).unwrap();
        assert_eq!(
            merge.push(1, batch(cp(10, 1, 9), vec![])),
            Err(MergeError::NonAdvancingBatch(1))
        );
        assert_eq!(
            merge.push(1, batch(cp(11, 1, 9), vec![event(11, 2, 0, 1)])),
            Err(MergeError::EventOnWrongChain(1))
        );
        assert_eq!(
            merge.push(1, batch(cp(11, 1, 9), vec![event(12, 1, 0, 1)])),
            Err(MergeError::EventBeyondBatch(1))
        );
        assert_eq!(
            merge.push(
                1,
                batch(cp(11, 1, 9), vec![event(11, 1, 1, 1), event(11, 1, 0, 2)])
            ),
            Err(MergeError::EventOutOfOrder(1))
        );
        assert_eq!(
            merge.push(1, batch(cp(11, 1, 9), vec![event(10, 1, 9, 1)])),
            Err(MergeError::EventOutOfOrder(1))
        );
        merge
            .push(1, batch(cp(11, 1, 9), vec![event(11, 1, 0, 3)]))
            .unwrap();
        assert_eq!(merge.drain_ready()[0].value, 3);
    }

    #[test]
    fn rejects_invalid_chain_lifecycle_and_checkpoints() {
        assert!(matches!(
            HistoricalMerge::<u8>::new([]),
            Err(MergeError::NoChains)
        ));
        assert!(matches!(
            HistoricalMerge::<u8>::new([u64::MAX]),
            Err(MergeError::InvalidCheckpoint(_))
        ));
        assert!(matches!(
            HistoricalMerge::<u8>::new([1, 1]),
            Err(MergeError::DuplicateChain(1))
        ));
        let mut merge = HistoricalMerge::<u8>::new([1]).unwrap();
        assert_eq!(merge.finish(2), Err(MergeError::UnknownChain(2)));
        assert!(matches!(
            merge.push(
                1,
                batch(
                    Checkpoint {
                        event_type: 10,
                        ..Checkpoint::ZERO
                    },
                    vec![]
                )
            ),
            Err(MergeError::InvalidCheckpoint(_))
        ));
        merge.finish(1).unwrap();
        assert_eq!(merge.finish(1), Err(MergeError::ChainFinished(1)));
        assert_eq!(
            merge.push(1, batch(cp(10, 1, 9), vec![])),
            Err(MergeError::ChainFinished(1))
        );
    }

    #[test]
    fn staggered_batches_match_a_globally_sorted_reference() {
        let mut merge = HistoricalMerge::new([1, 2, 3]).unwrap();
        let mut actual = Vec::new();
        let mut expected = Vec::new();

        for timestamp in 1..=8 {
            for chain in [3, 1, 2] {
                let events = if (timestamp + chain) % 3 == 0 {
                    Vec::new()
                } else {
                    vec![
                        event(timestamp, chain, 0, (timestamp * 10 + chain) as u8),
                        event(timestamp, chain, 0, (timestamp * 10 + chain) as u8),
                    ]
                };
                expected.extend(events.iter().map(|event| (event.checkpoint, event.value)));
                merge
                    .push(chain, batch(cp(timestamp, chain, 9), events))
                    .unwrap();
                actual.extend(
                    merge
                        .drain_ready()
                        .into_iter()
                        .map(|event| (event.checkpoint, event.value)),
                );
            }
        }
        for chain in [2, 1, 3] {
            merge.finish(chain).unwrap();
            actual.extend(
                merge
                    .drain_ready()
                    .into_iter()
                    .map(|event| (event.checkpoint, event.value)),
            );
        }
        expected.sort_by_key(|event| event.0);
        assert_eq!(actual, expected);
    }
}
