//! Checkpoint boundaries for the omnichain runtime.

use std::{error::Error, fmt};
use stele_checkpoint::{Checkpoint, CheckpointError};

use crate::Block;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainProgress {
    pub chain_id: u64,
    pub start: Block,
    pub end: Option<Block>,
    pub current: Option<Block>,
    pub finalized: Block,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressTag {
    Start,
    End,
    Current,
    Finalized,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProgressError {
    NoChains,
    MissingCurrent(u64),
    WrongChain(u64),
    InvalidCheckpoint(CheckpointError),
}

impl fmt::Display for ProgressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoChains => write!(f, "at least one chain is required"),
            Self::MissingCurrent(chain) => write!(f, "chain {chain} has no current block"),
            Self::WrongChain(chain) => write!(
                f,
                "progress contains a block from another chain than {chain}"
            ),
            Self::InvalidCheckpoint(error) => write!(f, "invalid checkpoint: {error}"),
        }
    }
}

impl Error for ProgressError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidCheckpoint(error) => Some(error),
            _ => None,
        }
    }
}

impl ChainProgress {
    fn is_end(&self) -> bool {
        self.end
            .zip(self.current)
            .is_some_and(|(end, current)| current.number >= end.number)
    }

    fn checkpoint(&self, tag: ProgressTag) -> Result<Option<Checkpoint>, ProgressError> {
        let block = match tag {
            ProgressTag::Start => Some(self.start),
            ProgressTag::End => self.end,
            ProgressTag::Current => Some(
                self.current
                    .ok_or(ProgressError::MissingCurrent(self.chain_id))?,
            ),
            ProgressTag::Finalized => Some(self.finalized),
        };
        let Some(block) = block else {
            return Ok(None);
        };
        if block.chain_id != self.chain_id {
            return Err(ProgressError::WrongChain(self.chain_id));
        }
        let checkpoint = if tag == ProgressTag::Start {
            Checkpoint::block_start(block.timestamp, self.chain_id, block.number)
        } else {
            Checkpoint::block_end(block.timestamp, self.chain_id, block.number)
        };
        checkpoint
            .encode()
            .map_err(ProgressError::InvalidCheckpoint)?;
        Ok(Some(checkpoint))
    }
}

/// Return the omnichain boundary. An unbounded end returns `None`;
/// current progress must be available for every chain.
pub fn omnichain_checkpoint(
    chains: &[ChainProgress],
    tag: ProgressTag,
) -> Result<Option<Checkpoint>, ProgressError> {
    if chains.is_empty() {
        return Err(ProgressError::NoChains);
    }

    let mut checkpoints = Vec::with_capacity(chains.len());
    for chain in chains {
        checkpoints.push(chain.checkpoint(tag)?);
    }

    match tag {
        ProgressTag::End => {
            if checkpoints.iter().any(Option::is_none) {
                Ok(None)
            } else {
                Ok(checkpoints.into_iter().flatten().max())
            }
        }
        ProgressTag::Current if chains.iter().all(ChainProgress::is_end) => {
            Ok(checkpoints.into_iter().flatten().max())
        }
        ProgressTag::Current => Ok(chains
            .iter()
            .zip(checkpoints)
            .filter(|(chain, _)| !chain.is_end())
            .filter_map(|(_, checkpoint)| checkpoint)
            .min()),
        ProgressTag::Start | ProgressTag::Finalized => Ok(checkpoints.into_iter().flatten().min()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_checkpoint::MAX_COMPONENT;

    fn block(chain_id: u64, number: u64, timestamp: u64) -> Block {
        Block {
            chain_id,
            number,
            timestamp,
        }
    }

    fn progress(chain_id: u64, timestamp: u64) -> ChainProgress {
        ChainProgress {
            chain_id,
            start: block(chain_id, 1, timestamp),
            end: Some(block(chain_id, 10, timestamp + 10)),
            current: Some(block(chain_id, 5, timestamp + 5)),
            finalized: block(chain_id, 4, timestamp + 4),
        }
    }

    #[test]
    fn starts_and_finalized_use_minimum_with_inclusive_boundaries() {
        let chains = [progress(1, 10), progress(2, 20)];
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::Start).unwrap(),
            Some(Checkpoint::block_start(10, 1, 1))
        );
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::Finalized).unwrap(),
            Some(Checkpoint::block_end(14, 1, 4))
        );
    }

    #[test]
    fn end_is_unbounded_if_any_chain_has_no_end_otherwise_maximum() {
        let mut chains = [progress(1, 10), progress(2, 20)];
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::End).unwrap(),
            Some(Checkpoint::block_end(30, 2, 10))
        );
        chains[0].end = None;
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::End).unwrap(),
            None
        );
    }

    #[test]
    fn completed_chains_do_not_hold_back_current_progress() {
        let mut chains = [progress(1, 10), progress(2, 20)];
        chains[0].current = Some(block(1, 10, 20));
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::Current).unwrap(),
            Some(Checkpoint::block_end(25, 2, 5))
        );
        chains[1].current = Some(block(2, 10, 30));
        assert_eq!(
            omnichain_checkpoint(&chains, ProgressTag::Current).unwrap(),
            Some(Checkpoint::block_end(30, 2, 10))
        );
    }

    #[test]
    fn rejects_missing_or_invalid_inputs() {
        assert_eq!(
            omnichain_checkpoint(&[], ProgressTag::Start),
            Err(ProgressError::NoChains)
        );
        let mut chain = progress(1, 10);
        chain.current = None;
        assert_eq!(
            omnichain_checkpoint(&[chain], ProgressTag::Current),
            Err(ProgressError::MissingCurrent(1))
        );
        chain.start.chain_id = 2;
        assert_eq!(
            omnichain_checkpoint(&[chain], ProgressTag::Start),
            Err(ProgressError::WrongChain(1))
        );
        chain.start.chain_id = 1;
        chain.start.number = MAX_COMPONENT + 1;
        assert!(matches!(
            omnichain_checkpoint(&[chain], ProgressTag::Start),
            Err(ProgressError::InvalidCheckpoint(_))
        ));
    }
}
