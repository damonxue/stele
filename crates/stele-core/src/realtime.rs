//! Local realtime chain state: reorg reconciliation and timestamp-based
//! finality.
//!
//! The caller fetches block headers. This module owns the unfinalized window
//! and decides whether an incoming header extends the local chain, reorgs it or
//! must wait for missing blocks. It does not fetch event data, persist state or
//! invoke handlers.
//!
//! Block timestamps drive finality through `reorgWindow` in seconds.
//! A block is finalized once a later block's timestamp is at least
//! `reorg_window_seconds` ahead of it.

use std::error::Error;
use std::fmt;

use crate::events::{B256, BlockRef};

/// Maximum accepted `reorgWindow` value.
pub const MAX_REORG_WINDOW_SECONDS: u64 = 600;

/// What the caller must do after feeding a header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RealtimeUpdate {
    /// A new tip was appended. The caller should index the block's events.
    Block { block: BlockRef },
    /// The local chain was rolled back to `common_ancestor`. The caller must
    /// revert indexed state after that block and feed the new header again.
    Reorg {
        common_ancestor: BlockRef,
        /// Removed tips, ordered from oldest to newest.
        reorged: Vec<BlockRef>,
    },
    /// Blocks at or before `block` can no longer be reorged out and their
    /// recorded data may be finalized.
    Finalize { block: BlockRef },
}

/// Failures while advancing the local chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdvanceError<E> {
    /// The header is more than one block ahead. Fetch the inclusive range and
    /// feed the blocks in order before retrying the original header.
    MissingBlocks { from: u64, to: u64 },
    /// Fetching a parent failed; local state was not modified.
    Fetch(E),
    /// The fetched block's hash does not match the requested hash.
    InvalidParent { requested: B256 },
    /// The reorg reaches the finalized block, so it cannot be recovered by
    /// walking backwards. Local state was not modified.
    UnrecoverableReorg { finalized: BlockRef },
}

impl<E: fmt::Display> fmt::Display for AdvanceError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBlocks { from, to } => {
                write!(f, "missing blocks {from} through {to}")
            }
            Self::Fetch(error) => write!(f, "failed to fetch parent block: {error}"),
            Self::InvalidParent { requested } => {
                write!(f, "fetched parent block does not match hash {requested}")
            }
            Self::UnrecoverableReorg { finalized } => write!(
                f,
                "reorg reaches the finalized block {} ({:02x?})",
                finalized.number, finalized.hash
            ),
        }
    }
}

impl<E: Error + 'static> Error for AdvanceError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fetch(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitError {
    ReorgWindowTooLarge(u64),
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReorgWindowTooLarge(window) => {
                write!(
                    f,
                    "reorg window {window}s exceeds {MAX_REORG_WINDOW_SECONDS}s"
                )
            }
        }
    }
}

impl Error for InitError {}

/// Local view of one chain: a finalized block and a contiguous unfinalized
/// suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalChain {
    finalized: BlockRef,
    unfinalized: Vec<BlockRef>,
    reorg_window_seconds: u64,
}

impl LocalChain {
    /// Restores a local chain. `unfinalized` must be ordered, contiguous and
    /// descend from `finalized`; this is not re-validated because it comes from
    /// persisted progress.
    pub fn new(
        finalized: BlockRef,
        unfinalized: Vec<BlockRef>,
        reorg_window_seconds: u64,
    ) -> Result<Self, InitError> {
        if reorg_window_seconds > MAX_REORG_WINDOW_SECONDS {
            return Err(InitError::ReorgWindowTooLarge(reorg_window_seconds));
        }
        Ok(Self {
            finalized,
            unfinalized,
            reorg_window_seconds,
        })
    }

    pub fn finalized(&self) -> &BlockRef {
        &self.finalized
    }

    pub fn unfinalized(&self) -> &[BlockRef] {
        &self.unfinalized
    }

    /// Latest locally known block, finalized or not.
    pub fn latest(&self) -> &BlockRef {
        self.unfinalized.last().unwrap_or(&self.finalized)
    }

    /// Feeds one header and returns the resulting updates in application order.
    ///
    /// A duplicate tip is a no-op and returns no updates. [`AdvanceError`]
    /// leaves the local chain unchanged, so the header can be retried after the
    /// caller resolves the error.
    pub fn advance<E>(
        &mut self,
        block: BlockRef,
        mut fetch_parent: impl FnMut(B256) -> Result<BlockRef, E>,
    ) -> Result<Vec<RealtimeUpdate>, AdvanceError<E>> {
        let latest = *self.latest();

        // The same tip was already handled.
        if latest.hash == block.hash {
            return Ok(Vec::new());
        }

        // A non-advancing height necessarily replaces history.
        if latest.number >= block.number {
            return self.reconcile_reorg(block, &mut fetch_parent);
        }

        if latest.number + 1 < block.number {
            return Err(AdvanceError::MissingBlocks {
                from: latest.number + 1,
                to: block.number - 1,
            });
        }

        // Same height + 1 but a different parent means the parent was reorged.
        if block.parent_hash != latest.hash {
            return self.reconcile_reorg(block, &mut fetch_parent);
        }

        self.unfinalized.push(block);
        let mut updates = vec![RealtimeUpdate::Block { block }];

        if let Some(finalized) = self.advance_finality(block) {
            updates.push(RealtimeUpdate::Finalize { block: finalized });
        }

        Ok(updates)
    }

    /// Walks the remote parent chain until it meets an unfinalized local block.
    ///
    /// Work happens on a copy so a fetch failure or an unrecoverable reorg
    /// leaves the local chain untouched.
    fn reconcile_reorg<E>(
        &mut self,
        block: BlockRef,
        fetch_parent: &mut impl FnMut(B256) -> Result<BlockRef, E>,
    ) -> Result<Vec<RealtimeUpdate>, AdvanceError<E>> {
        let mut local = self.unfinalized.clone();
        // Every local block at or above the
        // incoming height.
        let mut reorged: Vec<BlockRef> = local
            .iter()
            .copied()
            .filter(|candidate| candidate.number >= block.number)
            .collect();
        local.retain(|candidate| candidate.number < block.number);

        let mut remote = block;
        loop {
            let parent = local.last().copied().unwrap_or(self.finalized);
            if parent.hash == remote.parent_hash {
                break;
            }
            let Some(tip) = local.pop() else {
                return Err(AdvanceError::UnrecoverableReorg {
                    finalized: self.finalized,
                });
            };
            let requested = remote.parent_hash;
            let fetched = fetch_parent(requested).map_err(AdvanceError::Fetch)?;
            if fetched.hash != requested {
                return Err(AdvanceError::InvalidParent { requested });
            }
            reorged.insert(0, tip);
            remote = fetched;
        }

        let common_ancestor = *local.last().unwrap_or(&self.finalized);
        self.unfinalized = local;
        Ok(vec![RealtimeUpdate::Reorg {
            common_ancestor,
            reorged,
        }])
    }

    /// Advances finality using the timestamp window and returns the new
    /// finalized block, if it moved.
    fn advance_finality(&mut self, latest: BlockRef) -> Option<BlockRef> {
        let oldest = *self.unfinalized.first()?;
        if latest.timestamp.saturating_sub(oldest.timestamp) < self.reorg_window_seconds {
            return None;
        }

        let mut pending = 0;
        for (index, candidate) in self.unfinalized.iter().enumerate() {
            if latest.timestamp.saturating_sub(candidate.timestamp) >= self.reorg_window_seconds {
                pending = index;
            } else {
                break;
            }
        }
        let finalized = self.unfinalized[pending];
        self.unfinalized.drain(..=pending);
        self.finalized = finalized;
        Some(finalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64, timestamp: u64, hash: u8, parent: u8) -> BlockRef {
        BlockRef {
            number,
            timestamp,
            hash: B256([hash; 32]),
            parent_hash: B256([parent; 32]),
        }
    }

    fn genesis() -> BlockRef {
        block(0, 1_000, 0, 0)
    }

    #[test]
    fn accepts_contiguous_blocks_and_finalizes_outside_the_window() {
        let mut chain = LocalChain::new(genesis(), Vec::new(), 100).unwrap();
        let blocks = [
            block(1, 1_100, 1, 0),
            block(2, 1_150, 2, 1),
            block(3, 1_200, 3, 2),
            block(4, 1_300, 4, 3),
        ];

        // With a 100s window, block 1 is final once block 3 arrives.
        assert_eq!(
            chain
                .advance(blocks[0], |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [RealtimeUpdate::Block { block: blocks[0] }]
        );
        assert_eq!(
            chain
                .advance(blocks[1], |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [RealtimeUpdate::Block { block: blocks[1] }]
        );
        assert_eq!(
            chain
                .advance(blocks[2], |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [
                RealtimeUpdate::Block { block: blocks[2] },
                RealtimeUpdate::Finalize { block: blocks[0] }
            ]
        );
        assert_eq!(chain.finalized(), &blocks[0]);
        assert_eq!(chain.unfinalized(), &blocks[1..=2]);

        assert_eq!(
            chain
                .advance(blocks[3], |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [
                RealtimeUpdate::Block { block: blocks[3] },
                RealtimeUpdate::Finalize { block: blocks[2] }
            ]
        );
        assert_eq!(chain.finalized(), &blocks[2]);
        assert_eq!(chain.unfinalized(), &[blocks[3]]);
    }

    #[test]
    fn duplicates_are_no_ops_and_gaps_are_reported() {
        let mut chain = LocalChain::new(genesis(), Vec::new(), 60).unwrap();
        let one = block(1, 1_100, 1, 0);
        chain
            .advance(one, |_| -> Result<BlockRef, ()> { unreachable!() })
            .unwrap();

        assert_eq!(
            chain
                .advance(one, |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            []
        );

        let three = block(3, 1_200, 3, 2);
        assert_eq!(
            chain.advance(three, |_| -> Result<BlockRef, ()> { unreachable!() }),
            Err(AdvanceError::MissingBlocks { from: 2, to: 2 })
        );
        assert_eq!(chain.unfinalized(), &[one]);
    }

    #[test]
    fn reorgs_the_tip_using_the_remote_parent_chain() {
        let one = block(1, 1_100, 1, 0);
        let two = block(2, 1_150, 2, 1);
        let three = block(3, 1_200, 3, 2);
        let mut chain = LocalChain::new(genesis(), vec![one, two, three], 60).unwrap();

        let three_prime = block(3, 1_205, 33, 2);
        let updates = chain
            .advance(three_prime, |_| -> Result<BlockRef, ()> {
                unreachable!("the parent is the local tip")
            })
            .unwrap();
        assert_eq!(
            updates,
            [RealtimeUpdate::Reorg {
                common_ancestor: two,
                reorged: vec![three],
            }]
        );
        assert_eq!(chain.unfinalized(), &[one, two]);

        // Feeding the replacement block again appends it; block 1 is now
        // outside the 60s window.
        assert_eq!(
            chain
                .advance(three_prime, |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [
                RealtimeUpdate::Block { block: three_prime },
                RealtimeUpdate::Finalize { block: one },
            ]
        );
        assert_eq!(chain.latest(), &three_prime);
        assert_eq!(chain.finalized(), &one);
    }

    #[test]
    fn walks_back_over_fetched_remote_ancestors() {
        let one = block(1, 1_100, 1, 0);
        let two = block(2, 1_150, 2, 1);
        let three = block(3, 1_200, 3, 2);
        let four = block(4, 1_250, 4, 3);
        let mut chain = LocalChain::new(genesis(), vec![one, two, three, four], 60).unwrap();

        // New chain: 5' <- 4' <- 3' <- 2 (the local block).
        let three_prime = block(3, 1_202, 33, 2);
        let four_prime = block(4, 1_252, 44, 33);
        let five_prime = block(5, 1_302, 55, 44);

        let updates = chain
            .advance(five_prime, |hash| -> Result<BlockRef, ()> {
                if hash == B256([44; 32]) {
                    Ok(four_prime)
                } else if hash == B256([33; 32]) {
                    Ok(three_prime)
                } else {
                    unreachable!("unexpected parent request")
                }
            })
            .unwrap();
        assert_eq!(
            updates,
            [RealtimeUpdate::Reorg {
                common_ancestor: two,
                reorged: vec![three, four],
            }]
        );
        assert_eq!(chain.unfinalized(), &[one, two]);
    }

    #[test]
    fn reorg_beyond_the_finalized_block_is_unrecoverable_and_non_mutating() {
        let one = block(1, 1_100, 1, 0);
        let two = block(2, 1_150, 2, 1);
        let mut chain = LocalChain::new(one, vec![two], 60).unwrap();
        let before = chain.clone();

        // The new block claims genesis 0 as parent but the local finalized
        // block is 1, so there is nothing left to walk back.
        let fork = block(2, 1_151, 22, 0);
        assert_eq!(
            chain.advance(fork, |_| -> Result<BlockRef, ()> {
                unreachable!("local chain is exhausted first")
            }),
            Err(AdvanceError::UnrecoverableReorg { finalized: one })
        );
        assert_eq!(chain, before);
    }

    #[test]
    fn fetch_failures_and_mismatched_parents_leave_state_unchanged() {
        let one = block(1, 1_100, 1, 0);
        let two = block(2, 1_150, 2, 1);
        let mut chain = LocalChain::new(genesis(), vec![one, two], 60).unwrap();
        let before = chain.clone();
        let fork = block(3, 1_200, 33, 22);

        assert_eq!(
            chain.advance(fork, |_| Err("rpc down")),
            Err(AdvanceError::Fetch("rpc down"))
        );
        assert_eq!(chain, before);

        assert_eq!(
            chain.advance(fork, |_| -> Result<BlockRef, &str> {
                Ok(block(2, 1_150, 99, 1))
            }),
            Err(AdvanceError::InvalidParent {
                requested: B256([22; 32])
            })
        );
        assert_eq!(chain, before);
    }

    #[test]
    fn zero_window_finalizes_the_new_tip() {
        let mut chain = LocalChain::new(genesis(), Vec::new(), 0).unwrap();
        let one = block(1, 1_100, 1, 0);
        assert_eq!(
            chain
                .advance(one, |_| -> Result<BlockRef, ()> { unreachable!() })
                .unwrap(),
            [
                RealtimeUpdate::Block { block: one },
                RealtimeUpdate::Finalize { block: one }
            ]
        );
        assert_eq!(chain.finalized(), &one);
        assert!(chain.unfinalized().is_empty());
    }

    #[test]
    fn rejects_reorg_windows_above_the_stele_limit() {
        assert_eq!(
            LocalChain::new(genesis(), Vec::new(), MAX_REORG_WINDOW_SECONDS + 1),
            Err(InitError::ReorgWindowTooLarge(MAX_REORG_WINDOW_SECONDS + 1))
        );
        assert!(LocalChain::new(genesis(), Vec::new(), MAX_REORG_WINDOW_SECONDS).is_ok());
    }
}
