//! Inclusive block interval arithmetic and historical sync planning.

use std::num::NonZeroU64;

/// A nonempty, inclusive interval of block numbers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BlockRange {
    start: u64,
    end: u64,
}

impl BlockRange {
    /// Constructs a range, or returns `None` when `start > end`.
    pub fn new(start: u64, end: u64) -> Option<Self> {
        (start <= end).then_some(Self { start, end })
    }

    /// First included block.
    pub fn start(self) -> u64 {
        self.start
    }
    /// Last included block.
    pub fn end(self) -> u64 {
        self.end
    }
    /// Number of blocks, including the full `u64` domain.
    pub fn len(self) -> u128 {
        u128::from(self.end) - u128::from(self.start) + 1
    }
    /// Block ranges are never empty.
    pub fn is_empty(self) -> bool {
        false
    }

    /// Splits into consecutive inclusive chunks no larger than `size`.
    pub fn chunks(self, size: NonZeroU64) -> Chunks {
        Chunks {
            next: Some(self.start),
            end: self.end,
            size,
        }
    }
}

/// Lazy chunk iterator, including for very large ranges.
pub struct Chunks {
    next: Option<u64>,
    end: u64,
    size: NonZeroU64,
}

impl Iterator for Chunks {
    type Item = BlockRange;
    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next?;
        let end = start.saturating_add(self.size.get() - 1).min(self.end);
        self.next = if end == self.end { None } else { Some(end + 1) };
        Some(BlockRange { start, end })
    }
}

/// Sorted, disjoint, nonadjacent inclusive ranges.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockRangeSet(Vec<BlockRange>);

impl BlockRangeSet {
    /// Normalizes arbitrary ranges, merging overlap and adjacency.
    pub fn new(mut ranges: Vec<BlockRange>) -> Self {
        ranges.sort_unstable();
        let mut result: Vec<BlockRange> = Vec::new();
        for range in ranges {
            if let Some(last) = result.last_mut() {
                if range.start <= last.end.saturating_add(1) {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            result.push(range);
        }
        Self(result)
    }

    /// Returns normalized ranges.
    pub fn ranges(&self) -> &[BlockRange] {
        &self.0
    }
    /// The exact count of blocks in the set.
    pub fn len(&self) -> u128 {
        self.0.iter().map(|r| r.len()).sum()
    }
    /// Whether this set contains no blocks.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Union of two sets.
    pub fn union(&self, other: &Self) -> Self {
        Self::new(self.0.iter().chain(&other.0).copied().collect())
    }
    /// Intersection of two sets.
    pub fn intersection(&self, other: &Self) -> Self {
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < self.0.len() && j < other.0.len() {
            let (a, b) = (self.0[i], other.0[j]);
            if let Some(r) = BlockRange::new(a.start.max(b.start), a.end.min(b.end)) {
                out.push(r);
            }
            if a.end < b.end {
                i += 1
            } else {
                j += 1
            }
        }
        Self::new(out)
    }
    /// Difference `self - other`.
    pub fn difference(&self, other: &Self) -> Self {
        let mut out = Vec::new();
        let mut j = 0;
        for a in &self.0 {
            let mut cursor = Some(a.start);
            while j < other.0.len() && other.0[j].end < a.start {
                j += 1;
            }
            let mut k = j;
            while k < other.0.len() && other.0[k].start <= a.end {
                let b = other.0[k];
                let Some(start) = cursor else { break };
                if b.start > start {
                    out.push(BlockRange {
                        start,
                        end: b.start - 1,
                    });
                }
                if b.end >= a.end {
                    cursor = None;
                    break;
                }
                cursor = Some(start.max(b.end + 1));
                k += 1;
            }
            if let Some(start) = cursor {
                out.push(BlockRange { start, end: a.end });
            }
        }
        Self(out)
    }
    /// Removes all blocks after the given ancestor, including at the `u64` boundary.
    pub fn retain_through(&self, ancestor: u64) -> Self {
        self.intersection(&Self::new(vec![BlockRange {
            start: 0,
            end: ancestor,
        }]))
    }
}

/// Cached coverage of all fragments belonging to a filter or factory.
#[derive(Clone, Debug)]
pub struct Coverage {
    /// Applicable block bounds (intersected with the requested interval).
    pub bounds: BlockRange,
    /// One set per fragment. An empty list means no cached coverage.
    pub fragments: Vec<BlockRangeSet>,
}

impl Coverage {
    fn complete(&self) -> BlockRangeSet {
        let mut fragments = self.fragments.iter();
        let Some(first) = fragments.next() else {
            return BlockRangeSet::default();
        };
        fragments.fold(first.clone(), |acc, set| acc.intersection(set))
    }
}

/// One filter and the factories on which its address discovery depends.
#[derive(Clone, Debug)]
pub struct FilterCoverage {
    /// Coverage of the filter's fragments.
    pub filter: Coverage,
    /// Coverage of each factory's fragments.
    pub factories: Vec<Coverage>,
}

/// Computes the union of missing intervals using the factory invalidation rule.
///
/// Once a factory has a gap, filter coverage at and after the earliest missing
/// factory block is ignored, even when the factory's own bounds end earlier.
pub fn required_intervals(request: BlockRange, filters: &[FilterCoverage]) -> BlockRangeSet {
    let request_set = BlockRangeSet::new(vec![request]);
    let mut required = BlockRangeSet::default();
    for filter in filters {
        let mut factory_missing = BlockRangeSet::default();
        for factory in &filter.factories {
            let bounds = request_set.intersection(&BlockRangeSet::new(vec![factory.bounds]));
            factory_missing = factory_missing.union(&bounds.difference(&factory.complete()));
        }
        let mut cached = filter.filter.complete();
        if let Some(first) = factory_missing.ranges().first() {
            cached = cached.difference(&BlockRangeSet::new(vec![BlockRange {
                start: first.start,
                end: request.end,
            }]));
        }
        let bounds = request_set.intersection(&BlockRangeSet::new(vec![filter.filter.bounds]));
        required = required
            .union(&bounds.difference(&cached))
            .union(&factory_missing);
    }
    required
}
