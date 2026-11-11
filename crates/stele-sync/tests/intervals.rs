use std::num::NonZeroU64;
use stele_sync::interval::{
    required_intervals, BlockRange, BlockRangeSet, Coverage, FilterCoverage,
};

fn range(a: u64, b: u64) -> BlockRange {
    BlockRange::new(a, b).unwrap()
}
fn set(ranges: &[(u64, u64)]) -> BlockRangeSet {
    BlockRangeSet::new(ranges.iter().map(|&(a, b)| range(a, b)).collect())
}
fn mask(set: &BlockRangeSet) -> u16 {
    (0..8)
        .filter(|n| {
            set.ranges()
                .iter()
                .any(|r| r.start() <= *n && *n <= r.end())
        })
        .fold(0, |mask, n| mask | (1 << n))
}
fn from_mask(mask: u16) -> BlockRangeSet {
    BlockRangeSet::new(
        (0..8)
            .filter(|n| mask & (1 << n) != 0)
            .map(|n| range(n, n))
            .collect(),
    )
}

#[test]
fn exhaustive_small_set_algebra() {
    for a in 0..256 {
        for b in 0..256 {
            let (left, right) = (from_mask(a), from_mask(b));
            assert_eq!(mask(&left.union(&right)), a | b);
            assert_eq!(mask(&left.intersection(&right)), a & b);
            assert_eq!(mask(&left.difference(&right)), a & !b & 255);
        }
    }
}

#[test]
fn boundaries_and_chunks() {
    assert!(BlockRange::new(1, 0).is_none());
    let full = range(0, u64::MAX);
    assert_eq!(full.len(), u128::from(u64::MAX) + 1);
    assert_eq!(
        set(&[(u64::MAX - 1, u64::MAX), (0, 0)])
            .difference(&set(&[(u64::MAX, u64::MAX)]))
            .ranges(),
        &[range(0, 0), range(u64::MAX - 1, u64::MAX - 1)]
    );
    assert_eq!(
        set(&[(0, 2), (3, 5), (u64::MAX, u64::MAX)]).ranges(),
        &[range(0, 5), range(u64::MAX, u64::MAX)]
    );
    assert_eq!(
        range(u64::MAX - 4, u64::MAX)
            .chunks(NonZeroU64::new(2).unwrap())
            .collect::<Vec<_>>(),
        vec![
            range(u64::MAX - 4, u64::MAX - 3),
            range(u64::MAX - 2, u64::MAX - 1),
            range(u64::MAX, u64::MAX)
        ]
    );
    assert_eq!(set(&[(0, u64::MAX)]).retain_through(10), set(&[(0, 10)]));
}

#[test]
fn planner_intersects_fragments_and_invalidates_after_factory_gap() {
    let filter = Coverage {
        bounds: range(10, 20),
        fragments: vec![set(&[(10, 20)]), set(&[(10, 18)])],
    };
    let mut factory = Coverage {
        bounds: range(2, 5),
        fragments: vec![set(&[(2, 5)])],
    };
    let plan = |factory: Coverage| {
        required_intervals(
            range(2, 20),
            &[FilterCoverage {
                filter: filter.clone(),
                factories: vec![factory],
            }],
        )
    };
    assert_eq!(plan(factory.clone()), set(&[(19, 20)]));
    factory.fragments = vec![set(&[(2, 4)])];
    assert_eq!(plan(factory), set(&[(5, 5), (10, 20)]));
    assert_eq!(
        required_intervals(
            range(1, 3),
            &[FilterCoverage {
                filter: Coverage {
                    bounds: range(1, 3),
                    fragments: vec![]
                },
                factories: vec![]
            }]
        ),
        set(&[(1, 3)])
    );
}
