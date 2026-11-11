//! Event matching primitives.
//!
//! Factory membership is block-granular, not transaction- or log-granular.
//! Trace callers must supply the *effective* outcome, including ancestor errors,
//! as errored trace subtrees are discarded before applying predicates.
//! Chain selection, RPC request merging, ABI decoding, and reorg management are
//! outside this module.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

pub use stele_types::{Address, CallType, Log, Selector, Topic, Trace, TraceOutcome};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockRange {
    pub from: Option<u64>,
    pub to: Option<u64>,
}

impl BlockRange {
    pub fn contains(self, block: u64) -> bool {
        self.from.is_none_or(|from| block >= from) && self.to.is_none_or(|to| block <= to)
    }
}

/// `AnyOf([])` matches nothing, while `Any` also matches a missing value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ValueFilter<T> {
    #[default]
    Any,
    One(T),
    AnyOf(Vec<T>),
}

impl<T: PartialEq> ValueFilter<T> {
    pub fn matches(&self, value: Option<&T>) -> bool {
        match self {
            Self::Any => true,
            Self::One(expected) => value == Some(expected),
            Self::AnyOf(expected) => value.is_some_and(|value| expected.contains(value)),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum AddressFilter {
    #[default]
    Any,
    One(Address),
    AnyOf(Vec<Address>),
    /// The ID identifies a registry entry, including an explicitly empty one.
    Factory(String),
}

/// Maps factory IDs to child addresses and their earliest creation block.
#[derive(Clone, Debug, Default)]
pub struct ChildAddresses {
    factories: HashMap<String, HashMap<Address, u64>>,
}

impl ChildAddresses {
    pub fn register_factory(&mut self, id: impl Into<String>) {
        self.factories.entry(id.into()).or_default();
    }

    pub fn insert(&mut self, id: &str, address: Address, block: u64) -> Result<(), MatchError> {
        let children = self
            .factories
            .get_mut(id)
            .ok_or_else(|| MatchError::UnknownFactory(id.to_owned()))?;
        children
            .entry(address)
            .and_modify(|existing| *existing = (*existing).min(block))
            .or_insert(block);
        Ok(())
    }

    pub fn contains(
        &self,
        id: &str,
        address: Option<Address>,
        block: u64,
    ) -> Result<bool, MatchError> {
        let children = self
            .factories
            .get(id)
            .ok_or_else(|| MatchError::UnknownFactory(id.to_owned()))?;
        Ok(address
            .and_then(|address| children.get(&address))
            .is_some_and(|created| *created <= block))
    }
}

impl AddressFilter {
    fn matches(
        &self,
        address: Option<Address>,
        block: u64,
        children: &ChildAddresses,
    ) -> Result<bool, MatchError> {
        match self {
            Self::Any => Ok(true),
            Self::One(expected) => Ok(address == Some(*expected)),
            Self::AnyOf(expected) => Ok(address.is_some_and(|address| expected.contains(&address))),
            Self::Factory(id) => children.contains(id, address, block),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchError {
    ZeroInterval,
    UnknownFactory(String),
    UnknownTraceOutcome,
    MissingChildTopic,
    ChildDataOutOfBounds,
}

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInterval => write!(f, "block interval must be nonzero"),
            Self::UnknownFactory(id) => write!(f, "factory {id:?} has not been registered"),
            Self::UnknownTraceOutcome => write!(f, "effective trace outcome is unknown"),
            Self::MissingChildTopic => write!(f, "factory child topic is missing"),
            Self::ChildDataOutOfBounds => write!(f, "factory child data word is out of bounds"),
        }
    }
}

impl Error for MatchError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockFilter {
    pub range: BlockRange,
    pub offset: u64,
    interval: u64,
}

impl BlockFilter {
    pub fn new(range: BlockRange, offset: u64, interval: u64) -> Result<Self, MatchError> {
        if interval == 0 {
            return Err(MatchError::ZeroInterval);
        }
        Ok(Self {
            range,
            offset,
            interval,
        })
    }

    pub fn matches(&self, block: u64) -> bool {
        self.range.contains(block) && block % self.interval == self.offset % self.interval
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildLocation {
    Topic1,
    Topic2,
    Topic3,
    /// Byte offset of the ABI word in log data (not its address suffix).
    Offset(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFactory {
    pub id: String,
    pub range: BlockRange,
    pub addresses: ValueFilter<Address>,
    pub event_selector: Topic,
    pub child_location: ChildLocation,
}

impl LogFactory {
    pub fn matches(&self, log: &Log) -> bool {
        self.range.contains(log.block_number)
            && self.addresses.matches(Some(&log.address))
            && log.topics.first() == Some(&self.event_selector)
    }

    /// Extracts the last 20 bytes of the selected 32-byte ABI word.
    /// Call only after `matches`; malformed or missing words are errors.
    pub fn child_address(&self, log: &Log) -> Result<Address, MatchError> {
        let word: &[u8] = match self.child_location {
            ChildLocation::Topic1 => &log.topics.get(1).ok_or(MatchError::MissingChildTopic)?.0,
            ChildLocation::Topic2 => &log.topics.get(2).ok_or(MatchError::MissingChildTopic)?.0,
            ChildLocation::Topic3 => &log.topics.get(3).ok_or(MatchError::MissingChildTopic)?.0,
            ChildLocation::Offset(offset) => {
                let end = offset
                    .checked_add(32)
                    .ok_or(MatchError::ChildDataOutOfBounds)?;
                log.data
                    .get(offset..end)
                    .ok_or(MatchError::ChildDataOutOfBounds)?
            }
        };
        let mut address = [0; 20];
        address.copy_from_slice(&word[12..32]);
        Ok(Address(address))
    }

    /// Records a matching creation log; existing earlier creation blocks win.
    pub fn discover(&self, log: &Log, children: &mut ChildAddresses) -> Result<bool, MatchError> {
        if !self.matches(log) {
            return Ok(false);
        }
        children.insert(&self.id, self.child_address(log)?, log.block_number)?;
        Ok(true)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogFilter {
    pub range: BlockRange,
    pub address: AddressFilter,
    pub topics: [ValueFilter<Topic>; 4],
}

impl LogFilter {
    pub fn matches(&self, log: &Log, children: &ChildAddresses) -> Result<bool, MatchError> {
        if !self.range.contains(log.block_number)
            || self
                .topics
                .iter()
                .enumerate()
                .any(|(i, filter)| !filter.matches(log.topics.get(i)))
        {
            return Ok(false);
        }
        self.address
            .matches(Some(log.address), log.block_number, children)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub block_number: u64,
    pub from: Address,
    pub to: Option<Address>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionFilter {
    pub range: BlockRange,
    pub from: AddressFilter,
    pub to: AddressFilter,
}

impl TransactionFilter {
    pub fn matches(
        &self,
        transaction: &Transaction,
        children: &ChildAddresses,
    ) -> Result<bool, MatchError> {
        Ok(self.range.contains(transaction.block_number)
            && self
                .from
                .matches(Some(transaction.from), transaction.block_number, children)?
            && self
                .to
                .matches(transaction.to, transaction.block_number, children)?)
    }
}

fn trace_is_successful(trace: &Trace) -> Result<bool, MatchError> {
    match trace.outcome {
        TraceOutcome::Successful => Ok(true),
        TraceOutcome::Reverted => Ok(false),
        TraceOutcome::Unknown => Err(MatchError::UnknownTraceOutcome),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceFilter {
    pub range: BlockRange,
    pub from: AddressFilter,
    pub to: AddressFilter,
    pub function_selector: ValueFilter<Selector>,
    pub call_type: Option<CallType>,
}

impl TraceFilter {
    pub fn matches(
        &self,
        block: u64,
        trace: &Trace,
        children: &ChildAddresses,
    ) -> Result<bool, MatchError> {
        if !self.range.contains(block) {
            return Ok(false);
        }
        // A short input cannot equal a four-byte function selector.
        let selector = trace
            .input
            .get(..4)
            .map(|bytes| Selector([bytes[0], bytes[1], bytes[2], bytes[3]]));
        if !self.function_selector.matches(selector.as_ref())
            || self
                .call_type
                .is_some_and(|call_type| call_type != trace.call_type)
        {
            return Ok(false);
        }
        Ok(self.from.matches(Some(trace.from), block, children)?
            && self.to.matches(trace.to, block, children)?
            && trace_is_successful(trace)?)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferFilter {
    pub range: BlockRange,
    pub from: AddressFilter,
    pub to: AddressFilter,
}

impl TransferFilter {
    pub fn matches(
        &self,
        block: u64,
        trace: &Trace,
        children: &ChildAddresses,
    ) -> Result<bool, MatchError> {
        if !self.range.contains(block)
            || !trace
                .value
                .is_some_and(|value| value.iter().any(|byte| *byte != 0))
            || matches!(trace.call_type, CallType::DelegateCall | CallType::CallCode)
        {
            return Ok(false);
        }
        Ok(self.from.matches(Some(trace.from), block, children)?
            && self.to.matches(trace.to, block, children)?
            && trace_is_successful(trace)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    fn topic(byte: u8) -> Topic {
        Topic([byte; 32])
    }

    fn log(block: u64) -> Log {
        Log {
            block_number: block,
            address: address(1),
            topics: vec![topic(1), topic(2), topic(3), topic(4)],
            data: Vec::new(),
        }
    }

    fn trace() -> Trace {
        Trace {
            from: address(1),
            to: Some(address(2)),
            call_type: CallType::Call,
            input: vec![1, 2, 3, 4, 5],
            value: Some([1; 32]),
            output: None,
            outcome: TraceOutcome::Successful,
        }
    }

    fn factory(location: ChildLocation) -> LogFactory {
        LogFactory {
            id: "factory".into(),
            range: BlockRange {
                from: Some(5),
                to: Some(10),
            },
            addresses: ValueFilter::One(address(1)),
            event_selector: topic(1),
            child_location: location,
        }
    }

    fn registry() -> ChildAddresses {
        let mut children = ChildAddresses::default();
        children.register_factory("factory");
        children
    }

    #[test]
    fn block_range_is_inclusive_and_unbounded() {
        let range = BlockRange {
            from: Some(5),
            to: Some(10),
        };
        for (block, expected) in [
            (0, false),
            (4, false),
            (5, true),
            (10, true),
            (11, false),
            (u64::MAX, false),
        ] {
            assert_eq!(range.contains(block), expected, "{block}");
        }
        assert!(BlockRange::default().contains(u64::MAX));
        assert!(
            !BlockRange {
                from: Some(2),
                to: Some(1)
            }
            .contains(1)
        );
    }

    #[test]
    fn block_filter_uses_offset_not_range_start_even_before_offset() {
        let filter = BlockFilter::new(
            BlockRange {
                from: Some(1),
                to: Some(12),
            },
            9,
            4,
        )
        .unwrap();
        for (block, expected) in [
            (0, false),
            (1, true),
            (5, true),
            (8, false),
            (9, true),
            (12, false),
            (13, false),
        ] {
            assert_eq!(filter.matches(block), expected, "{block}");
        }
        assert_eq!(
            BlockFilter::new(BlockRange::default(), 0, 0),
            Err(MatchError::ZeroInterval)
        );
        assert!(
            BlockFilter::new(BlockRange::default(), u64::MAX, 1)
                .unwrap()
                .matches(0)
        );
    }

    #[test]
    fn value_filters_distinguish_wildcard_missing_and_empty() {
        assert!(ValueFilter::<Topic>::Any.matches(None));
        assert!(!ValueFilter::One(topic(1)).matches(None));
        assert!(!ValueFilter::<Topic>::AnyOf(vec![]).matches(Some(&topic(1))));
        assert!(!ValueFilter::AnyOf(vec![topic(1)]).matches(None));
        assert!(ValueFilter::AnyOf(vec![topic(1), topic(2)]).matches(Some(&topic(2))));
        assert!(!ValueFilter::One(topic(1)).matches(Some(&topic(2))));
    }

    #[test]
    fn log_filters_match_all_four_topics_range_and_address() {
        let mut filter = LogFilter {
            range: BlockRange {
                from: Some(5),
                to: Some(10),
            },
            address: AddressFilter::AnyOf(vec![address(1), address(9)]),
            topics: [
                ValueFilter::One(topic(1)),
                ValueFilter::Any,
                ValueFilter::AnyOf(vec![topic(3)]),
                ValueFilter::One(topic(4)),
            ],
        };
        let children = registry();
        assert!(filter.matches(&log(5), &children).unwrap());
        assert!(filter.matches(&log(10), &children).unwrap());
        assert!(!filter.matches(&log(4), &children).unwrap());
        assert!(!filter.matches(&log(11), &children).unwrap());
        let mut event = log(6);
        for i in 0..4 {
            let old = event.topics[i];
            event.topics[i] = topic(8);
            assert_eq!(filter.matches(&event, &children).unwrap(), i == 1);
            event.topics[i] = old;
        }
        event.topics.truncate(3);
        assert!(!filter.matches(&event, &children).unwrap());
        filter.topics[3] = ValueFilter::Any;
        assert!(filter.matches(&event, &children).unwrap());
        event.address = address(7);
        assert!(!filter.matches(&event, &children).unwrap());
        filter.address = AddressFilter::AnyOf(vec![]);
        assert!(!filter.matches(&log(6), &children).unwrap());
    }

    #[test]
    fn factory_log_matching_range_selector_and_address_variants() {
        let mut f = factory(ChildLocation::Topic1);
        assert!(!f.matches(&log(4)));
        assert!(f.matches(&log(5)));
        assert!(f.matches(&log(10)));
        assert!(!f.matches(&log(11)));
        let mut event = log(6);
        event.address = address(9);
        assert!(!f.matches(&event));
        f.addresses = ValueFilter::AnyOf(vec![address(1), address(9)]);
        assert!(f.matches(&event));
        f.addresses = ValueFilter::Any;
        assert!(f.matches(&event));
        event.topics.clear();
        assert!(!f.matches(&event));
        event.topics.push(topic(9));
        assert!(!f.matches(&event));
        f.addresses = ValueFilter::AnyOf(vec![]);
        assert!(!f.matches(&log(6)));
    }

    #[test]
    fn child_extraction_topics_and_data_offsets_with_errors() {
        let mut event = log(6);
        let mut word = [0; 32];
        word[12..].copy_from_slice(&address(42).0);
        for (location, index) in [
            (ChildLocation::Topic1, 1),
            (ChildLocation::Topic2, 2),
            (ChildLocation::Topic3, 3),
        ] {
            event.topics[index] = Topic(word);
            assert_eq!(factory(location).child_address(&event), Ok(address(42)));
        }
        event.data = vec![8; 32];
        event.data.extend(word);
        assert_eq!(
            factory(ChildLocation::Offset(32)).child_address(&event),
            Ok(address(42))
        );
        assert_eq!(
            factory(ChildLocation::Offset(33)).child_address(&event),
            Err(MatchError::ChildDataOutOfBounds)
        );
        assert_eq!(
            factory(ChildLocation::Offset(usize::MAX)).child_address(&event),
            Err(MatchError::ChildDataOutOfBounds)
        );
        event.topics.truncate(1);
        assert_eq!(
            factory(ChildLocation::Topic1).child_address(&event),
            Err(MatchError::MissingChildTopic)
        );
    }

    #[test]
    fn discovery_is_block_granular_and_survives_factory_end() {
        let f = factory(ChildLocation::Topic1);
        let mut event = log(7);
        let mut word = [0; 32];
        word[12..].copy_from_slice(&address(42).0);
        event.topics[1] = Topic(word);
        let mut children = registry();
        assert!(f.discover(&event, &mut children).unwrap());
        assert!(!children.contains("factory", Some(address(42)), 6).unwrap());
        assert!(children.contains("factory", Some(address(42)), 7).unwrap());
        assert!(
            children
                .contains("factory", Some(address(42)), 100)
                .unwrap()
        );
        assert!(!children.contains("factory", None, 100).unwrap());
        event.block_number = 11;
        assert!(!f.discover(&event, &mut children).unwrap());
        // Out-of-order discoveries retain the earliest known creation block.
        event.block_number = 5;
        assert!(f.discover(&event, &mut children).unwrap());
        event.block_number = 9;
        assert!(f.discover(&event, &mut children).unwrap());
        assert!(children.contains("factory", Some(address(42)), 5).unwrap());
        assert!(!children.contains("factory", Some(address(42)), 4).unwrap());
        let f = LogFilter {
            address: AddressFilter::Factory("factory".into()),
            ..Default::default()
        };
        let mut child_log = log(5);
        child_log.address = address(42);
        assert!(f.matches(&child_log, &children).unwrap());
        child_log.block_number = 4;
        assert!(!f.matches(&child_log, &children).unwrap());
    }

    #[test]
    fn missing_factory_is_an_error_not_an_empty_factory() {
        let mut children = ChildAddresses::default();
        assert_eq!(
            children.contains("f", None, 0),
            Err(MatchError::UnknownFactory("f".into()))
        );
        assert_eq!(
            children.insert("f", address(1), 0),
            Err(MatchError::UnknownFactory("f".into()))
        );
        children.register_factory("f");
        assert!(!children.contains("f", Some(address(1)), 0).unwrap());
        children.insert("f", address(1), 1).unwrap();
        children.register_factory("f");
        assert!(children.contains("f", Some(address(1)), 1).unwrap());
    }

    #[test]
    fn transactions_match_from_to_null_ranges_and_factory_sides() {
        let mut children = registry();
        children.insert("factory", address(2), 5).unwrap();
        let mut transaction = Transaction {
            block_number: 5,
            from: address(1),
            to: Some(address(2)),
        };
        let mut filter = TransactionFilter {
            range: BlockRange {
                from: Some(5),
                to: Some(6),
            },
            from: AddressFilter::One(address(1)),
            to: AddressFilter::Factory("factory".into()),
        };
        assert!(filter.matches(&transaction, &children).unwrap());
        transaction.block_number = 4;
        assert!(!filter.matches(&transaction, &children).unwrap());
        transaction.block_number = 7;
        assert!(!filter.matches(&transaction, &children).unwrap());
        transaction.block_number = 6;
        transaction.to = None;
        assert!(!filter.matches(&transaction, &children).unwrap());
        filter.to = AddressFilter::Any;
        assert!(filter.matches(&transaction, &children).unwrap());
        filter.from = AddressFilter::Factory("factory".into());
        assert!(!filter.matches(&transaction, &children).unwrap());
        transaction.from = address(2);
        assert!(filter.matches(&transaction, &children).unwrap());
        filter.to = AddressFilter::AnyOf(vec![]);
        assert!(!filter.matches(&transaction, &children).unwrap());
    }

    #[test]
    fn traces_match_selector_prefix_call_type_addresses_and_ranges() {
        let children = registry();
        let mut filter = TraceFilter {
            range: BlockRange {
                from: Some(5),
                to: Some(10),
            },
            from: AddressFilter::One(address(1)),
            to: AddressFilter::One(address(2)),
            function_selector: ValueFilter::One(Selector([1, 2, 3, 4])),
            call_type: Some(CallType::Call),
        };
        let mut event = trace();
        assert!(filter.matches(5, &event, &children).unwrap());
        assert!(filter.matches(10, &event, &children).unwrap());
        assert!(!filter.matches(4, &event, &children).unwrap());
        assert!(!filter.matches(11, &event, &children).unwrap());
        event.call_type = CallType::StaticCall;
        assert!(!filter.matches(5, &event, &children).unwrap());
        filter.call_type = None;
        assert!(filter.matches(5, &event, &children).unwrap());
        event.input = vec![1, 2, 3];
        assert!(!filter.matches(5, &event, &children).unwrap());
        filter.function_selector = ValueFilter::Any;
        assert!(filter.matches(5, &event, &children).unwrap());
        event.to = None;
        assert!(!filter.matches(5, &event, &children).unwrap());
        filter.to = AddressFilter::Any;
        assert!(filter.matches(5, &event, &children).unwrap());
        event.from = address(3);
        assert!(!filter.matches(5, &event, &children).unwrap());
    }

    #[test]
    fn trace_and_transfer_factory_membership_checks_both_sides() {
        let mut children = registry();
        children.register_factory("other");
        children.insert("factory", address(1), 5).unwrap();
        children.insert("other", address(2), 6).unwrap();
        let from = AddressFilter::Factory("factory".into());
        let to = AddressFilter::Factory("other".into());
        let tf = TraceFilter {
            from: from.clone(),
            to: to.clone(),
            ..Default::default()
        };
        let vf = TransferFilter {
            from,
            to,
            ..Default::default()
        };
        for block in [4, 5] {
            assert!(!tf.matches(block, &trace(), &children).unwrap());
            assert!(!vf.matches(block, &trace(), &children).unwrap());
        }
        assert!(tf.matches(6, &trace(), &children).unwrap());
        assert!(vf.matches(6, &trace(), &children).unwrap());
        let mut event = trace();
        event.to = None;
        assert!(!tf.matches(6, &event, &children).unwrap());
        assert!(!vf.matches(6, &event, &children).unwrap());
    }

    #[test]
    fn transfers_require_nonzero_full_width_value_and_exclude_two_call_types() {
        let filter = TransferFilter::default();
        let children = registry();
        let mut event = trace();
        event.value = None;
        assert!(!filter.matches(1, &event, &children).unwrap());
        event.value = Some([0; 32]);
        assert!(!filter.matches(1, &event, &children).unwrap());
        let mut value = [0; 32];
        value[0] = 1;
        event.value = Some(value);
        for (call_type, expected) in [
            (CallType::Call, true),
            (CallType::CallCode, false),
            (CallType::DelegateCall, false),
            (CallType::StaticCall, true),
            (CallType::Create, true),
            (CallType::Create2, true),
            (CallType::SelfDestruct, true),
        ] {
            event.call_type = call_type;
            assert_eq!(
                filter.matches(1, &event, &children).unwrap(),
                expected,
                "{call_type:?}"
            );
        }
    }

    #[test]
    fn reverted_or_ancestor_reverted_frames_do_not_match_and_unknown_is_error() {
        let children = registry();
        let tf = TraceFilter::default();
        let vf = TransferFilter::default();
        let mut event = trace();
        for (outcome, expected) in [
            (TraceOutcome::Successful, Ok(true)),
            (TraceOutcome::Reverted, Ok(false)),
            (TraceOutcome::Unknown, Err(MatchError::UnknownTraceOutcome)),
        ] {
            event.outcome = outcome;
            assert_eq!(tf.matches(1, &event, &children), expected);
            assert_eq!(vf.matches(1, &event, &children), expected);
        }
        // A definitely non-matching frame needs no outcome to reject it.
        event.value = None;
        assert_eq!(vf.matches(1, &event, &children), Ok(false));
    }
}
