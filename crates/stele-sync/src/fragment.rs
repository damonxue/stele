//! Fragment identifiers and coverage-compatible adjacent IDs.
//!
//! Address, selector, and location strings must already be normalized and
//! validated by the caller, as built filters require. Bounds on a
//! normal filter are deliberately not part of its fragment ID.

/// A factory address descriptor embedded in another fragment's ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FactoryAddress {
    /// Parent address, or any parent.
    pub address: Option<String>,
    /// Factory event selector.
    pub event_selector: String,
    /// Location such as `topic1` or `offset0`.
    pub child_address_location: String,
    /// Factory lower bound, if specified.
    pub from_block: Option<u64>,
    /// Factory upper bound, if specified.
    pub to_block: Option<u64>,
}

/// Address filter for a single fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentAddress {
    /// Any address.
    Any,
    /// One concrete address.
    Address(String),
    /// Addresses discovered from one factory parent.
    Factory(FactoryAddress),
}

fn opt<T: ToString>(value: &Option<T>) -> String {
    value
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "null".into())
}

impl FragmentAddress {
    /// Encodes the address portion of a fragment ID.
    pub fn id(&self) -> String {
        match self {
            Self::Any => "null".into(),
            Self::Address(value) => value.clone(),
            Self::Factory(f) => format!(
                "{}_{}_{}_{}_{}",
                opt(&f.address),
                f.event_selector,
                f.child_address_location,
                opt(&f.from_block),
                opt(&f.to_block)
            ),
        }
    }

    fn adjacent(&self) -> Vec<Self> {
        match self {
            Self::Address(_) => vec![self.clone(), Self::Any],
            _ => vec![self.clone()],
        }
    }
}

/// One atomic fragment. Selectors are exact except
/// nullable topics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Fragment {
    /// A periodic block filter.
    Block {
        chain_id: u64,
        interval: u64,
        offset: u64,
    },
    /// Transactions filtered by sender and recipient.
    Transaction {
        chain_id: u64,
        from: FragmentAddress,
        to: FragmentAddress,
    },
    /// Calls filtered by sender, recipient and function selector.
    Trace {
        chain_id: u64,
        from: FragmentAddress,
        to: FragmentAddress,
        function_selector: String,
        receipts: bool,
    },
    /// Logs for a single address and topic combination.
    Log {
        chain_id: u64,
        address: FragmentAddress,
        topic0: String,
        topics: [Option<String>; 3],
        receipts: bool,
    },
    /// Native transfers.
    Transfer {
        chain_id: u64,
        from: FragmentAddress,
        to: FragmentAddress,
        receipts: bool,
    },
    /// Factory creation logs for a single parent address.
    FactoryLog {
        chain_id: u64,
        factory: FactoryAddress,
    },
}

impl Fragment {
    /// Encodes the textual fragment ID (not a hash).
    pub fn id(&self) -> String {
        match self {
            Self::Block {
                chain_id,
                interval,
                offset,
            } => format!("block_{chain_id}_{interval}_{offset}"),
            Self::Transaction { chain_id, from, to } => {
                format!("transaction_{chain_id}_{}_{}", from.id(), to.id())
            }
            Self::Trace {
                chain_id,
                from,
                to,
                function_selector,
                receipts,
            } => format!(
                "trace_{chain_id}_{}_{}_{function_selector}_{}",
                from.id(),
                to.id(),
                u8::from(*receipts)
            ),
            Self::Log {
                chain_id,
                address,
                topic0,
                topics,
                receipts,
            } => format!(
                "log_{chain_id}_{}_{topic0}_{}_{}_{}_{}",
                address.id(),
                opt(&topics[0]),
                opt(&topics[1]),
                opt(&topics[2]),
                u8::from(*receipts)
            ),
            Self::Transfer {
                chain_id,
                from,
                to,
                receipts,
            } => format!(
                "transfer_{chain_id}_{}_{}_{}",
                from.id(),
                to.id(),
                u8::from(*receipts)
            ),
            Self::FactoryLog { chain_id, factory } => format!(
                "factory_log_{chain_id}_{}",
                FragmentAddress::Factory(factory.clone()).id()
            ),
        }
    }

    /// IDs whose cached data can cover this fragment, including its own ID.
    ///
    /// Broader address/topic filters and receipt-bearing data can cover a
    /// narrower filter; a factory descriptor is only covered by itself.
    pub fn adjacent_ids(&self) -> Vec<String> {
        let mut result = Vec::new();
        let receipt_options = |receipts| {
            if receipts {
                vec![true]
            } else {
                vec![false, true]
            }
        };
        match self {
            Self::Block { .. } | Self::FactoryLog { .. } => result.push(self.id()),
            Self::Transaction { chain_id, from, to } => {
                for f in from.adjacent() {
                    for t in to.adjacent() {
                        result.push(
                            Self::Transaction {
                                chain_id: *chain_id,
                                from: f.clone(),
                                to: t,
                            }
                            .id(),
                        );
                    }
                }
            }
            Self::Trace {
                chain_id,
                from,
                to,
                function_selector,
                receipts,
            } => {
                for f in from.adjacent() {
                    for t in to.adjacent() {
                        for r in receipt_options(*receipts) {
                            result.push(
                                Self::Trace {
                                    chain_id: *chain_id,
                                    from: f.clone(),
                                    to: t.clone(),
                                    function_selector: function_selector.clone(),
                                    receipts: r,
                                }
                                .id(),
                            );
                        }
                    }
                }
            }
            Self::Transfer {
                chain_id,
                from,
                to,
                receipts,
            } => {
                for f in from.adjacent() {
                    for t in to.adjacent() {
                        for r in receipt_options(*receipts) {
                            result.push(
                                Self::Transfer {
                                    chain_id: *chain_id,
                                    from: f.clone(),
                                    to: t.clone(),
                                    receipts: r,
                                }
                                .id(),
                            );
                        }
                    }
                }
            }
            Self::Log {
                chain_id,
                address,
                topic0,
                topics,
                receipts,
            } => {
                let choices = topics.each_ref().map(|topic| {
                    if topic.is_some() {
                        vec![topic.clone(), None]
                    } else {
                        vec![None]
                    }
                });
                for a in address.adjacent() {
                    for t1 in &choices[0] {
                        for t2 in &choices[1] {
                            for t3 in &choices[2] {
                                for r in receipt_options(*receipts) {
                                    result.push(
                                        Self::Log {
                                            chain_id: *chain_id,
                                            address: a.clone(),
                                            topic0: topic0.clone(),
                                            topics: [t1.clone(), t2.clone(), t3.clone()],
                                            receipts: r,
                                        }
                                        .id(),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        result
    }
}

/// Expands a log filter's address and topic alternatives into atomic fragments.
///
/// Use `[FragmentAddress::Any]` or `[None]` for wildcards. An empty
/// alternatives list produces no fragments; topic0 is one fixed selector.
pub fn log_fragments(
    chain_id: u64,
    addresses: &[FragmentAddress],
    topic0: &str,
    topics: [&[Option<String>]; 3],
    receipts: bool,
) -> Vec<Fragment> {
    let mut fragments = Vec::new();
    for address in addresses {
        for t1 in topics[0] {
            for t2 in topics[1] {
                for t3 in topics[2] {
                    fragments.push(Fragment::Log {
                        chain_id,
                        address: address.clone(),
                        topic0: topic0.into(),
                        topics: [t1.clone(), t2.clone(), t3.clone()],
                        receipts,
                    });
                }
            }
        }
    }
    fragments
}

/// Expands sender and recipient alternatives into atomic transaction fragments.
pub fn transaction_fragments(
    chain_id: u64,
    from: &[FragmentAddress],
    to: &[FragmentAddress],
) -> Vec<Fragment> {
    let mut fragments = Vec::new();
    for f in from {
        for t in to {
            fragments.push(Fragment::Transaction {
                chain_id,
                from: f.clone(),
                to: t.clone(),
            });
        }
    }
    fragments
}

/// Unions coverage of the adjacent IDs for one requested fragment.
pub fn adjacent_coverage(
    fragment: &Fragment,
    stored: &std::collections::BTreeMap<String, crate::interval::BlockRangeSet>,
) -> crate::interval::BlockRangeSet {
    fragment
        .adjacent_ids()
        .iter()
        .filter_map(|id| stored.get(id))
        .fold(crate::interval::BlockRangeSet::default(), |acc, set| {
            acc.union(set)
        })
}
