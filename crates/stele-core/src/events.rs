//! Event construction and block grouping.
//!
//! Inputs are fetched chain data that the caller has already normalized: block,
//! transaction, receipt, trace and log collections for one chain, sorted by
//! `(block number, transaction index, log index)` as appropriate. This module
//! matches them against registered event callbacks, assigns checkpoints and
//! returns events in checkpoint order.
//!
//! ABI decoding and contract attribution are intentionally outside this module.
//! It also does not fetch data, discover factory children (the caller populates
//! [ChildAddresses]) or decide which callbacks must receive a transaction
//! receipt beyond the flags stored on [EventCallback].

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use stele_checkpoint::{Checkpoint, CheckpointError};

use crate::filter::{
    BlockFilter, ChildAddresses, LogFilter, MatchError, TraceFilter, Transaction,
    TransactionFilter, TransferFilter,
};
use crate::{Block, EventPosition};

pub use stele_types::{
    B256, BlockRef, CallType, Log, LogRef, ReceiptRef, Trace, TraceOutcome, TraceRef,
    TransactionRef,
};

/// A registered event filter. Each variant corresponds to one trigger
/// source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    Block(BlockFilter),
    Transaction(TransactionFilter),
    Trace(TraceFilter),
    Log(LogFilter),
    Transfer(TransferFilter),
}

/// One indexing callback and the filter that selects its events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventCallback {
    /// Handler key, for example `Token:Transfer`.
    pub name: String,
    /// Contract, account or block source identifier.
    pub source_id: String,
    pub chain_id: u64,
    /// Whether a matched trace or log callback requires the transaction
    /// receipt. Transaction callbacks always require one.
    pub include_transaction_receipt: bool,
    pub filter: Filter,
}

/// One event produced for a matching callback.
///
/// The optional payload fields follow this shape: block events carry only `block`,
/// log events carry `log` and usually a transaction, trace and transfer events
/// carry `trace` and a transaction, and transaction events carry `transaction`
/// plus `receipt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEvent {
    pub checkpoint: Checkpoint,
    pub chain_id: u64,
    /// Index into the callback slice passed to [`build_events`].
    pub callback_index: usize,
    pub block: BlockRef,
    pub transaction: Option<TransactionRef>,
    pub receipt: Option<ReceiptRef>,
    pub trace: Option<TraceRef>,
    pub log: Option<LogRef>,
}

/// Fetched data for one chain. The caller owns sorting and normalization.
pub struct EventData<'a> {
    pub blocks: &'a [BlockRef],
    pub transactions: &'a [TransactionRef],
    pub receipts: &'a [ReceiptRef],
    pub traces: &'a [TraceRef],
    pub logs: &'a [LogRef],
    /// Factory membership, used by `AddressFilter::Factory` filters.
    pub children: &'a ChildAddresses,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError {
    MissingBlock {
        chain_id: u64,
        block_number: u64,
    },
    MissingTransaction {
        chain_id: u64,
        block_number: u64,
        transaction_index: u64,
    },
    MissingReceipt {
        chain_id: u64,
        block_number: u64,
        transaction_index: u64,
    },
    Match(MatchError),
    InvalidCheckpoint(CheckpointError),
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBlock {
                chain_id,
                block_number,
            } => write!(
                f,
                "failed to build events: missing block {block_number} for chain {chain_id}"
            ),
            Self::MissingTransaction {
                chain_id,
                block_number,
                transaction_index,
            } => write!(
                f,
                "failed to build events: missing transaction {transaction_index} in block {block_number} for chain {chain_id}"
            ),
            Self::MissingReceipt {
                chain_id,
                block_number,
                transaction_index,
            } => write!(
                f,
                "failed to build events: missing transaction receipt for block {block_number} and transaction index {transaction_index} for chain {chain_id}"
            ),
            Self::Match(error) => write!(f, "failed to match event data: {error}"),
            Self::InvalidCheckpoint(error) => write!(f, "failed to encode checkpoint: {error}"),
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Match(error) => Some(error),
            Self::InvalidCheckpoint(error) => Some(error),
            _ => None,
        }
    }
}

/// Builds raw events for one chain and returns them in checkpoint order.
///
/// Matching follows these rules:
///
/// - block callbacks run once per matching block and use the maximum
///   transaction index, so their events sort after the block's other events;
/// - transaction callbacks skip reverted transactions and require a receipt;
/// - trace and transfer callbacks share one trace index per transaction that
///   advances only when a frame matched at least one callback;
/// - log callbacks use the log index and tolerate a missing transaction.
///
/// Events with equal checkpoints keep callback registration order.
pub fn build_events(
    chain_id: u64,
    callbacks: &[EventCallback],
    data: &EventData<'_>,
) -> Result<Vec<RawEvent>, BuildError> {
    let mut block_callbacks = Vec::new();
    let mut transaction_callbacks = Vec::new();
    let mut trace_callbacks = Vec::new();
    let mut log_callbacks = Vec::new();
    let mut transfer_callbacks = Vec::new();

    for (index, callback) in callbacks.iter().enumerate() {
        if callback.chain_id != chain_id {
            continue;
        }
        match &callback.filter {
            Filter::Block(_) => block_callbacks.push(index),
            Filter::Transaction(_) => transaction_callbacks.push(index),
            Filter::Trace(_) => trace_callbacks.push(index),
            Filter::Log(_) => log_callbacks.push(index),
            Filter::Transfer(_) => transfer_callbacks.push(index),
        }
    }

    let mut blocks: BTreeMap<u64, &BlockRef> = BTreeMap::new();
    for block in data.blocks {
        // Scanning uses a forward-only cursor, so the first
        // occurrence wins for duplicate block numbers.
        blocks.entry(block.number).or_insert(block);
    }
    let transactions = index_by_position(data.transactions.iter().map(|transaction| {
        (
            (transaction.block_number, transaction.transaction_index),
            transaction,
        )
    }));
    let receipts = index_by_position(
        data.receipts
            .iter()
            .map(|receipt| ((receipt.block_number, receipt.transaction_index), receipt)),
    );

    let mut events = Vec::new();

    for block in data.blocks {
        for &index in &block_callbacks {
            let Filter::Block(filter) = &callbacks[index].filter else {
                unreachable!("callback index list was classified by filter");
            };
            if filter.matches(block.number) {
                events.push(RawEvent {
                    checkpoint: checkpoint(chain_id, block, EventPosition::Block)?,
                    chain_id,
                    callback_index: index,
                    block: *block,
                    transaction: None,
                    receipt: None,
                    trace: None,
                    log: None,
                });
            }
        }
    }

    for transaction in data.transactions {
        let block = blocks
            .get(&transaction.block_number)
            .ok_or(BuildError::MissingBlock {
                chain_id,
                block_number: transaction.block_number,
            })?;
        let receipt = receipts.get(&(transaction.block_number, transaction.transaction_index));
        let matched_transaction = Transaction {
            block_number: transaction.block_number,
            from: transaction.from,
            to: transaction.to,
        };

        for &index in &transaction_callbacks {
            let Filter::Transaction(filter) = &callbacks[index].filter else {
                unreachable!("callback index list was classified by filter");
            };
            if !filter
                .matches(&matched_transaction, data.children)
                .map_err(BuildError::Match)?
            {
                continue;
            }
            let Some(receipt) = receipt else {
                return Err(BuildError::MissingReceipt {
                    chain_id,
                    block_number: transaction.block_number,
                    transaction_index: transaction.transaction_index,
                });
            };
            // Reverted transactions are stored, but their effects are invisible.
            if !receipt.success {
                continue;
            }
            events.push(RawEvent {
                checkpoint: checkpoint(
                    chain_id,
                    block,
                    EventPosition::Transaction {
                        transaction_index: transaction.transaction_index,
                    },
                )?,
                chain_id,
                callback_index: index,
                block: **block,
                transaction: Some(*transaction),
                receipt: Some(**receipt),
                trace: None,
                log: None,
            });
        }
    }

    let mut trace_index = 0;
    let mut trace_position: Option<(u64, u64)> = None;

    for trace in data.traces {
        let position = (trace.block_number, trace.transaction_index);
        if trace_position != Some(position) {
            trace_index = 0;
            trace_position = Some(position);
        }

        let block = blocks
            .get(&trace.block_number)
            .ok_or(BuildError::MissingBlock {
                chain_id,
                block_number: trace.block_number,
            })?;
        let transaction = transactions
            .get(&position)
            .ok_or(BuildError::MissingTransaction {
                chain_id,
                block_number: trace.block_number,
                transaction_index: trace.transaction_index,
            })?;
        let receipt = receipts.get(&position);
        let mut matched = false;

        for &index in &trace_callbacks {
            let Filter::Trace(filter) = &callbacks[index].filter else {
                unreachable!("callback index list was classified by filter");
            };
            if !filter
                .matches(block.number, &trace.trace, data.children)
                .map_err(BuildError::Match)?
            {
                continue;
            }
            let receipt = required_receipt(
                chain_id,
                callbacks[index].include_transaction_receipt,
                trace.block_number,
                trace.transaction_index,
                receipt,
            )?;
            events.push(RawEvent {
                checkpoint: checkpoint(
                    chain_id,
                    block,
                    EventPosition::Trace {
                        transaction_index: trace.transaction_index,
                        trace_index,
                    },
                )?,
                chain_id,
                callback_index: index,
                block: **block,
                transaction: Some(**transaction),
                receipt,
                trace: Some(trace.clone()),
                log: None,
            });
            matched = true;
        }

        for &index in &transfer_callbacks {
            let Filter::Transfer(filter) = &callbacks[index].filter else {
                unreachable!("callback index list was classified by filter");
            };
            if !filter
                .matches(block.number, &trace.trace, data.children)
                .map_err(BuildError::Match)?
            {
                continue;
            }
            let receipt = required_receipt(
                chain_id,
                callbacks[index].include_transaction_receipt,
                trace.block_number,
                trace.transaction_index,
                receipt,
            )?;
            events.push(RawEvent {
                checkpoint: checkpoint(
                    chain_id,
                    block,
                    EventPosition::Transfer {
                        transaction_index: trace.transaction_index,
                        trace_index,
                    },
                )?,
                chain_id,
                callback_index: index,
                block: **block,
                transaction: Some(**transaction),
                receipt,
                trace: Some(trace.clone()),
                log: None,
            });
            matched = true;
        }

        if matched {
            trace_index += 1;
        }
    }

    for log in data.logs {
        let block = blocks
            .get(&log.log.block_number)
            .ok_or(BuildError::MissingBlock {
                chain_id,
                block_number: log.log.block_number,
            })?;
        let position = (log.log.block_number, log.transaction_index);
        let transaction = transactions.get(&position).copied();
        let receipt = receipts.get(&position);

        for &index in &log_callbacks {
            let Filter::Log(filter) = &callbacks[index].filter else {
                unreachable!("callback index list was classified by filter");
            };
            if !filter
                .matches(&log.log, data.children)
                .map_err(BuildError::Match)?
            {
                continue;
            }
            let receipt = required_receipt(
                chain_id,
                callbacks[index].include_transaction_receipt,
                log.log.block_number,
                log.transaction_index,
                receipt,
            )?;
            events.push(RawEvent {
                checkpoint: checkpoint(
                    chain_id,
                    block,
                    EventPosition::Log {
                        transaction_index: log.transaction_index,
                        log_index: log.log_index,
                    },
                )?,
                chain_id,
                callback_index: index,
                block: **block,
                transaction: transaction.copied(),
                receipt,
                trace: None,
                log: Some(log.clone()),
            });
        }
    }

    // Stable ordering keeps callback registration order for equal checkpoints.
    events.sort_by_key(|event| event.checkpoint);
    Ok(events)
}

/// One block's events and the checkpoint a committed batch advances to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBatch {
    pub chain_id: u64,
    /// End-of-block checkpoint, matching `splitEvents`.
    pub checkpoint: Checkpoint,
    pub events: Vec<RawEvent>,
}

/// Splits an ordered event stream into per-block batches.
///
/// A batch boundary is a change of block hash, not a change of block number, so
/// events from different histories at the same height are never merged. The
/// caller is expected to pass events already sorted by checkpoint.
pub fn split_events(events: Vec<RawEvent>) -> Result<Vec<EventBatch>, BuildError> {
    let mut batches: Vec<EventBatch> = Vec::new();
    let mut current: Option<(u64, B256)> = None;

    for event in events {
        if current != Some((event.chain_id, event.block.hash)) {
            let checkpoint =
                Checkpoint::block_end(event.block.timestamp, event.chain_id, event.block.number);
            checkpoint.encode().map_err(BuildError::InvalidCheckpoint)?;
            batches.push(EventBatch {
                chain_id: event.chain_id,
                checkpoint,
                events: Vec::new(),
            });
            current = Some((event.chain_id, event.block.hash));
        }
        batches
            .last_mut()
            .expect("a batch was created before inserting the event")
            .events
            .push(event);
    }

    Ok(batches)
}

fn checkpoint(
    chain_id: u64,
    block: &BlockRef,
    position: EventPosition,
) -> Result<Checkpoint, BuildError> {
    position
        .checkpoint(Block {
            timestamp: block.timestamp,
            chain_id,
            number: block.number,
        })
        .map_err(BuildError::InvalidCheckpoint)
}

fn required_receipt(
    chain_id: u64,
    include: bool,
    block_number: u64,
    transaction_index: u64,
    receipt: Option<&&ReceiptRef>,
) -> Result<Option<ReceiptRef>, BuildError> {
    if !include {
        return Ok(None);
    }
    receipt
        .map(|receipt| Some(**receipt))
        .ok_or(BuildError::MissingReceipt {
            chain_id,
            block_number,
            transaction_index,
        })
}

fn index_by_position<'a, T>(
    items: impl Iterator<Item = ((u64, u64), &'a T)>,
) -> BTreeMap<(u64, u64), &'a T> {
    let mut map = BTreeMap::new();
    for (position, item) in items {
        // The first occurrence wins for duplicate positions, consistent with
        // forward-only cursors over sorted data.
        map.entry(position).or_insert(item);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{
        Address, AddressFilter, CallType, Selector, Topic, TraceOutcome, ValueFilter,
    };
    use stele_checkpoint::MAX_COMPONENT;

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    fn topic(byte: u8) -> Topic {
        Topic([byte; 32])
    }

    fn block(number: u64, timestamp: u64, hash_byte: u8) -> BlockRef {
        BlockRef {
            number,
            timestamp,
            hash: B256([hash_byte; 32]),
            parent_hash: B256([hash_byte.wrapping_sub(1); 32]),
        }
    }

    fn transaction(index: u64, from: u8, to: Option<u8>) -> TransactionRef {
        TransactionRef {
            block_number: 1,
            transaction_index: index,
            hash: B256([index as u8; 32]),
            from: address(from),
            to: to.map(address),
        }
    }

    fn receipt(index: u64, success: bool) -> ReceiptRef {
        ReceiptRef {
            block_number: 1,
            transaction_index: index,
            from: address(2),
            to: Some(address(1)),
            success,
        }
    }

    fn log(index: u64, address_byte: u8) -> LogRef {
        LogRef {
            transaction_index: 0,
            log_index: index,
            log: Log {
                block_number: 1,
                address: address(address_byte),
                topics: vec![topic(1), topic(2)],
                data: vec![0xaa],
            },
        }
    }

    fn trace(index: u64, from: u8, to: u8, value: Option<[u8; 32]>) -> TraceRef {
        TraceRef {
            block_number: 1,
            transaction_index: index,
            trace_address: vec![],
            trace: Trace {
                from: address(from),
                to: Some(address(to)),
                call_type: CallType::Call,
                input: vec![1, 2, 3, 4],
                value,
                output: None,
                outcome: TraceOutcome::Successful,
            },
        }
    }

    fn trace_with_input(
        index: u64,
        from: u8,
        to: u8,
        value: Option<[u8; 32]>,
        input: Vec<u8>,
    ) -> TraceRef {
        let mut trace = trace(index, from, to, value);
        trace.trace.input = input;
        trace
    }

    fn callback(name: &str, filter: Filter) -> EventCallback {
        EventCallback {
            name: name.into(),
            source_id: "source".into(),
            chain_id: 1,
            include_transaction_receipt: false,
            filter,
        }
    }

    fn log_callback(name: &str) -> EventCallback {
        callback(
            name,
            Filter::Log(LogFilter {
                range: Default::default(),
                address: Default::default(),
                topics: [
                    ValueFilter::One(topic(1)),
                    Default::default(),
                    Default::default(),
                    Default::default(),
                ],
            }),
        )
    }

    fn data<'a>(
        blocks: &'a [BlockRef],
        transactions: &'a [TransactionRef],
        receipts: &'a [ReceiptRef],
        traces: &'a [TraceRef],
        logs: &'a [LogRef],
        children: &'a ChildAddresses,
    ) -> EventData<'a> {
        EventData {
            blocks,
            transactions,
            receipts,
            traces,
            logs,
            children,
        }
    }

    #[test]
    fn orders_events_by_evm_position_and_checkpoint() {
        let blocks = [block(1, 100, 1)];
        let transactions = [transaction(0, 2, Some(1)), transaction(1, 2, Some(1))];
        let receipts = [receipt(0, true), receipt(1, true)];
        let logs = [log(0, 1), log(1, 1)];
        let traces = [trace(0, 2, 1, Some([1; 32]))];
        let children = ChildAddresses::default();
        let callbacks = [
            callback(
                "A:Trace",
                Filter::Trace(TraceFilter {
                    range: Default::default(),
                    from: Default::default(),
                    to: Default::default(),
                    function_selector: Default::default(),
                    call_type: None,
                }),
            ),
            log_callback("A:Log"),
            callback(
                "A:Transaction",
                Filter::Transaction(TransactionFilter::default()),
            ),
            callback(
                "A:Block",
                Filter::Block(BlockFilter::new(Default::default(), 0, 1).unwrap()),
            ),
        ];

        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &transactions, &receipts, &traces, &logs, &children),
        )
        .unwrap();

        let positions: Vec<_> = events
            .iter()
            .map(|event| {
                (
                    event.checkpoint.transaction_index,
                    event.checkpoint.event_type,
                    event.checkpoint.event_index,
                )
            })
            .collect();
        assert_eq!(
            positions,
            [
                (0, 2, 0),             // transaction 0
                (0, 5, 0),             // log 0 in transaction 0
                (0, 5, 1),             // log 1 in transaction 0
                (0, 7, 0),             // trace in transaction 0
                (1, 2, 0),             // transaction 1
                (MAX_COMPONENT, 5, 0), // block event sorts last
            ]
        );
        assert_eq!(events.last().unwrap().callback_index, 3);
        assert!(events.last().unwrap().transaction.is_none());
    }

    #[test]
    fn reverted_transactions_are_skipped_but_missing_receipts_are_errors() {
        let blocks = [block(1, 100, 1)];
        let transactions = [transaction(0, 2, Some(1)), transaction(1, 2, Some(1))];
        let children = ChildAddresses::default();
        let callbacks = [callback(
            "A:Transaction",
            Filter::Transaction(TransactionFilter::default()),
        )];

        let events = build_events(
            1,
            &callbacks,
            &data(
                &blocks,
                &transactions,
                &[receipt(0, false), receipt(1, true)],
                &[],
                &[],
                &children,
            ),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].transaction.unwrap().transaction_index, 1);
        assert_eq!(events[0].receipt.unwrap().transaction_index, 1);

        assert_eq!(
            build_events(
                1,
                &callbacks,
                &data(
                    &blocks,
                    &transactions,
                    &[receipt(1, true)],
                    &[],
                    &[],
                    &children
                ),
            ),
            Err(BuildError::MissingReceipt {
                chain_id: 1,
                block_number: 1,
                transaction_index: 0,
            })
        );
        assert_eq!(
            build_events(
                1,
                &callbacks,
                &data(
                    &blocks,
                    &transactions,
                    &[receipt(0, true)],
                    &[],
                    &[],
                    &children
                ),
            ),
            Err(BuildError::MissingReceipt {
                chain_id: 1,
                block_number: 1,
                transaction_index: 1,
            })
        );
    }

    #[test]
    fn trace_index_advances_only_for_matching_frames_per_transaction() {
        let blocks = [block(1, 100, 1)];
        let transactions = [transaction(0, 2, Some(1)), transaction(1, 2, Some(1))];
        let children = ChildAddresses::default();
        let zero = [0; 32];

        // Frames that match neither callback must not consume a trace index.
        let traces = [
            trace_with_input(0, 2, 1, Some([1; 32]), vec![1, 2, 3, 4]),
            trace_with_input(0, 2, 3, Some([1; 32]), vec![9, 9, 9, 9]),
            trace_with_input(0, 2, 1, Some([1; 32]), vec![9, 9, 9, 9]),
            trace_with_input(0, 2, 1, Some(zero), vec![1, 2, 3, 4]),
            trace_with_input(0, 2, 3, Some(zero), vec![9, 9, 9, 9]),
            trace_with_input(1, 2, 1, Some(zero), vec![1, 2, 3, 4]),
        ];
        let callbacks = [
            callback(
                "A:Trace",
                Filter::Trace(TraceFilter {
                    range: Default::default(),
                    from: Default::default(),
                    to: Default::default(),
                    function_selector: ValueFilter::One(Selector([1, 2, 3, 4])),
                    call_type: None,
                }),
            ),
            callback(
                "A:Transfer",
                Filter::Transfer(TransferFilter {
                    range: Default::default(),
                    from: AddressFilter::One(address(2)),
                    to: AddressFilter::One(address(1)),
                }),
            ),
        ];

        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &transactions, &[], &traces, &[], &children),
        )
        .unwrap();

        assert_eq!(
            events
                .iter()
                .map(|event| (event.callback_index, event.checkpoint.event_index))
                .collect::<Vec<_>>(),
            [(0, 0), (1, 0), (1, 1), (0, 2), (0, 0)]
        );
    }

    #[test]
    fn transfer_matching_uses_value_and_call_type() {
        let blocks = [block(1, 100, 1)];
        let transactions = [transaction(0, 2, Some(1))];
        let children = ChildAddresses::default();
        let zero = [0; 32];
        let traces = [
            trace(0, 2, 1, Some(zero)),
            TraceRef {
                trace_address: vec![],
                trace: Trace {
                    call_type: CallType::DelegateCall,
                    ..trace(0, 2, 1, Some([1; 32])).trace
                },
                ..trace(0, 2, 1, Some([1; 32]))
            },
            trace(0, 2, 1, Some([1; 32])),
        ];
        let callbacks = [callback(
            "A:Transfer",
            Filter::Transfer(TransferFilter::default()),
        )];

        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &transactions, &[], &traces, &[], &children),
        )
        .unwrap();
        // Only the third frame moves value; the first matching frame gets
        // trace index 0.
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].checkpoint.event_index, 0);
    }

    #[test]
    fn logs_match_topics_and_callback_order() {
        let blocks = [block(1, 100, 1)];
        let children = ChildAddresses::default();
        let callbacks = [
            log_callback("A:Log"),
            callback(
                "A:Log2",
                Filter::Log(LogFilter {
                    range: Default::default(),
                    address: Default::default(),
                    topics: Default::default(),
                }),
            ),
        ];
        let logs = [log(0, 1), log(1, 2)];
        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &[], &[], &[], &logs, &children),
        )
        .unwrap();
        // First callback matches topic 0, second matches all logs.
        assert_eq!(events.len(), 4);
        assert_eq!(
            events
                .iter()
                .map(|event| (event.log.as_ref().unwrap().log_index, event.callback_index))
                .collect::<Vec<_>>(),
            [(0, 0), (0, 1), (1, 0), (1, 1)]
        );
        // No transaction is required for logs.
        assert!(events.iter().all(|event| event.transaction.is_none()));
    }

    #[test]
    fn factory_and_receipt_flags_are_applied_to_trace_callbacks() {
        let blocks = [block(1, 100, 1)];
        let transactions = [transaction(0, 2, Some(1))];
        let children = ChildAddresses::default();
        let callbacks = [EventCallback {
            include_transaction_receipt: true,
            ..callback(
                "A:Trace",
                Filter::Trace(TraceFilter {
                    range: Default::default(),
                    from: Default::default(),
                    to: Default::default(),
                    function_selector: Default::default(),
                    call_type: None,
                }),
            )
        }];
        let traces = [trace(0, 2, 1, Some([1; 32]))];

        assert_eq!(
            build_events(
                1,
                &callbacks,
                &data(&blocks, &transactions, &[], &traces, &[], &children),
            ),
            Err(BuildError::MissingReceipt {
                chain_id: 1,
                block_number: 1,
                transaction_index: 0,
            })
        );

        let events = build_events(
            1,
            &callbacks,
            &data(
                &blocks,
                &transactions,
                &[receipt(0, true)],
                &traces,
                &[],
                &children,
            ),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].receipt.unwrap().transaction_index, 0);
    }

    #[test]
    fn missing_blocks_and_transactions_are_reported() {
        let blocks = [block(1, 100, 1)];
        let children = ChildAddresses::default();
        let callbacks = [callback(
            "A:Trace",
            Filter::Trace(TraceFilter {
                range: Default::default(),
                from: Default::default(),
                to: Default::default(),
                function_selector: Default::default(),
                call_type: None,
            }),
        )];
        let traces = [trace(0, 2, 1, Some([1; 32]))];

        assert_eq!(
            build_events(
                1,
                &callbacks,
                &data(&blocks, &[], &[], &traces, &[], &children),
            ),
            Err(BuildError::MissingTransaction {
                chain_id: 1,
                block_number: 1,
                transaction_index: 0,
            })
        );

        assert_eq!(
            build_events(1, &callbacks, &data(&[], &[], &[], &traces, &[], &children),),
            Err(BuildError::MissingBlock {
                chain_id: 1,
                block_number: 1,
            })
        );
    }

    #[test]
    fn nonmatching_callbacks_of_other_chains_are_ignored() {
        let blocks = [block(1, 100, 1)];
        let children = ChildAddresses::default();
        let o = callback(
            "A:Log",
            Filter::Log(LogFilter {
                range: Default::default(),
                address: Default::default(),
                topics: Default::default(),
            }),
        );
        let callbacks = [
            EventCallback {
                chain_id: 2,
                ..o.clone()
            },
            o,
        ];
        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &[], &[], &[], &[log(0, 1)], &children),
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].callback_index, 1);
    }

    #[test]
    fn splits_events_on_block_hash_changes() {
        let blocks = [block(1, 100, 1), block(2, 200, 2)];
        let children = ChildAddresses::default();
        let callbacks = [callback(
            "A:Log",
            Filter::Log(LogFilter {
                range: Default::default(),
                address: Default::default(),
                topics: Default::default(),
            }),
        )];
        let logs = [
            LogRef {
                log: Log {
                    block_number: 1,
                    ..log(0, 1).log
                },
                ..log(0, 1)
            },
            LogRef {
                log: Log {
                    block_number: 2,
                    ..log(1, 1).log
                },
                ..log(1, 1)
            },
        ];
        let events = build_events(
            1,
            &callbacks,
            &data(&blocks, &[], &[], &[], &logs, &children),
        )
        .unwrap();
        assert_eq!(events.len(), 2);

        let batches = split_events(events).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].events.len(), 1);
        assert_eq!(batches[0].events.len(), 1);
        assert_eq!(batches[0].checkpoint, Checkpoint::block_end(100, 1, 1));
        assert_eq!(batches[1].checkpoint, Checkpoint::block_end(200, 1, 2));

        // The same height with a different hash is a different batch.
        let mut same_height = batches[1].events.clone();
        same_height[0].block.hash = B256([9; 32]);
        let batches = split_events(same_height).unwrap();
        assert_eq!(batches.len(), 1);
    }
}
