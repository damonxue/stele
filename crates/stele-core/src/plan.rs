//! Conversion from declarative configuration to per-chain indexing plans.
//!
//! A [`ChainPlan`] is everything the engine needs for one chain: the block
//! range, the callbacks to match, and the factories to discover. Plans are a
//! pure function of [`stele_config::Config`]; no RPC calls are made and no
//! files other than ABI paths are read.
//!
//! ## Ranges
//!
//! Each source keeps its own `start_block`/`end_block`, which become the
//! matching range of its callbacks. The chain-level [`ChainPlan::start_block`]
//! is the earliest source start, defaulting to `0` when no source declares one;
//! [`ChainPlan::end_block`] is unbounded when any source is unbounded and
//! otherwise the latest source end.
//!
//! ## Factories
//!
//! A matching factory is always a log emitted by the parent contract. The
//! child address is located with [`ChildLocation`]: an indexed topic for
//! `parameter = { topic = n }`, a byte offset for
//! `parameter = { offset = n }`, and a derived topic or offset for a nested
//! parameter path such as `pool` or `pool.address`.
//!
//! When a contract declares both `address` and `factory`, the factory wins:
//! static addresses cannot be mixed with a dynamic child set in a single
//! callback. This matches the `Factory` address filter.

use std::collections::{BTreeMap, BTreeSet};

use alloy_dyn_abi::{DynSolType, Specifier};
use alloy_json_abi::{Event as AbiEvent, JsonAbi};
use serde_json::Value;
use stele_config::{
    AccountConfig, BlockConfig, Config, ContractConfig, FactoryConfig, FactoryParameter,
};
use stele_types::Topic;

use crate::EngineError;
use crate::decode::{DecodeKind, DecodeSpec};
use crate::events::{EventCallback, Filter};
use crate::filter::{
    AddressFilter, BlockFilter, BlockRange, ChildLocation, LogFilter, Selector, TraceFilter,
    TransactionFilter, TransferFilter, ValueFilter,
};

/// Everything produced by [`build_plans`].
#[derive(Clone, Debug, Default)]
pub struct PlanBuild {
    /// One plan per chain, sorted by chain id.
    pub plans: Vec<ChainPlan>,
}

/// One factory declaration, resolved against the parent ABI.
///
/// In addition to the contract-derived fields, the plan carries the parent
/// event selector and address filter so the engine can scan creation logs
/// without re-reading the configuration or ABIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactoryPlan {
    /// Address-registry identifier used by
    /// [`crate::filter::AddressFilter::Factory`] on the child callbacks.
    pub id: String,
    /// Name of the parent contract source.
    pub parent_source: String,
    /// Factory event name declared by the parent ABI.
    pub event: String,
    /// How the event declares the child address.
    pub parameter: FactoryParameter,
    /// Blocks in which creation logs are scanned.
    pub range: BlockRange,
    /// Name of the child contract source.
    pub child_contract: String,
    /// Topic 0 of the parent creation event.
    pub event_selector: Topic,
    /// Addresses allowed to emit the creation event.
    pub parent_addresses: AddressFilter,
    /// Position of the child address inside the creation log.
    pub child_location: ChildLocation,
}

/// One callback together with the ABI item used to decode it.
#[derive(Clone, Debug)]
pub struct CallbackPlan {
    pub callback: EventCallback,
    /// `None` for accounts and blocks, which have no ABI payload.
    pub decode: Option<DecodeSpec>,
}

/// All indexing work for one chain.
#[derive(Clone, Debug)]
pub struct ChainPlan {
    pub chain_id: u64,
    pub chain_name: String,
    /// Earliest block indexed on this chain, or `0` when unspecified.
    pub start_block: u64,
    /// Last block indexed on this chain, or `None` for "follow the tip".
    pub end_block: Option<u64>,
    pub reorg_window_seconds: u64,
    pub callbacks: Vec<CallbackPlan>,
    pub factories: Vec<FactoryPlan>,
}

impl ChainPlan {
    /// Whether any callback filters on call traces.
    ///
    /// Trace and transfer callbacks read `BlockData::traces`, so a plan without
    /// them does not need `debug_traceBlockByNumber` requests.
    pub fn needs_traces(&self) -> bool {
        self.callbacks.iter().any(|callback| {
            matches!(
                callback.callback.filter,
                Filter::Trace(_) | Filter::Transfer(_)
            )
        })
    }
}

/// Builds per-chain plans from `config`.
///
/// # Errors
///
/// Returns [`EngineError::User`] when the configuration references an unknown
/// chain, an ABI cannot be loaded or parsed, a named event is absent from its
/// ABI, a factory parameter cannot be located, or a block interval is zero.
/// This function does not call [`Config::validate`]; callers that own a
/// configuration file should validate it first for cross-reference checks that
/// are not plan-local.
pub fn build_plans(config: &Config) -> Result<PlanBuild, EngineError> {
    let mut abis = BTreeMap::new();
    for (name, contract) in &config.contracts {
        abis.insert(name.clone(), load_abi(name, &contract.abi)?);
    }

    let mut chains: BTreeMap<u64, ChainPlanBuilder> = BTreeMap::new();

    for (name, contract) in &config.contracts {
        let chain = chain_for(config, &contract.chain, name)?;
        let abi = abis
            .get(name)
            .ok_or_else(|| EngineError::User(format!("contract {name:?} has no loaded ABI")))?;
        let builder = chains.entry(chain.id).or_insert_with(|| {
            ChainPlanBuilder::new(
                chain.id,
                contract.chain.clone(),
                u64::from(chain.reorg_window_seconds),
            )
        });

        add_contract_callbacks(builder, name, contract, abi)?;
        builder.track(BlockRange {
            from: contract.start_block,
            to: contract.end_block,
        });

        if let Some(factory) = &contract.factory {
            let plan = build_factory(config, &abis, name, contract, factory)?;
            builder.track(plan.range);
            builder.factories.push(plan);
        }
    }

    for (name, account) in &config.accounts {
        let chain = chain_for(config, &account.chain, name)?;
        let builder = chains.entry(chain.id).or_insert_with(|| {
            ChainPlanBuilder::new(
                chain.id,
                account.chain.clone(),
                u64::from(chain.reorg_window_seconds),
            )
        });
        add_account_callbacks(builder, name, account);
        builder.track(BlockRange {
            from: account.start_block,
            to: account.end_block,
        });
    }

    for (name, block) in &config.blocks {
        let chain = chain_for(config, &block.chain, name)?;
        let builder = chains.entry(chain.id).or_insert_with(|| {
            ChainPlanBuilder::new(
                chain.id,
                block.chain.clone(),
                u64::from(chain.reorg_window_seconds),
            )
        });
        add_block_callback(builder, name, block)?;
        builder.track(BlockRange {
            from: block.start_block,
            to: block.end_block,
        });
    }

    Ok(PlanBuild {
        plans: chains.into_values().map(ChainPlanBuilder::finish).collect(),
    })
}

struct ChainPlanBuilder {
    chain_id: u64,
    chain_name: String,
    reorg_window_seconds: u64,
    start_block: Option<u64>,
    end_block: Option<u64>,
    unbounded_end: bool,
    callbacks: Vec<CallbackPlan>,
    factories: Vec<FactoryPlan>,
}

impl ChainPlanBuilder {
    fn new(chain_id: u64, chain_name: String, reorg_window_seconds: u64) -> Self {
        Self {
            chain_id,
            chain_name,
            reorg_window_seconds,
            start_block: None,
            end_block: None,
            unbounded_end: false,
            callbacks: Vec::new(),
            factories: Vec::new(),
        }
    }

    fn track(&mut self, range: BlockRange) {
        if let Some(from) = range.from {
            self.start_block = Some(self.start_block.map_or(from, |current| current.min(from)));
        }
        match range.to {
            Some(to) => {
                self.end_block = Some(self.end_block.map_or(to, |current| current.max(to)));
            }
            None => self.unbounded_end = true,
        }
    }

    fn finish(self) -> ChainPlan {
        ChainPlan {
            chain_id: self.chain_id,
            chain_name: self.chain_name,
            start_block: self.start_block.unwrap_or(0),
            end_block: if self.unbounded_end {
                None
            } else {
                self.end_block
            },
            reorg_window_seconds: self.reorg_window_seconds,
            callbacks: self.callbacks,
            factories: self.factories,
        }
    }
}

fn chain_for<'a>(
    config: &'a Config,
    chain_name: &str,
    source: &str,
) -> Result<&'a stele_config::ChainConfig, EngineError> {
    config.chains.get(chain_name).ok_or_else(|| {
        EngineError::User(format!(
            "source {source:?} references unknown chain {chain_name:?}"
        ))
    })
}

/// Loads a configured ABI. Compiler artifacts with a top-level `abi` array are
/// accepted alongside bare ABI arrays.
fn load_abi(name: &str, source: &stele_config::AbiSource) -> Result<JsonAbi, EngineError> {
    let value = source.load_json().map_err(|error| {
        EngineError::User(format!("contract {name:?} ABI cannot be loaded: {error}"))
    })?;
    let items = match value {
        Value::Array(items) => Value::Array(items),
        Value::Object(mut object) => object.remove("abi").ok_or_else(|| {
            EngineError::User(format!(
                "contract {name:?} ABI must be an array or an artifact with an `abi` array"
            ))
        })?,
        _ => {
            return Err(EngineError::User(format!(
                "contract {name:?} ABI must be a JSON array or artifact object"
            )));
        }
    };
    serde_json::from_value::<JsonAbi>(items)
        .map_err(|error| EngineError::User(format!("contract {name:?} ABI is not valid: {error}")))
}

fn add_contract_callbacks(
    builder: &mut ChainPlanBuilder,
    name: &str,
    contract: &ContractConfig,
    abi: &JsonAbi,
) -> Result<(), EngineError> {
    if contract.factory.is_none() && !contract.has_address() {
        return Err(EngineError::User(format!(
            "contract {name:?} must declare at least one address or a factory"
        )));
    }
    let range = BlockRange {
        from: contract.start_block,
        to: contract.end_block,
    };
    let address = contract_address_filter(name, contract);

    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    match &contract.events {
        Some(events) => {
            for event in events {
                if seen.insert(event.clone()) {
                    names.push(event.clone());
                }
            }
        }
        None => {
            for event in abi.events() {
                if seen.insert(event.name.clone()) {
                    names.push(event.name.clone());
                }
            }
        }
    }

    for event_name in names {
        let event = abi
            .event(&event_name)
            .and_then(|events| events.first())
            .ok_or_else(|| {
                EngineError::User(format!(
                    "contract {name:?} declares event {event_name:?} but its ABI does not contain it"
                ))
            })?;
        let key = format!("{name}:{event_name}");
        builder.callbacks.push(CallbackPlan {
            callback: EventCallback {
                name: key.clone(),
                source_id: name.to_owned(),
                chain_id: builder.chain_id,
                include_transaction_receipt: false,
                filter: Filter::Log(LogFilter {
                    range,
                    address: address.clone(),
                    topics: [
                        ValueFilter::One(Topic(event.selector().0)),
                        ValueFilter::Any,
                        ValueFilter::Any,
                        ValueFilter::Any,
                    ],
                }),
            },
            decode: Some(DecodeSpec {
                event_name: key,
                abi: abi.clone(),
                kind: DecodeKind::Log { event: event_name },
            }),
        });
    }

    if let Some(calls) = &contract.calls {
        let mut seen = BTreeSet::new();
        for function_name in calls {
            if !seen.insert(function_name.clone()) {
                continue;
            }
            let function = abi
                .function(function_name)
                .and_then(|functions| functions.first())
                .ok_or_else(|| {
                    EngineError::User(format!(
                        "contract {name:?} declares call {function_name:?} but its ABI does not contain it"
                    ))
                })?;
            let key = format!("{name}.{function_name}()");
            builder.callbacks.push(CallbackPlan {
                callback: EventCallback {
                    name: key.clone(),
                    source_id: name.to_owned(),
                    chain_id: builder.chain_id,
                    include_transaction_receipt: false,
                    filter: Filter::Trace(TraceFilter {
                        range,
                        from: AddressFilter::Any,
                        to: address.clone(),
                        function_selector: ValueFilter::One(Selector(function.selector().0)),
                        call_type: None,
                    }),
                },
                decode: Some(DecodeSpec {
                    event_name: key,
                    abi: abi.clone(),
                    kind: DecodeKind::Function {
                        function: function_name.clone(),
                    },
                }),
            });
        }
    }
    Ok(())
}

fn contract_address_filter(name: &str, contract: &ContractConfig) -> AddressFilter {
    if contract.factory.is_some() {
        return AddressFilter::Factory(name.to_owned());
    }
    match contract.addresses() {
        [] => AddressFilter::Any,
        [address] => AddressFilter::One(*address),
        addresses => AddressFilter::AnyOf(addresses.to_vec()),
    }
}

fn add_account_callbacks(builder: &mut ChainPlanBuilder, name: &str, account: &AccountConfig) {
    let range = BlockRange {
        from: account.start_block,
        to: account.end_block,
    };
    let watched = match account.address.as_slice() {
        [address] => AddressFilter::One(*address),
        addresses => AddressFilter::AnyOf(addresses.to_vec()),
    };

    if account.include_transactions {
        builder.callbacks.push(CallbackPlan {
            callback: EventCallback {
                name: format!("{name}:transaction:from"),
                source_id: name.to_owned(),
                chain_id: builder.chain_id,
                include_transaction_receipt: true,
                filter: Filter::Transaction(TransactionFilter {
                    range,
                    from: watched.clone(),
                    to: AddressFilter::Any,
                }),
            },
            decode: None,
        });
        builder.callbacks.push(CallbackPlan {
            callback: EventCallback {
                name: format!("{name}:transaction:to"),
                source_id: name.to_owned(),
                chain_id: builder.chain_id,
                include_transaction_receipt: true,
                filter: Filter::Transaction(TransactionFilter {
                    range,
                    from: AddressFilter::Any,
                    to: watched.clone(),
                }),
            },
            decode: None,
        });
    }

    if account.include_transfers {
        builder.callbacks.push(CallbackPlan {
            callback: EventCallback {
                name: format!("{name}:transfer:from"),
                source_id: name.to_owned(),
                chain_id: builder.chain_id,
                include_transaction_receipt: false,
                filter: Filter::Transfer(TransferFilter {
                    range,
                    from: watched.clone(),
                    to: AddressFilter::Any,
                }),
            },
            decode: None,
        });
        builder.callbacks.push(CallbackPlan {
            callback: EventCallback {
                name: format!("{name}:transfer:to"),
                source_id: name.to_owned(),
                chain_id: builder.chain_id,
                include_transaction_receipt: false,
                filter: Filter::Transfer(TransferFilter {
                    range,
                    from: AddressFilter::Any,
                    to: watched,
                }),
            },
            decode: None,
        });
    }
}

fn add_block_callback(
    builder: &mut ChainPlanBuilder,
    name: &str,
    block: &BlockConfig,
) -> Result<(), EngineError> {
    let filter = BlockFilter::new(
        BlockRange {
            from: block.start_block,
            to: block.end_block,
        },
        block.offset,
        block.interval,
    )
    .map_err(|error| {
        EngineError::User(format!(
            "block source {name:?} has an invalid interval: {error}"
        ))
    })?;
    builder.callbacks.push(CallbackPlan {
        callback: EventCallback {
            name: format!("{name}:block"),
            source_id: name.to_owned(),
            chain_id: builder.chain_id,
            include_transaction_receipt: false,
            filter: Filter::Block(filter),
        },
        decode: None,
    });
    Ok(())
}

fn build_factory(
    config: &Config,
    abis: &BTreeMap<String, JsonAbi>,
    child_name: &str,
    child: &ContractConfig,
    factory: &FactoryConfig,
) -> Result<FactoryPlan, EngineError> {
    let parent = config.contracts.get(&factory.parent).ok_or_else(|| {
        EngineError::User(format!(
            "contract {child_name:?} declares factory parent {:?}, which is not registered",
            factory.parent
        ))
    })?;
    if parent.chain != child.chain {
        return Err(EngineError::User(format!(
            "contract {child_name:?} and its factory parent {:?} must use the same chain",
            factory.parent
        )));
    }
    let parent_abi = abis.get(&factory.parent).ok_or_else(|| {
        EngineError::User(format!(
            "factory parent {:?} has no loaded ABI",
            factory.parent
        ))
    })?;
    let event = parent_abi
        .event(&factory.event)
        .and_then(|events| events.first())
        .ok_or_else(|| {
            EngineError::User(format!(
                "factory event {:?} is not present in the ABI of parent {:?}",
                factory.event, factory.parent
            ))
        })?;
    let child_location = child_location(event, &factory.parameter).map_err(|message| {
        EngineError::User(format!(
            "contract {child_name:?} factory parameter {:?}: {message}",
            factory.parameter
        ))
    })?;

    Ok(FactoryPlan {
        id: child_name.to_owned(),
        parent_source: factory.parent.clone(),
        event: factory.event.clone(),
        parameter: factory.parameter.clone(),
        range: BlockRange {
            from: factory.start_block.or(child.start_block),
            to: factory.end_block.or(child.end_block),
        },
        child_contract: child_name.to_owned(),
        event_selector: Topic(event.selector().0),
        parent_addresses: contract_address_filter(&factory.parent, parent),
        child_location,
    })
}

/// Locates the child address inside a creation log.
fn child_location(event: &AbiEvent, parameter: &FactoryParameter) -> Result<ChildLocation, String> {
    match parameter {
        FactoryParameter::Topic(1) => Ok(ChildLocation::Topic1),
        FactoryParameter::Topic(2) => Ok(ChildLocation::Topic2),
        FactoryParameter::Topic(3) => Ok(ChildLocation::Topic3),
        FactoryParameter::Topic(topic) => {
            Err(format!("topic {topic} is out of range, expected 1..=3"))
        }
        FactoryParameter::Offset(offset) => Ok(ChildLocation::Offset(*offset)),
        FactoryParameter::Nested(path) => nested_child_location(event, path),
    }
}

fn nested_child_location(event: &AbiEvent, path: &str) -> Result<ChildLocation, String> {
    let segments: Vec<&str> = path.split('.').collect();
    let head = segments[0];
    let position = event
        .inputs
        .iter()
        .position(|input| input.name == head)
        .ok_or_else(|| {
            format!(
                "parameter {head:?} is not present in event {:?}; expected one of [{}]",
                event.name,
                event
                    .inputs
                    .iter()
                    .map(|input| format!("{:?}", input.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    let input = &event.inputs[position];

    if input.indexed {
        if segments.len() > 1 {
            return Err(format!(
                "indexed parameter {path:?} cannot be traversed; indexed tuples are stored as a hash"
            ));
        }
        let indexed_position = event.inputs[..position]
            .iter()
            .filter(|input| input.indexed)
            .count();
        return match indexed_position {
            0 => Ok(ChildLocation::Topic1),
            1 => Ok(ChildLocation::Topic2),
            2 => Ok(ChildLocation::Topic3),
            _ => Err("an event cannot have more than three indexed parameters".to_owned()),
        };
    }

    let body_position = event.inputs[..position]
        .iter()
        .filter(|input| !input.indexed)
        .count();
    let mut offset = body_position * 32;
    let mut components = input.components.as_slice();
    let resolved = input
        .resolve()
        .map_err(|error| format!("parameter {head:?} cannot be resolved: {error}"))?;

    for (depth, segment) in segments.iter().enumerate().skip(1) {
        if components.is_empty() {
            return Err(format!(
                "parameter {path:?} traverses a non-tuple value at {segment:?}"
            ));
        }
        let index = components
            .iter()
            .position(|component| component.name == *segment)
            .ok_or_else(|| format!("parameter {segment:?} is not a field of {path:?}"))?;
        for component in &components[..index] {
            let ty = component.resolve().map_err(|error| {
                format!("component {:?} cannot be resolved: {error}", component.name)
            })?;
            offset += head_size(&ty)?;
        }
        let ty = components[index]
            .resolve()
            .map_err(|error| format!("component {segment:?} cannot be resolved: {error}"))?;
        let is_last = depth == segments.len() - 1;
        if is_last {
            if !matches!(ty, DynSolType::Address) {
                return Err(format!(
                    "parameter {path:?} resolves to {ty}, expected address"
                ));
            }
            return Ok(ChildLocation::Offset(offset));
        }
        if ty.is_dynamic() {
            return Err(format!(
                "parameter {path:?} traverses dynamic value {segment:?}"
            ));
        }
        components = components[index].components.as_slice();
    }

    match resolved {
        DynSolType::Address => Ok(ChildLocation::Offset(offset)),
        ty => Err(format!(
            "parameter {path:?} resolves to {ty}, expected address"
        )),
    }
}

/// Bytes a value occupies in an ABI head: dynamic values are a 32-byte offset,
/// static values are inlined at their full encoded width.
fn head_size(ty: &DynSolType) -> Result<usize, String> {
    if ty.is_dynamic() {
        return Ok(32);
    }
    static_size(ty).ok_or_else(|| format!("cannot size the static type {ty}"))
}

fn static_size(ty: &DynSolType) -> Option<usize> {
    match ty {
        DynSolType::Bool
        | DynSolType::Int(_)
        | DynSolType::Uint(_)
        | DynSolType::FixedBytes(_)
        | DynSolType::Address
        | DynSolType::Function => Some(32),
        DynSolType::FixedArray(inner, length) => static_size(inner)?.checked_mul(*length),
        DynSolType::Tuple(members) => members.iter().try_fold(0usize, |total, member| {
            total.checked_add(static_size(member)?)
        }),
        _ => None,
    }
}

/// Direct child address for a factory event, used by tests and diagnostics.
pub fn factory_child_location(
    abi: &JsonAbi,
    event: &str,
    parameter: &FactoryParameter,
) -> Result<ChildLocation, EngineError> {
    let event = abi
        .event(event)
        .and_then(|events| events.first())
        .ok_or_else(|| {
            EngineError::User(format!("factory event {event:?} is not present in the ABI"))
        })?;
    child_location(event, parameter)
        .map_err(|message| EngineError::User(format!("factory event {event:?}: {message}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use stele_config::{AbiSource, ChainConfig, DatabaseConfig, FactoryConfig, OrderingMode};
    use stele_types::Address;

    fn address(value: &str) -> Address {
        value.parse().expect("test address is valid")
    }

    fn chain() -> ChainConfig {
        ChainConfig {
            id: 1,
            rpc: vec!["https://eth.example".to_owned()],
            polling_interval_ms: 1_000,
            reorg_window_seconds: 180,
            cache_rpc_requests: true,
        }
    }

    fn config() -> Config {
        let mut chains = BTreeMap::new();
        chains.insert("mainnet".to_owned(), chain());

        let mut contracts = BTreeMap::new();
        contracts.insert(
            "Factory".to_owned(),
            ContractConfig {
                abi: AbiSource::Inline(json!([{
                    "type": "event",
                    "name": "PoolCreated",
                    "anonymous": false,
                    "inputs": [
                        { "name": "pool", "type": "address", "indexed": true },
                        { "name": "token0", "type": "address", "indexed": false }
                    ]
                }])),
                chain: "mainnet".to_owned(),
                address: Some(vec![address("0x00000000000000000000000000000000000000f0")]),
                factory: None,
                start_block: Some(10),
                end_block: Some(100),
                events: None,
                calls: None,
            },
        );
        contracts.insert(
            "Pool".to_owned(),
            ContractConfig {
                abi: AbiSource::Inline(json!([{
                    "type": "event",
                    "name": "Swap",
                    "anonymous": false,
                    "inputs": [
                        { "name": "sender", "type": "address", "indexed": true },
                        { "name": "amount0", "type": "uint256", "indexed": false }
                    ]
                }])),
                chain: "mainnet".to_owned(),
                address: None,
                factory: Some(FactoryConfig {
                    parent: "Factory".to_owned(),
                    event: "PoolCreated".to_owned(),
                    parameter: FactoryParameter::Nested("pool".to_owned()),
                    start_block: None,
                    end_block: None,
                }),
                start_block: Some(20),
                end_block: None,
                events: Some(vec!["Swap".to_owned()]),
                calls: None,
            },
        );

        let mut accounts = BTreeMap::new();
        accounts.insert(
            "Alice".to_owned(),
            AccountConfig {
                chain: "mainnet".to_owned(),
                address: vec![address("0x00000000000000000000000000000000000000a1")],
                start_block: Some(5),
                end_block: None,
                include_transactions: true,
                include_transfers: true,
            },
        );

        let mut blocks = BTreeMap::new();
        blocks.insert(
            "hourly".to_owned(),
            BlockConfig {
                chain: "mainnet".to_owned(),
                interval: 300,
                offset: 10,
                start_block: Some(1),
                end_block: Some(1_000),
            },
        );

        Config {
            ordering: OrderingMode::Multichain,
            database: DatabaseConfig {
                url: "postgres://localhost/stele".to_owned(),
                schema: "public".to_owned(),
            },
            chains,
            contracts,
            accounts,
            blocks,
        }
    }

    fn find<'a>(plan: &'a ChainPlan, name: &str) -> &'a CallbackPlan {
        plan.callbacks
            .iter()
            .find(|callback| callback.callback.name == name)
            .unwrap_or_else(|| panic!("missing callback {name}"))
    }

    #[test]
    fn builds_contract_account_block_and_factory_plans() {
        let build = build_plans(&config()).unwrap();
        assert_eq!(build.plans.len(), 1);
        let plan = &build.plans[0];
        assert_eq!(plan.chain_id, 1);
        assert_eq!(plan.chain_name, "mainnet");
        assert_eq!(plan.start_block, 1);
        assert_eq!(plan.end_block, None);
        assert_eq!(plan.reorg_window_seconds, 180);

        let factory_event = find(plan, "Factory:PoolCreated");
        let Filter::Log(filter) = &factory_event.callback.filter else {
            panic!("factory parent must be a log callback");
        };
        assert_eq!(
            filter.address,
            AddressFilter::One(address("0x00000000000000000000000000000000000000f0"))
        );
        assert_eq!(
            filter.range,
            BlockRange {
                from: Some(10),
                to: Some(100)
            }
        );
        assert_eq!(filter.topics[0], ValueFilter::One(factory_selector()));
        assert!(
            filter.topics[1..]
                .iter()
                .all(|topic| *topic == ValueFilter::Any)
        );
        assert!(matches!(
            factory_event.decode.as_ref().map(|spec| &spec.kind),
            Some(DecodeKind::Log { event }) if event == "PoolCreated"
        ));

        let child_event = find(plan, "Pool:Swap");
        let Filter::Log(filter) = &child_event.callback.filter else {
            panic!("child contract must be a log callback");
        };
        assert_eq!(filter.address, AddressFilter::Factory("Pool".to_owned()));
        assert_eq!(
            filter.range,
            BlockRange {
                from: Some(20),
                to: None
            }
        );

        assert_eq!(plan.factories.len(), 1);
        let factory = &plan.factories[0];
        assert_eq!(factory.id, "Pool");
        assert_eq!(factory.parent_source, "Factory");
        assert_eq!(factory.child_contract, "Pool");
        assert_eq!(factory.event, "PoolCreated");
        assert_eq!(
            factory.range,
            BlockRange {
                from: Some(20),
                to: None
            }
        );
        assert_eq!(factory.event_selector, factory_selector());
        assert_eq!(
            factory.parent_addresses,
            AddressFilter::One(address("0x00000000000000000000000000000000000000f0"))
        );
        assert_eq!(factory.child_location, ChildLocation::Topic1);

        for name in ["Alice:transaction:from", "Alice:transaction:to"] {
            let callback = &find(plan, name).callback;
            assert!(callback.include_transaction_receipt);
            assert!(matches!(callback.filter, Filter::Transaction(_)));
        }
        for name in ["Alice:transfer:from", "Alice:transfer:to"] {
            let callback = &find(plan, name).callback;
            assert!(!callback.include_transaction_receipt);
            assert!(matches!(callback.filter, Filter::Transfer(_)));
        }

        let Filter::Block(filter) = &find(plan, "hourly:block").callback.filter else {
            panic!("block source must be a block callback");
        };
        assert!(filter.matches(310));
        assert!(!filter.matches(311));
        assert!(!filter.matches(1_610));
        assert!(!filter.matches(0));
    }

    fn factory_selector() -> Topic {
        let abi =
            JsonAbi::parse(["event PoolCreated(address indexed pool, address token0)"]).unwrap();
        Topic(abi.event("PoolCreated").unwrap()[0].selector().0)
    }

    fn nested_abi() -> JsonAbi {
        serde_json::from_value(json!([
            {
                "type": "event",
                "name": "Created",
                "anonymous": false,
                "inputs": [
                    {
                        "name": "meta",
                        "type": "tuple",
                        "indexed": false,
                        "components": [
                            { "name": "addr", "type": "address" },
                            { "name": "id", "type": "uint256" }
                        ]
                    },
                    { "name": "index", "type": "uint256", "indexed": false }
                ]
            },
            {
                "type": "event",
                "name": "Indexed",
                "anonymous": false,
                "inputs": [{ "name": "pool", "type": "address", "indexed": true }]
            }
        ]))
        .unwrap()
    }

    #[test]
    fn resolves_nested_factory_parameters_to_topic_or_offset() {
        let abi = nested_abi();
        assert_eq!(
            factory_child_location(
                &abi,
                "Created",
                &FactoryParameter::Nested("meta.addr".to_owned())
            )
            .unwrap(),
            ChildLocation::Offset(0)
        );
        assert_eq!(
            factory_child_location(
                &abi,
                "Indexed",
                &FactoryParameter::Nested("pool".to_owned())
            )
            .unwrap(),
            ChildLocation::Topic1
        );
        assert_eq!(
            factory_child_location(&abi, "Created", &FactoryParameter::Topic(2)).unwrap(),
            ChildLocation::Topic2
        );
        assert_eq!(
            factory_child_location(&abi, "Created", &FactoryParameter::Offset(64)).unwrap(),
            ChildLocation::Offset(64)
        );
        assert!(
            factory_child_location(
                &abi,
                "Created",
                &FactoryParameter::Nested("index".to_owned())
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_events_and_missing_chains_are_user_errors() {
        let mut configured = config();
        configured.contracts.get_mut("Pool").unwrap().events = Some(vec!["Missing".to_owned()]);
        assert!(matches!(
            build_plans(&configured),
            Err(EngineError::User(message)) if message.contains("Missing")
        ));

        let mut configured = config();
        configured.contracts.get_mut("Pool").unwrap().chain = "unknown".to_owned();
        assert!(matches!(
            build_plans(&configured),
            Err(EngineError::User(message)) if message.contains("unknown chain")
        ));
    }

    #[test]
    fn configured_calls_become_trace_callbacks() {
        let mut chains = BTreeMap::new();
        chains.insert("mainnet".to_owned(), chain());
        let mut contracts = BTreeMap::new();
        contracts.insert(
            "Token".to_owned(),
            ContractConfig {
                abi: AbiSource::Inline(json!([
                    {
                        "type": "function",
                        "name": "transfer",
                        "stateMutability": "nonpayable",
                        "inputs": [
                            { "name": "to", "type": "address" },
                            { "name": "value", "type": "uint256" }
                        ],
                        "outputs": [{ "name": "", "type": "bool" }]
                    }
                ])),
                chain: "mainnet".to_owned(),
                address: Some(vec![address("0x00000000000000000000000000000000000000aa")]),
                factory: None,
                start_block: Some(1),
                end_block: None,
                events: None,
                calls: Some(vec!["transfer".to_owned()]),
            },
        );
        let configured = Config {
            ordering: OrderingMode::Multichain,
            database: DatabaseConfig {
                url: "postgres://localhost/test".to_owned(),
                schema: "public".to_owned(),
            },
            chains,
            contracts,
            accounts: BTreeMap::new(),
            blocks: BTreeMap::new(),
        };

        let plan = build_plans(&configured)
            .expect("plan builds")
            .plans
            .remove(0);
        let callback = plan
            .callbacks
            .iter()
            .find(|callback| callback.callback.name == "Token.transfer()")
            .expect("call callback is registered");
        let Filter::Trace(filter) = &callback.callback.filter else {
            panic!("call callback must use a trace filter");
        };
        assert_eq!(
            filter.function_selector,
            ValueFilter::One(Selector([0xa9, 0x05, 0x9c, 0xbb]))
        );
        assert!(matches!(
            callback.decode.as_ref().map(|decode| &decode.kind),
            Some(DecodeKind::Function { function }) if function == "transfer"
        ));
    }
}
