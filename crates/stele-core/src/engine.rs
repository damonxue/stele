//! The indexing runtime: historical backfill, factory discovery and live
//! following.
//!
//! The engine composes the other runtime modules:
//!
//! 1. [`crate::events::build_events`] matches fetched chain data against the
//!    callbacks in a [`ChainPlan`].
//! 2. [`crate::decode`] turns matched logs and traces into JSON arguments.
//! 3. [`crate::handler::HandlerSet`] runs user handlers inside a
//!    [`stele_store::StoreBatch`].
//! 4. [`crate::realtime::LocalChain`] tracks the unfinalized window and detects
//!    reorgs while following the tip.
//!
//! ## Commit points
//!
//! Every indexed block commits exactly one store batch at
//! `Checkpoint::block_end(timestamp, chain_id, number)`, even when the block
//! produced no events. This makes progress a reliable resume point: a second
//! run continues after the last committed block instead of replaying it.
//!
//! ## Ordering
//!
//! - [`stele_config::OrderingMode::Multichain`] and
//!   [`stele_config::OrderingMode::Isolated`] index one chain at a time, in
//!   the chain's own checkpoint order.
//! - [`stele_config::OrderingMode::Omnichain`] merges prepared blocks across
//!   chains by checkpoint, so a block on every chain is executed in global
//!   checkpoint order. Live following stays per chain and is therefore only
//!   ordered within a chain.
//!
//! ## Dispatch
//!
//! Events whose handler key has no registration are skipped. Plans therefore
//! include every ABI event even when only some of them have handlers, matching
//! the build step without requiring the engine to know the handler set at
//! plan time.
//!
//! ## Failures
//!
//! A decode failure is counted and the event is skipped. A handler error stops
//! dispatch immediately: `User` and `Fatal` return from the engine, `Transient`
//! rolls the block batch back and retries the whole block with exponential
//! backoff up to [`EngineOptions::max_retries`].
//!
//! ## Setup
//!
//! `<source>:setup` handlers run once per chain before its first block, inside
//! a batch committed at the chain's start checkpoint. Setup is skipped when the
//! chain already has committed progress: re-running it would need a
//! non-advancing checkpoint, which the store rejects. A restart therefore
//! resumes indexing without repeating setup writes.
//!
//! ## Factory persistence
//!
//! Factory children discovered from creation logs are persisted through
//! [`stele_store::Store::append_factory_addresses`] after the discovering
//! block commits, and loaded again once per chain before the first block is
//! processed. Without that, a restart would resume after the checkpoint that
//! created the child and silently miss its later logs. [`Engine::apply_reorg`]
//! deletes persisted children above the common ancestor with
//! [`stele_store::Store::remove_factory_addresses_after`].
//!
//! ## Live recovery
//!
//! Block headers are not persisted. When live following starts, the engine
//! rebuilds the unfinalized window by walking `block_by_hash` from the last
//! indexed block until it reaches the reorg window or the chain's start. A
//! reorg deeper than that window returns [`EngineError::Fatal`].
//!
//! Live following is per chain even in omnichain mode; global ordering is
//! enforced during backfill, which is where the ordering guarantee matters for
//! deterministic results.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use stele_checkpoint::{Checkpoint, MAX_COMPONENT};
use stele_store::Store;
use stele_types::{Address, B256, BlockData, BlockRef, LogRef};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::decode;
use crate::events::{self, EventCallback, EventData, Filter, RawEvent};
use crate::filter::{AddressFilter, ChildAddresses, LogFactory, ValueFilter};
use crate::handler::{DecodedEvent, EventContext, HandlerError, HandlerSet};
use crate::indexing::InvocationError;
use crate::plan::{ChainPlan, FactoryPlan};
use crate::realtime::{AdvanceError, LocalChain, MAX_REORG_WINDOW_SECONDS, RealtimeUpdate};

/// How often a running backfill reports its rate and remaining work.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// Failure while talking to a chain.
///
/// Implementations should map transport and provider errors to `Transport` or
/// `Rpc`, and malformed responses to `InvalidResponse`, so the engine can log
/// useful diagnostics. The engine treats every variant as fatal for the
/// current run; retries happen inside the chain adapter.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ChainError {
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("chain error: {0}")]
    Other(String),
}

impl From<stele_sync::SyncError> for ChainError {
    fn from(error: stele_sync::SyncError) -> Self {
        match error {
            stele_sync::SyncError::Transport(message) => Self::Transport(message),
            stele_sync::SyncError::Rpc { code, message } => Self::Rpc(format!("{code}: {message}")),
            stele_sync::SyncError::InvalidResponse(message) => Self::InvalidResponse(message),
            stele_sync::SyncError::NotFound(message) => Self::NotFound(message),
            error => Self::Other(error.to_string()),
        }
    }
}

/// Read access to one chain.
///
/// Implementations are expected to return canonical data: `block_data` for a
/// number must describe the block currently in the canonical chain.
#[async_trait]
pub trait ChainSource: Send + Sync {
    /// Latest block number known to the provider.
    async fn latest_block_number(&self) -> Result<u64, ChainError>;

    /// Fetches everything needed to index `number`. `fetch_traces` asks for
    /// call traces and may be used to skip expensive `debug_traceBlockByNumber`
    /// calls.
    async fn block_data(&self, number: u64, fetch_traces: bool) -> Result<BlockData, ChainError>;

    /// Fetches a header by hash, used to walk back to a reorg's common
    /// ancestor.
    async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, ChainError>;
}

#[async_trait]
impl ChainSource for stele_sync::RpcClient {
    async fn latest_block_number(&self) -> Result<u64, ChainError> {
        stele_sync::RpcClient::latest_block_number(self)
            .await
            .map_err(ChainError::from)
    }

    async fn block_data(&self, number: u64, fetch_traces: bool) -> Result<BlockData, ChainError> {
        // Going through a concrete-lifetime helper keeps the provider client's
        // internal closure inference out of the higher-ranked lifetime that
        // `async_trait` requires.
        fetch_block_data(self, number, fetch_traces).await
    }

    async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, ChainError> {
        stele_sync::RpcClient::block_by_hash(self, hash)
            .await
            .map_err(ChainError::from)
    }
}

/// Fetches block data with a lifetime that is tied to the client reference
/// rather than to the `async_trait` wrapper.
/// Asserts that a future is `Send` when the compiler cannot prove it.
///
/// `stele_sync::RpcClient::block_data` captures a borrowed client (`Send`
/// because `RpcClient` is `Sync`) and owned `Vec`/`BlockData` values. Its
/// internal `stream::iter(map(async ...))` hits a higher-ranked closure
/// inference limitation (rust-lang/rust#110338), so rustc cannot prove the
/// coroutine is `Send`.
///
/// This type is private to the module and is only constructed around that
/// future, so no other value can acquire a false `Send` assertion.
struct AssumeSendFuture<F>(F);

// SAFETY: see the type documentation. The wrapped future is `Send` by
// construction; the compiler only lacks the higher-ranked proof.
unsafe impl<F> Send for AssumeSendFuture<F> {}

impl<F: std::future::Future> std::future::Future for AssumeSendFuture<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: the field is structurally pinned and never moved out of the
        // wrapper after construction.
        unsafe { self.map_unchecked_mut(|this| &mut this.0) }.poll(context)
    }
}

fn fetch_block_data<'a>(
    client: &'a stele_sync::RpcClient,
    number: u64,
    fetch_traces: bool,
) -> impl std::future::Future<Output = Result<BlockData, ChainError>> + Send + 'a {
    use futures_util::FutureExt;
    AssumeSendFuture(stele_sync::RpcClient::block_data(
        client,
        number,
        fetch_traces,
    ))
    .map(|result| result.map_err(ChainError::from))
}

/// Engine-wide failures.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Invalid configuration, plans or handler input. Indexing stops.
    #[error("user error: {0}")]
    User(String),
    /// A transient failure survived every retry. The caller may restart.
    #[error("transient error: {0}")]
    Transient(String),
    /// An unrecoverable condition, including a reorg below the finalized
    /// block. Indexing stops.
    #[error("fatal error: {0}")]
    Fatal(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("chain error: {0}")]
    Chain(String),
}

impl From<stele_store::StoreError> for EngineError {
    fn from(error: stele_store::StoreError) -> Self {
        Self::Store(error.to_string())
    }
}

impl From<ChainError> for EngineError {
    fn from(error: ChainError) -> Self {
        Self::Chain(error.to_string())
    }
}

/// Runtime knobs.
#[derive(Clone, Debug)]
pub struct EngineOptions {
    /// Cross-chain execution order.
    pub ordering: stele_config::OrderingMode,
    /// Maximum number of prepared blocks per chain in omnichain backfill.
    /// Values below one are treated as one.
    pub batch_size: usize,
    /// Whether to request call traces. When `false`, trace and transfer
    /// callbacks are skipped. Traces are also skipped when no plan contains a
    /// trace or transfer callback, because fetching them costs one
    /// `debug_traceBlockByNumber` request per block.
    pub fetch_traces: bool,
    /// Follow the tip after backfill instead of returning.
    pub follow: bool,
    /// Delay between tip polls in live mode.
    pub poll_interval: Duration,
    /// Additional attempts for a block whose handler returned
    /// [`HandlerError::Transient`].
    pub max_retries: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            ordering: stele_config::OrderingMode::Multichain,
            batch_size: 10,
            fetch_traces: true,
            follow: false,
            poll_interval: Duration::from_secs(1),
            max_retries: 3,
        }
    }
}

/// Counters exposed by a running engine.
///
/// The counters are cumulative for the lifetime of the engine. Cloning the
/// struct shares the counters.
#[derive(Clone, Debug, Default)]
pub struct Metrics {
    pub events_processed: Arc<AtomicU64>,
    pub decode_failures: Arc<AtomicU64>,
    pub reorgs: Arc<AtomicU64>,
}

impl Metrics {
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn decode_failures(&self) -> u64 {
        self.decode_failures.load(Ordering::Relaxed)
    }

    pub fn reorgs(&self) -> u64 {
        self.reorgs.load(Ordering::Relaxed)
    }
}

/// Mutable per-chain state. Guarded by the engine mutex.
struct ChainRuntime {
    children: ChildAddresses,
    /// Factory discoveries by block, used to rebuild `children` on a revert.
    discoveries: BTreeMap<u64, Vec<(String, Address)>>,
    /// Whether persisted factory children have been loaded from the store.
    seeded: bool,
    last_indexed: Option<BlockRef>,
    local: Option<LocalChain>,
}

/// The indexing engine.
///
/// The engine owns no background tasks: [`Engine::run`] and
/// [`Engine::run_backfill`] drive it on the caller's task. State that changes
/// while running lives behind a [`tokio::sync::Mutex`]; immutable plans and
/// handler registrations are shared through [`Arc`].
pub struct Engine {
    store: Arc<dyn Store>,
    sources: BTreeMap<u64, Arc<dyn ChainSource>>,
    plans: BTreeMap<u64, Arc<ChainPlan>>,
    callbacks: BTreeMap<u64, Vec<EventCallback>>,
    factories: BTreeMap<u64, Vec<FactoryRuntime>>,
    factory_ids: BTreeMap<u64, BTreeSet<String>>,
    contracts: BTreeMap<u64, BTreeMap<String, Address>>,
    handlers: Arc<HandlerSet>,
    options: EngineOptions,
    /// Effective trace fetching: [`EngineOptions::fetch_traces`] narrowed to
    /// plans that actually filter on traces.
    fetch_traces: bool,
    metrics: Arc<Metrics>,
    runtime: Mutex<BTreeMap<u64, ChainRuntime>>,
}

impl Engine {
    /// Builds an engine from resolved plans.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::User`] for duplicate chain plans, plans without a
    /// matching source, chain ids that cannot be encoded in a checkpoint, or a
    /// reorg window above the 600 second limit.
    pub fn new(
        store: Arc<dyn Store>,
        sources: BTreeMap<u64, Arc<dyn ChainSource>>,
        plans: Vec<ChainPlan>,
        handlers: Arc<HandlerSet>,
        options: EngineOptions,
    ) -> Result<Self, EngineError> {
        let mut by_chain: BTreeMap<u64, Arc<ChainPlan>> = BTreeMap::new();
        for plan in plans {
            if !sources.contains_key(&plan.chain_id) {
                return Err(EngineError::User(format!(
                    "chain plan {:?} ({}) has no chain source",
                    plan.chain_name, plan.chain_id
                )));
            }
            if plan.reorg_window_seconds > MAX_REORG_WINDOW_SECONDS {
                return Err(EngineError::User(format!(
                    "chain {} reorg window {}s exceeds {}s",
                    plan.chain_id, plan.reorg_window_seconds, MAX_REORG_WINDOW_SECONDS
                )));
            }
            Checkpoint::block_end(0, plan.chain_id, 0)
                .encode()
                .map_err(|error| {
                    EngineError::User(format!(
                        "chain {} cannot be encoded in a checkpoint: {error}",
                        plan.chain_id
                    ))
                })?;
            if by_chain.insert(plan.chain_id, Arc::new(plan)).is_some() {
                return Err(EngineError::User("duplicate chain plans".to_owned()));
            }
        }

        let mut callbacks = BTreeMap::new();
        let mut factories = BTreeMap::new();
        let mut factory_ids = BTreeMap::new();
        let mut contracts = BTreeMap::new();
        let mut runtime = BTreeMap::new();

        for (chain_id, plan) in &by_chain {
            let mut children = ChildAddresses::default();
            let mut ids = BTreeSet::new();

            for factory in &plan.factories {
                children.register_factory(&factory.id);
                ids.insert(factory.id.clone());
                let mut parent_ids = Vec::new();
                collect_factory_ids(&factory.parent_addresses, &mut parent_ids);
                ids.extend(parent_ids);
            }
            for callback in &plan.callbacks {
                for id in filter_factory_ids(&callback.callback.filter) {
                    children.register_factory(&id);
                    ids.insert(id);
                }
            }
            for id in &ids {
                children.register_factory(id);
            }

            factories.insert(
                *chain_id,
                plan.factories
                    .iter()
                    .map(|factory| FactoryRuntime {
                        matcher: log_factory(factory),
                        parent: factory.parent_addresses.clone(),
                    })
                    .collect::<Vec<_>>(),
            );
            factory_ids.insert(*chain_id, ids);
            callbacks.insert(
                *chain_id,
                plan.callbacks
                    .iter()
                    .map(|callback| callback.callback.clone())
                    .collect(),
            );
            contracts.insert(*chain_id, contracts_map(plan));
            runtime.insert(
                *chain_id,
                ChainRuntime {
                    children,
                    discoveries: BTreeMap::new(),
                    seeded: false,
                    last_indexed: None,
                    local: None,
                },
            );
        }

        let fetch_traces =
            options.fetch_traces && by_chain.values().any(|plan| plan.needs_traces());
        tracing::debug!(
            chains = by_chain.len(),
            callbacks = callbacks.values().map(Vec::len).sum::<usize>(),
            factories = factories.values().map(Vec::len).sum::<usize>(),
            ordering = ?options.ordering,
            follow = options.follow,
            fetch_traces,
            "engine configured"
        );

        Ok(Self {
            store,
            sources,
            plans: by_chain,
            callbacks,
            factories,
            factory_ids,
            contracts,
            handlers,
            options,
            fetch_traces,
            metrics: Arc::new(Metrics::default()),
            runtime: Mutex::new(runtime),
        })
    }

    /// Counters for this engine.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Backfills every chain and, when [`EngineOptions::follow`] is set,
    /// follows the tip until an unrecoverable error occurs.
    pub async fn run(&self) -> Result<(), EngineError> {
        self.run_backfill().await?;
        if self.options.follow {
            self.run_live().await?;
        }
        Ok(())
    }

    /// Runs only the historical phase.
    pub async fn run_backfill(&self) -> Result<(), EngineError> {
        if self.plans.is_empty() {
            return Ok(());
        }
        tracing::info!(
            chains = self.plans.len(),
            ordering = ?self.options.ordering,
            fetch_traces = self.fetch_traces,
            "starting backfill"
        );
        match self.options.ordering {
            stele_config::OrderingMode::Omnichain => self.backfill_omnichain().await?,
            stele_config::OrderingMode::Multichain | stele_config::OrderingMode::Isolated => {
                for chain_id in self.plans.keys().copied().collect::<Vec<_>>() {
                    self.backfill_chain(chain_id).await?;
                }
            }
        }
        tracing::info!(
            events = self.metrics.events_processed(),
            decode_failures = self.metrics.decode_failures(),
            "backfill complete"
        );
        Ok(())
    }

    /// Indexes one fetched block and commits it.
    ///
    /// This is the unit of work shared by backfill and live following. It can
    /// be called directly by embedders that drive block delivery themselves.
    /// Returns `Ok(None)` when the block is outside the plan's range or was
    /// already indexed at the same hash.
    ///
    /// Before the first block of a chain is indexed, persisted factory children
    /// are loaded once from the store (see [`Engine::seed_factories`]).
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::User`] when a different block is already indexed
    /// at the same height; call [`Engine::apply_reorg`] first. All other
    /// failures are propagated from the store, the event builder or a handler.
    pub async fn process_block(
        &self,
        chain_id: u64,
        data: BlockData,
    ) -> Result<Option<BlockRef>, EngineError> {
        self.seed_factories(chain_id).await?;
        let plan = self.plan(chain_id)?;
        let block = data.block.ok_or_else(|| {
            EngineError::Chain(format!(
                "block data for chain {chain_id} does not contain a header"
            ))
        })?;
        if block.number < plan.start_block || plan.end_block.is_some_and(|end| block.number > end) {
            return Ok(None);
        }

        {
            let state = self.runtime.lock().await;
            let runtime = state
                .get(&chain_id)
                .ok_or_else(|| unknown_chain(chain_id))?;
            if let Some(last) = runtime.last_indexed {
                if last.number > block.number
                    || (last.number == block.number && last.hash == block.hash)
                {
                    return Ok(None);
                }
                if last.number == block.number && last.hash != block.hash {
                    return Err(EngineError::User(format!(
                        "block {} on chain {chain_id} is already indexed with a different hash; revert to the common ancestor first",
                        block.number
                    )));
                }
            }
        }

        let (checkpoint, events, discoveries) = {
            let mut state = self.runtime.lock().await;
            let runtime = state
                .get_mut(&chain_id)
                .ok_or_else(|| unknown_chain(chain_id))?;
            let log_factories = self.factories.get(&chain_id).cloned().unwrap_or_default();
            let discoveries = discover_factories(&log_factories, &data.logs, runtime)?;
            let raw = self.build_raw_events(chain_id, &block, &data, &runtime.children)?;
            let events = self.decode_events(&plan, &raw);
            (
                Checkpoint::block_end(block.timestamp, chain_id, block.number),
                events,
                discoveries,
            )
        };

        self.execute_block(chain_id, &block, checkpoint, &events)
            .await?;
        // Only a committed block persists its discoveries, so a failed block
        // leaves no address that no indexed block produced.
        self.persist_discoveries(chain_id, &discoveries).await?;

        tracing::debug!(
            chain_id,
            block = block.number,
            hash = %block.hash.to_hex(),
            events = events.len(),
            discoveries = discoveries.len(),
            "indexed block"
        );

        let mut state = self.runtime.lock().await;
        if let Some(runtime) = state.get_mut(&chain_id) {
            runtime.last_indexed = Some(block);
        }
        Ok(Some(block))
    }

    /// Reverts indexed state after a reorg.
    ///
    /// The store is reverted to the end of `common_ancestor`, persisted factory
    /// addresses discovered above it are deleted, factory discoveries above it
    /// are discarded in memory, and the chain's last indexed block becomes
    /// `common_ancestor`. The caller is responsible for fetching and indexing
    /// the replacement blocks.
    pub async fn apply_reorg(
        &self,
        chain_id: u64,
        common_ancestor: &BlockRef,
    ) -> Result<(), EngineError> {
        let plan = self.plan(chain_id)?;
        let checkpoint =
            Checkpoint::block_end(common_ancestor.timestamp, chain_id, common_ancestor.number);
        let previous_tip = self.last_indexed(chain_id).await?;
        tracing::warn!(
            chain_id,
            common_ancestor = common_ancestor.number,
            previous_tip = previous_tip.map(|block| block.number),
            hash = %common_ancestor.hash.to_hex(),
            timestamp = common_ancestor.timestamp,
            "applying reorg: reverting indexed state to the common ancestor"
        );
        self.store.revert(chain_id, &checkpoint).await?;
        // Children created above the ancestor belong to abandoned blocks. The
        // store is not part of the undo log, so prune it explicitly.
        for factory in &plan.factories {
            self.store
                .remove_factory_addresses_after(chain_id, &factory.id, common_ancestor.number)
                .await?;
        }
        self.metrics.reorgs.fetch_add(1, Ordering::Relaxed);

        let ids = self.factory_ids.get(&chain_id).cloned().unwrap_or_default();
        let mut state = self.runtime.lock().await;
        let runtime = state
            .get_mut(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        rebuild_children(runtime, &ids, common_ancestor.number);
        runtime.last_indexed = Some(*common_ancestor);
        runtime.local = None;
        Ok(())
    }

    async fn backfill_chain(&self, chain_id: u64) -> Result<(), EngineError> {
        let plan = self.plan(chain_id)?;
        let progress = self.store.progress(chain_id).await?;
        let latest = self.latest(chain_id).await?;
        let end = plan.end_block.unwrap_or(latest).min(latest);

        let mut next = plan.start_block;
        let mut first = None;
        match progress {
            Some(progress) => {
                next = resume_block(&progress.checkpoint, plan.start_block);
                tracing::info!(
                    chain = %plan.chain_name,
                    chain_id,
                    resume_from = next,
                    end_block = end,
                    checkpoint = ?progress.checkpoint,
                    "chain backfill resumed"
                );
            }
            None => {
                tracing::info!(
                    chain = %plan.chain_name,
                    chain_id,
                    start_block = plan.start_block,
                    end_block = end,
                    "chain backfill started"
                );
                let data = self.fetch(chain_id, plan.start_block).await?;
                let block = require_block(&data, chain_id)?;
                self.run_setup(chain_id, &block).await?;
                first = Some(data);
            }
        }

        let started = Instant::now();
        let mut last_report = started;
        let mut indexed = 0u64;
        while next <= end {
            let data = match first.take() {
                Some(data) if data.block.is_some_and(|block| block.number == next) => data,
                _ => self.fetch(chain_id, next).await?,
            };
            self.process_block(chain_id, data).await?;
            indexed += 1;
            if last_report.elapsed() >= PROGRESS_INTERVAL {
                last_report = Instant::now();
                let remaining = end.saturating_sub(next);
                let (rate, eta_seconds) = progress_estimate(indexed, remaining, started.elapsed());
                tracing::info!(
                    chain = %plan.chain_name,
                    chain_id,
                    indexed_through = next,
                    end_block = end,
                    remaining_blocks = remaining,
                    blocks_per_second = rate,
                    eta_seconds,
                    "chain backfill progress"
                );
            }
            next = next.saturating_add(1);
        }
        tracing::info!(
            chain = %plan.chain_name,
            chain_id,
            blocks = indexed,
            end_block = end,
            "chain backfill complete"
        );
        Ok(())
    }

    /// Indexes chains in global checkpoint order by selecting the prepared
    /// block with the smallest `block_end` checkpoint.
    async fn backfill_omnichain(&self) -> Result<(), EngineError> {
        struct Cursor {
            chain_id: u64,
            next: u64,
            end: u64,
            prepared: VecDeque<BlockData>,
            capacity: usize,
        }

        let capacity = self.options.batch_size.max(1);
        let mut cursors = Vec::new();
        for chain_id in self.plans.keys().copied().collect::<Vec<_>>() {
            let plan = self.plan(chain_id)?;
            let progress = self.store.progress(chain_id).await?;
            let latest = self.latest(chain_id).await?;
            let end = plan.end_block.unwrap_or(latest).min(latest);

            let mut next = plan.start_block;
            match progress {
                Some(progress) => next = resume_block(&progress.checkpoint, plan.start_block),
                None => {
                    let data = self.fetch(chain_id, plan.start_block).await?;
                    let block = require_block(&data, chain_id)?;
                    self.run_setup(chain_id, &block).await?;
                }
            }
            cursors.push(Cursor {
                chain_id,
                next,
                end,
                prepared: VecDeque::new(),
                capacity,
            });
        }

        let remaining: u64 = cursors
            .iter()
            .map(|cursor| cursor.end.saturating_sub(cursor.next).saturating_add(1))
            .sum();
        let started = Instant::now();
        let mut last_report = started;
        let mut processed = 0u64;
        tracing::info!(
            chains = cursors.len(),
            blocks = remaining,
            batch_size = capacity,
            "starting omnichain backfill"
        );

        loop {
            for cursor in &mut cursors {
                while cursor.prepared.len() < cursor.capacity && cursor.next <= cursor.end {
                    let data = self.fetch(cursor.chain_id, cursor.next).await?;
                    cursor.prepared.push_back(data);
                    cursor.next = cursor.next.saturating_add(1);
                }
            }

            let mut best: Option<(usize, Checkpoint)> = None;
            for (index, cursor) in cursors.iter().enumerate() {
                let Some(data) = cursor.prepared.front() else {
                    continue;
                };
                let Some(block) = data.block else {
                    continue;
                };
                let checkpoint =
                    Checkpoint::block_end(block.timestamp, cursor.chain_id, block.number);
                if best.is_none_or(|(_, current)| checkpoint < current) {
                    best = Some((index, checkpoint));
                }
            }

            let Some((index, _)) = best else {
                break;
            };
            let (chain_id, data) = {
                let cursor = &mut cursors[index];
                let Some(data) = cursor.prepared.pop_front() else {
                    continue;
                };
                (cursor.chain_id, data)
            };
            self.process_block(chain_id, data).await?;
            processed += 1;
            if last_report.elapsed() >= PROGRESS_INTERVAL {
                last_report = Instant::now();
                let remaining = remaining.saturating_sub(processed);
                let (rate, eta_seconds) =
                    progress_estimate(processed, remaining, started.elapsed());
                tracing::info!(
                    blocks = processed,
                    remaining_blocks = remaining,
                    blocks_per_second = rate,
                    eta_seconds,
                    "omnichain backfill progress"
                );
            }
        }
        tracing::info!(
            blocks = processed,
            events = self.metrics.events_processed(),
            "omnichain backfill complete"
        );
        Ok(())
    }

    async fn run_setup(&self, chain_id: u64, block: &BlockRef) -> Result<(), EngineError> {
        let plan = self.plan(chain_id)?;
        let mut keys = Vec::new();
        let mut seen = BTreeSet::new();
        for callback in &plan.callbacks {
            if seen.insert(callback.callback.source_id.clone()) {
                keys.push(format!("{}:setup", callback.callback.source_id));
            }
        }
        for factory in &plan.factories {
            if seen.insert(factory.child_contract.clone()) {
                keys.push(format!("{}:setup", factory.child_contract));
            }
        }
        keys.retain(|key| self.handlers.has_setup(key));
        if keys.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            chain_id,
            block = block.number,
            handlers = keys.len(),
            "running chain setup handlers"
        );

        let checkpoint = Checkpoint::block_start(block.timestamp, chain_id, block.number);
        let contracts = self.contracts.get(&chain_id).cloned().unwrap_or_default();
        let mut attempt = 0usize;
        loop {
            let batch = self.store.begin(chain_id, checkpoint).await?;
            let context = EventContext {
                store: &*batch,
                chain_id,
                block,
                checkpoint: &checkpoint,
                contracts: &contracts,
            };
            let mut failure = None;
            for key in &keys {
                if let Err(error) = self.handlers.run_setup_for(key, &context).await {
                    failure = Some(error);
                    break;
                }
            }
            match failure {
                None => {
                    batch.commit().await?;
                    return Ok(());
                }
                Some(error) => {
                    let _ = batch.rollback().await;
                    match classify(error) {
                        Failure::Transient(message) if attempt < self.options.max_retries => {
                            attempt += 1;
                            let delay = backoff(attempt);
                            tracing::warn!(
                                chain_id,
                                block = block.number,
                                attempt,
                                max_retries = self.options.max_retries,
                                delay_ms = delay.as_millis() as u64,
                                %message,
                                "setup handler failed; retrying"
                            );
                            tokio::time::sleep(delay).await;
                        }
                        Failure::Transient(message) => {
                            return Err(EngineError::Transient(message));
                        }
                        Failure::User(message) => return Err(EngineError::User(message)),
                        Failure::Fatal(message) => return Err(EngineError::Fatal(message)),
                        Failure::Internal(message) => return Err(EngineError::Fatal(message)),
                    }
                }
            }
        }
    }

    async fn execute_block(
        &self,
        chain_id: u64,
        block: &BlockRef,
        checkpoint: Checkpoint,
        events: &[DecodedEvent],
    ) -> Result<(), EngineError> {
        if events.is_empty() {
            // An empty block still commits so progress advances past it.
            let batch = self.store.begin(chain_id, checkpoint).await?;
            batch.commit().await?;
            return Ok(());
        }

        let contracts = self.contracts.get(&chain_id).cloned().unwrap_or_default();
        let mut attempt = 0usize;
        loop {
            let batch = self.store.begin(chain_id, checkpoint).await?;
            let context = EventContext {
                store: &*batch,
                chain_id,
                block,
                checkpoint: &checkpoint,
                contracts: &contracts,
            };
            let mut failure = None;
            'dispatch: for event in events {
                match self.handlers.run_event(&context, event).await {
                    Ok(()) => {
                        self.metrics
                            .events_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        failure = Some(error);
                        break 'dispatch;
                    }
                }
            }
            match failure {
                None => {
                    batch.commit().await?;
                    return Ok(());
                }
                Some(error) => {
                    let _ = batch.rollback().await;
                    match classify(error) {
                        Failure::Transient(message) if attempt < self.options.max_retries => {
                            attempt += 1;
                            let delay = backoff(attempt);
                            tracing::warn!(
                                chain_id,
                                block = block.number,
                                events = events.len(),
                                attempt,
                                max_retries = self.options.max_retries,
                                delay_ms = delay.as_millis() as u64,
                                %message,
                                "handler failed; retrying block"
                            );
                            tokio::time::sleep(delay).await;
                        }
                        Failure::Transient(message) => {
                            return Err(EngineError::Transient(message));
                        }
                        Failure::User(message) => return Err(EngineError::User(message)),
                        Failure::Fatal(message) => return Err(EngineError::Fatal(message)),
                        Failure::Internal(message) => return Err(EngineError::Fatal(message)),
                    }
                }
            }
        }
    }

    /// Polls every chain in turn until an unrecoverable failure.
    async fn run_live(&self) -> Result<(), EngineError> {
        let chain_ids: Vec<u64> = self.plans.keys().copied().collect();
        tracing::info!(
            chains = chain_ids.len(),
            poll_interval_ms = self.options.poll_interval.as_millis() as u64,
            "following the chain tip"
        );
        loop {
            for chain_id in &chain_ids {
                self.poll_chain(*chain_id).await?;
            }
            tokio::time::sleep(self.options.poll_interval).await;
        }
    }

    async fn poll_chain(&self, chain_id: u64) -> Result<(), EngineError> {
        let plan = self.plan(chain_id)?;
        let remote_latest = self.latest(chain_id).await?;

        let mut local = match self.runtime_local(chain_id).await? {
            Some(local) => local,
            None => {
                let last = self.last_indexed(chain_id).await?;
                match last {
                    Some(last) => self.restore_local(chain_id, &plan, last).await?,
                    None => {
                        if remote_latest < plan.start_block {
                            return Ok(());
                        }
                        let data = self.fetch(chain_id, plan.start_block).await?;
                        let block = require_block(&data, chain_id)?;
                        self.process_block(chain_id, data).await?;
                        let local = LocalChain::new(block, Vec::new(), plan.reorg_window_seconds)
                            .map_err(|error| EngineError::User(error.to_string()))?;
                        self.set_local(chain_id, Some(local)).await;
                        return Ok(());
                    }
                }
            }
        };

        // A provider that lags behind the local chain must not be read as a
        // reorg. Equal heights are still fed so a same-height replacement is
        // detected.
        if remote_latest >= plan.start_block && remote_latest >= local.latest().number {
            let tip_data = self.fetch(chain_id, remote_latest).await?;
            let tip = require_block(&tip_data, chain_id)?;
            self.feed_header(chain_id, &plan, tip, &mut local).await?;
        }
        self.set_local(chain_id, Some(local)).await;
        Ok(())
    }

    /// Rebuilds the local unfinalized window from the indexed tip by walking
    /// parent hashes back to the reorg-window boundary.
    ///
    /// Headers are not persisted, so the window is recovered over RPC. When a
    /// parent is unavailable the walk stops, and a reorg below that point is
    /// reported as unrecoverable.
    async fn restore_local(
        &self,
        chain_id: u64,
        plan: &ChainPlan,
        last: BlockRef,
    ) -> Result<LocalChain, EngineError> {
        let source = self
            .sources
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        let mut unfinalized = vec![last];
        let mut cursor = last;
        while last.timestamp.saturating_sub(cursor.timestamp) < plan.reorg_window_seconds
            && cursor.number > plan.start_block
        {
            match source.block_by_hash(cursor.parent_hash).await {
                Ok(parent) if parent.number.saturating_add(1) == cursor.number => {
                    unfinalized.push(parent);
                    cursor = parent;
                }
                _ => break,
            }
        }
        // Collected newest first; `LocalChain` takes oldest first.
        unfinalized.reverse();
        let finalized = unfinalized.remove(0);
        LocalChain::new(finalized, unfinalized, plan.reorg_window_seconds)
            .map_err(|error| EngineError::User(error.to_string()))
    }

    /// Feeds `header` and any gap headers into the local chain, indexing
    /// blocks, reverting reorgs and finalizing as the local chain reports.
    async fn feed_header(
        &self,
        chain_id: u64,
        plan: &ChainPlan,
        header: BlockRef,
        local: &mut LocalChain,
    ) -> Result<(), EngineError> {
        let source = self
            .sources
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        if header.number < local.latest().number {
            // A header below the local tip is stale, not a reorg.
            return Ok(());
        }
        let mut known: HashMap<B256, BlockRef> = HashMap::new();
        known.insert(local.finalized().hash, *local.finalized());
        for block in local.unfinalized() {
            known.insert(block.hash, *block);
        }
        known.insert(header.hash, header);

        let mut cache: HashMap<u64, BlockData> = HashMap::new();
        let mut queue = VecDeque::from([header]);

        while let Some(next) = queue.pop_front() {
            let advanced = {
                let known_ref = &known;
                local.advance(next, |hash| known_ref.get(&hash).copied().ok_or(hash))
            };

            match advanced {
                Ok(updates) => {
                    for update in updates {
                        match update {
                            RealtimeUpdate::Block { block } => {
                                let data = match cache.remove(&block.number) {
                                    Some(data) => data,
                                    None => self.fetch(chain_id, block.number).await?,
                                };
                                match data.block {
                                    Some(fetched) if fetched.hash == block.hash => {
                                        known.insert(block.hash, block);
                                        if block.number >= plan.start_block {
                                            self.process_block(chain_id, data).await?;
                                        }
                                    }
                                    Some(fetched) => {
                                        // The canonical block changed between
                                        // the header and data requests. Re-feed
                                        // the new header, which triggers a reorg.
                                        known.insert(fetched.hash, fetched);
                                        queue.push_front(fetched);
                                    }
                                    None => {
                                        return Err(EngineError::Chain(format!(
                                            "block data for block {} on chain {chain_id} has no header",
                                            block.number
                                        )));
                                    }
                                }
                            }
                            RealtimeUpdate::Reorg {
                                common_ancestor, ..
                            } => {
                                self.apply_reorg(chain_id, &common_ancestor).await?;
                                // Headers queued during this pass were fetched
                                // from the pre-reorg chain. Drop them and let the
                                // next poll fetch the canonical chain.
                                queue.clear();
                                cache.clear();
                                return Ok(());
                            }
                            RealtimeUpdate::Finalize { block } => {
                                let checkpoint =
                                    Checkpoint::block_end(block.timestamp, chain_id, block.number);
                                self.store.finalize(chain_id, &checkpoint).await?;
                                tracing::debug!(chain_id, block = block.number, "finalized block");
                            }
                        }
                    }
                }
                Err(AdvanceError::MissingBlocks { from, to }) => {
                    let mut gap = Vec::new();
                    for number in from..=to {
                        let data = self.fetch(chain_id, number).await?;
                        let block = require_block(&data, chain_id)?;
                        known.insert(block.hash, block);
                        cache.insert(number, data);
                        gap.push(block);
                    }
                    queue.push_front(next);
                    for block in gap.into_iter().rev() {
                        queue.push_front(block);
                    }
                }
                Err(AdvanceError::Fetch(hash)) => {
                    let block = source.block_by_hash(hash).await?;
                    known.insert(block.hash, block);
                    queue.push_front(next);
                }
                Err(AdvanceError::InvalidParent { requested }) => {
                    return Err(EngineError::Chain(format!(
                        "chain {chain_id} returned a block that does not match hash {requested}"
                    )));
                }
                Err(AdvanceError::UnrecoverableReorg { finalized }) => {
                    return Err(EngineError::Fatal(format!(
                        "chain {chain_id} reorg reached finalized block {}",
                        finalized.number
                    )));
                }
            }
        }
        Ok(())
    }

    fn build_raw_events(
        &self,
        chain_id: u64,
        block: &BlockRef,
        data: &BlockData,
        children: &ChildAddresses,
    ) -> Result<Vec<RawEvent>, EngineError> {
        let callbacks = self
            .callbacks
            .get(&chain_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let blocks = [*block];
        let event_data = EventData {
            blocks: &blocks,
            transactions: &data.transactions,
            receipts: &data.receipts,
            traces: &data.traces,
            logs: &data.logs,
            children,
        };
        events::build_events(chain_id, callbacks, &event_data)
            .map_err(|error| EngineError::Chain(error.to_string()))
    }

    fn decode_events(&self, plan: &ChainPlan, raw: &[RawEvent]) -> Vec<DecodedEvent> {
        let mut decoded = Vec::new();
        for event in raw {
            let Some(callback) = plan.callbacks.get(event.callback_index) else {
                continue;
            };
            if !self.fetch_traces
                && matches!(
                    &callback.callback.filter,
                    Filter::Trace(_) | Filter::Transfer(_)
                )
            {
                continue;
            }
            if !self.handlers.has_event(&callback.callback.name) {
                continue;
            }
            let (args, result) = match &callback.decode {
                Some(spec) => match decode::decode(
                    spec,
                    event.log.as_ref().map(|log| &log.log),
                    event.trace.as_ref().map(|trace| &trace.trace),
                ) {
                    Ok(values) => (values.args, values.result),
                    Err(error) => {
                        self.metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            chain_id = event.chain_id,
                            block_number = event.block.number,
                            handler = %callback.callback.name,
                            %error,
                            "skipping event that could not be decoded"
                        );
                        continue;
                    }
                },
                None => (Value::Null, Value::Null),
            };
            decoded.push(DecodedEvent {
                name: callback.callback.name.clone(),
                source_id: callback.callback.source_id.clone(),
                chain_id: event.chain_id,
                checkpoint: event.checkpoint,
                block: event.block,
                transaction: event.transaction,
                receipt: event.receipt,
                trace: event.trace.clone(),
                log: event.log.clone(),
                args,
                result,
            });
        }
        decoded
    }

    fn plan(&self, chain_id: u64) -> Result<Arc<ChainPlan>, EngineError> {
        self.plans
            .get(&chain_id)
            .cloned()
            .ok_or_else(|| unknown_chain(chain_id))
    }

    /// Loads persisted factory children for `chain_id`, once per engine.
    ///
    /// Backfill resumes from the committed checkpoint and never replays older
    /// blocks, so children discovered before a restart exist only in the store.
    /// Each loaded address is registered in the child registry and recorded in
    /// `discoveries` at its creation block, which keeps reorg rebuilds exact.
    /// A chain without persisted addresses (a fresh index) loads nothing and
    /// behaves exactly as before.
    async fn seed_factories(&self, chain_id: u64) -> Result<(), EngineError> {
        {
            let state = self.runtime.lock().await;
            let runtime = state
                .get(&chain_id)
                .ok_or_else(|| unknown_chain(chain_id))?;
            if runtime.seeded {
                return Ok(());
            }
        }

        // Read outside the runtime lock; the store call may be slow.
        let plan = self.plan(chain_id)?;
        let mut loaded = Vec::with_capacity(plan.factories.len());
        for factory in &plan.factories {
            let addresses = self.store.factory_addresses(chain_id, &factory.id).await?;
            loaded.push((factory.id.clone(), addresses));
        }

        let mut state = self.runtime.lock().await;
        let runtime = state
            .get_mut(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        if runtime.seeded {
            // Another caller seeded while the store read was in flight.
            return Ok(());
        }
        let loaded_addresses: usize = loaded.iter().map(|(_, addresses)| addresses.len()).sum();
        tracing::debug!(
            chain_id,
            factories = loaded.len(),
            addresses = loaded_addresses,
            "loaded persisted factory children"
        );
        for (id, addresses) in loaded {
            runtime.children.register_factory(&id);
            for (address, block) in addresses {
                runtime
                    .children
                    .insert(&id, address, block)
                    .map_err(|error| EngineError::Chain(error.to_string()))?;
                runtime
                    .discoveries
                    .entry(block)
                    .or_default()
                    .push((id.clone(), address));
            }
        }
        runtime.seeded = true;
        Ok(())
    }

    /// Persists addresses discovered while indexing a block.
    ///
    /// Called after the block batch commits: a block that rolls back or fails
    /// must not leave addresses that no indexed block produced. The store call
    /// is idempotent, so re-indexed blocks and restarts converge to the earliest
    /// creation block.
    async fn persist_discoveries(
        &self,
        chain_id: u64,
        discoveries: &[(String, Address, u64)],
    ) -> Result<(), EngineError> {
        if discoveries.is_empty() {
            return Ok(());
        }
        let mut by_factory: BTreeMap<&str, Vec<(Address, u64)>> = BTreeMap::new();
        for (id, address, block) in discoveries {
            by_factory
                .entry(id.as_str())
                .or_default()
                .push((*address, *block));
        }
        for (factory_id, values) in by_factory {
            self.store
                .append_factory_addresses(chain_id, factory_id, &values)
                .await?;
        }
        Ok(())
    }

    async fn fetch(&self, chain_id: u64, number: u64) -> Result<BlockData, EngineError> {
        let source = self
            .sources
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        source
            .block_data(number, self.fetch_traces)
            .await
            .map_err(EngineError::from)
    }

    async fn latest(&self, chain_id: u64) -> Result<u64, EngineError> {
        let source = self
            .sources
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?;
        source
            .latest_block_number()
            .await
            .map_err(EngineError::from)
    }

    async fn last_indexed(&self, chain_id: u64) -> Result<Option<BlockRef>, EngineError> {
        let state = self.runtime.lock().await;
        Ok(state
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?
            .last_indexed)
    }

    async fn runtime_local(&self, chain_id: u64) -> Result<Option<LocalChain>, EngineError> {
        let state = self.runtime.lock().await;
        Ok(state
            .get(&chain_id)
            .ok_or_else(|| unknown_chain(chain_id))?
            .local
            .clone())
    }

    async fn set_local(&self, chain_id: u64, local: Option<LocalChain>) {
        let mut state = self.runtime.lock().await;
        if let Some(runtime) = state.get_mut(&chain_id) {
            runtime.local = local;
        }
    }
}

enum Failure {
    User(String),
    Transient(String),
    Fatal(String),
    /// A dispatch bug, such as a missing registration that the engine should
    /// have filtered out. Treated as fatal.
    Internal(String),
}

fn classify(error: InvocationError<HandlerError>) -> Failure {
    match error {
        InvocationError::UnknownHandler(name) => {
            Failure::Internal(format!("no handler registered for {name:?}"))
        }
        InvocationError::WrongHandlerKind(name) => {
            Failure::Internal(format!("handler {name:?} has the wrong kind"))
        }
        InvocationError::Handler {
            source: HandlerError::User(message),
            ..
        } => Failure::User(message),
        InvocationError::Handler {
            source: HandlerError::Transient(message),
            ..
        } => Failure::Transient(message),
        InvocationError::Handler {
            source: HandlerError::Fatal(message),
            ..
        } => Failure::Fatal(message),
    }
}

fn backoff(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10) as u32;
    Duration::from_millis(100u64.saturating_mul(1u64 << exponent).min(30_000))
}

fn resume_block(checkpoint: &Checkpoint, start: u64) -> u64 {
    let next = if checkpoint.transaction_index == MAX_COMPONENT {
        checkpoint.block_number.saturating_add(1)
    } else {
        checkpoint.block_number
    };
    next.max(start)
}

/// Blocks per second and projected remaining seconds for a backfill.
fn progress_estimate(indexed: u64, remaining: u64, elapsed: Duration) -> (f64, Option<u64>) {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 || indexed == 0 {
        return (0.0, None);
    }
    let rate = indexed as f64 / seconds;
    let eta = if rate > 0.0 {
        Some((remaining as f64 / rate) as u64)
    } else {
        None
    };
    (rate, eta)
}

fn require_block(data: &BlockData, chain_id: u64) -> Result<BlockRef, EngineError> {
    data.block.ok_or_else(|| {
        EngineError::Chain(format!(
            "block data for chain {chain_id} does not contain a header"
        ))
    })
}

fn unknown_chain(chain_id: u64) -> EngineError {
    EngineError::User(format!("no plan or source for chain {chain_id}"))
}

fn log_factory(factory: &FactoryPlan) -> LogFactory {
    LogFactory {
        id: factory.id.clone(),
        range: factory.range,
        addresses: parent_value_filter(&factory.parent_addresses),
        event_selector: factory.event_selector,
        child_location: factory.child_location,
    }
}

/// [`LogFactory`] accepts value filters only. A parent that is itself a
/// factory child is checked against the child registry separately.
fn parent_value_filter(filter: &AddressFilter) -> ValueFilter<Address> {
    match filter {
        AddressFilter::Any => ValueFilter::Any,
        AddressFilter::One(address) => ValueFilter::One(*address),
        AddressFilter::AnyOf(addresses) => ValueFilter::AnyOf(addresses.clone()),
        AddressFilter::Factory(_) => ValueFilter::Any,
    }
}

fn parent_matches(
    filter: &AddressFilter,
    address: Address,
    block: u64,
    children: &ChildAddresses,
) -> Result<bool, EngineError> {
    match filter {
        AddressFilter::Any => Ok(true),
        AddressFilter::One(expected) => Ok(address == *expected),
        AddressFilter::AnyOf(expected) => Ok(expected.contains(&address)),
        AddressFilter::Factory(id) => children
            .contains(id, Some(address), block)
            .map_err(|error| EngineError::Chain(error.to_string())),
    }
}

fn contracts_map(plan: &ChainPlan) -> BTreeMap<String, Address> {
    let mut contracts = BTreeMap::new();
    for callback in &plan.callbacks {
        let Filter::Log(filter) = &callback.callback.filter else {
            continue;
        };
        let address = match &filter.address {
            AddressFilter::One(address) => Some(*address),
            AddressFilter::AnyOf(addresses) => addresses.first().copied(),
            _ => None,
        };
        if let Some(address) = address {
            contracts
                .entry(callback.callback.source_id.clone())
                .or_insert(address);
        }
    }
    contracts
}

fn filter_factory_ids(filter: &Filter) -> Vec<String> {
    let mut ids = Vec::new();
    match filter {
        Filter::Block(_) => {}
        Filter::Log(filter) => collect_factory_ids(&filter.address, &mut ids),
        Filter::Transaction(filter) => {
            collect_factory_ids(&filter.from, &mut ids);
            collect_factory_ids(&filter.to, &mut ids);
        }
        Filter::Trace(filter) => {
            collect_factory_ids(&filter.from, &mut ids);
            collect_factory_ids(&filter.to, &mut ids);
        }
        Filter::Transfer(filter) => {
            collect_factory_ids(&filter.from, &mut ids);
            collect_factory_ids(&filter.to, &mut ids);
        }
    }
    ids
}

fn collect_factory_ids(filter: &AddressFilter, ids: &mut Vec<String>) {
    if let AddressFilter::Factory(id) = filter {
        ids.push(id.clone());
    }
}

/// One factory plan with its resolved matcher and parent address filter.
#[derive(Clone)]
struct FactoryRuntime {
    matcher: LogFactory,
    parent: AddressFilter,
}

/// Records child addresses emitted in this block before events are built, so
/// children are visible to the whole block.
///
/// Returns every discovery in log order as `(factory id, address, block)`, so
/// the caller can persist them once the block commits.
fn discover_factories(
    factories: &[FactoryRuntime],
    logs: &[LogRef],
    runtime: &mut ChainRuntime,
) -> Result<Vec<(String, Address, u64)>, EngineError> {
    let mut discovered = Vec::new();
    for log in logs {
        for factory in factories {
            let matcher = &factory.matcher;
            if !matcher.matches(&log.log)
                || !parent_matches(
                    &factory.parent,
                    log.log.address,
                    log.log.block_number,
                    &runtime.children,
                )?
            {
                continue;
            }
            let address = matcher.child_address(&log.log).map_err(|error| {
                EngineError::Chain(format!(
                    "factory {:?} emitted an invalid child address in block {}: {error}",
                    matcher.id, log.log.block_number
                ))
            })?;
            runtime
                .children
                .insert(&matcher.id, address, log.log.block_number)
                .map_err(|error| EngineError::Chain(error.to_string()))?;
            tracing::debug!(
                factory = %matcher.id,
                address = %address.to_hex(),
                block = log.log.block_number,
                "discovered child contract"
            );
            runtime
                .discoveries
                .entry(log.log.block_number)
                .or_default()
                .push((matcher.id.clone(), address));
            discovered.push((matcher.id.clone(), address, log.log.block_number));
        }
    }
    Ok(discovered)
}

/// Rebuilds the child registry from discoveries at or before `block`.
fn rebuild_children(runtime: &mut ChainRuntime, ids: &BTreeSet<String>, block: u64) {
    let mut children = ChildAddresses::default();
    for id in ids {
        children.register_factory(id);
    }
    for (number, entries) in &runtime.discoveries {
        if *number > block {
            continue;
        }
        for (id, address) in entries {
            // The id was registered from the same plan that produced the
            // discovery, so this cannot fail; ignore the result rather than
            // panic if a caller hand-built a mismatched plan.
            let _ = children.insert(id, *address, *number);
        }
    }
    runtime.children = children;
    runtime.discoveries.retain(|number, _| *number <= block);
}
