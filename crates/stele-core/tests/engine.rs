//! End-to-end engine tests against [`stele_store::MemoryStore`] and a
//! scripted chain source.
//!
//! These tests exercise the integration layer rather than the individual
//! matching primitives: plans from configuration, ABI decoding, handler
//! dispatch inside a store batch, progress commits, factory discovery and
//! reorg rollback.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use stele_config::{
    AbiSource, AccountConfig, BlockConfig, ChainConfig, Config, ContractConfig, DatabaseConfig,
    FactoryConfig, FactoryParameter, OrderingMode,
};
use stele_core::events::Filter;
use stele_core::filter::ValueFilter;
use stele_core::handler::HandlerError;
use stele_core::{
    ChainError, ChainPlan, ChainSource, Engine, EngineError, EngineOptions, build_plans,
};
use stele_schema::{ColumnDef, ScalarType, SchemaDef, TableDef};
use stele_store::{CellValue, MemoryStore, Row, Store, TableRef};
use stele_types::{
    Address, B256, BlockData, BlockRef, Log, LogRef, ReceiptRef, Topic, TransactionRef,
};

const SCHEMA: &str = "public";
const TABLE: &str = "transfers";

fn token_address() -> Address {
    "0x00000000000000000000000000000000000000aa"
        .parse()
        .unwrap()
}

fn factory_address() -> Address {
    "0x00000000000000000000000000000000000000f0"
        .parse()
        .unwrap()
}

fn alice() -> Address {
    "0x00000000000000000000000000000000000000a1"
        .parse()
        .unwrap()
}

fn bob() -> Address {
    "0x00000000000000000000000000000000000000b2"
        .parse()
        .unwrap()
}

fn schema() -> SchemaDef {
    SchemaDef {
        tables: vec![TableDef {
            schema: SCHEMA.to_owned(),
            name: TABLE.to_owned(),
            columns: vec![
                ColumnDef::new("id", ScalarType::Text, false),
                ColumnDef::new("block", ScalarType::BigInt, false),
                ColumnDef::new("amount", ScalarType::Text, false),
            ],
            primary_key: vec!["id".to_owned()],
            indexes: vec![],
        }],
        enums: vec![],
        views: vec![],
    }
}

fn table() -> TableRef {
    TableRef::new(SCHEMA, TABLE)
}

fn key(id: &str) -> Row {
    let mut row = Row::new();
    row.insert("id".to_owned(), CellValue::Text(id.to_owned()));
    row
}

/// A chain source backed by an in-memory block map.
struct FakeChainSource {
    blocks: Mutex<BTreeMap<u64, BlockData>>,
}

impl FakeChainSource {
    fn new(blocks: impl IntoIterator<Item = BlockData>) -> Self {
        Self {
            blocks: Mutex::new(
                blocks
                    .into_iter()
                    .map(|data| (data.block.expect("test blocks have headers").number, data))
                    .collect(),
            ),
        }
    }

    fn replace(&self, data: BlockData) {
        let number = data.block.expect("test blocks have headers").number;
        self.blocks.lock().unwrap().insert(number, data);
    }
}

#[async_trait]
impl ChainSource for FakeChainSource {
    async fn latest_block_number(&self) -> Result<u64, ChainError> {
        Ok(self
            .blocks
            .lock()
            .unwrap()
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0))
    }

    async fn block_data(&self, number: u64, _fetch_traces: bool) -> Result<BlockData, ChainError> {
        self.blocks
            .lock()
            .unwrap()
            .get(&number)
            .cloned()
            .ok_or_else(|| ChainError::NotFound(format!("block {number}")))
    }

    async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, ChainError> {
        self.blocks
            .lock()
            .unwrap()
            .values()
            .filter_map(|data| data.block)
            .find(|block| block.hash == hash)
            .ok_or_else(|| ChainError::NotFound(format!("block {hash}")))
    }
}

fn chain_config() -> ChainConfig {
    ChainConfig {
        id: 1,
        rpc: vec!["https://eth.example".to_owned()],
        polling_interval_ms: 1_000,
        reorg_window_seconds: 180,
        cache_rpc_requests: true,
    }
}

fn database() -> DatabaseConfig {
    DatabaseConfig {
        url: "postgres://localhost/stele".to_owned(),
        schema: SCHEMA.to_owned(),
    }
}

fn token_abi() -> serde_json::Value {
    json!([{
        "type": "event",
        "name": "Transfer",
        "anonymous": false,
        "inputs": [
            { "name": "from", "type": "address", "indexed": true },
            { "name": "to", "type": "address", "indexed": true },
            { "name": "amount", "type": "uint256", "indexed": false }
        ]
    }])
}

fn token_config(start: u64, end: Option<u64>) -> Config {
    let mut chains = BTreeMap::new();
    chains.insert("mainnet".to_owned(), chain_config());

    let mut contracts = BTreeMap::new();
    contracts.insert(
        "Token".to_owned(),
        ContractConfig {
            abi: AbiSource::Inline(token_abi()),
            chain: "mainnet".to_owned(),
            address: Some(vec![token_address()]),
            factory: None,
            start_block: Some(start),
            end_block: end,
            events: Some(vec!["Transfer".to_owned()]),
            calls: None,
        },
    );

    Config {
        ordering: OrderingMode::Multichain,
        database: database(),
        chains,
        contracts,
        accounts: BTreeMap::<String, AccountConfig>::new(),
        blocks: BTreeMap::<String, BlockConfig>::new(),
    }
}

fn plans(config: &Config) -> Vec<ChainPlan> {
    build_plans(config)
        .expect("configuration produces plans")
        .plans
}

fn transfer_selector(plan: &ChainPlan) -> Topic {
    plan.callbacks
        .iter()
        .find(|callback| callback.callback.name == "Token:Transfer")
        .and_then(|callback| match &callback.callback.filter {
            Filter::Log(filter) => match filter.topics[0] {
                ValueFilter::One(topic) => Some(topic),
                _ => None,
            },
            _ => None,
        })
        .expect("Transfer callback carries its selector")
}

fn word_address(address: Address) -> Topic {
    let mut word = [0; 32];
    word[12..].copy_from_slice(address.as_bytes());
    Topic(word)
}

fn amount_word(amount: u128) -> Vec<u8> {
    let mut word = [0; 32];
    word[16..].copy_from_slice(&amount.to_be_bytes());
    word.to_vec()
}

fn block_hash(value: u8) -> B256 {
    B256([value; 32])
}

/// Builds one block with a single successful ERC-20-like transfer.
#[allow(clippy::too_many_arguments)]
fn transfer_block(
    number: u64,
    timestamp: u64,
    hash: B256,
    parent_hash: B256,
    selector: Topic,
    amount: u128,
) -> BlockData {
    BlockData {
        block: Some(BlockRef {
            number,
            hash,
            parent_hash,
            timestamp,
        }),
        transactions: vec![TransactionRef {
            block_number: number,
            transaction_index: 0,
            hash: block_hash(number as u8),
            from: alice(),
            to: Some(token_address()),
        }],
        receipts: vec![ReceiptRef {
            block_number: number,
            transaction_index: 0,
            from: alice(),
            to: Some(token_address()),
            success: true,
        }],
        traces: vec![],
        logs: vec![LogRef {
            transaction_index: 0,
            log_index: 0,
            log: Log {
                block_number: number,
                address: token_address(),
                topics: vec![selector, word_address(alice()), word_address(bob())],
                data: amount_word(amount),
            },
        }],
    }
}

type Recorded = Arc<Mutex<Vec<(u64, u64, String)>>>;

fn transfer_handlers(recorded: Recorded) -> Arc<stele_core::HandlerSet> {
    let mut handlers = stele_core::HandlerSet::new();
    handlers
        .register_event("Token:Transfer".to_owned(), move |context, event| {
            let recorded = Arc::clone(&recorded);
            let table = table();
            let id = format!(
                "{}:{}",
                event.block.number,
                event.log.as_ref().map(|log| log.log_index).unwrap_or(0)
            );
            let key = key(&id);
            let block_number = event.block.number;
            let log_index = event.log.as_ref().map(|log| log.log_index).unwrap_or(0);
            let amount = event.args["amount"]
                .as_str()
                .expect("decoded amount is a string")
                .to_owned();
            let mut value = Row::new();
            value.insert("block".to_owned(), CellValue::Int(block_number as i64));
            value.insert("amount".to_owned(), CellValue::Text(amount.clone()));
            Box::pin(async move {
                context
                    .store
                    .insert(&table, &key, &value)
                    .await
                    .map_err(|error| HandlerError::Fatal(error.to_string()))?;
                recorded
                    .lock()
                    .unwrap()
                    .push((block_number, log_index, amount));
                Ok(())
            })
        })
        .unwrap();
    Arc::new(handlers)
}

fn build_engine(
    store: Arc<dyn Store>,
    source: Arc<dyn ChainSource>,
    plans: Vec<ChainPlan>,
    handlers: Arc<stele_core::HandlerSet>,
    follow: bool,
) -> Engine {
    let mut sources = BTreeMap::new();
    sources.insert(1, source);
    Engine::new(
        store,
        sources,
        plans,
        handlers,
        EngineOptions {
            follow,
            fetch_traces: false,
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn backfill_writes_rows_advances_progress_and_is_idempotent() {
    let config = token_config(1, Some(3));
    let plans = plans(&config);
    let selector = transfer_selector(&plans[0]);

    let source = Arc::new(FakeChainSource::new([
        transfer_block(1, 100, block_hash(1), B256::ZERO, selector, 100),
        transfer_block(2, 200, block_hash(2), block_hash(1), selector, 200),
        transfer_block(3, 300, block_hash(3), block_hash(2), selector, 300),
    ]));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let engine = build_engine(
        Arc::clone(&store),
        source.clone(),
        plans.clone(),
        transfer_handlers(Arc::clone(&recorded)),
        false,
    );

    engine.run().await.expect("backfill succeeds");

    assert_eq!(
        *recorded.lock().unwrap(),
        [
            (1, 0, "100".to_owned()),
            (2, 0, "200".to_owned()),
            (3, 0, "300".to_owned()),
        ]
    );
    let progress = store.progress(1).await.unwrap().expect("progress recorded");
    assert_eq!(
        progress.checkpoint,
        stele_store::Checkpoint::block_end(300, 1, 3)
    );
    assert_eq!(engine.metrics().events_processed(), 3);
    assert_eq!(engine.metrics().decode_failures(), 0);

    let row = store.find(&table(), &key("2:0")).await.unwrap().unwrap();
    assert_eq!(row["amount"], CellValue::Text("200".to_owned()));

    // A second run resumes after the committed block. Nothing advances, so the
    // handler is not called again and the store rejects no batches.
    let second = build_engine(
        Arc::clone(&store),
        source,
        plans,
        transfer_handlers(Arc::clone(&recorded)),
        false,
    );
    second.run().await.expect("second backfill is a no-op");
    assert_eq!(recorded.lock().unwrap().len(), 3);
    assert_eq!(second.metrics().events_processed(), 0);
}

#[tokio::test]
async fn reorg_reverts_rows_and_reindexes_the_replacement_chain() {
    let config = token_config(1, Some(3));
    let plans = plans(&config);
    let selector = transfer_selector(&plans[0]);

    let block1 = transfer_block(1, 100, block_hash(1), B256::ZERO, selector, 100);
    let source = Arc::new(FakeChainSource::new([
        block1.clone(),
        transfer_block(2, 200, block_hash(2), block_hash(1), selector, 200),
        transfer_block(3, 300, block_hash(3), block_hash(2), selector, 300),
    ]));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let engine = build_engine(
        Arc::clone(&store),
        source.clone(),
        plans,
        Arc::clone(&transfer_handlers(Arc::clone(&recorded))),
        false,
    );
    engine.run_backfill().await.expect("backfill succeeds");
    assert_eq!(recorded.lock().unwrap().len(), 3);

    // Blocks 2 and 3 are replaced by a different history at the same heights.
    let replacement2 = transfer_block(2, 210, block_hash(0x12), block_hash(1), selector, 220);
    let replacement3 = transfer_block(3, 310, block_hash(0x13), block_hash(0x12), selector, 330);
    source.replace(replacement2.clone());
    source.replace(replacement3.clone());

    let ancestor = block1.block.expect("block header");
    engine
        .apply_reorg(1, &ancestor)
        .await
        .expect("revert to the common ancestor");
    assert_eq!(engine.metrics().reorgs(), 1);
    assert!(
        store.find(&table(), &key("1:0")).await.unwrap().is_some(),
        "the common ancestor keeps its rows"
    );
    assert!(store.find(&table(), &key("2:0")).await.unwrap().is_none());
    assert!(store.find(&table(), &key("3:0")).await.unwrap().is_none());
    // The store is reverted; the test's own handler log is not store state.
    recorded.lock().unwrap().clear();

    engine
        .process_block(1, replacement2)
        .await
        .expect("replacement block 2 is indexed");
    engine
        .process_block(1, replacement3)
        .await
        .expect("replacement block 3 is indexed");

    assert_eq!(
        *recorded.lock().unwrap(),
        [(2, 0, "220".to_owned()), (3, 0, "330".to_owned())]
    );
    let progress = store.progress(1).await.unwrap().expect("progress recorded");
    assert_eq!(
        progress.checkpoint,
        stele_store::Checkpoint::block_end(310, 1, 3)
    );
}

/// Configuration with a `Factory` that emits `PoolCreated` and a `Pool`
/// contract discovered from it.
fn factory_config(end: Option<u64>) -> Config {
    let mut chains = BTreeMap::new();
    chains.insert("mainnet".to_owned(), chain_config());

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
            address: Some(vec![factory_address()]),
            factory: None,
            start_block: Some(1),
            end_block: end,
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
            start_block: Some(1),
            end_block: end,
            events: None,
            calls: None,
        },
    );

    Config {
        ordering: OrderingMode::Multichain,
        database: database(),
        chains,
        contracts,
        accounts: BTreeMap::<String, AccountConfig>::new(),
        blocks: BTreeMap::<String, BlockConfig>::new(),
    }
}

fn pool_address(value: u8) -> Address {
    Address([value; 20])
}

/// Returns the `PoolCreated` and `Swap` topic selectors of a factory plan.
fn factory_selectors(plan: &ChainPlan) -> (Topic, Topic) {
    let factory_selector = plan.factories[0].event_selector;
    let swap_selector = plan
        .callbacks
        .iter()
        .find(|callback| callback.callback.name == "Pool:Swap")
        .and_then(|callback| match &callback.callback.filter {
            Filter::Log(filter) => match filter.topics[0] {
                ValueFilter::One(topic) => Some(topic),
                _ => None,
            },
            _ => None,
        })
        .expect("Swap selector");
    (factory_selector, swap_selector)
}

/// Builds a block whose only log is a `PoolCreated` emitted by the factory.
fn pool_created_block(
    number: u64,
    timestamp: u64,
    hash: B256,
    parent_hash: B256,
    selector: Topic,
    pool: Address,
) -> BlockData {
    BlockData {
        block: Some(BlockRef {
            number,
            hash,
            parent_hash,
            timestamp,
        }),
        transactions: vec![TransactionRef {
            block_number: number,
            transaction_index: 0,
            hash: block_hash(number as u8),
            from: alice(),
            to: Some(factory_address()),
        }],
        receipts: vec![ReceiptRef {
            block_number: number,
            transaction_index: 0,
            from: alice(),
            to: Some(factory_address()),
            success: true,
        }],
        traces: vec![],
        logs: vec![LogRef {
            transaction_index: 0,
            log_index: 0,
            log: Log {
                block_number: number,
                address: factory_address(),
                topics: vec![selector, word_address(pool)],
                data: word_address(token_address()).0.to_vec(),
            },
        }],
    }
}

/// Appends a `Swap` log emitted by `pool` to `data`.
fn push_swap(data: &mut BlockData, selector: Topic, pool: Address, amount: u128) {
    let number = data.block.expect("test blocks have headers").number;
    let log_index = data.logs.len() as u64;
    data.logs.push(LogRef {
        transaction_index: 0,
        log_index,
        log: Log {
            block_number: number,
            address: pool,
            topics: vec![selector, word_address(alice())],
            data: amount_word(amount),
        },
    });
}

/// Builds a block whose only log is a `Swap` emitted by `pool`.
fn swap_block(
    number: u64,
    timestamp: u64,
    hash: B256,
    parent_hash: B256,
    selector: Topic,
    pool: Address,
    amount: u128,
) -> BlockData {
    let mut data = BlockData {
        block: Some(BlockRef {
            number,
            hash,
            parent_hash,
            timestamp,
        }),
        transactions: vec![TransactionRef {
            block_number: number,
            transaction_index: 0,
            hash: block_hash(number as u8),
            from: alice(),
            to: Some(pool),
        }],
        receipts: vec![ReceiptRef {
            block_number: number,
            transaction_index: 0,
            from: alice(),
            to: Some(pool),
            success: true,
        }],
        traces: vec![],
        logs: vec![],
    };
    push_swap(&mut data, selector, pool, amount);
    data
}

/// Records each `Pool:Swap` under the single key `swap`.
fn swap_handlers(recorded: Recorded) -> Arc<stele_core::HandlerSet> {
    let mut handlers = stele_core::HandlerSet::new();
    handlers
        .register_event("Pool:Swap".to_owned(), move |context, event| {
            let recorded = Arc::clone(&recorded);
            let table = table();
            let key = key("swap");
            let block_number = event.block.number;
            let log_index = event.log.as_ref().map(|log| log.log_index).unwrap_or(0);
            let amount = event.args["amount0"]
                .as_str()
                .expect("decoded amount is a string")
                .to_owned();
            let mut value = Row::new();
            value.insert("block".to_owned(), CellValue::Int(block_number as i64));
            value.insert("amount".to_owned(), CellValue::Text(amount.clone()));
            Box::pin(async move {
                context
                    .store
                    .insert(&table, &key, &value)
                    .await
                    .map_err(|error| HandlerError::Fatal(error.to_string()))?;
                recorded
                    .lock()
                    .unwrap()
                    .push((block_number, log_index, amount));
                Ok(())
            })
        })
        .unwrap();
    Arc::new(handlers)
}

#[tokio::test]
async fn factory_children_are_visible_in_the_discovery_block() {
    let plans = plans(&factory_config(None));
    let (factory_selector, swap_selector) = factory_selectors(&plans[0]);
    let pool = pool_address(0xc0);

    let mut data = pool_created_block(1, 100, block_hash(1), B256::ZERO, factory_selector, pool);
    push_swap(&mut data, swap_selector, pool, 77);

    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));

    let engine = build_engine(
        Arc::clone(&store),
        Arc::new(FakeChainSource::new([data])),
        plans,
        swap_handlers(Arc::clone(&recorded)),
        false,
    );
    engine.run().await.expect("factory block is indexed");

    assert_eq!(
        *recorded.lock().unwrap(),
        [(1, 1, "77".to_owned())],
        "the child address discovered in this block is visible to its Swap log"
    );
    assert_eq!(engine.metrics().events_processed(), 1);
    let row = store.find(&table(), &key("swap")).await.unwrap().unwrap();
    assert_eq!(row["amount"], CellValue::Text("77".to_owned()));
    assert_eq!(
        store.factory_addresses(1, "Pool").await.unwrap(),
        [(pool, 1)],
        "discoveries are persisted with the block that created them"
    );
}

#[tokio::test]
async fn factory_children_survive_a_restart() {
    let pool = pool_address(0xc0);
    let first_plans = plans(&factory_config(Some(1)));
    let (factory_selector, swap_selector) = factory_selectors(&first_plans[0]);

    let source = Arc::new(FakeChainSource::new([
        pool_created_block(1, 100, block_hash(1), B256::ZERO, factory_selector, pool),
        swap_block(
            2,
            200,
            block_hash(2),
            block_hash(1),
            swap_selector,
            pool,
            77,
        ),
    ]));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));

    // The first run stops after block 1, before the child emitted anything.
    let first = build_engine(
        Arc::clone(&store),
        Arc::clone(&source) as Arc<dyn ChainSource>,
        first_plans,
        swap_handlers(Arc::clone(&recorded)),
        false,
    );
    first.run().await.expect("backfill of block 1 succeeds");
    assert_eq!(first.metrics().events_processed(), 0);
    assert!(
        recorded.lock().unwrap().is_empty(),
        "block 1 contains no Swap"
    );
    assert!(store.find(&table(), &key("swap")).await.unwrap().is_none());
    assert_eq!(
        store.factory_addresses(1, "Pool").await.unwrap(),
        [(pool, 1)],
        "the discovery is persisted by the block that produced it"
    );

    // A new engine resumes at block 2 without replaying block 1. It only
    // recognizes `pool` because the address was loaded from the store.
    let second = build_engine(
        Arc::clone(&store),
        source,
        plans(&factory_config(Some(2))),
        swap_handlers(Arc::clone(&recorded)),
        false,
    );
    second.run().await.expect("restart indexes block 2");

    assert_eq!(*recorded.lock().unwrap(), [(2, 0, "77".to_owned())]);
    assert_eq!(second.metrics().events_processed(), 1);
    let row = store
        .find(&table(), &key("swap"))
        .await
        .unwrap()
        .expect("the Swap from the persisted child is indexed");
    assert_eq!(row["amount"], CellValue::Text("77".to_owned()));
}

#[tokio::test]
async fn reorg_removes_persisted_factory_children_above_the_ancestor() {
    let first_pool = pool_address(0xc0);
    let second_pool = pool_address(0xc1);
    let plans = plans(&factory_config(Some(2)));
    let (factory_selector, swap_selector) = factory_selectors(&plans[0]);

    let block1 = pool_created_block(
        1,
        100,
        block_hash(1),
        B256::ZERO,
        factory_selector,
        first_pool,
    );
    let mut block2 = pool_created_block(
        2,
        200,
        block_hash(2),
        block_hash(1),
        factory_selector,
        second_pool,
    );
    push_swap(&mut block2, swap_selector, first_pool, 55);

    let source = Arc::new(FakeChainSource::new([block1.clone(), block2]));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let engine = build_engine(
        Arc::clone(&store),
        source,
        plans,
        swap_handlers(Arc::clone(&recorded)),
        false,
    );
    engine.run().await.expect("backfill succeeds");
    assert_eq!(
        store.factory_addresses(1, "Pool").await.unwrap(),
        [(first_pool, 1), (second_pool, 2)]
    );
    recorded.lock().unwrap().clear();

    let ancestor = block1.block.expect("block header");
    engine
        .apply_reorg(1, &ancestor)
        .await
        .expect("revert to the common ancestor");

    assert_eq!(
        store.factory_addresses(1, "Pool").await.unwrap(),
        [(first_pool, 1)],
        "children created above the ancestor are dropped from the store"
    );
    assert!(store.find(&table(), &key("swap")).await.unwrap().is_none());

    // The surviving child is still recognized after the in-memory rebuild.
    let replacement = swap_block(
        2,
        210,
        block_hash(0x22),
        block_hash(1),
        swap_selector,
        first_pool,
        66,
    );
    engine
        .process_block(1, replacement)
        .await
        .expect("replacement block 2 is indexed");
    assert_eq!(*recorded.lock().unwrap(), [(2, 0, "66".to_owned())]);
    assert_eq!(engine.metrics().reorgs(), 1);
}

#[tokio::test]
async fn decode_failures_are_counted_and_skipped() {
    let config = token_config(1, Some(1));
    let plans = plans(&config);
    let selector = transfer_selector(&plans[0]);

    let mut data = transfer_block(1, 100, block_hash(1), B256::ZERO, selector, 100);
    data.logs.push(LogRef {
        transaction_index: 0,
        log_index: 1,
        log: Log {
            block_number: 1,
            address: token_address(),
            topics: vec![selector, word_address(alice()), word_address(bob())],
            // A Transfer body needs one word; this is empty.
            data: vec![],
        },
    });

    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let engine = build_engine(
        Arc::clone(&store),
        Arc::new(FakeChainSource::new([data])),
        plans,
        transfer_handlers(Arc::clone(&recorded)),
        false,
    );
    engine.run().await.expect("backfill skips undecodable logs");

    assert_eq!(*recorded.lock().unwrap(), [(1, 0, "100".to_owned())]);
    assert_eq!(engine.metrics().decode_failures(), 1);
    assert_eq!(engine.metrics().events_processed(), 1);
}

#[tokio::test]
async fn transient_handler_errors_are_retried_and_user_errors_stop() {
    let config = token_config(1, Some(1));
    let plans = plans(&config);
    let selector = transfer_selector(&plans[0]);
    let data = transfer_block(1, 100, block_hash(1), B256::ZERO, selector, 100);

    let attempts = Arc::new(AtomicUsize::new(0));
    let mut handlers = stele_core::HandlerSet::new();
    let counter = Arc::clone(&attempts);
    handlers
        .register_event("Token:Transfer".to_owned(), move |_, _| {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    Err(HandlerError::Transient("try again".to_owned()))
                } else {
                    Ok(())
                }
            })
        })
        .unwrap();
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let engine = build_engine(
        Arc::clone(&store),
        Arc::new(FakeChainSource::new([data.clone()])),
        plans.clone(),
        Arc::new(handlers),
        false,
    );
    engine.run().await.expect("transient failure is retried");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(engine.metrics().events_processed(), 1);

    let mut handlers = stele_core::HandlerSet::new();
    handlers
        .register_event("Token:Transfer".to_owned(), |_, _| {
            Box::pin(async { Err(HandlerError::User("bad input".to_owned())) })
        })
        .unwrap();
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let engine = build_engine(
        Arc::clone(&store),
        Arc::new(FakeChainSource::new([data])),
        plans,
        Arc::new(handlers),
        false,
    );
    assert!(matches!(
        engine.run().await,
        Err(EngineError::User(message)) if message == "bad input"
    ));
    assert!(
        store.progress(1).await.unwrap().is_none(),
        "a failed block does not advance progress"
    );
}

/// A source that scripts one same-height reorg and then fails, terminating the
/// otherwise unbounded live loop.
struct LiveSource {
    blocks: Mutex<BTreeMap<u64, BlockData>>,
    polls: AtomicUsize,
    selector: Topic,
}

impl LiveSource {
    fn new(blocks: impl IntoIterator<Item = BlockData>, selector: Topic) -> Self {
        Self {
            blocks: Mutex::new(
                blocks
                    .into_iter()
                    .map(|data| (data.block.expect("test blocks have headers").number, data))
                    .collect(),
            ),
            polls: AtomicUsize::new(0),
            selector,
        }
    }
}

#[async_trait]
impl ChainSource for LiveSource {
    async fn latest_block_number(&self) -> Result<u64, ChainError> {
        let poll = self.polls.fetch_add(1, Ordering::SeqCst);
        match poll {
            0 | 1 => Ok(3),
            2 => {
                let parent = self
                    .blocks
                    .lock()
                    .unwrap()
                    .get(&2)
                    .and_then(|data| data.block)
                    .expect("block 2 exists");
                let replacement =
                    transfer_block(3, 310, block_hash(0x23), parent.hash, self.selector, 999);
                self.blocks.lock().unwrap().insert(3, replacement);
                Ok(3)
            }
            3 => Ok(3),
            _ => Err(ChainError::Other("script finished".to_owned())),
        }
    }

    async fn block_data(&self, number: u64, _fetch_traces: bool) -> Result<BlockData, ChainError> {
        self.blocks
            .lock()
            .unwrap()
            .get(&number)
            .cloned()
            .ok_or_else(|| ChainError::NotFound(format!("block {number}")))
    }

    async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, ChainError> {
        self.blocks
            .lock()
            .unwrap()
            .values()
            .filter_map(|data| data.block)
            .find(|block| block.hash == hash)
            .ok_or_else(|| ChainError::NotFound(format!("block {hash}")))
    }
}

#[tokio::test]
async fn live_following_indexes_blocks_and_reverts_a_reorg() {
    let config = token_config(1, None);
    let plans = plans(&config);
    let selector = transfer_selector(&plans[0]);
    let source = Arc::new(LiveSource::new(
        [
            transfer_block(1, 100, block_hash(1), B256::ZERO, selector, 100),
            transfer_block(2, 200, block_hash(2), block_hash(1), selector, 200),
            transfer_block(3, 300, block_hash(3), block_hash(2), selector, 300),
        ],
        selector,
    ));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new(schema()));
    let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
    let mut sources: BTreeMap<u64, Arc<dyn ChainSource>> = BTreeMap::new();
    sources.insert(1, source);
    let engine = Engine::new(
        Arc::clone(&store),
        sources,
        plans,
        transfer_handlers(Arc::clone(&recorded)),
        EngineOptions {
            follow: true,
            fetch_traces: false,
            poll_interval: std::time::Duration::from_millis(1),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let result = engine.run().await;
    assert!(
        matches!(result, Err(EngineError::Chain(_))),
        "the scripted source terminates the live loop: {result:?}"
    );
    assert_eq!(engine.metrics().reorgs(), 1);
    assert_eq!(
        store.find(&table(), &key("3:0")).await.unwrap().unwrap()["amount"],
        CellValue::Text("999".to_owned()),
        "the replacement block replaces the reverted row"
    );
    assert_eq!(
        store.find(&table(), &key("1:0")).await.unwrap().unwrap()["amount"],
        CellValue::Text("100".to_owned())
    );
    assert_eq!(
        store.find(&table(), &key("2:0")).await.unwrap().unwrap()["amount"],
        CellValue::Text("200".to_owned())
    );
}
