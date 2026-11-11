# Rust 实现接口契约

本文是并行实现的唯一接口基准。各 crate 的内部实现可以调整，但跨 crate 的公开类型和方法签名必须与本文一致；需要变更时先改本文。

## 0. 依赖方向

```text
stele-types        (无内部依赖)
stele-checkpoint   (无内部依赖)
stele-schema       -> stele-types
stele-config       -> stele-types
stele-sync         -> stele-types, stele-checkpoint
stele-store        -> stele-types, stele-schema, stele-checkpoint
stele-api          -> stele-types, stele-schema, stele-store, stele-checkpoint
stele-core         -> 以上全部
stele              -> stele-core, stele-store, stele-schema, stele-config, stele-sync
stele-cli          -> stele
stele-macros       -> (编译期，无内部依赖)
```

## 1. stele-types

已实现。所有 crate 必须使用这里的类型：

```rust
pub struct Address(pub [u8; 20]);
pub struct Topic(pub [u8; 32]);
pub struct B256(pub [u8; 32]);
pub struct BlockRef { pub number: u64, pub hash: B256, pub parent_hash: B256, pub timestamp: u64 }
pub struct TransactionRef { pub block_number: u64, pub transaction_index: u64, pub hash: B256, pub from: Address, pub to: Option<Address> }
pub struct ReceiptRef { pub block_number: u64, pub transaction_index: u64, pub from: Address, pub to: Option<Address>, pub success: bool }
pub enum CallType { Call, CallCode, DelegateCall, StaticCall, Create, Create2, SelfDestruct }
pub enum TraceOutcome { Successful, Reverted, Unknown }
pub struct Trace { pub from: Address, pub to: Option<Address>, pub call_type: CallType, pub input: Vec<u8>, pub value: Option<[u8; 32]>, pub output: Option<Vec<u8>>, pub outcome: TraceOutcome }
pub struct TraceRef { pub block_number: u64, pub transaction_index: u64, pub trace_address: Vec<u32>, pub trace: Trace }
pub struct Log { pub block_number: u64, pub address: Address, pub topics: Vec<Topic>, pub data: Vec<u8> }
pub struct LogRef { pub transaction_index: u64, pub log_index: u64, pub log: Log }
pub struct BlockData { pub block: Option<BlockRef>, pub transactions: Vec<TransactionRef>, pub receipts: Vec<ReceiptRef>, pub traces: Vec<TraceRef>, pub logs: Vec<LogRef> }
pub enum CellValue { Null, Bool(bool), Int(i64), Float(f64), Numeric(String), Text(String), Bytes(Vec<u8>), Json(serde_json::Value), Array(Vec<CellValue>) }
```

`Address`/`Topic`/`B256` 提供 `from_hex`、`to_hex`、`FromStr`、`Display`，serde 表示为小写 `0x` 字符串。

## 2. stele-config

```rust
pub enum OrderingMode { Omnichain, Multichain, Isolated }

pub struct DatabaseConfig { pub url: String, pub schema: String }

pub struct ChainConfig {
    pub id: u64,
    pub rpc: Vec<String>,
    pub polling_interval_ms: u64,
    pub reorg_window_seconds: u16,
    pub cache_rpc_requests: bool,
}

pub enum AbiSource { Path(String), Inline(serde_json::Value) }
impl AbiSource { pub fn load_json(&self) -> Result<serde_json::Value, ConfigError>; }

pub enum FactoryParameter {
    Nested(String),   // ABI 参数路径，例如 "pool"
    Topic(u8),        // topic 1..=3
    Offset(usize),    // data 字节偏移
}

pub struct FactoryConfig {
    pub parent: String,       // 已注册合约名
    pub event: String,        // 工厂事件名
    pub parameter: FactoryParameter,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
}

pub struct ContractConfig {
    pub abi: AbiSource,
    pub chain: String,
    pub address: Option<Vec<Address>>,  // factory 合约可为 None
    pub factory: Option<FactoryConfig>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub events: Option<Vec<String>>,    // None = ABI 内全部事件
}

pub struct AccountConfig {
    pub chain: String,
    pub address: Vec<Address>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub include_transactions: bool,  // 默认 true
    pub include_transfers: bool,     // 默认 true
}

pub struct BlockConfig {
    pub chain: String,
    pub interval: u64,
    pub offset: u64,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
}

pub struct Config {
    pub ordering: OrderingMode,
    pub database: DatabaseConfig,
    pub chains: BTreeMap<String, ChainConfig>,
    pub contracts: BTreeMap<String, ContractConfig>,
    pub accounts: BTreeMap<String, AccountConfig>,
    pub blocks: BTreeMap<String, BlockConfig>,
}

impl Config {
    pub fn from_toml_str(value: &str) -> Result<Self, ConfigError>;
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError>;
    pub fn validate(&self) -> Result<(), ConfigError>;
}
```

TOML 结构：

```toml
ordering = "multichain"
[database]
url = "postgres://localhost/stele"
schema = "public"

[chains.mainnet]
id = 1
rpc = ["https://..."]
polling_interval = 1000
reorg_window = 180
cache_rpc_requests = true

[contracts.Token]
abi = "abis/Token.json"
chain = "mainnet"
address = ["0x..."]
start_block = 1
end_block = 1000
events = ["Transfer", "Approval"]

[contracts.Pool]
abi = "abis/Pool.json"
chain = "mainnet"
start_block = 1
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = "pool"

[accounts.Alice]
chain = "mainnet"
address = ["0x..."]
start_block = 1

[blocks.hourly]
chain = "mainnet"
interval = 300
offset = 0
start_block = 1
```

`ordering` 接受 `"omnichain" | "multichain" | "isolated" | "experimental_isolated"`。

## 3. stele-schema

保留已有 `ScalarType`/`ColumnType`/`ColumnDef`/`TableDef`/`SchemaDef`/`quote_identifier`/`create_*_sql`。扩展：

```rust
pub enum ScalarType {
    Text, Integer, SmallInt, BigInt, Boolean, Real, DoublePrecision,
    Numeric { precision: u16, scale: u16 }, Jsonb, Bytes, Timestamp,
}

pub struct IndexDef { pub name: String, pub columns: Vec<String>, pub unique: bool }
pub struct ViewDef { pub name: String, pub sql: String }
pub struct EnumDef { pub name: String, pub values: Vec<String> }

// TableDef 增加：
pub indexes: Vec<IndexDef>,

// SchemaDef 增加：
pub enums: Vec<EnumDef>,
pub views: Vec<ViewDef>,

impl SchemaDef {
    pub fn create_statements(&self) -> Result<Vec<String>, SchemaError>;
    pub fn diff(&self, previous: &SchemaDef) -> Result<Vec<String>, SchemaError>;
}
```

`diff` 规则：新增表/列/索引/视图生成 `CREATE`；删除列或表生成 `DROP`；列类型变化生成 `ALTER COLUMN TYPE`；nullable 变化生成 `SET/DROP NOT NULL`。无法安全表达的变化返回 `SchemaError::UnsupportedChange`，不静默忽略。

## 4. stele-sync

```rust
pub struct RpcClient;
impl RpcClient {
    pub fn new(rpc_urls: Vec<String>) -> Result<Self, SyncError>;
    pub async fn chain_id(&self) -> Result<u64, SyncError>;
    pub async fn latest_block_number(&self) -> Result<u64, SyncError>;
    pub async fn block_by_number(&self, number: u64) -> Result<BlockRef, SyncError>;
    pub async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, SyncError>;
    /// 拉取一个块的全部数据：完整交易、回执、logs，以及可选 traces。
    pub async fn block_data(&self, number: u64, fetch_traces: bool) -> Result<BlockData, SyncError>;
    /// 按区间拉取 logs。`topics` 是每一层的 OR 列表；空列表表示通配。
    pub async fn logs(&self, range: BlockRange, addresses: Option<&[Address]>, topics: &[Vec<Topic>]) -> Result<Vec<LogRef>, SyncError>;
}

pub struct SyncStore;
impl SyncStore {
    pub async fn connect(url: &str, schema: &str) -> Result<Self, SyncError>;
    pub async fn initialize(&self) -> Result<(), SyncError>;
    pub async fn insert_block_data(&self, chain_id: u64, data: &BlockData) -> Result<(), SyncError>;
    pub async fn insert_intervals(&self, chain_id: u64, fragment_id: &str, ranges: &BlockRangeSet) -> Result<(), SyncError>;
    pub async fn get_intervals(&self, chain_id: u64, fragment_id: &str) -> Result<BlockRangeSet, SyncError>;
    pub async fn append_factory_addresses(&self, chain_id: u64, factory_id: &str, values: &[(Address, u64)]) -> Result<(), SyncError>;
    pub async fn factory_addresses(&self, chain_id: u64, factory_id: &str) -> Result<Vec<(Address, u64)>, SyncError>;
}
```

`SyncError` 用 `thiserror`，至少包含 `Transport(String)`、`Rpc { code: i64, message: String }`、`InvalidResponse(String)`、`Database(sqlx::Error)`。

`block_data` 要求：
- 使用 `eth_getBlockByNumber`（完整交易）、`eth_getTransactionReceipt`（可批量/并发）、`eth_getLogs`、`debug_traceBlockByNumber`（callTracer，可选）。
- 所有地址小写归一化；logs 按 `(transaction_index, log_index)` 排序；交易按 index 排序。
- traces 展平嵌套 callTracer 结果，`trace_address` 记录路径；父帧 revert 时子帧 `outcome = Reverted`。
- 多 RPC URL 轮换与重试（至少 3 次，指数退避）。

`SyncStore` 使用 schema 参数限定表名，表至少包含：`blocks`、`transactions`、`transaction_receipts`、`traces`、`logs`、`intervals`、`factories`、`factory_addresses`。主键与冲突处理必须幂等。

## 5. stele-store

```rust
pub struct TableRef { pub schema: String, pub name: String }
pub type Row = BTreeMap<String, CellValue>;

pub struct Progress { pub checkpoint: Checkpoint, pub finalized: Checkpoint }

#[async_trait::async_trait]
pub trait IndexingStore: Send + Sync {
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError>;
    async fn insert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;
    async fn update(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;
    async fn upsert(&self, table: &TableRef, key: &Row, value: &Row) -> Result<(), StoreError>;
    async fn delete(&self, table: &TableRef, key: &Row) -> Result<(), StoreError>;
    async fn raw(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError>;
}

#[async_trait::async_trait]
pub trait StoreBatch: IndexingStore {
    async fn commit(self: Box<Self>) -> Result<(), StoreError>;
    async fn rollback(self: Box<Self>) -> Result<(), StoreError>;
}

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn begin(&self, chain_id: u64, checkpoint: Checkpoint) -> Result<Box<dyn StoreBatch>, StoreError>;
    async fn progress(&self, chain_id: u64) -> Result<Option<Progress>, StoreError>;
    async fn revert(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError>;
    async fn finalize(&self, chain_id: u64, target: &Checkpoint) -> Result<(), StoreError>;
    async fn query(&self, sql: &str, params: &[CellValue]) -> Result<Vec<Row>, StoreError>;
    async fn find(&self, table: &TableRef, key: &Row) -> Result<Option<Row>, StoreError>;
    /// Factory child addresses persisted across restarts, ordered by creation block.
    async fn factory_addresses(&self, chain_id: u64, factory_id: &str) -> Result<Vec<(Address, u64)>, StoreError>;
    /// Inserts addresses idempotently; an existing address keeps the earliest block.
    async fn append_factory_addresses(&self, chain_id: u64, factory_id: &str, values: &[(Address, u64)]) -> Result<(), StoreError>;
    /// Removes discoveries above a reorg ancestor.
    async fn remove_factory_addresses_after(&self, chain_id: u64, factory_id: &str, block_number: u64) -> Result<(), StoreError>;
}

pub struct PostgresStore;
impl PostgresStore {
    pub async fn connect(url: &str) -> Result<Self, StoreError>;
    pub async fn initialize(&self, schema: &SchemaDef) -> Result<(), StoreError>;
}
impl Store for PostgresStore { /* ... */ }

pub struct MemoryStore;
impl MemoryStore {
    pub fn new(schema: SchemaDef) -> Self;
}
impl Store for MemoryStore { /* ... */ }
```

要求：
- 用户表使用 schema 中的真实列，动态 SQL；所有标识符通过 `stele-schema::quote_identifier`。
- `begin` 获取该链进度行锁；checkpoint 必须大于当前进度。
- 批次内写入进入缓存，`commit` 单事务完成：写 undo 记录、写行、写 checkpoint、推进进度。
- `revert` 恢复 target 之后所有 undo，删除其 checkpoint；`finalize` 丢弃 target 之前的 undo。
- undo 表按 `(chain_id, checkpoint, id)` 保存，必须能区分不同 schema 的同名表。
- `MemoryStore` 语义相同，用于无数据库测试。
- `raw` 只允许单条语句、无 DDL；参数化绑定，禁止字符串拼接。
- 列值绑定按列类型显式 cast：`$1::int4`、`$1::int8`、`$1::numeric`、`$1::text`、`$1::boolean`、`$1::float4`、`$1::float8`、`$1::jsonb`、`$1::bytea`、数组类型加 `[]`。

## 6. stele-core

现有 `events`、`filter`、`historical`、`indexing`、`progress`、`realtime` 保留。新增引擎：

```rust
#[async_trait::async_trait]
pub trait ChainSource: Send + Sync {
    async fn latest_block_number(&self) -> Result<u64, ChainError>;
    async fn block_data(&self, number: u64, fetch_traces: bool) -> Result<BlockData, ChainError>;
    async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, ChainError>;
}

pub struct DecodeSpec {
    pub event_name: String,
    pub abi: alloy_json_abi::JsonAbi,
    pub kind: DecodeKind,
}
pub enum DecodeKind { Log { event: String }, Function { function: String } }

pub struct CallbackPlan {
    pub callback: EventCallback,   // 现有类型
    pub decode: Option<DecodeSpec>,
}

pub struct FactoryPlan {
    pub id: String,
    pub parent_source: String,
    pub event: String,
    pub parameter: stele_config::FactoryParameter,
    pub range: filter::BlockRange,
    pub child_contract: String,
    // Added during implementation: derived from the parent ABI so the engine
    // can scan creation logs without re-reading the configuration.
    pub event_selector: Topic,
    pub parent_addresses: filter::AddressFilter,
    pub child_location: filter::ChildLocation,
}

pub struct ChainPlan {
    pub chain_id: u64,
    pub chain_name: String,
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub reorg_window_seconds: u64,
    pub callbacks: Vec<CallbackPlan>,
    pub factories: Vec<FactoryPlan>,
}

pub struct EngineOptions {
    pub ordering: stele_config::OrderingMode,
    pub batch_size: usize,
    pub fetch_traces: bool,
    pub follow: bool,
    pub poll_interval: std::time::Duration,
    pub max_retries: usize,
}

pub struct DecodedEvent {
    pub name: String,
    pub source_id: String,
    pub chain_id: u64,
    pub checkpoint: Checkpoint,
    pub block: BlockRef,
    pub transaction: Option<TransactionRef>,
    pub receipt: Option<ReceiptRef>,
    pub trace: Option<TraceRef>,
    pub log: Option<LogRef>,
    pub args: serde_json::Value,
    pub result: serde_json::Value,
}

pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'a>>;
pub struct HandlerSet;
impl HandlerSet {
    pub fn register_setup<F>(&mut self, key: String, handler: F) -> Result<(), DuplicateHandler>
    where F: for<'a> Fn(&'a EventContext<'_>) -> HandlerFuture<'a> + Send + Sync + 'static;
    pub fn register_event<F>(&mut self, key: String, handler: F) -> Result<(), DuplicateHandler>
    where F: for<'a> Fn(&'a EventContext<'_>, &'a DecodedEvent) -> HandlerFuture<'a> + Send + Sync + 'static;
    pub async fn run_setup(&self, context: &EventContext<'_>) -> Result<(), InvocationError<HandlerError>>;
    pub async fn run_event(&self, context: &EventContext<'_>, event: &DecodedEvent) -> Result<(), InvocationError<HandlerError>>;
}

pub struct EventContext<'a> {
    pub store: &'a dyn stele_store::IndexingStore,
    pub chain_id: u64,
    pub block: &'a BlockRef,
    pub checkpoint: &'a Checkpoint,
    pub contracts: &'a BTreeMap<String, Address>,
}

impl ChainSource for stele_sync::RpcClient { /* ... */ }

pub struct Engine;
impl Engine {
    pub fn new(
        store: Arc<dyn Store>,
        sources: BTreeMap<u64, Arc<dyn ChainSource>>,
        plans: Vec<ChainPlan>,
        handlers: Arc<HandlerSet>,
        options: EngineOptions,
    ) -> Result<Self, EngineError>;
    pub async fn run(&self) -> Result<(), EngineError>;
    pub async fn run_backfill(&self) -> Result<(), EngineError>;
}

pub enum HandlerError { User(String), Transient(String), Fatal(String) }
pub enum EngineError { User(String), Transient(String), Fatal(String), Store(String), Chain(String) }
```

引擎要求：
- 启动时按链调用一次 setup handler；key 使用 `<source>:setup`。
- backfill 从 `start_block` 到 `min(end_block, latest)`，逐块拉取、发现工厂、构建并解码事件、按 checkpoint 执行、每块提交。
- 工厂地址在构建事件前写入 `ChildAddresses`；reorg 时撤销共同祖先之后的子地址。
- ABI 解码失败记录并跳过该事件，不中断索引。
- `Multichain`/`Isolated`：各链内部按 checkpoint 顺序；`Omnichain`：跨链按 checkpoint 合并后执行。
- live：使用 `LocalChain` 检测 reorg；`Reorg` 时回滚 store 到共同祖先的 block end，并重新拉取；`Finalize` 时调用 `store.finalize`。
- handler 返回 `Transient` 时按 `max_retries` 退避重试当前块；`User`/`Fatal` 立即停止。
- `follow=false` 在 backfill 完成后返回。

## 7. stele-api

保留现有 `ApiConfig`、`ApiState::new`、`router`、`validate_sql`。扩展：

```rust
pub struct ApiConfig {
    // 现有字段保留
    pub graphql: bool,
    pub live_interval_ms: u64,
    pub custom_schema: Option<SchemaDef>,
}
pub fn router(state: ApiState) -> axum::Router; // 由 crate 自行统一
```

新增路由：
- `POST /graphql`：GraphQL 查询，按 `SchemaDef` 动态生成每个表的查询字段；支持 `limit`、`offset`、`orderBy`、`where`（JSON 字符串等值过滤）。
- `GET /sql/live`：SSE，查询参数 `sql` 与可选 `params`；结果变化时推送 `data: {...}`，未变化不推送。
- `GET /status`：返回每链 `{ chain_id, checkpoint, finalized }`，数据来自 `stele_store.progress`。
- `GET /metrics`：Prometheus 文本，至少暴露索引块数、事件数、reorg 次数、查询数。

GraphQL 依赖 `async-graphql` 与 `async-graphql-axum`。

## 8. stele、stele-macros、stele-cli

```rust
// stele
pub struct SteleBuilder;
impl SteleBuilder {
    pub fn new() -> Self;
    pub fn config(self, config: stele_config::Config) -> Self;
    pub fn schema(self, schema: stele_schema::SchemaDef) -> Self;
    pub fn setup<F>(self, key: impl Into<String>, handler: F) -> Self;
    pub fn on<F>(self, key: impl Into<String>, handler: F) -> Self;
    pub fn run(self) -> impl std::future::Future<Output = Result<(), stele_core::EngineError>>;
}
pub use stele_core::{DecodedEvent, EventContext, HandlerError};

// stele-macros
#[derive(OnchainTable)] // 支持 #[table(name/schema)], #[primary_key], #[column(name)]
pub struct UserTable { ... }
```

`stele-macros` 生成 `impl UserTable { pub fn table_def() -> stele_schema::TableDef }` 并提供 `UserTable::table()`。类型映射：`Address` -> `Text`，`bool` -> `Boolean`，`i32` -> `Integer`，`i64` -> `BigInt`，`f64` -> `DoublePrecision`，`String` -> `Text`，`Vec<u8>` -> `Bytes`，`serde_json::Value` -> `Jsonb`，`Option<T>` 标记 nullable。

CLI（clap）：

```text
stele start --config <path> [--once] [--port <port>] [--host <host>]
stele serve --config <path> [--port <port>]
stele dev --config <path>
stele db migrate --config <path>
stele healthcheck --url <url>
```

`start` 运行引擎并在同一进程启动 API；`serve` 只启动 API；`dev` 轮询配置文件修改并重启子任务；`db migrate` 仅执行 schema 迁移；`healthcheck` 请求 `/health` 并以状态码退出。

## 9. 测试要求

- 每个 crate 至少包含：类型解析、边界条件、错误路径单测。
- `stele-core` 必须有一个端到端测试：`FakeChainSource` + `MemoryStore` + 真实 ABI 解码 + handler 写入，验证 backfill、事件顺序、checkpoint 与 reorg 回滚。
- 数据库测试标记 `#[ignore]`，通过环境变量 `STELE_TEST_DATABASE_URL` 启用。
- 所有 crate 必须通过 `cargo clippy -p <crate> --all-targets`（允许既有代码风格差异，但不得引入新警告）。
