# 配置与 Schema

## 1. 配置模型

```rust
pub struct SteleConfig {
    pub chains: IndexMap<ChainName, ChainConfig>,
    pub contracts: IndexMap<ContractName, ContractConfig>,
    pub accounts: Option<AccountsConfig>,
    pub blocks: Option<BlocksConfig>,
    pub ordering: Ordering,          // Multichain | Isolated
    pub reorg_window: u32,           // 默认 200
}

pub struct ChainConfig {
    pub id: u64,
    pub rpc: Vec<String>,            // 至少一个，推荐两个
    pub ws: Option<String>,
    pub polling_interval_ms: u64,    // HTTP 轮询周期，默认 1000
    pub finality: Finality,          // Confirmations(u32) | FinalizedTag
    pub reorg_window: Option<u32>,   // 覆盖全局
    pub cache_rpc_requests: bool,    // 是否复用 sync cache
}

pub struct ContractConfig {
    pub abi: AbiSource,              // Path(PathBuf) | Inline(String) | Registered(&'static str)
    pub chain: ChainName,
    pub address: AddressSource,      // Single(Address) | List(Vec<Address>) | Factory(FactoryRef)
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub include_transaction_receipts: bool,
    pub include_call_traces: bool,
}
```

`AddressSource::Factory` 指向另一个已注册合约的事件与参数：

```toml
[contracts.PropertyTokenFactory]
chain = "op"
abi = "abis/PropertyTokenFactory.json"
address = "0xd09290f599Ee74ab3914D496cD2c70eea3f51790"
start_block = 118000000

[contracts.PropertyToken]
chain = "op"
abi = "abis/PropertyToken.json"
start_block = 118000000

[contracts.PropertyToken.factory]
parent = "PropertyTokenFactory"
event = "PropertyTokenCreated"
parameter = "tokenAddress"
```

### 校验规则

启动时一次性完成，失败直接退出，不做降级：

1. `chain.id` 与 RPC 返回的 `eth_chainId` 一致。
2. 所有合约名唯一，事件名能在 ABI 中找到，且参数数量与 `factory.parameter` 匹配。
3. `start_block` 必填；`end_block >= start_block`。
4. 工厂的 `parent` 必须同链且已注册，工厂发现的事件必须有唯一索引的地址参数。
5. 同一合约的所有 filter 合并后必须是非空集合；`address` 列表去重。
6. `reorg_window` 小于链的 finality 确认数时给出警告。

配置来源有两种：`stele.toml` 加 ABI JSON 文件（运维可改），或全代码 builder（类型最紧）。两者最终生成同一个 `SteleConfig`，后续流程没有分支。

## 2. 合约与事件注册

编译期注册：

```rust
#[stele::contract(
    name = "PropertyToken",
    abi = "abis/PropertyToken.json",
    chain = "op",
    address = "0xd092...",
    start_block = 118_000_000,
    factory(parent = "PropertyTokenFactory", event = "PropertyTokenCreated", parameter = "tokenAddress")
)]
pub struct PropertyToken;
```

宏产出三部分：

- `impl Contract`：名称、链、地址来源、ABI 元数据。
- `pub mod <contract_snake_case>`：`sol!` 生成的事件结构，供 `indexer.on::<PropertyToken, property_token::Transfer>()` 使用。`PropertyToken` 生成 `property_token` 模块，避免与标记类型同名（模块和结构体同属类型命名空间，大小写不同才能共存）。
- `ContractRegistration { filters, fragments, factory }`：交给 config 校验。

动态子合约只影响地址集合，不影响事件类型。`factory_addresses` 表记录的 `(factory_id, address, block_number)` 在运行时展开成 filter 的地址列表。

## 3. Schema DSL

### 表定义

```rust
#[derive(OnchainTable)]
#[table(name = "holder", schema = "public")]
pub struct Holder {
    #[primary_key]
    pub address: Address,
    #[column(name = "token_address")]
    pub token: Address,
    pub balance: U256,
    #[default(0)]
    pub transfers: i32,
    #[index]
    pub updated_at: i64,
}
```

属性清单：

| 属性 | 作用 |
|---|---|
| `#[table(name = "...", schema = "...")]` | SQL 表名与 schema，默认 `public` |
| `#[column(name = "...")]` | 列名，默认把 Rust 的 snake_case 字段名原样使用 |
| `#[primary_key]` | 单列主键；多列用 `#[table(primary_key = "a,b")]` |
| `#[index]` / `#[index(name = "...", unique)]` | 索引，支持多列 `#[index(columns = "a,b")]` |
| `#[default(expr)]` | 数据库默认值或 Rust 侧默认值 |
| `#[references(Table, column = "id")]` | 外键，仅限同一 schema |
| `#[enum_values("A,B,C")]` | 生成 `text + CHECK` 约束 |

### 列类型映射

| Rust 类型 | SQL 类型 | 说明 |
|---|---|---|
| `String` / `&str` | `TEXT` | |
| `Address` / `B256` / `Hex` | `TEXT` | 统一小写保存，比较前归一化 |
| `U256` / `I256` | `NUMERIC(78,0)` | 覆盖 `uint256` / `int256` 全范围 |
| `i32` | `INTEGER` | |
| `i64` | `BIGINT` | 行内计数用它，不要用 `U256` |
| `bool` | `BOOLEAN` | |
| `f64` | `DOUBLE PRECISION` | 仅用于派生比率，不用于金额 |
| `serde_json::Value` | `JSONB` | |
| `Vec<T>`（标量 T） | `T[]` | 一维数组 |
| 自定义枚举 | `TEXT` + `CHECK` | 默认；`#[enum_values(..., pg_type)]` 可改用 PG enum |

`U256` 的 sqlx 编解码用文本走 `numeric`，不做二进制转换，避免 78 位边界问题。金额直接用 `U256`；汇率等需要小数的列另加 `NUMERIC(38,18)` 类型包装。

## 4. 迁移

两个表记录状态：

```sql
CREATE TABLE stele_schema.migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
```

流程：

1. 从 `pg_catalog` 读取目标 schema 的现状。
2. 用 derive 产出的 `TableDef` 与现状做 diff，生成 `Vec<MigrationStep>`。
3. 按安全级别分类：可自动（新增列、新增索引、放宽约束）、需确认（删列、改类型、加唯一约束）、禁止自动（改主键、删表）。
4. 开发模式：可自动步骤立即执行；需确认步骤打印 SQL 并要求 `--yes`。
5. 生产模式：只输出待执行的 SQL 文件，由发布流程执行；`start` 只校验 checksum，不自动迁移。

dev 模式默认保留数据并做增量 diff，只有 `stele dev --reset-schema` 才重建表。Rust 编译重启比 TS 热重载慢，每次重建会把回填时间拉长，所以默认不动已有数据。

### 重命名

diff 无法区分"删列加列"和"重命名"。需要显式声明：

```rust
#[table(name = "account", renamed_from = "accounts")]
```

没有声明时按删除处理，并在生成阶段给出警告。破坏性操作需要人工确认。

## 5. 视图

```rust
#[derive(OnchainView)]
#[view(name = "holder_ranking", sql = "SELECT address, balance FROM holder ORDER BY balance DESC")]
pub struct HolderRanking;
```

视图在迁移时 `CREATE OR REPLACE`，只读，不参与回滚。视图查询不保证与回滚同步，因此 `handler` 内不要读视图；API 与报表可以接受短暂滞后。

## 6. schema 命名空间

| 命名空间 | 归属 | 说明 |
|---|---|---|
| `stele_rpc_cache_1` | 框架 | RPC 缓存，可清空重建 |
| 遗留缓存 schema | 迁移 | 不再创建，只在迁移时读取 |
| `stele_schema` | stele | 迁移记录、进度、变更日志 |
| 用户 schema（默认 `public`） | 应用 | 仅 `db reset` |

内部表不能放进用户 schema，避免 `db reset` 误删和 SQL over HTTP 越权访问。
