# Workspace 与公共 API

## 1. crate 划分

```text
stele/
├── Cargo.toml                 # workspace
├── crates/
│   ├── stele/                 # 门面：re-export 宏与运行时，用户只依赖它
│   ├── stele-macros/         # proc macro：contract、OnchainTable、事件注册
│   ├── stele-config/         # 配置模型、校验、filter/fragment 生成
│   ├── stele-schema/         # 表定义、列类型、索引、视图、迁移 diff
│   ├── stele-sync/           # RPC 客户端、区间计算、工厂发现、sync cache
│   ├── stele-store/          # 写入缓存、flush、变更日志、revert
│   ├── stele-core/           # runtime + indexing engine，编排以上模块
│   ├── stele-api/            # axum 路由：SQL、SSE、health、metrics
│   ├── stele-cli/            # 二进制：start / serve / dev / db / codegen
│   └── stele-test/           # anvil 夹具、golden replay、对比测试工具
├── examples/
│   ├── erc20/
│   └── factory/
└── benches/
```

依赖方向严格单向，禁止回指：

```text
stele-macros -> (编译期只产出代码)
stele-schema -> stele-config
stele-sync   -> stele-config, stele-schema (仅列类型)
stele-store  -> stele-schema, stele-config
stele-core   -> 以上全部
stele-api    -> stele-store, stele-schema
stele-cli    -> stele-core, stele-api
```

`stele-schema` 不依赖 `stele-sync`，同步层用自己的类型描述单行数据。两边共享的只有 `Address`、`Hash` 这类 alloy 基础类型。

## 2. 公共 API 草图

用户侧代码应当短，配置与类型都在编译期确定。

```rust
use stele::{Stele, OnchainTable, Hex, U256};
use contracts::{property_token::Transfer, PropertyToken};

#[derive(OnchainTable)]
#[table(name = "holder", schema = "public")]
pub struct Holder {
    #[primary_key]
    pub address: Hex,
    #[column(name = "token_address")]
    pub token: Hex,
    pub balance: U256,
    pub updated_at: i64,
}

// 宏根据 ABI 生成 `contracts::property_token` 模块，内含 sol! 事件类型。
#[stele::contract(
    name = "PropertyToken",
    abi = "abis/PropertyToken.json",
    chain = "op",
    address = "0xd092...",
    start_block = 118_000_000
)]
pub struct PropertyToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Stele::builder()
        .config_file("stele.toml")
        .schema::<Holder>()            // 也可以通过 schema! 宏一次注册多张表
        .on::<PropertyToken, Transfer>(|ctx, event| async move {
            let key = HolderKey { address: event.args.to, token: event.address };
            let mut row = ctx.db.find::<Holder>(&key).await?.unwrap_or_default();
            row.balance += event.args.value;
            ctx.db.upsert(row).await?;
            Ok(())
        })
        .run()
        .await
}
```

类型定义：

```rust
pub struct Event<E> {
    pub args: E,                 // sol! 生成的事件结构
    pub chain: ChainId,
    pub address: Address,        // 实际发出日志的合约，可能是工厂子合约
    pub block: BlockRef,
    pub tx: TxRef,
    pub log_index: u32,
    pub checkpoint: Checkpoint,
}

pub struct Context<'a> {
    pub chain: ChainId,
    pub block: BlockRef,
    pub db: &'a mut Store,
    pub client: &'a RpcClient,
    pub contracts: &'a Contracts,
    pub checkpoint: Checkpoint,
}

pub trait Handler<E>: Send + Sync + 'static {
    fn handle<'a>(&'a self, ctx: &'a mut Context<'_>, event: Event<E>)
        -> BoxFuture<'a, Result<(), IndexingError>>;
}
```

ABI 代码生成直接用 alloy 的 `sol!` 宏。工厂动态子合约的地址要到运行期才知道，事件结构由模板决定，因此仍走同一套 `Handler<E>`；地址解析由 sync 层的 `factory_addresses` 提供。

## 3. 依赖选型

| 依赖 | 版本 | 用途 | 说明 |
|---|---:|---|---|
| `tokio` | 1.x | 异步运行时 | 当前同步任务与 handler 都是 async |
| `alloy` | 2.5 | Provider、RPC 类型、合约调用 | 与 `rwa-saas-platform` 的选型一致 |
| `alloy-sol-types` | 1.7 | 事件与调用类型 | `sol!` 宏 |
| `alloy-json-abi` / `alloy-dyn-abi` | 与 alloy 配套 | 运行时 ABI 解码 | 工厂模板与热更新配置 |
| `sqlx` | 0.8 | Postgres 访问与迁移 | 编译期查询校验用于固定 SQL，动态表走运行时 |
| `axum` | 0.8 | HTTP 服务 | SQL over HTTP、SSE、health |
| `serde` / `toml` | 1 / 0.8 | 配置解析 | 配置也可完全由 builder 构造 |
| `clap` | 4 | CLI | 子命令与参数 |
| `tracing` + `tracing-subscriber` | 1 / 0.3 | 日志 | 结构化 JSON 可选 |
| `metrics` + `metrics-exporter-prometheus` | 0.24 / 0.16 | 指标 | 索引延迟、RPC 错误率、reorg 次数 |
| `pg_query` | 6.x | SQL 解析 | SQL over HTTP 的 AST 白名单校验 |
| `dashmap` | 6 | 并发缓存 | 索引缓存与 RPC 结果缓存 |
| `async-trait` | 0.1 | 对象安全的异步 trait | 或改用 `trait-variant` |
| `notify` | 7.x | 文件监视 | 仅 `dev` 子命令 |
| `alloy-node-bindings` | 与 alloy 配套 | 启动 anvil | 集成测试 |
| `proptest` | 1.x | 属性测试 | 区间代数与 checkpoint 排序 |
| `insta` | 1.x | 快照测试 | 迁移 SQL 与事件归一化输出 |
| `criterion` | 0.5 | 基准测试 | backfill 吞吐与缓存命中 |

具体版本以执行 `cargo add` 时 crates.io 上的版本为准。这里写的是主版本意图，后续升级需要单独验证。

## 4. feature 开关

| feature | 默认 | 作用 |
|---|---|---|
| `postgres` | 开 | Postgres 后端。V1 不支持其他数据库，保留开关只为编译期裁剪 |
| `api` | 开 | 引入 axum 与 SQL over HTTP |
| `graphql` | 关 | 生成 GraphQL schema（阶段 3 评估） |
| `isolated-order` | 关 | 每链独立线程与分区表，实验性 |
| `telemetry` | 关 | OpenTelemetry 导出 |

## 5. 错误模型

```rust
pub enum IndexingError {
    User(NonRetryableError),   // 主键冲突、约束违反、类型不匹配
    Transient(TransientError), // RPC 超时、连接断开、数据库死锁
    Fatal(FatalError),         // 越过 finalized 边界的 reorg、schema 不一致
}
```

- `User`：停止该 checkpoint，记录事件与堆栈，任务失败，不自动重试。
- `Transient`：回滚当前 checkpoint 的缓存，按退避重试；超过阈值转为 `Fatal`。
- `Fatal`：停止进程，保留数据库状态，等待人工介入。

分类在引擎内完成，不把 `sqlx::Error` 直接透传给用户代码。判定依据是 SQLSTATE（`40001`、`40P01`、`57P01` 等可重试）、RPC 错误码（限流、超时）和 handler 显式返回的错误类型。
