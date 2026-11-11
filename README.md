# stele

EVM 数据索引框架的 Rust 实现：TOML 配置、Postgres 存储、SQL/GraphQL 查询、可回滚的 checkpoint。

stele 用 Rust 实现配置、链数据同步、索引执行、Postgres 存储和查询 API：`config` 文件描述链与合约，`schema` 文件定义输出表，`src/*.rs` 里的 handler 处理事件，`OnchainTable` derive 声明表结构。

## 当前状态

可以运行完整的单进程索引应用：配置解析、RPC 同步、ABI 解码、handler 执行、Postgres 写入、reorg 回滚、finality 清理、SQL/GraphQL/SSE/metrics 查询和 CLI 均已接通。`cargo test --workspace` 覆盖各层，Postgres 集成测试通过独立临时库验证。

### 已实现

| 能力 | 说明 |
|---|---|
| 配置 | TOML 解析与校验：链、合约、账户、block interval、factory、按链覆盖、ABI 内联/路径、事件与函数调用选择 |
| 同步 | HTTP JSON-RPC（多 URL 故障切换、重试、退避）、区块/交易/回执/log/trace 拉取、区块区间与地址分片、`stele_rpc_cache_1` 缓存表、工厂地址持久化 |
| 事件 | 五种触发器（log、transaction、native transfer、call trace、block interval）、checkpoint 编码、事件排序、ABI 解码为 JSON |
| 工厂 | 工厂日志发现子合约地址、同块可见、按块记录并在 reorg 时撤销 |
| 排序 | `omnichain` 全局 checkpoint 合并、`multichain` 链内顺序、`isolated` 链隔离 |
| 存储 | 真实用户表、动态 SQL、批量写入缓存、undo 日志、按链进度、`revert` / `finalize`、内存后端 |
| 实时 | `reorgWindow` 按秒推进 finality、本地链回溯、不可恢复 reorg 报错、block/hash 校验 |
| API | `POST /sql`、`POST /graphql`、`GET /sql/live` SSE、`/health`、`/ready`、`/status`、`/metrics`、自定义 axum 路由 |
| CLI | `start`、`serve`、`dev`、`db migrate`、`healthcheck` |
| 宏 | `#[derive(OnchainTable)]`，支持 table/schema/primary_key/column 属性；生成代码引用 `stele_schema`，使用方需直接依赖该 crate |

### 设计取舍

这些是有意差异，不是未完成项：

- 配置使用 TOML，不支持直接加载 TypeScript 配置。
- indexing function 用 Rust 闭包注册（`SteleBuilder::on`），不执行 TypeScript。
- ABI 在运行时从 JSON 加载。`#[stele::contract]` + `sol!` 编译期事件结构未提供；handler 收到解码后的 `serde_json::Value`。
- 数据库只支持 Postgres。PGlite 未实现，`database.url` 必须是 Postgres 连接串。
- GraphQL 按表生成 `{ table(limit, offset, orderBy, where) { items total } }`；`where` 只支持等值过滤，没有比较与嵌套操作符。
- `dev` 监听配置文件并在变化时重启索引任务，不编译 Rust 代码，因此没有模块热重载。
- handler 的 `raw` SQL 只接受单条参数化语句，DDL 和多语句会被拒绝。

## 快速开始

`Cargo.toml`：

```toml
[dependencies]
stele = { path = "../stele/crates/stele" }
tokio = { version = "1", features = ["full"] }
```

`stele.toml`：

```toml
ordering = "multichain"

[database]
url = "postgres://postgres:postgres@localhost:5432/app"
schema = "public"

[chains.mainnet]
id = 1
rpc = ["http://127.0.0.1:8545"]
reorg_window = 180

[contracts.Token]
abi = "abis/erc20.json"
chain = "mainnet"
address = ["0x0000000000000000000000000000000000000001"]
start_block = 0
events = ["Transfer"]
```

`src/main.rs`：

```rust
use stele::{CellValue, DecodedEvent, EventContext, HandlerError, SteleBuilder, Row, TableRef};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = stele::Config::from_file("stele.toml")?;
    SteleBuilder::new()
        .config(config)
        .on("Token:Transfer", |ctx: &EventContext<'_>, event: &DecodedEvent| {
            Box::pin(async move {
                let table = TableRef { schema: "public".into(), name: "transfer".into() };
                let id = event.log.as_ref().map(|log| log.log_index).unwrap_or(0).to_string();
                let mut key = Row::new();
                key.insert("id".into(), CellValue::Text(id.clone()));
                let mut row = key.clone();
                row.insert("amount".into(), CellValue::Text(
                    event.args["value"].as_str().unwrap_or("0").to_owned(),
                ));
                ctx.store.upsert(&table, &key, &row).await
                    .map_err(|error| HandlerError::Transient(error.to_string()))
            })
        })
        .follow(true)
        .api("127.0.0.1", 42069)
        .run()
        .await?;
    Ok(())
}
```

完整示例见 [`crates/stele/examples/erc20.rs`](crates/stele/examples/erc20.rs)。RWAX 示例见 [`examples/rwax`](examples/rwax/)：按 `CONTRACT_ENV`/`CONTRACT_CHAIN_ID` 从 `rwax-contracts-registry` 生成 ABI 与地址到 Rust 源码，包含 PropertyToken factory 与 TradeContract/PropertyToken 事件。

CLI：

```text
cargo run -p stele-cli -- start --config stele.toml
cargo run -p stele-cli -- serve --config stele.toml
cargo run -p stele-cli -- db migrate --config stele.toml
cargo run -p stele-cli -- healthcheck --url http://127.0.0.1:42069
```

## crate 划分

| crate | 内容 |
|---|---|
| `stele-types` | Address/Topic/B256、区块/交易/log/trace、`CellValue` |
| `stele-checkpoint` | 75 位 checkpoint 编码与事件类型 |
| `stele-config` | TOML 配置模型与校验 |
| `stele-schema` | 列类型、表/索引/视图/枚举、DDL 与 diff |
| `stele-sync` | JSON-RPC 客户端、区块数据组装、`stele_rpc_cache_1` 表 |
| `stele-store` | Postgres 与内存存储、写缓存、undo、revert/finalize |
| `stele-core` | filter 匹配、事件构建、ABI 解码、计划、引擎、reorg |
| `stele-api` | SQL、GraphQL、SSE、status、metrics |
| `stele-macros` | `OnchainTable` derive |
| `stele` | 门面与 builder |
| `stele-cli` | 命令行 |

## 测试

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all -- --check
```

Postgres 集成测试默认标记为 ignored。使用一次性的临时数据库：

```sh
initdb -D /tmp/stele_pg -U postgres --auth=trust
pg_ctl -D /tmp/stele_pg -o "-p 55432 -k /tmp" -l /tmp/stele_pg.log start
createdb -h 127.0.0.1 -p 55432 -U postgres stele_test

STELE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/stele_test \
STELE_API_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/stele_test \
STELE_STORE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/stele_test \
cargo test --workspace -- --ignored

pg_ctl -D /tmp/stele_pg stop
```

不要把这些变量指向应用数据库。

## 文档

| 文档 | 内容 |
|---|---|
| [`docs/01-architecture.md`](docs/01-architecture.md) | 分层、数据流、概念映射 |
| [`docs/02-workspace.md`](docs/02-workspace.md) | crate 划分与公共 API 草图 |
| [`docs/03-config-and-schema.md`](docs/03-config-and-schema.md) | 配置模型、合约注册、schema DSL |
| [`docs/04-sync-layer.md`](docs/04-sync-layer.md) | RPC 同步层、fragment 与 interval、工厂发现、reorg 缓存 |
| [`docs/05-runtime-and-ordering.md`](docs/05-runtime-and-ordering.md) | checkpoint、事件排序、backfill 与 live |
| [`docs/06-indexing-and-store.md`](docs/06-indexing-and-store.md) | handler、Context、写缓存、变更日志与回滚 |
| [`docs/07-api-cli-ops.md`](docs/07-api-cli-ops.md) | SQL、SSE、CLI、观测与部署 |
| [`docs/08-roadmap.md`](docs/08-roadmap.md) | 原始分阶段计划（已完成） |
| [`docs/09-implementation-plan.md`](docs/09-implementation-plan.md) | 评审结论与决策背景 |
| [`docs/10-rust-api-contract.md`](docs/10-rust-api-contract.md) | Rust crate 接口契约 |
| [`docs/decisions.md`](docs/decisions.md) | ADR 记录 |

## 行为约定

stele 的行为由本仓库的文档和测试定义：checkpoint 布局、事件类型与排序、`reorgWindow` 秒数语义、同步缓存分层、factory 地址发现、SQL over HTTP 的资源限制。
