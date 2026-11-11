# API、CLI 与运维

## 1. SQL over HTTP

端点：`POST /sql`，请求体：

```json
{ "sql": "SELECT address, balance FROM holder WHERE balance > $1 ORDER BY balance DESC LIMIT 50", "params": ["1000"] }
```

服务端行为：

1. 设置 `search_path` 为配置的用户 schema，SQL 里不允许写 schema 限定名。
2. 用 `pg_query` 解析 AST：根节点必须是 `SelectStmt`，只允许一条语句。
3. 按 AST 白名单校验节点类型与函数：允许 `Select`、`Join`、`Where`、`GroupBy`、`OrderBy`、`Limit`、聚合与常用标量函数；拒绝 `DELETE`、`SET`、`COPY`、`pg_advisory_lock`、`pg_read_file` 等。
4. 在 `READ ONLY` 事务中执行，设置 `statement_timeout = 500ms`、`lock_timeout = 500ms`、`work_mem = 512MB`。
5. 结果限制最大行数（默认 10 万），超出返回错误，不截断。

校验失败返回 400 与具体原因，执行超时返回 408，其余数据库错误返回 500 且不泄露 SQL。

安全边界：

- 框架不内建认证。`/sql` 默认只监听 `127.0.0.1`，对外暴露必须由反代或用户 middleware 加认证。
- 提供 `ApiConfig::readonly_role`：API 进程使用只读数据库角色，索引进程使用读写角色，两者不共用凭据。
- 每个连接限制并发查询数，可按 IP 或 token 限流的接口留给部署层。

## 2. 实时查询（SSE）

端点：`POST /sql/live`，`Accept: text/event-stream`，SQL 放在请求体里。查询可能很长，认证头也更适合 POST。

```http
POST /sql/live
Accept: text/event-stream
Content-Type: application/json

{"sql": "SELECT count(*) FROM holder"}
```

实现：

- 查询注册后立即执行一次并返回 `event: result`。
- 每次索引 flush 提交后，对该 checkpoint 重跑所有注册查询；结果哈希变化时才推送 `event: update`。
- 每个连接同一时刻只有一个查询在跑；上一次未完成时跳过本轮，不排队堆积。
- 连接数与单查询成本都有上限（默认 100 连接），超出返回 503。

结果哈希用序列化字节的 BLAKE3，避免依赖数据库的 `txid` 和行顺序。

## 3. 自定义端点

用户可以通过 `Router` 合并自己的路由：

```rust
let app = stele::api::router()
    .merge(my_custom_routes());
```

自定义路由与框架路由共享同一个 `axum::Router` 与连接池，默认不继承 `/sql` 的只读限制。用户自定义查询要自己保证安全。

## 4. CLI

| 命令 | 作用 |
|---|---|
| `stele start` | 同步 + 索引 + API，生产入口 |
| `stele serve` | 只读 API，不写用户表，可多副本 |
| `stele dev` | 监督进程 + 子进程，文件变化重启 |
| `stele db migrate` | 生成并应用迁移 SQL |
| `stele db reset` | 清空用户表与内部表，需 `--yes` |
| `stele db reset-sync` | 只清空同步缓存，保留用户表 |
| `stele db status` | 进度、滞后、区间缺口、reorg 窗口 |
| `stele healthcheck` | 供容器探针调用，返回退出码 |
| `stele bench` | 回填基准，输出 events/s 与缓存命中率 |

`start` 的行为：

- 启动时校验 schema checksum，不一致直接退出，不自动迁移。
- 先启动 HTTP，`/health` 立即返回 200，`/ready` 在追上 finality 之前返回 503（可通过 `ready_after_backfill = false` 关闭）。
- 收到 `SIGTERM` 后停止同步、flush 当前缓存、关闭连接，最多等待 30 秒。

`dev` 的重载方式是重启子进程：

1. `notify` 监听 `src/`、`stele.toml`、ABI 文件。
2. 编译失败时保留上一个可用二进制，继续服务，不退出。
3. 编译成功则启动新进程，旧进程优雅退出。
4. schema 变化时按 diff 结果处理；`--reset-schema` 才重建表。
5. 日志显示每个事件的 handler、耗时、写入行数，可在配置里关闭。

## 5. 观测

### 日志

输出走 `tracing`，级别由 `RUST_LOG` 控制，默认 `info`。`RUST_LOG` 只写 target（例如另一个程序设置的 `RUST_LOG=rustfs=debug`）时，未列出的 target 默认会被压到 `ERROR`，因此两个入口都会自动补一个 `info` 全局指令；显式写了全局级别的 `RUST_LOG`（如 `RUST_LOG=warn`）仍然优先。

```sh
RUST_LOG=stele_core=debug,stele_sync=debug,example_rwax=debug cargo run -p example-rwax
```

| 级别 | 内容 |
|---|---|
| `info` | 启动配置摘要、数据库 schema 初始化、backfill 起止、每 10 秒一次的进度（速率与 ETA）、开始跟随 chain tip、reorg、API 监听地址 |
| `debug` | 每个已提交区块、工厂子合约发现、RPC 端点失败与重试间隔、store 批量提交与回滚、事件写入 |
| `trace` | store 事务开启 |

结构化字段：`chain`、`chain_id`、`block`、`checkpoint`、`contract`、`event`、`attempt`、`delay_ms`、`blocks_per_second`、`eta_seconds`、`error_kind`。

错误日志要包含可复现信息：完整事件引用（链、块、交易、日志序号）与 handler 名。整个 `payload` 不要打进日志，可能包含业务敏感数据。

RPC URL 不写进日志：托管 provider 通常把 API key 放在 path 或 query 里。日志只记录去掉 userinfo、path 和 query 后的 `scheme://host`；`SyncError::Transport` 的消息在写日志前也会被替换成同样的形式。

### 指标

| 指标 | 类型 | 说明 |
|---|---|---|
| `stele_indexing_events_total{chain,contract,event}` | counter | 已处理事件 |
| `stele_indexing_duration_seconds{event}` | histogram | handler 耗时 |
| `stele_indexing_errors_total{kind}` | counter | 按 `user/transient/fatal` 分类 |
| `stele_store_flush_rows{table}` | histogram | 批量写入行数 |
| `stele_store_cache_rows` | gauge | 缓存行数，观察内存 |
| `stele_reorg_total{chain,depth}` | counter | reorg 次数 |
| `stele_reorg_rollback_rows` | histogram | 回滚行数 |
| `sync_lag_blocks{chain}` | gauge | 见 `04-sync-layer.md` |
| `rpc_requests_total{chain,method,status}` | counter | RPC 成本与错误 |
| `stele_http_request_duration_seconds{path}` | histogram | API 延迟 |

### 健康检查

| 端点 | 语义 |
|---|---|
| `/health` | 进程存活，数据库连接可用 |
| `/ready` | 已追上 finality（或配置允许回填期间就绪） |
| `/status` | JSON：各链同步进度、索引进度、reorg 窗口、最后 flush 时间 |

`/status` 不返回数据库连接串、RPC URL 里的 key、表结构细节。

## 6. 部署

- PostgreSQL 16 及以上，`nummultirange` 与 `NULLS NOT DISTINCT` 可用。
- 单个索引实例；多实例需要外部选主，框架不内置。
- `serve` 可横向扩展，使用只读角色与只读副本。
- RPC 至少两个 provider，主备切换在客户端内部完成，不依赖负载均衡掩盖单点故障。
- 磁盘规划：同步缓存与变更日志是主要增长源，清理策略见 `04` 与 `06`。建议把 `stele_rpc_cache_1` 放在独立表空间，便于单独扩容。

## 7. 备份与恢复

- 用户表与 `stele_schema` 一起备份；`stele_rpc_cache_1` 可以不备份，重建只消耗 RPC。
- 恢复后先跑 `db status` 确认 `progress` 与 `change_log` 一致，再从安全 checkpoint 继续。
- 只恢复用户表而不恢复 `stele_schema` 会让回滚信息缺失，后续 reorg 会静默产生错误数据。
