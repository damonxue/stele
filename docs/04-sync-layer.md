# 同步层

同步层把链上原始数据按区间拉取并写入 Postgres，供 runtime 展开成事件流。它不解析业务事件，也不调用用户代码。

## 1. 缓存表

这些表按固定列名建表，是 RPC 结果的本地缓存，不属于用户数据。连接到 Postgres 时一次性建出全部 9 张，即使某个配置只索引日志、其他表一直为空。删掉整个 schema 只会让下次启动重新消耗 RPC。

缓存 schema 带版本号（`stele_rpc_cache_1`）有两条原因。trace 的数据模型改了：新增 `traceAddress`，去掉 `traceIndex` 和 `subcalls`，旧缓存的行不能直接复用。旧版工厂缓存还有两处会漏掉子合约事件的 bug。新建一个带版本号的 schema，迁移时可以只复制仍然有效的数据：blocks、transactions、transaction_receipts、logs、rpc_request_results，以及一部分 intervals。trace、transfer、factory 相关的区间跳过，由新版本重新拉取。旧 schema 保持不动，旧实例还能继续跑，两个版本只是不共享缓存。缓存本身可以重建，这次复制属于尽力而为。以后缓存布局再变，可以继续加 `_2`、`_3` 和对应的 `copy.sql`，不用原地修改旧表。

簿记对象不在这个 schema 里。stele 把进度、checkpoint 和 undo 集中到 `stele_schema`（原因见 `decisions.md` 的 ADR-0005），`stele_rpc_cache_1` 只放缓存表，可以随时清空重建。

```sql
CREATE SCHEMA IF NOT EXISTS stele_rpc_cache_1;

CREATE TABLE stele_rpc_cache_1.blocks (
    chain_id bigint NOT NULL,
    number bigint NOT NULL,
    timestamp bigint NOT NULL,
    hash varchar(66) NOT NULL,
    parent_hash varchar(66) NOT NULL,
    logs_bloom varchar(514) NOT NULL,
    miner varchar(42) NOT NULL,
    gas_used numeric(78,0) NOT NULL,
    gas_limit numeric(78,0) NOT NULL,
    base_fee_per_gas numeric(78,0),
    nonce varchar(18),
    mix_hash varchar(66),
    state_root varchar(66) NOT NULL,
    receipts_root varchar(66) NOT NULL,
    transactions_root varchar(66) NOT NULL,
    sha3_uncles varchar(66),
    size numeric(78,0) NOT NULL,
    difficulty numeric(78,0) NOT NULL,
    total_difficulty numeric(78,0),
    extra_data text NOT NULL,
    PRIMARY KEY (chain_id, number)
);

CREATE TABLE stele_rpc_cache_1.logs (
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    log_index integer NOT NULL,
    transaction_index integer NOT NULL,
    block_hash varchar(66) NOT NULL,
    transaction_hash varchar(66) NOT NULL,
    address varchar(42) NOT NULL,
    topic0 varchar(66),
    topic1 varchar(66),
    topic2 varchar(66),
    topic3 varchar(66),
    data text NOT NULL,
    PRIMARY KEY (chain_id, block_number, log_index)
);

CREATE TABLE stele_rpc_cache_1.transactions (
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    transaction_index integer NOT NULL,
    hash varchar(66) NOT NULL,
    block_hash varchar(66) NOT NULL,
    "from" varchar(42) NOT NULL,
    "to" varchar(42),
    input text NOT NULL,
    value numeric(78,0) NOT NULL,
    nonce integer NOT NULL,
    r varchar(66),
    s varchar(66),
    v numeric(78,0),
    type text NOT NULL,
    gas numeric(78,0) NOT NULL,
    gas_price numeric(78,0),
    max_fee_per_gas numeric(78,0),
    max_priority_fee_per_gas numeric(78,0),
    access_list text,
    PRIMARY KEY (chain_id, block_number, transaction_index)
);

CREATE TABLE stele_rpc_cache_1.transaction_receipts (
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    transaction_index integer NOT NULL,
    transaction_hash varchar(66) NOT NULL,
    block_hash varchar(66) NOT NULL,
    "from" varchar(42) NOT NULL,
    "to" varchar(42),
    contract_address varchar(42),
    logs_bloom varchar(514) NOT NULL,
    gas_used numeric(78,0) NOT NULL,
    cumulative_gas_used numeric(78,0) NOT NULL,
    effective_gas_price numeric(78,0) NOT NULL,
    status text NOT NULL,
    type text NOT NULL,
    PRIMARY KEY (chain_id, block_number, transaction_index)
);

CREATE TABLE stele_rpc_cache_1.traces (
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    transaction_index integer NOT NULL,
    trace_address integer[] NOT NULL,
    "from" varchar(42) NOT NULL,
    "to" varchar(42),
    input text NOT NULL,
    output text,
    value numeric(78,0),
    type text NOT NULL,
    gas numeric(78,0) NOT NULL,
    gas_used numeric(78,0) NOT NULL,
    PRIMARY KEY (chain_id, block_number, transaction_index, trace_address)
);

CREATE TABLE stele_rpc_cache_1.rpc_request_results (
    request_hash text NOT NULL,
    chain_id bigint NOT NULL,
    block_number bigint,
    result text NOT NULL,
    PRIMARY KEY (chain_id, request_hash)
);

CREATE TABLE stele_rpc_cache_1.intervals (
    fragment_id text PRIMARY KEY,
    chain_id bigint NOT NULL,
    blocks nummultirange NOT NULL
);

CREATE TABLE stele_rpc_cache_1.factories (
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    fragment_id text NOT NULL UNIQUE
);

CREATE TABLE stele_rpc_cache_1.factory_addresses (
    factory_id integer NOT NULL,
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    address text NOT NULL,
    PRIMARY KEY (factory_id, address)
);

CREATE INDEX factories_fragment_id_idx
    ON stele_rpc_cache_1.factories USING btree (fragment_id);
CREATE INDEX rpc_request_results_chain_id_block_number_index
    ON stele_rpc_cache_1.rpc_request_results USING btree (chain_id, block_number);
```

各表由哪类配置写入：

| 表 | 内容 | 何时有数据 |
|---|---|---|
| `blocks` | 区块头 | 所有配置 |
| `logs` | 合约事件 | 索引合约事件 |
| `transactions` | 交易体 | `accounts`，或 handler 读取 `event.transaction` |
| `transaction_receipts` | 回执 | `includeTransactionReceipts`，或读取 `event.transactionReceipt` |
| `traces` | 调用轨迹 | `accounts` 的 trace 模式，或 `includeCallTraces` |
| `factories` | 工厂 fragment 元数据 | 工厂配置 |
| `factory_addresses` | 发现的子合约地址 | 工厂配置 |
| `intervals` | 已同步区间 | 所有配置 |
| `rpc_request_results` | RPC 结果去重 | 开启 `cacheRpcRequests`（默认开启） |

只做合约事件索引时，实际有数据的是 `blocks`、`logs`、`intervals`、`rpc_request_results` 四张，其余为空。stele 可以选择按配置只建需要的表，但默认建满 9 张，保证表结构稳定。

`rpc_request_results` 保存已请求过的 RPC 结果，用于回填中断后的幂等续传和开发模式下的配额复用。生产环境可以按 `block_number` 定期清理早于 finality 的记录。

## 2. fragment 与 interval

一个 filter 由链、地址集合、事件签名集合、起始块决定。多个 filter 有重叠时，stele 把它们拆成互不相交的 fragment，每个 fragment 独立记录已同步区间，避免因某个合约新增地址而整段重拉。

```rust
pub struct Fragment {
    pub id: FragmentId,          // hash(chain, sorted addresses, sorted topics, from_block, to_block)
    pub chain: ChainId,
    pub addresses: Vec<Address>,
    pub topics: Vec<B256>,
    pub from_block: u64,
    pub to_block: Option<u64>,
}

pub struct CachedInterval {
    pub fragment: Fragment,
    pub blocks: BlockRangeSet,   // 已完整同步的区间
}
```

算法（按 fragment 覆盖计算）：

1. 对每个 filter 求它与 `[from_block, to_block]` 的交集。
2. 如果 filter 依赖工厂，先求工厂 fragment 的缺失区间。
3. 工厂区间一旦缺失，从最早缺失块到区间末尾全部视为缺失。新地址可能在那之后任何位置出现，只补一个缺口会让后续块漏扫。
4. 用 fragment 的已同步区间做差集，得到待拉取区间。
5. 多个 filter 的待拉取区间求并集，合并成尽量大的连续区间，减少 RPC 次数。

实现要点：Rust 侧用 `BlockRangeSet` 保存归一化后的 `Vec<(u64, u64)>`，写入数据库时编码为 `nummultirange`。区间运算写属性测试，覆盖相邻、重叠、空集、全开区间。

## 3. 历史回填

```text
for each required interval (升序):
    chunk = 初始 2000 块，按 provider 限流自适应调整
    fetch blocks / logs / txs / receipts / traces（仅取 filter 需要的类型）
    upsert 到 cache 表
    成功后把 chunk 区间并入 intervals.blocks
```

规则：

- 区间记录以 fragment 为单位。按合约或按块记录都会在地址集合变化时重复拉取。
- RPC 请求失败按错误类型处理：限流与超时重试并缩小 chunk；`null` 区块重试；其他错误直接失败。
- 一次 RPC 最多返回的日志条数由 provider 决定。收到 `limit exceeded` 类错误时二分区间。
- 交易、回执、trace 只在合约配置开启时拉取，避免无谓开销。
- trace 依赖节点的 `debug_traceBlockByNumber` 或 `trace_block`，不是所有 RPC 都支持。启动时探测一次，不支持且配置了 trace 就报错。

## 4. 实时同步

```text
loop:
    head = eth_blockNumber
    finalized = finality 水位（见下）
    if finalized <= last_synced: sleep
    fetch [last_synced+1, min(head, last_synced+N)]
    verify parent_hash(block[0]) == hash(last_synced_block)
    if mismatch: 进入 reorg 处理
    write cache
    notify runtime
```

finality 两种模式：

| 模式 | 取值 | 语义 | 代价 |
|---|---|---|---|
| `Confirmations(n)` | head - n | 沿用现有 rwax 行为，OP 上约 n=65 | n 个块的重组风险 |
| `FinalizedTag` | `eth_getBlockByNumber("finalized")` | 跟随 L1 最终性 | 延迟更长 |

两种模式都在链配置里显式声明，不提供隐式默认值。`chain.finality` 同时决定 `reorg_window` 的下界。

### reorg 处理

1. 从分叉点向后，删除 cache 表中 `block_number > fork` 的非规范行。cache 不是事实表，可以直接删。
2. 回退 `intervals` 中覆盖分叉点之后的区间记录，使下一轮重新拉取。
3. 通知 runtime 执行数据库回滚（见 `06-indexing-and-store.md`）。
4. 重新拉取新链数据，重复的事件按主键覆盖。

reorg 深度超过 `reorg_window` 时直接 `Fatal`，不尝试修复。

## 5. 工厂发现

工厂子合约地址来自工厂合约的事件，处理方式与其他事件相同，但多一步写回：

```text
解码工厂事件 -> 提取 address 参数
  -> INSERT INTO factory_addresses (factory_id, chain_id, block_number, address)
  -> 更新该 fragment 的地址集合
  -> 从该地址的 created_block 起补扫该 fragment 的缺失区间
```

边界情况：

- 同一次交易内先创建子合约、后由子合约发出事件：当前块按 `log_index` 排序即可正确，因为子合约地址在本块日志里已经存在。
- 子合约地址在更早的块已有日志：不可能，合约在创建块才存在，`created_block` 作为下界即可。
- 工厂事件在 `to_block` 之外的块：不处理。
- 重复投递的工厂事件：`factory_addresses` 主键去重。

## 6. 并发与限流

- 每个链一个同步任务，链间并行，链内串行。
- 链内允许预取：拉取任务比索引进度提前 N 个区间，写 cache，不调用 handler。N 默认 2，可配置。
- RPC 客户端按 provider 维护令牌桶与失败计数，主备切换时保留 chunk 自适应状态。
- 同一 `(chain, from, to, address, topics)` 的请求在进程内去重，避免多个 fragment 重复请求。

## 7. 指标

| 指标 | 用途 |
|---|---|
| `sync_lag_blocks{chain}` | head 与已同步块的差值 |
| `sync_cache_rate{chain}` | 命中 cache 的比例，回填速度的参考指标 |
| `rpc_requests_total{chain,method,status}` | 限流与成本分析 |
| `rpc_request_duration_seconds{method}` | 慢请求定位 |
| `sync_chunk_size_blocks{chain}` | 自适应是否正常 |
| `reorg_detected_total{chain,depth}` | reorg 频率与深度分布 |
| `factory_addresses_total{chain,factory}` | 工厂发现是否停滞 |
