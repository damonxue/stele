# Runtime 与事件排序

## 1. checkpoint 编码

checkpoint 使用定长字符串布局，字典序等于事件顺序，可以直接做数据库索引和游标比较。

```text
block_timestamp(10) | chain_id(16) | block_number(16) | tx_index(16) | event_type(1) | event_index(16)
```

```rust
pub struct Checkpoint([u8; 75]);

impl Checkpoint {
    pub fn new(parts: CheckpointParts) -> Self { /* 逐段零填充并拼接 */ }
    pub fn as_str(&self) -> &str { /* ASCII 视图，构造时已验证全部为数字 */ }
}

impl Ord for Checkpoint {
    fn cmp(&self, other: &Self) -> Ordering { self.as_str().cmp(other.as_str()) }
}
```

设计约束：

- 不用 `(u64, u32, ...)` 元组比较。多链排序的语义是"按区块时间戳，再按链 ID"，编码顺序必须固定，否则跨链顺序会随实现变化。
- 时间戳 10 位可覆盖到 2277 年；chain id、块号、交易序号、事件序号各 16 位，超出即报错，不静默截断。
- `event_type` 区分触发源（交易、日志、trace、区块间隔），`event_index` 区分同交易内的日志序号或 trace 序号。新增触发源时占用未使用的取值，不改已有取值。

## 2. 事件模型

```rust
pub enum Event {
    Log(LogEvent),
    Transaction(TransactionEvent),
    Transfer(TransferEvent),
    Trace(TraceEvent),
    BlockInterval(BlockIntervalEvent),
}

pub struct LogEvent {
    pub chain: ChainId,
    pub block: BlockRef,          // number, hash, timestamp, parent_hash
    pub tx: TxRef,                // hash, index, from, to
    pub log_index: u32,
    pub address: Address,         // 实际发出日志的合约，可能是工厂子合约
    pub contract: ContractName,   // 解析后归属的注册合约名
    pub topics: [Option<B256>; 4],
    pub data: Bytes,
    pub checkpoint: Checkpoint,
}
```

工厂模式下，`contract` 来自 `factory_addresses` 反查得到的模板名，配置文件里的单一地址不够用。解析不到模板的日志直接丢弃并计数，不进入 handler。

事件参数计划使用 `sol!` 在编译期生成，handler 收到的 `event.args` 是具体结构体。日志或调用 trace 的 ABI 解码失败时记录告警和计数，并跳过该事件。

## 3. 单链内排序

必须保持 EVM 执行顺序：

```text
block_number -> transaction_index -> log_index
```

- checkpoint 在交易序号之后按事件类型及事件序号排序：transaction 为 2，log 为 5，trace 和 native transfer 为 7。区块事件的交易序号使用最大值，在该块其他事件后处理。
- 不允许并发调用同一链的 handler。Rust 的异步调度很容易误用 `join_all`，引擎层必须显式串行化。

## 4. 多链排序

默认模式是 `multichain`，全局 checkpoint 排序由 `omnichain` 提供。`omnichain` 的安全推进水位受慢链影响：

| 模式 | 语义 | 前提 |
|---|---|---|
| `Omnichain` | 全局按 checkpoint 排序 | 无 |
| `Multichain` | 链内按 checkpoint 排序，跨链不保证执行顺序 | 无 |
| `Isolated` | 每链独立推进，互不阻塞 | 限制跨链表访问 |

`Isolated` 模式要求链隔离表的主键包含 `chain_id`；是否允许读写还取决于表定义及当前链。当前引擎按链顺序处理事件，但不校验表主键、不阻止跨链读写；这些隔离规则需要由应用层或后续存储层校验补齐。

## 5. backfill 与 live

两者共用事件生成与 handler 调用，只有取数方式不同。

| 阶段 | 取数 | 并发 | 结束条件 |
|---|---|---|---|
| backfill | 按缺失区间批量拉取 | 可预取多个区间 | 追到 finality 水位 |
| live | 轮询增量块 | 单步推进 | 进程生命周期 |

切换点在 `sync_live_started`：已同步块达到 finality 水位后切换到实时轮询，不重启进程，不重置游标。

backfill 与 live 同时写同一链的 cache 会让区间记录互相覆盖。同步层按链持有写锁，backfill 完成后才交给 live。

## 6. 预取与背压

```text
sync task ──(cache write)──> prefetch queue ──> event builder ──> handler loop
```

- prefetch 队列按 checkpoint 有序，容量固定（默认 4096 事件），满了就阻塞同步任务。
- 事件构建（解码）可以并行，handler 调用必须串行。解码并行度默认等于 CPU 核数的一半。
- handler 循环按 checkpoint 顺序消费；某个 checkpoint 失败时，队列中该 checkpoint 之后的事件不能先执行。

## 7. 区块间隔任务

配置：

```toml
[blocks.hourlyCheck]
chain = "op"
interval = 300          # 每 300 个块触发一次
start_block = 118000000
```

触发时机是该块的全部日志处理完之后，事件类型为 `BlockInterval`。适用场景：定期快照、汇率刷新、聚合统计。资金结算不要放进 interval handler，它没有交易级别的幂等保证，应该走应用层任务队列。

## 8. 目标性能

阶段目标（以 ERC-20 全量回填 137 万个事件约 32 秒为参考）：

- 回填：≥ 50k events/s（本地 anvil + 本地 Postgres）
- live：单事件处理 P99 < 5ms（不含 RPC 等待）
- 内存：100 万行写入缓存时 < 512MB
- flush：单 checkpoint 批量写入，1000 行 < 20ms

达不到时依次检查：是否逐事件开事务、是否逐行 INSERT、是否在 handler 内做同步 RPC。
