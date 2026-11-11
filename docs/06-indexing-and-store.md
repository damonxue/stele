# 索引与存储

这一层处理四件事：处理函数看到什么、写入何时落库、reorg 如何回滚、崩溃后从哪里恢复。

## 1. 处理函数注册

```rust
pub trait Handler<E>: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        ctx: &'a mut Context<'_>,
        event: Event<E>,
    ) -> BoxFuture<'a, Result<(), IndexingError>>;
}

impl<F, Fut, E> Handler<E> for F
where
    F: Fn(&mut Context<'_>, Event<E>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), IndexingError>>,
    E: Send + 'static,
{ /* blanket impl */ }
```

注册表按 `(contract, event_signature)` 索引：

```rust
pub struct HandlerRegistry {
    logs: HashMap<(ContractId, B256), BoxedLogHandler>,
    transactions: HashMap<(ContractId, Selector), BoxedTxHandler>,
    transfers: HashMap<ContractId, BoxedTransferHandler>,
    traces: HashMap<(ContractId, Selector), BoxedTraceHandler>,
    blocks: HashMap<BlockIntervalId, BoxedBlockHandler>,
}
```

未注册的事件在解码前就被过滤，避免无谓的 ABI 解析。一个事件注册了多个 handler 时按注册顺序串行调用，任一失败则该 checkpoint 整体失败。

`Transfer` 指 ERC-20/ERC-721 的 `Transfer` 日志的简化视图，与 `Log` 二选一使用；同时注册两者会导致同一日志触发两次，启动时校验并报错。

## 2. Store 与 Context

```rust
impl Store {
    pub async fn find<T: OnchainTable>(&self, key: &T::Key) -> Result<Option<T>, IndexingError>;
    pub async fn insert<T: OnchainTable>(&mut self, row: T) -> Result<(), IndexingError>;
    pub async fn update<T: OnchainTable>(&mut self, key: &T::Key, set: T::Update) -> Result<T, IndexingError>;
    pub async fn upsert<T: OnchainTable>(&mut self, row: T) -> Result<(), IndexingError>;
    pub async fn delete<T: OnchainTable>(&mut self, key: &T::Key) -> Result<(), IndexingError>;
    pub async fn raw_sql(&mut self, query: Query<'_>) -> Result<Vec<Row>, IndexingError>;
}

impl Context<'_> {
    pub async fn call<M: SolCall>(&self, contract: Address, call: M) -> Result<M::Return, IndexingError>;
    pub fn contract<C: Contract>(&self) -> C::Instance<'_>;
}
```

规则：

- 写操作只作用于 `OnchainTable`，视图与外部表拒绝写入。
- 主键更新直接报 `NonRetryable`，不做静默覆盖。
- `call` 走带缓存的只读 Provider：同一 `(chain, block, address, calldata)` 只请求一次；缓存按 checkpoint 失效。
- `raw_sql` 允许，但执行前必须 `flush` 缓存，执行后失效相关表的缓存行，保证变更日志完整。

## 3. 内存缓存

写入先进入缓存，不直接落库。回填阶段每事件一次事务会把吞吐压到几千条每秒。

```rust
pub struct Store {
    rows: DashMap<(TableId, RowKey), Arc<Row>>,
    dirty: DashMap<(TableId, RowKey), DirtyKind>,
    pending_checkpoint: Checkpoint,
    flushed_checkpoint: Checkpoint,
}

pub enum DirtyKind { Insert, Update, Delete }
```

- 行以 `Arc<Row>` 保存，更新时复制出新 `Arc`；旧版本由变更日志记录，内存里不保留历史。
- 同一 checkpoint 内对同一行的多次写入合并成一次 flush。
- 缓存大小设上限（默认 200 万行或 512MB，先到者生效）。达到上限时强制 flush 当前批次，索引进度只在完整 checkpoint flush 后推进。
- 插入是乐观的：不查库直接写缓存，唯一约束冲突在 flush 时暴露，按 `User` 错误处理。

## 4. flush

```text
begin
  SET LOCAL stele.checkpoint = '<checkpoint>'
  for each table with dirty rows:
      INSERT ... ON CONFLICT (pk) DO UPDATE   -- 或 COPY 到临时表再 MERGE
      DELETE ... WHERE pk IN (...)
  update stele_schema.progress
commit
```

三条约束：

1. 一个 flush 是一个事务。触发器写变更日志与业务写入在同一事务内，回滚时一起消失。
2. 批量写入。行数超过 100 时用 `COPY` 到临时表 + `INSERT ... SELECT ... ON CONFLICT`，否则用多值 `INSERT`。
3. 进度表只在 flush 成功后推进。崩溃时从 `progress` 读取，回滚窗口内未提交的缓存直接丢弃。

flush 触发条件：处理完一个 checkpoint 且距上次 flush 超过 50ms，或缓存行数超过 5000，或遇到 `raw_sql`。低频小事件可以每 N 个 checkpoint 合并，但同一状态转移不能跨 checkpoint 拆开。

## 5. 变更日志

每张用户表自动挂触发器：

```sql
CREATE TABLE stele_schema.change_log (
    id bigserial PRIMARY KEY,
    table_name text NOT NULL,
    operation char(1) NOT NULL CHECK (operation IN ('I','U','D')),
    row_key jsonb NOT NULL,
    before jsonb,
    after jsonb,
    checkpoint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX change_log_checkpoint_idx ON stele_schema.change_log (checkpoint, id);

CREATE FUNCTION stele_schema.capture_change() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF current_setting('stele.reverting', true) = 'on' THEN
        RETURN NULL;
    END IF;
    INSERT INTO stele_schema.change_log
        (table_name, operation, row_key, before, after, checkpoint)
    VALUES (
        TG_TABLE_NAME,
        left(TG_OP, 1),
        stele_schema.row_key(TG_TABLE_NAME, COALESCE(NEW, OLD)),
        CASE WHEN TG_OP <> 'I' THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP <> 'D' THEN to_jsonb(NEW) END,
        current_setting('stele.checkpoint')
    );
    RETURN NULL;
END $$;
```

采用触发器的理由有两点：

- raw SQL 与未来的批量导入也能被记录，回滚不会漏行。
- 变更日志与业务写入在同一事务，崩溃时天然一致。

代价是每行写入多一次插入。基准测试要单独测这一项；如果成为瓶颈，退路是关闭触发器、由 Store 生成 undo 记录，并把 raw SQL 标记为不支持回滚（需要显式配置）。

`row_key` 由主键列拼成 JSON，回滚时按 `table_name + row_key` 定位。复合主键按列名排序后取值，避免依赖列顺序。实现上提供 `stele_schema.row_key(table_name, row)` 函数，从 `pg_catalog` 读取主键列。

回滚时会再次触发表上的触发器，因此触发器开头要检查会话变量：

```sql
IF current_setting('stele.reverting', true) = 'on' THEN
    RETURN NULL;
END IF;
```

迁移时为索引角色授予 `stele_schema.change_log` 的 `INSERT` 与 `SELECT`，只读角色不授予。触发器函数以表 owner 权限执行时，应在函数上显式 `SECURITY DEFINER` 并固定 `search_path`，避免被调用方篡改。

## 6. 回滚

```text
fn revert(store, target_checkpoint):
    SET LOCAL stele.reverting = 'on'
    rows = SELECT * FROM change_log
           WHERE checkpoint > $target
           ORDER BY checkpoint DESC, id DESC
    for row in rows:
        match operation:
            'I' => DELETE FROM table WHERE pk = row.row_key
            'U' => UPDATE table SET (before) WHERE pk = row.row_key
            'D' => INSERT INTO table (after) WHERE pk = row.row_key
    DELETE FROM change_log WHERE checkpoint > $target
    UPDATE progress SET checkpoint = $target
```

`stele.reverting = 'on'` 在本事务内关闭变更捕获，避免回滚动作本身产生新的日志行。`SET LOCAL` 在事务提交或回滚时自动失效，不需要手动恢复。

按 `checkpoint DESC, id DESC` 逆序执行，等价于事件的反向执行。整段回滚在一个事务里完成，失败则保持原状并重试。

回滚完成后，runtime 通知 sync 层把这些 checkpoint 对应的区间标记为缺失，重新拉取新链数据。

## 7. 崩溃恢复

启动顺序：

1. 读 `progress` 得到最后提交的 checkpoint。
2. 取该 checkpoint 之前 `reorg_window` 个块作为安全边界：live 阶段可能已处理未最终数据，回滚窗口内的全部变更，恢复到 `checkpoint - reorg_window` 对应位置。
3. 清空 `stele_rpc_cache_1` 中该边界之后的缓存区间。
4. 从安全边界继续回填。

不采用"信任数据库现状直接续跑"的优化。回滚的成本低于一次错误记账。

## 8. finalized 清理

每处理到新的 finality 水位：

```sql
DELETE FROM stele_schema.change_log
WHERE checkpoint <= $finalized_checkpoint;
```

清理后，这些 checkpoint 不再可回滚。如果后续 reorg 越过该水位：

```rust
Err(FatalError::ReorgBeyondFinalized {
    depth,
    window: self.reorg_window,
})
```

进程退出，保留现场，等待人工判断。这种情况按错误处理，不做自动修复。

## 9. 错误与重试

| 来源 | 判定 | 处理 |
|---|---|---|
| 唯一约束、外键、CHECK、类型不匹配 | `User` | 记录事件、堆栈、checkpoint；停止任务 |
| handler 返回 `Err` | `User` | 同上，不重试 |
| SQLSTATE `40001` / `40P01` | `Transient` | 回滚 flush，退避重试，最多 5 次 |
| 连接断开、RPC 超时 | `Transient` | 重建连接后重试 |
| reorg 越过 finalized | `Fatal` | 退出进程 |
| schema checksum 不一致 | `Fatal` | 退出进程 |

重试必须回滚到上一次成功 flush 的 checkpoint，不能在缓存已部分修改的状态上重跑 handler。

## 10. 性能要点

- flush 使用 `COPY` 时，触发器开销需要单独测量；必要时按表选择是否启用变更日志。
- 单行 `find` 命中缓存时不访问数据库；未命中也不回查，回填阶段数据尚未落库。
- 解码与 handler 分离：解码可并行，handler 串行。
- handler 内避免 `await` 外部 HTTP。框架不禁止，但外部 HTTP 是常见的尾延迟来源，文档与示例中不推荐。
