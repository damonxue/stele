# 架构决策记录

每条记录按背景、决定、后果、替代方案排列。

---

## ADR-0001：运行时语义优先，不做 API 兼容

背景：配置与执行接口在 Rust 和 TypeScript 之间无法逐字复刻。

决定：以运行时语义（排序、reorg 窗口、backfill/live、store 行为）为唯一契约，配置文件格式与 API 形状由 Rust 侧定义。概念名保留（contract、fragment、checkpoint、reorg window），便于描述和做行级对比。

后果：使用方需要按 Rust 配置与 handler 重写应用，不能直接复用 TypeScript 配置。对比测试可以用同一份合约与区间验证输出。

替代方案：在 Rust 里嵌入 JS 运行时执行 TypeScript 配置文件。放弃的原因是引入 V8、构建链复杂、类型优势消失。

---

## ADR-0002：同步缓存使用固定表名与列

背景：同步缓存是纯框架内部数据，可以任意设计。

决定：使用 `stele_rpc_cache_1` schema、固定表名与列名，`numeric` 宽度统一为 `78,0`。共 9 张：blocks、logs、transactions、transaction_receipts、traces、rpc_request_results、intervals、factories、factory_addresses。

后果：缓存结构稳定，可以直接用历史回填结果做对照。代价是继承了既有命名的历史包袱，例如 `transaction_receipts.contract_address` 的语义不精确。

替代方案：重新设计为更规范的 `chain.blocks` / `chain.logs`。放弃的原因是对比成本高，收益只是列名更整齐。

---

## ADR-0003：Schema 用 derive 宏 + diff 生成迁移

背景：表定义需要与 TypeScript 侧保持对应，并在开发模式自动建表。

决定：Rust 侧用 `#[derive(OnchainTable)]`，运行时由宏产出的元数据与 `pg_catalog` 做 diff，生成迁移步骤。开发模式可自动应用安全步骤，生产模式只产出 SQL 文件。

后果：表定义与查询类型都在编译期检查，迁移可审计。代价是需要维护一套 diff 引擎，重命名等语义歧义必须显式声明。

替代方案：手写 `sqlx::migrate!` 迁移文件。放弃的原因是表结构改动要写两遍（Rust 类型与 SQL），容易漂移。

---

## ADR-0004：V1 只做 SQL over HTTP 与 SSE，不做 GraphQL

背景：需要按 schema 自动生成 GraphQL。Rust 侧有 `async-graphql`，但需要从 schema 生成类型与解析器。

决定：先提供 `/sql` 与 `/sql/live`，覆盖同一批读场景；`graphql` feature 预留，阶段 3 之后评估。

后果：前端需要写 SQL 或使用轻量查询构造器，不能直接用 GraphQL 客户端。换来的是更小的实现面与更直接的安全模型。

替代方案：直接上 `async-graphql`。不采用的原因是类型映射（`U256`、`numeric`）与分页语义工作量大，且现有 rwax 前端通过 Rust API 访问数据，不需要 GraphQL。

---

## ADR-0005：回滚依赖数据库触发器写变更日志

背景：reorg 需要把表恢复到分叉点。两种实现：应用侧生成 undo 记录，或数据库触发器记录变更。按表拆分时，每张用户表配一张 `_reorg__<table>`。

决定：每张用户表挂 `AFTER INSERT/UPDATE/DELETE` 触发器，统一写 `stele_schema.change_log`，用会话变量 `stele.checkpoint` 标注所属 checkpoint。stele 把回滚记录合并成一张表，回滚与清理都按 checkpoint 扫描。

后果：raw SQL 与批量导入也能被完整记录；变更日志与业务写入同事务，崩溃一致。代价是每行写入多一次插入，触发器需要随迁移一起维护。

替代方案：Store 在 flush 时根据缓存差异生成 undo。不采用的原因是绕过 Store 的写入无法回滚。若基准显示触发器不可接受，按表关闭并显式记录该限制。

---

## ADR-0006：写入先进入内存缓存，批量 flush

背景：逐事件开事务在回填阶段会把吞吐压到几千条每秒。

决定：行数据以 `Arc<Row>` 存在并发 map 中，写入时复制，按 checkpoint 批次 flush。索引进度只在完整 checkpoint 落库后推进。

后果：回填吞吐由数据库往返次数决定，可以做到 5 万条每秒以上。代价是崩溃时窗口内的数据要丢弃，内存占用随批量增大。

替代方案：每事件一个事务。放弃的原因是吞吐不足，实测数据也说明缓存是必要的。

---

## ADR-0007：开发模式用监督进程重启，不做进程内热重载

背景：Node 里可以热替换模块，Rust 没有等价的安全机制。

决定：`dev` 启动监督进程，监听文件变化，编译成功后启动新子进程，旧进程优雅退出。编译失败保留旧进程继续服务。

后果：Rust 的编译时间直接进入反馈循环。缓解手段是示例工程小、按 feature 裁剪、`cargo check` 先行。重启方式也不会出现新旧代码共存的半状态。

替代方案：动态库热加载。放弃的原因是不安全，且与 async 运行时状态冲突。

---

## ADR-0008：V1 串行处理同一链的事件，跨链并发不做

背景：`isolated` 模式让每条链独立推进，牺牲跨链顺序。

决定：V1 只实现 `Multichain` 排序，全局按 checkpoint 执行，链间可以并行拉取但 handler 串行。`Isolated` 留到阶段 2 之后。

后果：慢链会拖住快链，V1 接受这个代价。一旦引入 `isolated`，所有表必须包含 `chain_id` 主键并分区，属于破坏性 schema 约束，要提前规划。

替代方案：一开始就做 `isolated`。不采用的原因是跨链业务逻辑少见，顺序语义先保持简单可控。

---

## ADR-0009：不内置认证、多实例选主与托管

背景：这些能力都交给部署层。

决定：`/sql` 默认绑定 `127.0.0.1`，认证由反代或用户 middleware 提供；索引进程单实例，多副本需要外部选主；不提供托管服务与遥测上报。

后果：部署方需要自行解决鉴权、TLS、选主。换来的是没有隐藏行为，框架不引入额外的信任边界。

替代方案：内建 JWT 与 advisory lock 选主。放弃的原因是不同部署环境的认证体系差异太大，内建方案会变成又一个要维护的产品。
