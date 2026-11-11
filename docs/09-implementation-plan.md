# 实施评审与计划

## 已确认的实施目标

后续确认以本节为准。下文保留原始评审和答复，其中关于 V1 非目标、待选语义和阶段范围的建议不再作为已批准的限制。

- 完成全部阶段，包括三种排序模式和以秒计算的 reorg 窗口。
- V1 目标覆盖该基线的功能，包括 GraphQL、PGlite 和开发模式。Rust 不直接运行 TypeScript 索引函数或 npm 插件；对应能力需要 Rust API 和独立验收。
- 索引 handler 支持与基线一致的写入型 SQL，并验证写入缓存、事务和 reorg 回滚行为。
- 服务在内网开放，数据库访问仍使用适当的只读角色和网络边界。
- 以 RWAX TypeScript 索引器为目标应用进行对比。对比前需要记录双方实际锁定的依赖版本，并建立一份基线 fixture。

实现状态：阶段 0 到 3 的主体能力已落地并通过测试，详见 [README](../README.md) 的“当前状态”。对齐状态仍应通过可重复的功能测试、reorg 测试和目标应用行级对比确认，不能仅凭 API 名称判断。

## 结论

可以实施。这里描述的是一个新的 Rust 索引框架，需要按阶段交付和验证，不能按一次普通功能改动估算。

本次审查已阅读 [README](../README.md) 列出的全部设计文档。当前实现以三种排序模式和以秒计算的 reorg 窗口为验收方向，审查时工作树没有未提交改动。

现有设计已经定义了主要模块、数据流和验收方向。开始编码前，需要先确认下文列出的语义选择，并修订几处会影响兼容性或数据正确性的文档。本文只记录评审和计划，不代表批准开始实现。

## 已确认的基础

- [同步层设计](./04-sync-layer.md) 中的 `stele_rpc_cache_1` 九张表使用固定列名，适合作为缓存行级对比的基础。
- [运行时设计](./05-runtime-and-ordering.md) 的 checkpoint 长度和字段布局为 75 位定长字符串。
- [Schema 与存储设计](./03-config-and-schema.md) 和 [索引与存储设计](./06-indexing-and-store.md) 已明确 Postgres、批量写入和 reorg 回滚是 V1 的核心能力。
- GraphQL、PGlite 和开发模式已纳入 V1 的功能对齐目标；具体实现采用 Rust 方案。托管服务仍不属于目标。

## 需要先修订的文档

| 文档 | 发现 | 建议 | 对首个可运行版本的影响 |
|---|---|---|---|
| [README](../README.md) | 未写明验收范围与版本基线。 | 固定验收范围与更新流程；范围变更时新增一次兼容性评审。 | 需要 |
| [Workspace 与公共 API](./02-workspace.md) | `Handler<E>` 的 async 生命周期、异构 handler 注册表和 `sol!` 宏生成尚未验证。示例使用了 `anyhow::Result`，依赖表没有定义它。 | 将 handler 擦除适配层和宏展开列为实现前 spike；确定公共错误类型和 MSRV，再锁定 Cargo 依赖版本。 | 需要 |
| [配置与 Schema](./03-config-and-schema.md) | 当前模型是单一 `contract -> chain`，尚不支持同一个 ABI 的按链覆盖、事件 topic 过滤、`"latest"` 范围、账户和 block interval 的按链配置。 | 明确 Rust V1 是只支持单链注册，还是支持等价展开；若选择等价展开，补充 `AccountConfig`、`BlockConfig`、`Filter` 和按链覆盖的模型。 | 需要 |
| [运行时与事件排序](./05-runtime-and-ordering.md) | 文档将默认 `Multichain` 写成全局排序。`multichain` 没有跨链顺序保证；全局 `(timestamp, chain id, block number)` 顺序属于 `omnichain`。 | 引入并定义 `Omnichain`、`Multichain` 和 `Isolated` 的语义，更新架构、路线图和 ADR-0008。 | 需要 |
| [运行时与事件排序](./05-runtime-and-ordering.md) | 文档称区块事件先执行，后文又称 interval 在区块其他事件后执行。区块 checkpoint 使用最大 transaction index，实际在该块的交易、日志和 trace 后处理。 | 删除矛盾描述，写出固定排序表和对应的 golden tests。 | 需要 |
| [索引与存储](./06-indexing-and-store.md) | `Transfer` 被描述为 ERC-20/ERC-721 日志的简化视图，实际应为账户触发的 native transfer，由带 value 的 call trace 派生。还需 `setup` 处理函数，并拒绝同一事件名注册多个 handler。 | 明确 V1 支持的触发器、注册重复规则和 `setup` 是否纳入范围。任何语义变更都写入 ADR。 | 需要 |
| [索引与存储](./06-indexing-and-store.md) | 统一 `change_log` 只保存 `table_name`，无法区分不同 schema 的同名表；JSON 回放也没有定义列类型恢复和标识符引用规则。`raw_sql` 的允许范围与缓存失效规则不够具体。 | 把目标表标识改为 schema 加 table，或改为 `regclass`；先完成复合主键、跨 schema、插入、更新、删除和崩溃恢复的 Postgres spike。将 `raw_sql` 限制为可审计的受支持操作。 | 需要 |
| [配置与 Schema](./03-config-and-schema.md) | 普通 Postgres view 会随着基表的事务提交和回滚反映当前数据，不会产生独立的回滚滞后。 | 将“视图不参与回滚且可能滞后”的表述改为普通 view 的一致读语义；若以后增加 materialized view，再单独定义刷新和滞后策略。 | 需要 |
| [API、CLI 与运维](./07-api-cli-ops.md) | `/sql` 的 AST 白名单思路正确，但 `work_mem = 512MB` 与 100 个连接组合会放大内存上限。公开绑定、只读角色和认证责任也需要成为部署前置条件。 | 将资源限制配置化且保守设置；把公开监听、认证和只读数据库角色列为生产部署的必选项，并为 SQL 拒绝规则增加攻击用例。 | 阶段 3 前需要 |
| [实施路线](./08-roadmap.md) 与 [决策记录](./decisions.md) | 路线从功能开发开始，没有先验证 handler API 和通用回滚日志两个风险最高的点。 | 在阶段 0 前增加短期 spike，并把已确认的有意差异记录为 ADR。 | 需要 |

## 语义与行为决定

### 跨链排序

配置参考定义了三种模式：

| 模式 | 语义 |
|---|---|
| `omnichain` | 全局确定性排序，按 block timestamp、chain id 和 block number 推进；慢链会限制整体进度。 |
| `multichain` | 默认模式。每条链按自身 EVM 顺序处理，跨链没有顺序保证。 |
| `experimental_isolated` | 每条链独立处理，并要求表主键包含 `chain_id`。 |

当前设计把 `Multichain` 定义成第一种语义。Rust API 沿用这三种模式名称，避免使用者理解出现歧义。

### reorg 窗口和 finality

`reorgWindow` 按区块时间戳计算秒数，默认 180 秒，允许范围是 0 到 600 秒。相关逻辑在 finality 与链配置中。

当前 Rust 设计使用默认 200 个块，并提供 `Confirmations` 与 `FinalizedTag`。这可以成为更明确的 Rust 原生设计，但与按秒的窗口语义不同。需要在以下方案中选择一个：

1. 以按秒的 reorg 窗口为基准，`reorg_window` 改为秒数，并按时间戳寻找 finality 边界。
2. 保留块数和最终性标签，记录为有意差异，并让对比测试只比较已经共同确认的区间。

这个选择会影响同步、缓存清理、崩溃恢复和行级对比工具，因此必须在阶段 0 前确定。

### 事件与注册

事件构造和索引函数需要说明以下行为：

- transaction、log、trace、native transfer 和 block interval 的 checkpoint 使用固定事件类型值。区块事件在同一块的其他事件之后处理。
- native transfer 来自 trace，不是 ERC-20 或 ERC-721 的 `Transfer` 日志。
- 同一事件名只能注册一个处理函数。
- 每个合约可注册一次 `setup`，在事件索引开始前运行。

Rust V1 不需要在第一阶段覆盖所有触发器，但需要在 [路线图](./08-roadmap.md) 中明确每个阶段支持哪些触发器。第一阶段建议只支持静态地址的 log 事件，其他触发器通过保留的注册接口返回明确的“不支持”错误，而不是静默忽略。

### 配置、工厂和多链注册

合约配置需要支持按链覆盖、过滤器和 `"latest"`。factory 模式不要求工厂本身也被注册为用户合约，并支持嵌套参数路径或显式 topic/offset 位置。账户也可使用 factory。

当前设计中的 `FactoryRef { parent, event, parameter }` 更窄。建议先确认是否只支持已注册工厂及顶层 address 参数。若是，应在 README 和 ADR 中明确这是 V1 限制；若否，工厂配置和 fragment 测试需要扩展。

## 实现前需要确认的事项

1. 首个批准范围是阶段 0 的同步与解码闭环，还是完整阶段 0 到 4。
> 是完整的阶段0-4, 全部阶段完成;
2. 跨链排序和 reorg finality 采用哪套语义。
> 参考已经确定的语义
3. V1 是否必须支持 accounts、native transfer、call trace、block interval、`setup` 和多 handler。建议按路线图分阶段支持。
> v1版本就要支持全部功能
4. 目标应用是否是 RWAX TypeScript 索引器，以及用于行级对比的合约、链、区间和期望表结构。
> 使得
5. 生产环境的 Postgres 版本、允许的 Rust MSRV、部署平台和是否需要多用户 schema。
> 不需要, 保持一致就好.
6. API 是否会对公网开放；如会开放，认证方式、反向代理、TLS、限流和只读数据库角色由谁提供。
> 不对公网开发, 内网开放
7. 是否允许 handler 执行写入型 raw SQL。建议 V1 只允许参数化的受支持写操作，或先只提供只读 raw SQL。
> 保持一致.

## 建议的实施顺序

### 准备和 spike

这一步先验证最难替换的设计选择，不交付完整框架。

1. 创建 Cargo workspace、CI 基础检查和 Postgres 16 测试容器配置，固定 Rust toolchain 与依赖版本。
2. 为 checkpoint 和 `stele_rpc_cache_1` 建立 golden fixture。测试 Rust 编码、缓存 DDL 和规范化行数据。
3. 实现最小的 `sol!` 生成、类型擦除注册表和 async handler 原型，验证闭包可以在持有 `Context` 时安全 await。
4. 在 Postgres 中验证统一变更日志：两个 schema、复合主键、insert/update/delete、单事务回滚、进程中断恢复和 reorg 回放。

通过条件：

- 原型能编译，且没有依赖 `unsafe`、全局可变状态或未定义生命周期转换。
- checkpoint 和缓存 fixture 完全一致。
- 回滚原型恢复到同一份用户表数据，并能证明不会把 schema A 的记录回放到 schema B。

任何一项失败时，先调整 API 或改用每表回滚表，再进入功能开发。

### 阶段 0：单链日志同步和解码

范围：

- TOML 配置读取、单链静态地址合约、ABI 加载和启动校验。
- fragment、区间差集、`eth_getLogs` 回填和 `stele_rpc_cache_1` 写入。
- checkpoint 排序、事件解码和输出 JSON 的示例程序。

验收：

- 指定区间重复运行时，缓存行数和区间记录保持不变。
- 中断后能从已完成区间继续。
- 使用相同 RPC 数据时，两次运行的 logs、blocks、intervals 和 checkpoint 一致。

### 阶段 1：单链索引、存储和 reorg

范围：

- `OnchainTable`、迁移 diff、缓存写入、进度表、变更日志和恢复。
- 静态地址 log handler、批量 flush 和明确定义的错误分类。
- Anvil reorg 和进程中断测试。

验收：

- 1 块和 10 块深度 reorg 后，用户表与全量重放结果相同。
- 约束错误不推进 checkpoint；可重试错误从最后提交点恢复。
- 回滚窗口以已确认的单位和语义清理记录。

### 后续阶段

阶段 2 再加入 factory、多链、accounts、native transfer、trace 和 block interval。阶段 3 实现 SQL over HTTP、SSE、CLI 与部署边界。阶段 4 用目标应用做行级对比、性能测试和迁移演练。原有 [路线图](./08-roadmap.md) 的 12 到 15 周估算应在 spike 结果出来后重新确认。

## 评审阶段的变更范围

评审阶段新增了本文，并在 [README](../README.md) 中登记。实现进度和已知限制以 [README](../README.md) 与各 crate 的说明为准。
