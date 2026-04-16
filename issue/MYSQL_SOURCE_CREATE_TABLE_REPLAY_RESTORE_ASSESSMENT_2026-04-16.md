# MySQL pipeline restore 后 CreateTableEvent 未重放问题评估

## 一、文档目的

本文用于沉淀本次 `MySQL pipeline source` 在 `savepoint/checkpoint restore` 场景下，未能重新产出当前 `projection` 对应 `CreateTableEvent` 的问题分析、修复方案与影响评估。

本文重点回答 4 个问题：

- 这次问题应定性为 `MySQL source bug`，还是特意做的增强
- 为什么 fresh start 能自动补字段，而从 `savepoint` 恢复时不能
- 为什么最终选择在 `source` 侧做最小风险修复，而不是继续改 `runtime/schema restore` 语义
- 其他 source 是否需要同步适配，影响范围和风险在哪里

本文是对已有文档 `issue/MYSQL_DORIS_TRANSFORM_CREATE_TABLE_RESTORE_DESIGN_2026-04-14.md` 的后续补充。

前一篇文档偏向早期的 `runtime + Doris sink` 方案讨论；本文记录的是进一步深挖后，最终落地到 `MySQL source` 侧的最小风险实现与定性结论。

## 二、现场背景

客户现场 transform 配置形态如下：

```yaml
transform:
  - source-table: xxsc.(?!order_info|yearcard_team_detail|order_detail_model)
    projection: \*, current_timestamp AS confluent__last_updated
    converter-after-transform: SOFT_DELETE
```

这里有两个关键点：

- `\*` 表示 source table 的全部原始字段
- `current_timestamp AS confluent__last_updated` 是 transform 新增的计算列

客户实际观察到的现象是：

1. 作业全新启动时，下游能自动新增 `confluent__last_updated`
2. 作业从 savepoint 恢复时，下游不会自动新增 `confluent__last_updated`
3. 如果 restore 后 source 端暂时没有新的 binlog 变更，下游会一直停留在旧 4 列 schema

这说明问题不是：

- transform 自身不会生成新增列
- 下游 sink 完全不支持补列

而是：

- restore 后，当前 projection 对应的新 schema 没有被重新送到下游

## 三、问题本质

经过深挖，这次问题更准确的本质是：

- `MySQL pipeline source` 在 restore 场景下，没有主动 replay 当前 split 中已经持久化的 schema
- 因此下游只能继续沿用 savepoint 里恢复出的旧 schema
- 只有等到 restore 之后再出现新的 binlog DML 或 schema change，`CreateTableEvent` 才可能被懒触发补发

所以它不是一个“fresh start 也不正确”的普遍语义缺陷，而是一个 restore 语义缺口：

- fresh start 时，通常后面很快会有 snapshot watermark 或新的 binlog event，因此当前 schema 能继续下发
- restore 时，如果没有新的 binlog，schema 就不会被重新产出

这也解释了为什么用户会看到：

- 不 restore 时自动加字段
- restore 时不自动加字段

## 四、为什么 fresh start 可以，restore 不行

`MySQL pipeline` 原有逻辑对 `CreateTableEvent` 的发送时机是偏懒加载的。

关键代码在：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlPipelineRecordEmitter.java`

原有行为大致是：

1. split 初始化时，只把 schema 同步进 `DebeziumEventDeserializationSchema` 的内部 cache
2. 真正往下游 `output.collect(createTableEvent)`，主要发生在两类时机：
   - snapshot low watermark 到来时
   - 首条 DML / schema change 到来且该表尚未发过 `CreateTableEvent` 时

也就是：

- source 内部“知道当前 schema”
- 不等于下游已经“收到当前 schema 事件”

在 fresh start 下，这个设计通常没问题，因为后续很快会有事件触发补发。

但在 restore 下，如果：

- savepoint 恢复后马上进入 binlog 读取
- 且恢复后没有新的 binlog 事件

那么：

- source 内部虽然已经恢复出 split 和 table schema
- 但没有任何事件触发 `CreateTableEvent` 重新发送
- 下游只能继续沿用旧状态里的 4 列 schema

这正是 `projection: \*, current_timestamp AS confluent__last_updated` 在 restore 后无法触发下游补列的直接原因。

## 五、为什么最终定性为 MySQL source BUG，而不是通用增强

本次更合理的定性是：

- 这是 `MySQL pipeline source` 在 restore 语义下的缺陷修复
- 不是通用 runtime/schema restore 增强

原因有 4 个：

1. 用户当前 projection 已经定义了“当前表应该长什么样”
2. fresh start 本来就能把这个 projection 对应的 schema 送到下游
3. restore 后继续沿用旧 4 列 schema，只是因为 source 没有重放当前 schema，不符合当前作业定义
4. 问题并不在于“系统要不要支持一项新能力”，而在于“restore 后是否还能恢复到当前作业语义”

因此从产品语义上，它更接近：

- `restore 后未恢复当前 schema contract`

而不是：

- `新增了 restore 后主动发 schema 的新特性`

## 六、为什么最终选择 source 侧最小风险修复

一开始可考虑的方向有两类：

1. 改 `runtime/schema restore` 语义，让 restore 后统一重新产出当前 `projection` 对应的 `CreateTableEvent`
2. 改 `MySQL source` restore 行为，让它在 restore 后把 split 中已持久化的当前 schema replay 出来

最终选择第 2 类，原因是风险更小、边界更清楚。

### 6.1 runtime 方案的问题

如果把这次问题定性成 runtime 通用语义问题，意味着要改：

- 公共 schema restore 语义
- 公共 `CreateTableEvent` 传播逻辑
- 甚至可能影响所有 source / sink 的恢复链路

这样虽然“理论上更统一”，但代价很大：

- 很难证明其他 connector 都需要这个变化
- 很难保证不会把其他 source / sink 的历史边界一次性放大
- 很难把风险收敛在用户当前反馈的 MySQL restore 主链路上

### 6.2 source 方案的优势

MySQL 当前 restore 场景本身已经具备 replay 所需数据：

- `MySqlBinlogSplit` 中持久化了 `tableSchemas`
- savepoint/checkpoint 恢复时，这些 schema 会一起恢复出来

关键代码在：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/split/MySqlSplitSerializer.java`

这里 `snapshot split` 和 `binlog split` 都会序列化 `tableSchemas`。

因此 source 侧可以直接利用 restore 出来的 split state：

- 不需要重新去数据库做 schema 探测
- 不需要改公共 runtime 语义
- 不需要引入新的 state 格式

这就是最小风险实现的核心依据。

## 七、最终实现

本次最终实现只动了 MySQL 相关的 5 个文件，总体规模为：

- `5 files changed`
- `403 insertions`
- `3 deletions`

改动范围收敛在：

- MySQL source reader
- MySQL record emitter 扩展 hook
- MySQL pipeline record emitter
- 2 个针对性 testcase

### 7.1 `MySqlSourceReader` 增加 restore split 的 pending replay 语义

文件：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlSourceReader.java`

核心逻辑：

1. 不再依赖外部 `SAVEPOINT_PATH` 等配置猜测 restore
2. 按 Flink 实际 reader 生命周期处理：
   - `start()` 前注入到 reader 的 split，视为 restore split
3. 对 restore 的 binlog split 做标记
4. `initializedState()` 时把这个 restore 标记传给 emitter
5. `pollNext()` 优先消费 emitter 的 pending outputs
6. `isAvailable()` 在有 pending outputs 时立即就绪

这意味着 restore 后即使没有新的 binlog：

- reader 也可以先把 schema replay 事件吐给下游

### 7.2 `MySqlRecordEmitter` 增加通用 hook

文件：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlRecordEmitter.java`

新增 3 个扩展点：

- `pollPendingOutput()`
- `hasPendingOutput()`
- `applySplit(split, restoredFromSavepointOrCheckpoint)`

默认实现都是 no-op，因此：

- 非 pipeline MySQL emitter 行为不变
- 只有需要 restore replay 的 pipeline emitter 才会使用这些 hook
- 旧的 `applySplit(split)` 保留但标记为 `@Deprecated`，避免后续新实现继续挂在旧入口上

这也是本次“局部改动、不扩大公共语义”的关键。

### 7.3 `MySqlPipelineRecordEmitter` 在 restored binlog split 上 replay 当前 schema

文件：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlPipelineRecordEmitter.java`

核心逻辑：

1. `applySplit(split, restored)` 时，先把 split 里的 `tableSchemas` 同步进 deserializer cache
2. 如果当前是 `restoredFromSavepointOrCheckpoint && split.isBinlogSplit()`：
   - 把当前 schema 转成 `CreateTableEvent`
   - 放入 `pendingOutputs`
   - 同时加入 `alreadySendCreateTableTables`
3. `pollPendingOutput()` 按顺序把这些 replay 事件吐出去

这里额外做了两个控制：

- 对 `tableChanges` 按 `tableId` 排序，保证 restore replay 输出稳定
- replay 时先标记 `alreadySendCreateTableTables`，避免首条 DML 再重复补发

### 7.4 为什么只覆盖 restored binlog split

当前实现只对：

- `restore 后的 binlog split`

做主动 replay。

没有扩展到：

- 所有 snapshot split restore
- 所有公共 incremental source restore

这是有意的最小风险取舍。

原因是：

- 用户问题发生在 savepoint 后继续 binlog 读取的主链路
- MySQL 的 `CreateTableEvent` 缺口也主要体现在 restore 后没有新 binlog 时
- snapshot 阶段本来就有 low watermark 触发 schema 下发的路径

这里需要特别强调的是：

- “只覆盖 restored binlog split” 不是本次修复新引入的缺口
- 它是历史上一直存在、但此前没有被单独修复和验证的边界

也就是说，本次做的是：

- 把用户当前命中的主链路缺陷补齐

而不是：

- 把所有 restore 场景统一重构为完全一致的 schema replay 语义

## 八、测试覆盖与验证

本次补了两层 testcase。

### 8.1 reader 级 restore 语义验证

文件：

- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/test/java/org/apache/flink/cdc/connectors/mysql/source/reader/MySqlSourceReaderTest.java`

新增用例：

- `testRestoreSplitAssignedBeforeStartCanReplayPendingOutput`
- `testBinlogSplitAssignedAfterStartDoesNotReplayPendingOutput`
- `testSnapshotSplitAssignedBeforeStartDoesNotReplayPendingOutput`

这些用例分别验证：

- `start()` 前 addSplits 的 binlog split 会被识别为 restore split
- reader 在没有真实 binlog 输入时，也能先消费 emitter 的 pending output
- `start()` 后再分配的 binlog split 不会误判成 restore replay
- snapshot split 即使在 `start()` 前分配，也不会触发本次新增的 replay 语义

### 8.2 pipeline 级 savepoint restore 回归

文件：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/test/java/org/apache/flink/cdc/connectors/mysql/source/MysqlPipelineNewlyAddedTableITCase.java`

新增用例：

- `testSavepointRestoreReplaysCreateTableEventBeforeNewBinlog`

该用例验证：

1. 作业先正常启动
2. 消费一条真实 binlog `DataChangeEvent`
3. 触发 savepoint
4. 从 savepoint 恢复
5. restore 后在没有新 binlog 的情况下，第一条事件就是对应表的 `CreateTableEvent`
6. 随后再写入一条新数据，断言收到的是 `DataChangeEvent`，而不是重复的 `CreateTableEvent`

这个用例直接覆盖了用户当前最关心的问题：

- restore 后不依赖新 binlog，也要能重新把当前 schema 送到下游

### 8.3 已有验证记录

本次改动已有针对性验证记录：

```bash
mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc \
  -am \
  -Dtest=MySqlSourceReaderTest#testRestoreSplitAssignedBeforeStartCanReplayPendingOutput,MySqlSourceReaderTest#testBinlogSplitAssignedAfterStartDoesNotReplayPendingOutput,MySqlSourceReaderTest#testSnapshotSplitAssignedBeforeStartDoesNotReplayPendingOutput \
  -DfailIfNoTests=false test
```

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql -am \
  -Dtest=MysqlPipelineNewlyAddedTableITCase#testSavepointRestoreReplaysCreateTableEventBeforeNewBinlog \
  -DfailIfNoTests=false integration-test
```

```bash
mvn -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql -am \
  -DskipTests -DskipITs test-compile
```

实际验证结果为：

- `MySqlSourceReaderTest` 中 3 个 restore 相关 reader 用例均通过
- `MysqlPipelineNewlyAddedTableITCase#testSavepointRestoreReplaysCreateTableEventBeforeNewBinlog` 真实 Docker/Testcontainers IT 通过
- 当前验证结论支持：本次修复已覆盖 MySQL savepoint restore + binlog split + 无新 binlog 的用户主诉场景

## 九、影响范围评估

### 9.1 直接影响范围

本次代码影响范围明确收敛为：

- MySQL source reader
- MySQL pipeline emitter
- MySQL pipeline restore 主链路

不会直接影响：

- runtime 公共 schema restore 语义
- 其他 sink 的 metadata apply 逻辑
- 非 pipeline 的 MySQL emitter 主路径

### 9.2 行为变化范围

restore 后的行为变化只有一条核心差异：

- 对 restored binlog split，会在首条新 binlog 到来前，主动 replay 当前 schema 对应的 `CreateTableEvent`

这是一次性行为，不是持续性开销。

对 fresh start、普通运行态的影响非常小：

- fresh start 行为不变
- 非 restore 路径不变
- 后续 DML 处理逻辑不变

### 9.3 状态兼容性

这次实现没有引入新的 state 格式，也没有改 split 序列化协议。

依赖的是已有 state 中已经持久化的：

- `tableSchemas`

因此从状态兼容性看，风险较低：

- 不需要 state migration
- 不需要 savepoint 升级转换
- 不会破坏已有 split 反序列化逻辑

## 十、风险评估

### 10.1 可接受风险

本次改动存在的主要风险有：

1. restore 后会比以前更早看到 `CreateTableEvent`
2. 多表 binlog split restore 时，会一次性 replay 多个表的 schema
3. 某些下游如果历史上隐含依赖“restore 后没有 schema event”，现在可能更早暴露其兼容性边界

但这些风险整体可控，原因是：

- `CreateTableEvent` 本身就是合法 schema 事件
- replay 使用的是 split 中已持久化的当前 schema，不是推断出来的新语义
- `alreadySendCreateTableTables` 已避免 restore 后首条 DML 再重复补发
- replay 只发生一次，且仅限 restored binlog split
- reader 回放路径沿用 `output.collect(...)` 计数，未引入额外的 `numRecordsIn` 手工累加

### 10.2 已规避的高风险点

本次特意没有做以下事情：

- 没有修改 runtime 公共 schema 恢复语义
- 没有修改 base incremental source 公共 reader
- 没有为所有 source 一次性引入 restore replay
- 没有引入重新探测 DB schema 的额外逻辑
- 没有改变 savepoint/state 格式

这些都是为了把风险严格收敛在 MySQL 当前问题面上。

### 10.3 本轮 review 已消除的实现风险

在 PR 二轮 review 后，本次实现还额外收敛了几项工程风险：

- 已移除无关的 `PostTransformOperator` cosmetic diff，确保改动面仍然只落在 MySQL 相关文件
- 已补 `@Deprecated` 标记，引导后续扩展优先使用 `applySplit(split, restored)`
- IT 中的超时获取逻辑已补 `iterator.close()` 与 `future.cancel(true)`，降低 Testcontainers 阻塞导致的卡死风险

### 10.4 仍然保留的边界

当前实现并不等价于“所有 restore 场景都已完全统一”。

仍然保留的边界包括：

- 只对 restored `binlog split` 主动 replay
- snapshot restore 不刻意扩展
- 其他 source 尚未统一拥有这套 pending replay 机制

这不是遗漏，而是最小风险实现的明确边界。

这条边界的风险需要分开看：

1. 对当前 MySQL 用户问题的风险较低
2. 对跨 source 语义一致性的风险中等

原因是：

- 当前客户问题就是 `savepoint/checkpoint restore + binlog split + restore 后无新 binlog`
- 这条主链路已经被本次修复直接覆盖
- 未覆盖的 `snapshot restore` 和其他 source restore，更接近“历史仍保持原行为”，而不是“本次修复造成新的行为退化”

因此，这里的准确结论应是：

- 它是历史存在的边界，不是本次带来的新 bug
- 它不是当前 PR 的 blocker，但应在后续 source 级评估中继续跟进

## 十一、其他 source 是否都需要适配

当前不建议直接下结论：

- “所有 source 都必须跟着改”

更稳妥的结论是：

- 本次应先定性为 `MySQL 特有缺陷修复`
- 其他 source 需要按实现机制和 restore testcase 逐个核验
- 没有证据前，不建议直接上升为公共框架改动
- 现阶段不能把 “其他 source 还未适配” 解释为本次修复引入的新问题

### 11.1 Postgres 的初步判断

相关代码：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres/src/main/java/org/apache/flink/cdc/connectors/postgres/source/reader/PostgresPipelineRecordEmitter.java`
- `flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc/src/main/java/org/apache/flink/cdc/connectors/postgres/source/reader/PostgresSourceReader.java`

初步观察到：

- Postgres 在 `applySplit(split)` 时也会把 schema 同步到 deserializer
- 但真正的 `CreateTableEvent` 发送仍主要依赖 low watermark 或后续 DML/schema change
- 现有 restore testcase 是“restore 后再插入新数据，再验证出现 `CreateTableEvent`”

也就是说，Postgres 现有用例并没有证明：

- `restore 后无新 WAL` 时，当前 schema 会不会主动 replay

因此对 Postgres 的正确动作不是立刻照搬 MySQL 实现，而是先补同类 testcase：

- `restore 后不写任何新数据，直接断言是否先收到当前 CreateTableEvent`

### 11.2 Oracle 的初步判断

相关代码：

- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-oracle/src/main/java/org/apache/flink/cdc/connectors/oracle/source/reader/OraclePipelineRecordEmitter.java`
- `flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-oracle/src/main/java/org/apache/flink/cdc/connectors/oracle/source/reader/OracleTableSourceReader.java`

Oracle 的特点是：

- 启动时会初始化 `createTableEventCache`
- 并同步到 deserializer

但从事件输出路径看，它真正往下游发 `CreateTableEvent`，仍主要发生在：

- snapshot 全量发
- low watermark
- 后续 DML/schema change 到来时

现有 Oracle restore 用例同样是：

- restore 后先写新数据
- 再断言后面能看到 `CreateTableEvent`

所以 Oracle 也不能直接证明：

- `restore 后无新 redo log` 时，当前 schema 会主动 replay

### 11.3 为什么现在不直接改公共 base

Postgres / Oracle 当前都建立在 base incremental reader 体系上：

- `IncrementalSourceReader`
- `IncrementalSourceRecordEmitter`

这个公共 base 目前只有：

- `applySplit(split)`

并没有：

- restore pending output replay
- pre-start addSplits 视为 restore split 的专用 hook

如果现在直接把 MySQL 这套逻辑上提到 base：

- 影响面会一下子扩大到所有基于 incremental source 的 connector
- 风险明显高于本次 MySQL 局部修复

因此当前更合理的策略是：

1. 先把 MySQL 问题按 source bug 修掉
2. 再给 Postgres / Oracle 补相同 restore-no-new-data testcase
3. 只有当多个 source 都表现出同类缺陷时，再评估是否上提公共抽象

从风险归因上也应注意：

- 如果后续 Postgres / Oracle 被验证在 `restore 后无新日志` 场景下同样不主动 replay schema，应定性为各自历史实现边界
- 不应定性为 “MySQL 本次修复带来了新的跨 source 不一致 bug”

## 十二、可用性评估

从当前实现和验证情况看，本次修复的可用性判断如下。

### 12.1 对当前用户问题可用

它能直接解决的核心问题是：

- transform projection 修改后
- 作业从旧 savepoint/checkpoint 恢复
- restore 后 source 暂时没有新的 binlog
- 但下游仍需要立即感知当前 schema

这正对应客户当前场景：

- `projection: \*, current_timestamp AS confluent__last_updated`

restore 后，下游能够重新收到当前 projection 对应的 `CreateTableEvent`，从而触发自动补列。

### 12.2 对 fresh start 不构成回归

fresh start 下：

- 原有 schema 发送逻辑仍然保留
- 没有 restore replay 这条支路

因此不预期引入新的主路径行为变化。

### 12.3 对下游的预期变化是正向的

对于依赖 `CreateTableEvent` 做 schema 收敛的下游来说：

- restore 后能更早拿到当前 schema
- 不再依赖“必须等一条新 binlog 才能补 schema”

这和用户直觉是一致的，也是 restore 语义更合理的表现。

### 12.4 剩余边界对可用性的实际影响

当前剩余边界对可用性的影响可总结为：

- 对 MySQL 当前用户主场景：影响已消除
- 对 MySQL snapshot restore：暂未扩大语义，但未观察到当前客户问题
- 对其他 source：仍需单独验证，当前只能说“未知是否同类存在”，不能说“被本次改动引入”

因此从上线视角看：

- 本次修复对当前问题的收益是确定的
- 剩余边界主要影响后续能力一致性评估，不构成当前修复方案的阻塞

## 十三、建议的对外口径

如果需要在 PR、issue 或 release note 中描述这次变更，建议采用以下口径：

- 修复 `MySQL pipeline source` 在 savepoint/checkpoint restore 后无法主动重放当前 schema 的问题
- 该问题会导致 transform projection 已变化时，下游在无新 binlog 的情况下继续沿用旧 schema
- 本次修复在 restore 的 binlog split 上 replay 已持久化的 `CreateTableEvent`
- 改动范围收敛在 MySQL source，不是通用 schema restore 框架改造

不建议使用的口径是：

- 所有 source 的通用 schema restore 增强
- 统一重构了公共恢复语义

因为这会扩大这次改动的真实范围，也不符合当前最小风险实现。

## 十四、后续建议

### 14.1 先补 testcase，再决定其他 source 是否需要适配

建议对 Postgres / Oracle 至少补一条共同模式的 testcase：

1. 启动作业
2. 进入流式阶段
3. 打 savepoint
4. restore
5. restore 后不写任何新数据
6. 直接断言是否先收到当前 `CreateTableEvent`

如果这些 source 也失败，再讨论：

- source 侧各自补 replay
- 还是抽到 base incremental source 公共能力

### 14.2 暂不扩大到公共框架

在没有更多 source 证据前，不建议现在就把：

- restore split 识别
- pending replay
- schema replay

统一上提到公共 base。

当前最合理的推进顺序是：

- 先合入 MySQL 局部 bugfix
- 再做其他 source 的针对性验证

## 十五、最终结论

本次问题最终应定性为：

- `MySQL pipeline source` 在 restore 场景下的缺陷修复

不是：

- 通用 runtime/schema restore 增强

根因是：

- MySQL source 在 restore 后只恢复了 split 中的 schema cache
- 但没有在“无新 binlog”时主动 replay 当前 schema 对应的 `CreateTableEvent`
- 导致下游继续沿用旧 4 列 schema

本次实现选择在 MySQL source 侧完成修复，利用 split 中已持久化的 `tableSchemas`，在 restored binlog split 上主动 replay 当前 schema，做到：

- 只改 MySQL
- 不改公共 runtime
- 不改 state 格式
- 不依赖新 binlog
- 不引入额外 DB schema 探测

从影响范围、状态兼容性和行为变化看，这是一种边界清晰、风险可控、可用性较高的最小风险实现。

对其他 source 的结论则应保持克制：

- 目前不能直接说“都需要适配”
- 也不能直接说“都没有这个问题”
- 正确做法是按 restore-no-new-data testcase 逐个核验，再决定是否扩展

对当前仍保留的“只覆盖 MySQL restored binlog split”边界，则应明确写成：

- 它是历史存在的实现边界，不是本次修复引入的新 bug
- 它的主要影响是跨 source / 跨 restore 场景语义尚未完全统一
- 它不影响本次 PR 解决当前 MySQL 用户主诉问题的有效性
