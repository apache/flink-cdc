# MySQL Restore 后计算列未生效问题评估

## 一、文档目的

本文用于沉淀以下真实场景的最终结论：

- 旧作业 yaml 中没有 `CURRENT_TIMESTAMP AS confluent__last_updated`
- 旧 savepoint/checkpoint 里恢复出的 schema 也没有该列
- 新 yaml 新增了 `CURRENT_TIMESTAMP AS confluent__last_updated`
- 作业从旧 savepoint/checkpoint 恢复后，实测下游未自动补列，也未写入该列
- 同样的新 yaml 如果全新启动，不从 savepoint/checkpoint 恢复，则下游会自动补列并写入数据

本文重点回答 6 个问题：

1. 这是不是单纯的 `MySQL source bug`
2. 为什么此前“source 侧 replay CreateTableEvent”测试通过，但现场仍可能不闭环
3. `restore` 后新增计算列真正要经过哪几段链路
4. 当前分支是否已经覆盖并验证了用户真实场景
5. “只覆盖 MySQL restored binlog split”是本次带来的新问题，还是历史边界
6. 风险、收益、稳定性、可用性如何评估

## 二、现场背景

客户 transform 配置形态如下：

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

1. 全新启动时，下游能自动新增 `confluent__last_updated`
2. 从旧 savepoint/checkpoint 恢复时，下游不会自动新增 `confluent__last_updated`
3. 恢复后新数据也可能继续按旧 4 列处理，看起来像“新增计算列完全没生效”

这说明问题不能只看 source 是否 replay 了旧 schema，还必须看 end-to-end 链路是否真正把“新 projection 对应的新 schema”落到了下游。

## 三、修正后的问题本质

最初把问题近似理解为：

- `MySQL pipeline source restore 后没有 replay 当前 CreateTableEvent`

这个判断只覆盖了问题的一部分，不能单独证明用户真实场景已经闭环。

结合现场复盘和当前代码，真实问题应拆成 3 段：

### 3.1 source 侧

`MySQL source` 在 `savepoint/checkpoint restore` 后，必须先把 split state 中的当前 schema replay 出来。

如果这里没有 replay：

- `PreTransformOperator` 恢复后拿不到新的 `CreateTableEvent`
- 它也就无法按新的 projection 重新生成带 `confluent__last_updated` 的 schema

这部分确实是 `MySQL source` 的历史缺陷。

### 3.2 transform / schema 侧

即使 source replay 了 `CreateTableEvent`，也还要看 restore 后：

- `PreTransformOperator` 是否会按新 yaml 重新产出带计算列的 schema
- `SchemaOperator / SchemaCoordinator` 是否会把这个“扩展后的 CreateTableEvent”继续放行，而不是因为旧 state 存在就跳过

这部分不是 source 自己能单独证明的。

### 3.3 sink 侧

即使 source 和 transform 都正确，仍然要看 sink 如何处理：

- 下游表在 restore 前往往已经存在
- runtime/schema 层对一对一路由下的“扩展 schema”仍可能继续以 `CreateTableEvent` 形式往后传
- 如果 sink 在“表已存在 + 收到扩展 CreateTableEvent”时不会做 reconcile/add column，那么列仍然不会真正补到下游

所以最终结论是：

- `source replay` 是必要条件
- 但不是 end-to-end 闭环的充分条件

## 四、为什么 fresh start 能成功，而 restore 可能失败

这是现场最容易误判的地方。

### 4.1 fresh start 路径

全新启动时：

- source 正常发送 `CreateTableEvent`
- transform 直接按当前 yaml 生成最新 projection schema
- sink 侧通常面对的是“首次建表”场景

因此 `confluent__last_updated` 能自然出现在下游表中。

### 4.2 restore 路径

从旧 savepoint/checkpoint 恢复时：

- source / schema state 先恢复出旧 4 列视图
- 新 yaml 虽然已经变成 `*, CURRENT_TIMESTAMP AS confluent__last_updated`
- 但如果 source 不 replay 当前 schema，transform 根本拿不到重新推导新 schema 的触发点
- 即使 replay 了，sink 也面对“表已存在”的场景，而不是 fresh start 的首次建表

因此 restore 场景比 fresh start 多了两个额外要求：

1. source 必须 replay
2. sink 必须能处理“已存在表上的扩展 CreateTableEvent”

## 五、当前分支上的实现与职责划分

### 5.1 MySQL source 侧修复了什么

当前分支里的 MySQL 修复点是：

- 在 restored `binlog split` 上 replay split state 中已持久化的 `tableSchemas`
- 让 reader 在没有新 binlog 的情况下也能先把 `CreateTableEvent` 吐给下游

这解决的是：

- `restore 后无新 binlog 时，transform/schema 根本没有机会重新看到当前 schema`

因此这部分应定性为：

- `MySQL source` 的历史 bugfix

### 5.2 transform / schema 侧现在是什么语义

当前代码说明两点：

1. `PreTransformOperator` 已不再依赖自身持久化 state 恢复 schema，而是显式依赖 source 重新发出正确的 `CreateTableEvent`
2. restore 后如果收到扩展后的 `CreateTableEvent`，schema/runtime 侧不应把它误判成冗余并跳过

因此：

- source replay 是 transform restore 正确性的前提
- 但 transform/schema 本身也需要有针对性测试来证明不会吃掉这个扩展 schema

### 5.3 Doris sink 侧现在是什么语义

当前 Doris 侧实现已经支持：

- 当表已存在且收到一个包含更多列的 `CreateTableEvent` 时
- 不直接忽略，也不盲目报“表已存在”
- 而是把缺失列识别出来并执行 reconcile / add column

这部分不是 source bug，而是：

- sink 对 restore 后扩展 `CreateTableEvent` 的兼容能力

如果没有这部分，即使 source replay 修复存在，用户场景依然可能不闭环。

## 六、这次到底是 bugfix 还是增强

需要分开定性，不能一句话打平。

### 6.1 对 MySQL source 而言

这是 bugfix。

理由：

- 当前作业定义已经变化，restore 后理应恢复到当前作业语义
- source restore 后只恢复 cache、不重放当前 schema，不符合 restore 语义
- fresh start 与 restore 行为不一致，本质是历史缺陷

### 6.2 对 Doris sink 兼容而言

更准确的说法是：

- 这是为 restore 场景补齐“已存在表 + 扩展 CreateTableEvent”处理能力的必要修复

它不是这次 MySQL source 修复“带出来的新 bug”，而是此前链路上一直存在、但没有被这组场景完全打透的历史缺口。

### 6.3 对其他 source / sink 而言

当前不应直接上升为：

- “所有 source 都必须跟着改”
- “所有 sink 都已经天然兼容”

更稳妥的结论是：

- MySQL source replay gap 是 MySQL 的历史缺陷
- 其他 source 是否存在相同 restore-no-new-log 缺口，要靠同类 testcase 验证
- 其他 sink 是否支持“已存在表 + 扩展 CreateTableEvent”的 reconcile，也要按 sink 各自评估

## 七、当前分支的证明链条

要证明用户真实场景闭环，至少要有下面几层证据。

### 7.1 source reader 级

`MySqlSourceReaderTest` 已覆盖：

- `testRestoreSplitAssignedBeforeStartCanReplayPendingOutput`
- `testBinlogSplitAssignedAfterStartDoesNotReplayPendingOutput`
- `testSnapshotSplitAssignedBeforeStartDoesNotReplayPendingOutput`

它们证明：

- restored binlog split 会被识别并 replay pending output
- 非 restore 误判不会发生
- snapshot split 不会被错误纳入本次最小风险逻辑

### 7.2 source pipeline 级

`MysqlPipelineNewlyAddedTableITCase` 已覆盖：

- `testSavepointRestoreReplaysCreateTableEventBeforeNewBinlog`

它证明：

- restore 后即使没有新 binlog，也能先收到 source replay 的 `CreateTableEvent`

但这一层只证明 source replay，不足以直接证明“旧 savepoint + 新 projection 计算列”已经闭环。

### 7.3 MySQL restore + transform 级

当前分支新增：

- `MysqlPipelineNewlyAddedTableITCase#testSavepointRestoreReplaysTransformedCreateTableEventForComputedColumnProjectionChange`

它直接模拟真实用户场景：

1. 首次作业 projection 是 `*`
2. 打 savepoint
3. restore 后 projection 改为 `*, CURRENT_TIMESTAMP AS confluent__last_updated`
4. 在 `PreTransform + PostTransform` 之后取数
5. 断言 restore 后第一条就是扩展过的 `CreateTableEvent`
6. 再写一条新数据，断言新的 `DataChangeEvent` 已经带 5 列，且计算列非空

这条测试非常关键，因为它第一次把“旧 savepoint + 新 projection”本身证明出来了。

### 7.4 schema / runtime 级

当前代码里已有：

- `SchemaEvolveTest#testRestoredCreateTableEventWithExpandedSchemaIsNotSkipped`

它证明：

- runtime/schema 层面对“restore 后收到扩展 CreateTableEvent”时不会错误跳过
- 随后的 `DataChangeEvent` 会按新 schema 继续透传

此外当前分支新增：

- `DataSinkOperatorWithSchemaEvolveTest#testCreateTableEventWithExpandedSchemaAfterFailover`

它补充证明：

- sink wrapper 在 failover 后收到扩展 `CreateTableEvent` 和随后的扩展 `DataChangeEvent` 时，不会把新 schema 吞掉

### 7.5 Doris sink 级

当前代码里已有两条直接相关用例：

- `DorisMetadataApplierITCase#testDorisExistingTableReconcilesMissingComputedTimestampColumn`
- `DorisEventSerializerTest#testCreateTableEventRefreshesJsonSchemaAfterRestoreWithComputedTimestampColumn`

它们分别证明：

- Doris 已存在表收到扩展 `CreateTableEvent` 时会补缺失列
- Doris serializer 在 restore 后先看到旧 schema，再看到新 `CreateTableEvent` 时，会刷新写出 schema，并能正确写出 `confluent__last_updated`

## 八、对“当前分支是否彻底解决用户场景”的结论

结论需要分成两个层次。

### 8.1 如果只看最早那部分 MySQL source 修复

不能宣称已经彻底解决用户真实场景。

因为 source 修复只证明：

- `CreateTableEvent` 能 replay 回来

它没有单独证明：

- transform 是否按新 yaml 重新产出扩展 schema
- schema/runtime 是否会保留该扩展 schema
- sink 在表已存在时是否会补列

### 8.2 结合当前分支已有和新增测试后

对 `MySQL -> transform -> runtime/schema -> Doris` 这条用户主链路，可以给出更强结论：

- 当前分支已经有足够的证据链说明该场景被覆盖
- 尤其是 “旧 savepoint + 新 projection 计算列” 这条此前缺失的 restore+transform 用例，现在已经补上
- Doris 对“已存在表 + 扩展 CreateTableEvent”的 reconcile 也已有直接测试

因此，当前更准确的结论是：

- 这条用户主链路在当前分支上已经基本闭环
- 但这个结论依赖的不只是 source 修复，还依赖 transform/schema 与 Doris sink 侧现有能力

## 九、剩余边界与影响范围

### 9.1 只覆盖 MySQL restored binlog split

当前 source 侧主动 replay 只覆盖：

- MySQL restored `binlog split`

没有扩展到：

- snapshot restore
- 其他 source 的 restore

这条边界不是本次带来的新 bug，而是历史一直存在的实现边界。

准确口径应是：

- 本次补齐了当前客户命中的主链路
- 没有顺带把所有 restore 场景统一重构

### 9.2 其他 source 的风险

对 Postgres / Oracle 等其他 source：

- 目前不能直接说都需要照搬 MySQL
- 也不能直接说都没有这个问题

正确做法是先补一条同类 testcase：

1. 启动作业
2. 进入流式阶段
3. 打 savepoint
4. restore
5. restore 后不写任何新日志
6. 直接断言是否先收到当前 `CreateTableEvent`

如果失败，再定性为各自 source 的历史缺陷。

### 9.3 其他 sink 的风险

即使 source replay 能工作，也不代表所有 sink 都已经闭环。

任何 sink 只要采用类似语义：

- 表已存在
- 收到扩展 `CreateTableEvent`

就必须回答一个问题：

- 是忽略、报错，还是 reconcile/add column

因此本次真正已经被证明的 end-to-end 场景应收敛为：

- MySQL source
- transform/runtime 当前语义
- Doris sink

对其他 sink，仍建议按各自 metadata applier / serializer 行为再做验证。

## 十、风险、收益、稳定性、可用性评分

基于当前代码与测试覆盖，建议评分如下：

| 维度 | 评分 | 结论 |
| --- | --- | --- |
| 收益 | 9.5 / 10 | 直接命中客户主诉：旧 savepoint 恢复到新 projection 后，计算列需要立即生效 |
| 风险 | 3 / 10 | source 侧变更仍收敛在 MySQL restored binlog split，未扩大公共 base 语义 |
| 稳定性 | 8.5 / 10 | 证明链条已经覆盖 source、transform/schema、Doris sink；仍需持续观察真实 IT/CI 运行情况 |
| 可用性 | 9 / 10 | 对 MySQL + Doris 主场景可用性高；对其他 source/sink 不应过度承诺 |
| 综合 | 9 / 10 | 当前是边界清晰、收益明确、风险可控的实现 |

## 十一、建议的对外口径

建议 PR / issue / release note 采用下面的描述：

- 修复 MySQL pipeline source 在 savepoint/checkpoint restore 后无法主动 replay 当前 schema 的问题
- 补齐 restore 后 transform projection 变更场景下的 CreateTableEvent 回放验证
- 对 Doris，支持在表已存在时处理扩展 CreateTableEvent，并 reconcile 缺失列
- 当前闭环场景为 MySQL + transform/runtime + Doris，不应表述为“所有 source/sink 的统一 restore 增强”

不建议继续使用的口径：

- “source 一处修复后，所有 restore 新增列问题都已解决”
- “这是所有 source 的统一语义修复”

## 十二、最终结论

最终结论如下：

1. `MySQL source restore 后不 replay 当前 schema`，这是 MySQL 的历史 bug。
2. 但用户真实场景不是一个 source 单点问题，而是一条 end-to-end 链路问题。
3. 仅有 source 修复，不能单独证明“旧 savepoint + 新计算列 projection”一定已经闭环。
4. 当前分支之所以可以较有把握地说问题已被覆盖，是因为同时具备：
   - MySQL source replay 修复
   - restore + transform 的直接 testcase
   - schema/runtime 不跳过扩展 CreateTableEvent 的验证
   - Doris 对已存在表 reconcile 缺失列的验证
5. “只覆盖 MySQL restored binlog split”不是本次带来的新 bug，而是历史边界；它不是当前 PR 的 blocker，但后续仍应对其他 source 按同类 testcase 继续评估。

因此，对当前用户主链路的最准确结论是：

- 当前分支已经基本覆盖并验证了 `old savepoint/checkpoint + new projection(computed column) + MySQL + Doris` 这一真实场景
- 但这个结论不应被夸大成“source 一处修复后，全平台所有 restore 新增列问题都已彻底解决”

## 十三、2026-04-17 最终复核更新

这一节用于覆盖本文前面仍残留的旧口径。最新代码和真实 IT 结果表明，最终闭环点并不只是在 `Doris sink`。

### 13.1 最新根因收敛

最终根因已经收敛为两段缺口，而不是单一 source 问题：

1. `MySQL source` 在 `restore` 后没有 replay 当前 `CreateTableEvent`
2. 即使 source replay 了扩展后的 `CreateTableEvent`，`regular SchemaCoordinator` 在“外部表已存在”场景下，如果仍把它只当成幂等 `CREATE TABLE` 处理，很多 metadata applier 都不会自动转成 `ADD COLUMN`

这意味着：

- `source replay` 只解决“事件重新出现”
- `schema coordinator external reconcile` 才解决“已存在下游表真正补列”

### 13.2 最新实现语义

当前分支的最终实现不是“继续扩大 MySQL source replay 范围”，而是两层修复配合：

1. `MySQL source`
   - 仅对 restored `binlog split` replay split state 中已持久化的 schema
   - 保持最小风险，不改 state 格式，不扩散到 snapshot split / 其他 source
2. `flink-cdc-runtime` 的 `regular SchemaCoordinator`
   - 当收到 restore 后扩展过的 `CreateTableEvent`
   - 且 `SchemaChangeBehavior` 为 `EVOLVE / TRY_EVOLVE / LENIENT`
   - 且 external table 已存在、当前 evolved schema 与新 schema 不一致
   - 则对 external metadata applier 实际应用的是 `SchemaMergingUtils.getSchemaDifference(...)` 推导出的差异事件，例如 `AddColumnEvent`
   - 但对下游 writer / response event，仍保留原始扩展后的 `CreateTableEvent`

这个语义很关键：

- external metadata 真正做补列
- downstream writer 仍看到完整新 schema
- 不破坏原有 restore 后 writer 依赖 `CreateTableEvent` 刷新本地 schema 的路径

### 13.3 最新验证结果

截至 `2026-04-17`，已经完成以下验证：

1. runtime 回归测试通过
   - `SchemaEvolveTest#testRestoredCreateTableEventWithExpandedSchemaReconcilesExistingMetadata`
   - `DataSinkOperatorWithSchemaEvolveTest#testCreateTableEventWithExpandedSchemaAfterFailover`
   - `PostTransformOperatorRestoreTest#testRestoreWithUpdatedComputedProjectionStillExpandsCreateTableEvent`
2. MySQL source + transform restore 用例通过
   - `MysqlPipelineNewlyAddedTableITCase#testSavepointRestoreReplaysTransformedCreateTableEventForComputedColumnProjectionChange`
3. 真实 MySQL full pipeline restore IT 通过
   - `MysqlPipelineNewlyAddedTableITCase#testFullPipelineRestorePropagatesComputedColumnProjectionChangeToSink`
   - 实测日志显示：
     - 初始作业先产出 4 列 `CreateTableEvent`
     - restore 后先产出带 `confluent__last_updated` 的 5 列 `CreateTableEvent`
     - restore 后新插入数据带非空 `confluent__last_updated`

因此，针对“旧 savepoint/checkpoint 恢复到新 projection 计算列后，下游不补列、不写该列”的主诉，当前分支已经有真实 end-to-end 证据证明问题被打通。

### 13.4 本次验证过程中额外暴露的历史问题

在搭建 full pipeline IT 时，还额外发现了一个与本次业务修复无关、但会干扰验证的历史问题：

- `ValuesDatabase` 构造主键字符串时写死使用 `recordData.getString(primaryKeyIndex)`
- 当 values sink 表的主键是 `BIGINT` 时，会抛 `IndexOutOfBoundsException`

这个问题不是此次 restore 语义问题的根因，但会让 values sink 的 materialized-in-memory IT 提前失败，从而掩盖真正 restore 行为。

当前已补：

- `ValuesDatabase` 改为按列类型通用读取主键值
- `ValuesDatabaseTest#testApplyDataChangeEventWithNonStringPrimaryKey` 回归测试已通过

这项修复属于测试辅助 sink 的历史缺陷修复，不改变本次 production 语义判断。

### 13.5 对“是否彻底解决”的最终口径

最终可以给出的准确口径是：

- 对 `MySQL restored binlog split + transform projection 新增计算列 + regular schema operator + existing sink table` 这条主链路，问题已被修复并完成真实 IT 验证
- 这次不应再表述为“只是 Doris 兼容修复”；更准确地说，是 `regular SchemaCoordinator` 补齐了 existing table 上的 external reconcile 语义
- “只覆盖 MySQL restored binlog split”不是这次引入的新 bug，而是本来就存在的历史边界；当前 PR 只是选择最小风险先闭环用户命中的主路径
- snapshot restore、Postgres、Oracle 等其他 source 是否存在同类 restore-no-new-log 缺口，仍需用同类 testcase 分别验证

## 十四、2026-04-17 当前 PR 最终评估更新

这一节用于回答一个更直接的问题：

- 当前 PR 是否已经解决本次提到的问题和需求
- 当前 PR 的影响范围、剩余风险和可用性评分如何

### 14.1 当前结论

按本次真实主诉链路评估，当前 PR 结论是：

- 已解决

这里的“已解决”指的是以下完整链路已经闭环，而不是单点 source 修复：

1. 旧 savepoint/checkpoint 对应旧 YAML，没有 `CURRENT_TIMESTAMP AS confluent__last_updated`
2. 新 YAML 新增了 computed column projection
3. 作业从旧 savepoint/checkpoint 恢复
4. 下游表仍以旧 4 列存在
5. restore 后需要自动补列，并让后续写入带上新列值

当前 PR 能解决这一链路，依赖的是两段修复同时成立：

1. `MySQL source` 在 restore 后重新 replay 当前 `CreateTableEvent`
2. `regular SchemaCoordinator` 对“已存在下游表 + 扩展后的 CreateTableEvent”执行 external reconcile，把幂等 create 转成真实 schema diff，例如 `AddColumnEvent`

如果缺少第 2 段，那么即使 source replay 了 5 列 `CreateTableEvent`，很多已存在的下游表仍不会真正补列；这也是前面现场验证和最早 IT 结论不一致的根本原因。

### 14.2 代码与测试证据

当前 PR 的证据链已经完整覆盖 source、transform、runtime 和 full pipeline：

1. source restore replay
   - `MySqlSourceReader`
   - `MySqlPipelineRecordEmitter`
   - `MySqlSourceReaderTest#testRestoreSplitAssignedBeforeStartCanReplayPendingOutput`
2. transform restore 后仍按当前 projection 扩展 schema
   - `PostTransformOperatorRestoreTest#testRestoreWithUpdatedComputedProjectionStillExpandsCreateTableEvent`
3. runtime 对 existing table 做 external reconcile
   - `SchemaCoordinator#getSchemaChangesToApplyExternally(...)`
   - `SchemaEvolveTest#testRestoredCreateTableEventWithExpandedSchemaReconcilesExistingMetadata`
4. sink/writer 在 failover 后继续按扩展 schema 工作
   - `DataSinkOperatorWithSchemaEvolveTest#testCreateTableEventWithExpandedSchemaAfterFailover`
5. 真实 MySQL full pipeline savepoint IT
   - `MysqlPipelineNewlyAddedTableITCase#testFullPipelineRestorePropagatesComputedColumnProjectionChangeToSink`

真实 IT 的最终结果已经证明：

- 初始作业先产出旧 4 列 `CreateTableEvent`
- restore 后会先产出带 `confluent__last_updated` 的 5 列 `CreateTableEvent`
- restore 后新插入的数据能够带上非空 `confluent__last_updated`

这说明当前 PR 已经不只是“restore 后重新发事件”，而是已经覆盖“下游补列 + 后续数据写值”这两个用户真正关心的结果。

### 14.3 影响范围

当前 PR 的影响范围可以分成两层：

1. source 侧影响
   - 仅限 `MySQL restored binlog split`
   - fresh start 不变
   - snapshot restore 不变
   - 其他 source 不变
2. runtime 侧影响
   - 位于 `flink-cdc-runtime` 的 regular schema evolve 公共路径
   - 但只在以下条件同时满足时触发：
     - `SchemaChangeBehavior` 为 `EVOLVE / TRY_EVOLVE / LENIENT`
     - 收到的是扩展后的 `CreateTableEvent`
     - 当前 evolved schema 已存在
     - 当前 evolved schema 与 incoming schema 不一致

因此，它不是一个“广泛修改所有 source 行为”的高扩散改动，但也不是纯粹的 MySQL 私有修复。真正的 production 语义变化，落在 runtime 对 existing table 的 reconcile 行为上。

### 14.4 风险评估

当前剩余风险主要有三类：

1. 公共 runtime 路径风险
   - `SchemaCoordinator` 属于 regular schema evolve 公共路径
   - 理论上会影响所有走这条路径、并且会收到扩展 `CreateTableEvent` 的 sink
   - 但触发条件窄，且行为方向是把历史缺口显式化并补齐，不是放大 source 输入
2. 历史边界仍在
   - 本次仍只覆盖 `MySQL restored binlog split`
   - snapshot restore、Postgres、Oracle 等其他 source 是否存在同类问题，仍需分别验证
   - 这不是本次新引入的 bug，而是历史上就存在、此前未被完整覆盖的边界
3. 当前工作区混入无关改动
   - `TransformParser`
   - `TransformSqlOperatorTable`
   - `PostTransformOperator` 中未参与主修复语义的 logger 改动
   - 这些改动不改变本次主结论，但如果与主修复一起进入同一个 PR，会扩大 reviewer 的审阅范围并抬高合入风险

因此，对主修复链路本身的风险判断是“低到中低”；对“当前整个工作区改动集合”的风险判断则高于主修复本身。

### 14.5 可用性与评分

基于当前代码、测试覆盖和真实 IT 结果，建议将当前 PR 评分更新为：

| 维度 | 评分 | 结论 |
| --- | --- | --- |
| 解决度 | 9.5 / 10 | 已命中并解决“旧 savepoint/checkpoint 恢复到新 computed projection 后，下游不补列也不写值”的真实主诉 |
| 影响可控性 | 8.5 / 10 | source 侧收敛在 MySQL restored binlog split；runtime 侧虽是公共路径，但触发条件明确且收敛 |
| 风险 | 4 / 10 | 主修复风险较低；若将无关 parser/兼容性改动混入同一 PR，则 review 风险上升 |
| 稳定性 | 8.5 / 10 | 已有 source、runtime、transform、full pipeline 四层验证；仍需持续观察更多真实 sink 场景 |
| 可用性 | 9 / 10 | 对当前用户主链路可用性高；对其他 source/sink 不应过度承诺 |
| 综合 | 9 / 10 | 当前 PR 已经解决主问题，收益明确，风险总体可控 |

### 14.6 最终评审结论

对当前 PR 最准确的评审结论是：

- 它已经解决了本次提到的问题和需求，但解决方式不是单点 `MySQL source` 修复，而是 `MySQL restore replay + runtime existing-table reconcile` 的组合修复
- “只覆盖 MySQL restored binlog split”不是本次带来的新 bug，而是历史边界仍未扩展；这不是当前 PR 的 blocker
- 如果 PR 最终只保留主修复链路相关改动，则这是一个收益明确、影响可控、可以合入的修复
- 如果 parser/VARIANT/logger 等无关改动继续混在同一 PR，需要在合入前拆分或明确说明，否则会徒增 review 风险，但不改变主修复结论
