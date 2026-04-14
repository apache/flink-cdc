# MySQL CDC -> Doris transform 新增计算列恢复问题设计文档

## 一、文档目的

本文用于沉淀本次 `MySQL CDC -> Doris` 在 transform 中新增计算列后出现的 schema 恢复与 Doris 现有表兼容问题，覆盖以下内容：

- 现场问题与复现现象
- 根因分析
- 修复目标与设计方案
- 代码改动范围
- 风险与可用性评估
- 当前验证结果与后续建议

同时明确本次实现边界：

- 本次改动范围收敛为 “只确保 Doris 可用”
- `Paimon / StarRocks / OceanBase / Iceberg / Hudi / Fluss / MaxCompute` 等其他 sink 只做影响评估，不在本次实现范围内
- `Oracle` 在当前仓库中是 source connector，不存在同类 sink reconciliation 改动

本文聚焦的问题线是：

- transform `projection` 中新增 `CURRENT_TIMESTAMP AS CONFLUENT__LAST_UPDATED`
- 作业从旧 `cp/sp` 恢复后，新列 schema 没有真正下发到 Doris sink
- Doris 表已手工加列时，新列值仍然不写入
- Doris 表已存在但缺该列时，fresh start 直接报错，而不是自动补列
- Doris 已补列后，真实集群中 FE schema 可见性慢于 sink serializer 的默认等待窗口，导致首条写入误失败
- Doris FE schema 查询瞬时失败时，serializer 会直接失败，没有在 schema catch-up 预算内继续重试

## 二、问题背景

用户现场的核心配置形态如下：

```yaml
transform:
  - source-table: streampark.t_flink_app
    primary-keys: ID
    projection: |
      deploy_mode as DEPLOY_MODE,
      project_id,
      job_name,
      CURRENT_TIMESTAMP AS CONFLUENT__LAST_UPDATED
```

用户总结了 5 类可复现场景：

1. 全新启动，Doris 表不存在时，能自动建表，`CONFLUENT__LAST_UPDATED` 有值。
2. 从旧 `cp/sp` 恢复时，下游 Doris 表不会新增 `CONFLUENT__LAST_UPDATED`。
3. 从旧 `cp/sp` 恢复，并且下游已手工加列时，该列值仍然不会写入。
4. 下游表已存在但未手工加列，作业 fresh start 时直接报错。
5. 下游表已存在但未手工加列，从旧 `cp/sp` 恢复时不报错，但也不会自动新增该列。

其中用户明确要求：

- 第 3 类问题必须修复
- 第 4 类问题需要评估并尽量落成自动补列方案

## 三、问题定义

这次不是单纯的 “Doris 缺字段” 问题，而是三段链路同时存在缺陷：

1. runtime/common 层把新的 `CreateTableEvent` 错误判定为冗余，导致新 schema 在恢复场景下根本没有继续传播。
2. Doris sink 在处理已有物理表时，`CreateTableEvent` 缺少 reconciliation 逻辑，不能把目标 schema 和 Doris 现状自动对齐。
3. Doris serializer 对 Doris FE schema catch-up 和 schema 查询失败都采用了过于乐观的处理方式，既假设 FE 物理 schema 会很快可见，也把单次 schema 查询失败当成终态错误。

因此出现三类看似不同、实则同源的问题：

- 恢复时即使 Doris 已经手工加了列，sink 仍按旧 schema 写数据，所以值丢失。
- fresh start 时 Doris 表已存在但少一列，sink 直接走 `CREATE TABLE` 失败，不能自动补齐缺列。
- 即使 Doris `ADD COLUMN` 已经执行成功，只要 FE schema 仍短暂返回旧列集，serializer 也会在第一条新 schema 数据到达时提前失败。
- 即使只是 Doris FE schema 查询出现短暂失败，serializer 也会立即报错，而不是在同一 catch-up 时间预算内继续重试。

## 四、根因分析

### 4.1 根因一：`CreateTableEvent` 冗余判断过于激进

问题点位于：

- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/utils/SchemaUtils.java`

旧逻辑对 `CreateTableEvent` 的判断是：

- 只要当前 schema 已存在，就认为该 `CreateTableEvent` 冗余

也就是：

```java
return latestSchema.isPresent();
```

这在普通重复快照场景下问题不大，但在下面这个恢复场景下是错误的：

1. 旧作业生成了 3 列 schema，并写入 checkpoint/savepoint。
2. 用户修改 transform，新增了 `CONFLUENT__LAST_UPDATED`。
3. 新作业从旧 `cp/sp` 恢复。
4. 恢复后 runtime 仍拿着旧 schema 状态。
5. 新的 `CreateTableEvent(4列schema)` 到来时，被旧逻辑判定为 “表已存在，因此冗余”。
6. 结果：
   - coordinator 不再下发新的 `CreateTableEvent`
   - sink metadata applier 看不到新 schema
   - serializer 仍按旧 schema 输出

这正是用户第 2、3 类现象的直接根因。

同时这也意味着：

- `SchemaUtils.java:359` 这一公共层修正不是可选优化，而是本次 Doris 恢复修复的必要前置条件
- 如果不改这里，Doris 侧后续新增的 reconciliation 和 serializer catch-up 逻辑都无法在 restore 主链路上真正生效

### 4.2 根因二：Doris sink 对 “已有表 + CreateTableEvent” 缺少对齐能力

问题点位于：

- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplier.java`

旧逻辑处理 `CreateTableEvent` 的方式基本等价于：

- 直接执行 `CREATE TABLE`

这导致两类问题：

1. Doris 表已存在但少列时，fresh start 会直接失败。
2. Doris 表已存在且已手工加列时，即使物理表已经满足目标 schema，sink 也没有显式接受这份目标 schema 并刷新自己的 schema cache。

因此，Doris 侧不仅需要“建表”，还需要“已有表对齐”。

### 4.3 根因三：serializer 对 Doris FE schema catch-up 与查询失败都过于乐观

问题点位于：

- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializer.java`

serializer 在 JSON 写入路径下，会先查询 Doris FE 当前物理 schema，再确认 Doris 列集是否已经覆盖 input schema。

这个校验本身是合理且必须保留的，因为如果 Doris 物理列还没准备好就继续写，会直接造成新列值丢失或写入失败。

问题在于旧实现隐含了两个错误前提：

- `ADD COLUMN` 成功后，Doris FE schema 会在约 5 秒内稳定可见
- 单次 Doris FE schema 查询失败应立即视为终态错误

真实集群里这个前提并不总成立。于是会出现下面的时序：

1. metadata applier 已经对 Doris 执行了 `ADD COLUMN`
2. 新 schema 的 `DataChangeEvent` 到达 serializer
3. serializer 向 Doris FE 查询物理 schema
4. FE 暂时仍返回旧 3 列，而 input schema 已经是 4 列
5. serializer 判断 “Doris physical schema did not catch up with input schema” 并失败

因此这里的根因不是 “需要把校验去掉”，而是：

- 现有实现把外部系统 schema 可见性延迟，错误建模成了一个过小的固定等待窗口
- 现有实现没有把 schema 查询瞬时失败纳入同一个 catch-up 时间预算

## 五、修复目标

本次修复目标分为三部分：

### 5.1 必修目标

修复从旧 `cp/sp` 恢复后，新 transform 计算列没有真正生效的问题。

具体要求：

- 恢复后新的 `CreateTableEvent` 不能再被错误吞掉
- 即使 Doris 表是手工提前加列的，sink 也要开始按新 schema 写入该列值

### 5.2 增强目标

改善 Doris 表已存在但少 transform 新增列时的启动行为。

具体要求：

- fresh start 时，如果 Doris 表已存在但缺少新增列，不再直接失败
- 在可控范围内自动执行 `ADD COLUMN`

### 5.3 稳定性目标

在 Doris 已执行 schema 对齐后，serializer 不应再因为 FE schema 的短暂可见性延迟而误失败。

具体要求：

- 保留 Doris physical schema 覆盖校验，不做静默降级
- 将等待 Doris FE schema catch-up 的逻辑做成更稳的内部默认行为
- 将 Doris FE schema 查询的瞬时失败也纳入同一时间预算内重试
- 不把该等待参数暴露成用户侧配置

## 六、设计方案

### 6.1 方案一：修正 `CreateTableEvent` 冗余判定

把 `CreateTableEvent` 的冗余定义从：

- “表已存在即冗余”

改为：

- “表已存在且 schema 完全相同，才冗余”

新语义：

```java
return latestSchema.isPresent()
        && latestSchema.get().equals(createTableEvent.getSchema());
```

这样在恢复旧状态时：

- 如果 transform 没改，重复的 `CreateTableEvent` 仍然会被去重
- 如果 transform 改了，schema 已经发生变化，则新的 `CreateTableEvent` 会继续向下游传播

这个改动位于公共层，因此会影响所有依赖该冗余判断链路的 sink/operator。

需要特别说明：

- 对 Doris，这是必须的正确性修复
- 对其他 sink，这会让历史上被错误吞掉的 “schema 已变化的 `CreateTableEvent`” 真正继续传播
- 因此它可能暴露其他 sink 早已存在、但以前被冗余判断掩盖的 existing-table 兼容性边界

### 6.2 方案二：Doris `CreateTableEvent` 增加已有表 reconciliation

对 Doris sink 的 `CreateTableEvent` 处理改为两段式：

1. 先通过 Doris FE REST schema 查询，判断目标表是否已存在。
2. 分支处理：
   - 不存在：走原有 `CREATE TABLE`
   - 已存在：对比 Doris 物理列与目标 schema，进行 reconciliation

reconciliation 的具体语义是：

- 如果 Doris 已经具备目标 schema 的所有列：
  - 不报错
  - 接受该 `CreateTableEvent`
  - 刷新 sink 内部的 schema cache 为目标 schema

- 如果 Doris 缺少目标 schema 中的一部分列：
  - 计算缺失列集合
  - 依据目标 schema 中的相邻列关系推导 `ADD COLUMN` 位置
  - 逐列执行 `ADD COLUMN`
  - 完成后把 schema cache 刷新为目标 schema

### 6.3 当前 reconciliation 的边界

本次实现只处理：

- 缺列补列
- 已有列接受并同步 schema cache

本次不处理：

- 类型漂移自动修复
- rename 漂移自动修复
- drop 漂移自动修复
- 主键、分区键定义差异自动修复

这是一个有意收敛的范围控制，避免把本次问题扩大成通用 schema diff 引擎。

### 6.4 方案三：保留严格校验，但修正 serializer 的时间假设

对 Doris serializer 的策略调整为：

- 继续在 JSON 写入前校验 Doris physical schema 是否已经覆盖 input schema
- 不做静默 fallback，不允许跳过新增列直接写旧列集
- 将原先过小的等待窗口改为更稳的内部默认等待策略
- 将 Doris FE schema 查询失败也视为可恢复状态，只要还在时间预算内就继续重试
- 等待期间持续轮询 Doris FE schema，直到：
  - Doris schema 覆盖 input schema，则继续写入
  - Doris FE schema 查询瞬时失败，但还未超时，则继续重试
  - 超过内部默认等待窗口，才明确失败

这个方案遵循的是“修正错误系统假设”而不是“放松约束”：

- 不改变 correctness 边界
- 只修正 Doris 外部可见性延迟的建模方式
- 也不把这个内部实现细节暴露成用户参数，避免用户侧再承担调参成本

### 6.5 本次范围控制与非目标

本次实现范围明确收敛为 Doris：

- 只保证 `MySQL CDC -> transform projection 新增计算列 -> Doris sink` 的 fresh start / restore 可用性
- 不在本次 PR 中引入面向所有 sink 的统一 schema reconcile 抽象
- 不在本次 PR 中扩展 `MetadataApplier` 公共 SPI
- 不在本次 PR 中修复 `Paimon / StarRocks / OceanBase / Iceberg / Hudi / Fluss / MaxCompute` 的同类 existing-table 兼容问题

这是一个有意的范围控制：

- 当前问题必须先确保 Doris 主链路可用
- 统一抽象是后续可评估方向，但不适合作为本次问题单的最小修复范围

## 七、具体代码改动

### 7.1 公共层

文件：

- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/utils/SchemaUtils.java`

改动：

- 修改 `SchemaUtils.isSchemaChangeEventRedundant(...)`
- `CreateTableEvent` 仅在 schema 完全相等时才视为冗余

测试：

- `flink-cdc-common/src/test/java/org/apache/flink/cdc/common/utils/SchemaUtilsTest.java`
- 新增 `testCreateTableEventRedundancyRequiresSchemaEquality`

### 7.2 Doris sink

文件：

- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplier.java`

主要改动：

- 为 `CreateTableEvent` 新增已有表探测逻辑
- 新增 Doris schema fetcher 抽象，便于测试注入
- 新增 build/create/reconcile 辅助方法
- Doris 现有表探测只吞 `Exception`，不再吞并 `Throwable`
- 现有表缺列时执行 `ADD COLUMN`
- 现有表已满足目标 schema 时直接接受并刷新 schema cache

测试：

- `flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplierTest.java`
- 新增：
  - `testCreateTableEventReconcilesMissingColumnsForExistingTable`
  - `testCreateTableEventAcceptsExistingPhysicalColumnAlreadyPresent`

### 7.3 runtime 恢复路径回归

文件：

- `flink-cdc-runtime/src/test/java/org/apache/flink/cdc/runtime/operators/schema/regular/SchemaEvolveTest.java`

新增测试：

- `testRestoredCreateTableEventWithExpandedSchemaIsNotSkipped`

覆盖语义：

- 模拟 operator/coordinator 已从旧 `cp/sp` 恢复出旧 schema
- 再喂入扩展后的 `CreateTableEvent`
- 验证该事件不会被吞掉
- 验证后续 4 列数据可以继续走通

### 7.4 Doris serializer 恢复路径回归

文件：

- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializer.java`
- `flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializerTest.java`

主要改动：

- 保留 Doris physical schema 覆盖校验
- 将 FE schema catch-up 等待统一为时间预算版内部默认轮询窗口
- 新增时间预算版等待逻辑，统一覆盖真实集群的 schema 可见性延迟和 schema 查询瞬时失败
- 清理旧的 attempt-count 等待路径，避免同类逻辑双轨并存
- 不向外暴露新的 sink 配置参数

新增测试：

- `testCreateTableEventRefreshesJsonSchemaAfterRestoreWithComputedTimestampColumn`
- `testJsonSerializationRetriesUntilDorisSchemaCatchesUp`
- `testWaitUntilDorisSchemaCoversInputSchemaWithinTimeBudget`
- `testWaitUntilDorisSchemaCoversInputSchemaTimesOut`
- `testWaitUntilDorisSchemaCoversInputSchemaRetriesFetchFailuresWithinTimeBudget`
- `testWaitUntilDorisSchemaCoversInputSchemaFailsAfterRepeatedFetchFailures`

覆盖语义：

- 模拟 serializer 先缓存旧 schema
- 再接收带 `CONFLUENT__LAST_UPDATED` 的新 `CreateTableEvent`
- 验证 JSON 输出中会真正带上新增时间戳列
- 验证 Doris FE schema 延迟追上时，serializer 会继续等待并成功
- 验证 Doris FE schema 查询瞬时失败时，serializer 会在时间预算内重试
- 验证超过内部等待窗口时，serializer 才会失败

## 八、修复后对 5 类现场场景的覆盖结论

### 8.1 场景 1

现象：

- 全新启动，表不存在，自动建表并正常写入新增列

结论：

- 原本已可用，本次不改变该行为

### 8.2 场景 2

现象：

- 从旧 `cp/sp` 恢复，Doris 不新增 `CONFLUENT__LAST_UPDATED`

结论：

- 已修复
- 新的 `CreateTableEvent` 不再被错误判冗余

### 8.3 场景 3

现象：

- 从旧 `cp/sp` 恢复，且 Doris 已手工加列，但值不写入

结论：

- 已修复
- 新 schema 会继续传播到 sink
- serializer 将按新 schema 输出新增列值

### 8.4 场景 4

现象：

- Doris 表已存在但缺列，fresh start 报错

结论：

- 已增强
- 现在会自动探测已有表并补缺列，不再直接失败

### 8.5 场景 5

现象：

- Doris 表已存在但缺列，从旧 `cp/sp` 恢复时不报错，但也不补列

结论：

- 随场景 2 和 Doris reconciliation 一并修复
- 新 `CreateTableEvent` 会到达 Doris sink
- Doris sink 会执行已有表补列

### 8.6 真实集群补充场景

现象：

- Doris 已补列，但 FE schema 在短时间内仍返回旧列集
- Doris FE schema 查询偶发失败
- serializer 因此报：
  - `Doris physical schema ... did not catch up with input schema`
  - 或 schema fetch 相关异常

结论：

- 已增强
- serializer 现在会保留严格校验，并在更稳的内部默认窗口内等待 Doris FE schema 追平
- schema fetch 瞬时失败也会纳入同一时间预算重试
- 不再因为约 5 秒的过小等待假设或单次 FE 查询失败而提前失败

## 九、改动范围评估

### 9.1 生产代码范围

本次生产改动涉及 3 个文件：

- `flink-cdc-common/src/main/java/org/apache/flink/cdc/common/utils/SchemaUtils.java`
- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplier.java`
- `flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializer.java`

仍然是相对收敛的改动面，且新增行为都集中在本次问题涉及的关键路径。

需要额外说明：

- 其中 Doris 专属改动集中在 Doris connector
- 但 `SchemaUtils` 改动属于公共层语义修正，会改变所有 connector 在 “恢复旧状态 + changed CreateTableEvent” 场景下的行为路径
- 这种影响是有意的，因为旧行为在该场景下并不正确

### 9.2 非 Doris connector 影响评估

本次没有为其他 connector 做功能适配，但需要记录其影响边界：

- `Oracle`
  - 当前仓库中的 Oracle connector 是 source，不是 sink
  - 因此不存在 Doris 这类 sink metadata reconciliation 问题
  - 公共层 `CreateTableEvent` 冗余修正对 Oracle 的意义主要是恢复语义更正确

- `Paimon`
  - 会收到真正传播下来的 changed `CreateTableEvent`
  - 但当前实现对 existing table 仍没有与 Doris 对等的 reconcile 逻辑
  - 恢复后还会以 physical schema 为准重新广播 schema 并做 coercion，因此是当前最值得警惕的非 Doris connector

- `StarRocks / OceanBase / Iceberg / Hudi`
  - 当前大体仍停留在 `create-if-not-exists / ignore-if-exists` 路径
  - 公共层修正后，可能更容易暴露其 existing-table 兼容性边界

- `Fluss / MaxCompute`
  - 更偏向 fail-fast / sanity-check 语义
  - 公共层修正后，如果 external table schema 与目标 schema 不一致，更可能显式失败，而不是继续被冗余判断吞掉

结论：

- 这些不是本次要解决的范围
- 但 `SchemaUtils` 公共修正可能让其历史问题更容易显性化

### 9.3 测试范围

新增或扩展测试覆盖了 4 个层面：

- 公共冗余判断
- runtime 恢复路径
- Doris metadata applier
- Doris serializer

因此本次不是只有点状修复，也补上了关键回归保护。

## 十、风险评估

### 10.1 公共层风险：中等

`SchemaUtils.isSchemaChangeEventRedundant(...)` 是公共逻辑。

影响：

- 所有依赖该冗余判断的 sink/operator，在 “恢复旧状态 + 新 `CreateTableEvent` schema 变更” 场景下，都会开始真正处理新的 `CreateTableEvent`

这是预期修正，但仍属于全局行为变化，因此风险评估为中等。

这里必须强调：

- 如果不做这个改动，本次 Doris restore 修复并不成立
- 因为新的 schema 根本到不了 Doris sink，Doris 侧新增逻辑无法生效
- 因此这是必要修复，不是可选增强

### 10.2 Doris connector 风险：中低

新增的 Doris 逻辑只发生在 `CreateTableEvent` 分支：

- 表不存在仍然走原行为
- 表已存在时才进入 reconciliation

当前只做 “缺列补列” 和 “已有列接受”，没有扩展到复杂 schema diff，因此可控性较好。

### 10.3 Doris serializer 风险：中低

serializer 的变化不是放宽校验，而是延长内部默认等待窗口并改成时间预算式等待，同时把 schema fetch 瞬时失败也纳入该时间预算。

影响：

- 只有在新 schema 首次落到 Doris、且 Doris FE schema 还没追平时，sink writer 才会短暂阻塞等待
- 如果 Doris FE 长时间未追平，仍会明确失败，不会静默丢列
- 如果 Doris FE schema 查询连续失败，仍会在预算耗尽后带 cause 明确失败

风险点：

- 首条新 schema 数据的失败时延会比之前更长
- 瞬时 FE 抖动现在更可能被内部消化，而不是立即暴露成失败

但这是一个 correctness 优先的有意权衡，优于当前 5 秒内误失败。
### 10.4 诊断风险：中等

当前 Doris schema 探测如果抛异常，会先按 “表不存在” 兜底，再尝试 `CREATE TABLE`。

这有两个特点：

- 好处：不会因为探测链路脆弱而完全阻断 create path
- 代价：如果 FE 查询失败并非 “表不存在”，最终错误可能会在 `CREATE TABLE` 阶段暴露，定位信息不够精准

这是当前实现的已知折中。

### 10.5 功能边界风险：中等

当前自动对齐不处理：

- type drift
- rename drift
- drop drift
- 主键/分区键 drift

因此如果现场不是 “单纯少列”，而是更复杂的 Doris 物理表漂移，仍可能需要人工处理。

具体示例包括：

- Doris 已存在同名列，但类型与目标 schema 不一致
- Doris 已存在历史遗留列重命名/列顺序漂移
- 主键、分桶、分区等建表语义已经与目标 schema 偏离

### 10.6 剩余风险来源判断

需要区分 “历史已存在的边界” 和 “本次新增的 trade-off”。

历史已存在、只是本次更容易暴露出来的风险：

- projection 新增计算列不是 source 原生 DDL，这类变更在 restore 场景下一直缺少统一语义
- 非 Doris sink 对 “existing table + changed CreateTableEvent” 普遍缺少完善支持
- 复杂 schema drift 并不是本次才出现，而是历史上就没有通用解

本次新增但可控的行为变化 / trade-off：

- 公共层不再吞掉 schema 已变化的 `CreateTableEvent`，因此可能使其他 sink 的历史兼容性问题更快显性化
- Doris serializer 会在数据路径上短暂等待 Doris FE schema catch-up，这可能带来瞬时背压和 checkpoint 对齐延迟
- Doris `CreateTableEvent` 在 existing table 场景下会主动补齐缺列，这是有意的行为增强，而不是 accidental side effect

结论：

- 大部分剩余风险不是本次新造出来的，而是历史问题
- 本次主要是把历史上被错误隐藏的问题纠正并显性化，同时为 Doris 引入少量 correctness 优先的可控代价

## 十一、可用性评估

### 11.1 对当前问题的可用性：高

对本次用户提出的核心问题，当前方案可用性高，原因如下：

- 必修问题已从根因上修复，而不是通过 Doris 侧打补丁规避
- Doris 侧现有表缺列场景也已落地自动补列
- 关键恢复路径已有 runtime 和 serializer 双回归保护
- Doris FE schema 延迟可见场景也已有内部默认兜底和单测保护

补充量化判断：

- 对 Doris 本次问题链路的可用性评分：`8.5 / 10`

扣分点主要来自：

- 当前修复范围明确收敛为 Doris，不是通用 schema reconcile 框架
- 复杂 schema drift 仍未覆盖
- 公共层行为修正可能让非 Doris sink 的历史兼容性问题更容易暴露

### 11.2 对分支整体的可合入性：中高

原因：

- 生产改动面小
- 覆盖链路完整
- 无状态格式变更
- 无外部接口变更

但需要带着下面的边界认知合入：

- 本次 PR 的承诺范围是 Doris，不是所有 sink
- `SchemaUtils` 公共修正会影响其他 connector 的恢复路径
- 因此合入时应明确这是 “修正公共 restore 语义 + 保证 Doris 可用”，而不是 “通用所有 sink 的 existing-table 兼容方案”

## 十二、验证结果

已通过的关键验证命令如下：

```bash
mvn -pl flink-cdc-common -Dtest=SchemaUtilsTest test -DfailIfNoTests=false

mvn -pl flink-cdc-common,flink-cdc-runtime \
  -Dtest=SchemaEvolveTest#testRestoredCreateTableEventWithExpandedSchemaIsNotSkipped \
  test -DfailIfNoTests=false

mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierTest,DorisEventSerializerTest \
  test -DfailIfNoTests=false

mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierITCase#testDorisExistingTableReconcilesMissingComputedTimestampColumn+testDorisExistingTableWritesComputedTimestampWhenColumnAlreadyExists \
  test -DfailIfNoTests=false

mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierTest,DorisEventSerializerTest,DorisMetadataApplierITCase#testDorisExistingTableReconcilesMissingComputedTimestampColumn+testDorisExistingTableWritesComputedTimestampWhenColumnAlreadyExists \
  test -DfailIfNoTests=false
```

补充说明：

- runtime 侧如果只单跑 `flink-cdc-runtime`，可能仍会吃到本地旧版 `flink-cdc-common` 依赖
- 因此验证本次恢复回归时，需要把 `flink-cdc-common` 一起带进 reactor
- Doris serializer 的最新单测集已覆盖：
  - schema 延迟可见后追平
  - schema 一直不追平超时失败
  - schema fetch 瞬时失败后恢复成功
  - schema fetch 持续失败直到预算耗尽

## 十三、后续建议

### 13.1 建议补一条真实 savepoint 恢复 IT

当前单元测试已覆盖恢复语义，但还没有真实 `savepoint -> 修改 transform -> 恢复作业` 的端到端集成测试。

建议后续补一条更贴近现场的 IT，直接覆盖：

- 旧作业生成 savepoint
- 新作业新增 `CURRENT_TIMESTAMP AS CONFLUENT__LAST_UPDATED`
- 分别验证：
  - Doris 已手工加列
  - Doris 未手工加列，靠 sink 自动补列

### 13.2 建议后续评估 Doris schema fetch 的错误分类

如果需要进一步提升诊断质量，可以把 Doris schema fetch 的异常区分成：

- table not found
- transient FE error
- permission / auth error

从而避免把所有异常都统一回退到 `CREATE TABLE` 分支。

### 13.3 建议单独跟踪非 Doris connector 影响

建议后续按 connector 单独评估：

- Paimon
- StarRocks
- OceanBase
- Iceberg
- Hudi
- Fluss
- MaxCompute

目标不是在本次 PR 中一并修复，而是确认：

- 它们在 `changed CreateTableEvent` 真正传播后是否会暴露历史问题
- 这些问题应按 connector 单独修，还是未来再评估统一抽象

## 十四、最终结论

本次问题的本质不是 “Doris 没有这个列”，而是：

- runtime 在恢复旧状态后，把新的 `CreateTableEvent` 错误吞掉了
- Doris sink 又缺少对已有物理表的目标 schema 对齐能力
- Doris serializer 又对 Doris FE schema 可见性和 schema fetch 稳定性做了过于乐观的时间假设

本次修复采用 “公共层修正事件传播 + Doris 侧补齐已有表 reconciliation + serializer 内部默认等待增强” 的组合方案，既修掉了用户明确要求必须解决的恢复写值问题，也一并解决了已有 Doris 表缺列时的 fresh start 报错问题，以及真实集群里 Doris FE schema 延迟可见或 schema 查询瞬时失败导致的误失败问题。

从当前实现和验证情况看：

- 对本次问题可用性高
- 改动面可控
- 风险主要集中在公共冗余判断语义变化、其他 connector 的历史边界被显性化，以及 Doris 复杂 schema drift 尚未覆盖

同时需要清楚边界：

- 本次方案只承诺 Doris 可用性
- `SchemaUtils` 公共改动虽然会影响其他 connector，但它是 Doris restore 修复成立的必要条件
- 其他 connector 的潜在暴露风险大多属于历史存在的问题，不是本次 Doris 修复新引入的根问题

整体可以作为本轮问题修复方案落地。
