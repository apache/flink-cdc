# [CDC-3.6] Schema / Type / Doris 集成大 PR 说明文档

## 1. 文档目的

本文用于把 `release-3.6` 上这一轮“类型、schema、一致性、Doris 落库稳定性”相关的大改动整理成一份可维护、可回溯、可交接的完整说明文档。

文档目标不是替代每一份设计文档，而是把以下信息统一收口：

- 这次大 PR 为什么要做
- 它实际改了哪些模块
- 它解决了哪些线上或准线上问题
- 哪些是功能增强，哪些是缺陷修复
- 最新代码状态下已经验证到什么程度
- 后续维护需要重点关注哪些边界

## 2. 文档依据

本文基于以下资料整理：

- 集成 commit：
  - `9ae400fdc8e5b67f544beb31e5cc6349399f4fe9`
  - 标题：`[fix][CDC-3.6] Harden schema/type handling across Oracle, Debezium, MySQL and Doris`
- 背景文档：
  - `issue/DECIMAL_RUNTIME_NULL_CACHED_SCHEMA_BUG_DESIGN_2026-04-08.md`
  - `issue/MYSQL_DORIS_JSON_COLUMN_CASE_SYNC_DESIGN_2026-04-09.md`
  - `issue/ORACLE_DATE_LONG_IT_VALIDATION_SUMMARY_2026-04-05.md`
  - `issue/ORACLE_PIPELINE_EXPANDED_IT_REGRESSION_2026-04-05.md`
  - `issue/ORACLE_SOURCE_IT_STRUCTURE_AND_EXPANSION_2026-04-05.md`
- 当前分支补充状态：
  - `b001ed2f2a0f91579eb140b7ec121ae12a5b623c`
  - 标题：`[BUG] doris resolveAddColumnPosition bug fixed.`
- 最新本地验证：
  - `2026-04-09` 使用 `JDK 11` 完整重跑 Doris E2E 矩阵，`MySqlToDorisE2eITCase` 最终 `9/9` 通过

## 3. PR 总览

这不是一个单点修复 PR，而是一组围绕“schema identity、runtime value decoding、sink physical schema”统一收口的集成修复。

`9ae400fd` 的核心目标是把以下几层重新对齐：

- Oracle 源端、pipeline 侧、DDL parser 的类型语义
- Debezium cached `CreateTableEvent` schema 与 runtime record schema 的关系
- `RecordData` / `DecimalData` 的 runtime null 与非法字节处理语义
- MySQL mixed-case 元数据在 schema 生成、DDL 事件、Doris JSON 写入中的一致性
- Doris `format=json` 下 JSON key 与 Doris 物理列名的一致性
- `SchemaOperator regular -> Doris streaming sink` 在 schema barrier 上的真实切批行为

按 `git show --stat --summary 9ae400fd`，该集成 commit 规模为：

- `54 files changed`
- `6199 insertions`
- `408 deletions`

涉及模块包括：

- `flink-cdc-common`
- `flink-cdc-runtime`
- `flink-connector-debezium`
- `flink-cdc-base`
- MySQL / Oracle / Postgres source or pipeline connectors
- Doris pipeline connector
- Doris E2E tests
- `issue/` 下的设计与验证文档

## 4. 这次 PR 为什么必须做

这次 PR 不是“做几个局部 patch”，而是因为系统里同时暴露了三条真实故障链，而且它们在 3.6 上开始互相耦合：

### 4.1 cached schema 变强后，旧的 latent defect 被激活

在 `release-3.6` 上，shared Debezium data-record 解码开始更强地依赖 cached `CreateTableEvent` schema。这个方向本身是为了解决 Oracle variable-scale `NUMBER` 的 schema/value mismatch，但同时也把更早就潜伏的两个问题激活了：

- schema `NOT NULL` 被错误当成 runtime 永不为 `null`
- decimal 空字节直接下沉到 JVM `BigInteger`

结果是原本“潜伏但不易触发”的问题，在 3.6 变成了线上可见故障。

### 4.2 MySQL -> Doris 在 JSON 模式下暴露出字段名对齐问题

业务现场确认过的最典型现象是：

- `deploy_mode` 字段成功写入
- `ID` 字段没有写入值

同时：

- Flink job 正常运行
- MySQL snapshot / binlog 读取正常
- Doris stream load 返回 `Status: "Success"`

这说明问题不是“整条数据没到”，而是“列级字段匹配错位”。

### 4.3 schema evolution + JSON + 晚到事件 暴露出 Doris streaming barrier 不完整

在 `SchemaOperator regular -> Doris streaming sink` 这条链路上，`FlushEvent` 已经被当作 schema barrier 使用，但 Doris JSON streaming 路径没有真正结束 active stream load，会导致：

- `ADD COLUMN` 前后数据落入同一批 stream load
- 新 schema 的列值被静默丢掉
- 晚到旧 schema 事件表现正常，但新 schema 事件新增列仍然为 `null`

这已经不是简单 serializer 问题，而是 sink streaming 语义问题。

## 5. 本次明确解决的问题与典型症状

## 5.1 Decimal runtime-null / cached-schema 故障

典型异常：

```text
Caused by: java.lang.NumberFormatException: Zero length BigInteger
    at java.base/java.math.BigInteger.<init>(BigInteger.java:310)
    at org.apache.flink.cdc.common.data.DecimalData.fromUnscaledBytes(DecimalData.java:207)
    at org.apache.flink.cdc.runtime.operators.transform.PreTransformProcessor.processFillDataField(...)
```

业务影响：

- `PreTransformOperator` 失败
- 因为 `NoRestartBackoffTimeStrategy`，任务不会自动恢复
- 已观测到的异常表为 `trade_18.ma_order_search_0092`

真实故障链不是单点 decimal bug，而是四层叠加：

- cached schema 被当成 authority，而不是 hint
- `NOT NULL` 错误压制 runtime null-bit
- empty decimal bytes 没有语义化处理
- `includeSchemaChanges=false` 时 deserializer cache 可能失同步

本次修复后，系统遵守的规则变为：

- runtime row null-bit 才是最终真相
- cached schema 必须先做兼容性判断，不兼容就 fallback
- schema event 即使不下发，也必须更新内部 cache
- empty decimal bytes fail fast，并给出有语义的异常

## 5.2 MySQL -> Doris `format=json` 字段大小写错配

典型现场症状：

- `deploy_mode` 有值
- `ID` 没值

用户现场日志里同时能看到：

- MySQL snapshot 正常
- Doris stream load `Status: "Success"`

并且还带有两个与本问题无关的 warning：

```text
Config uses deprecated configuration key 'web.port' instead of proper key 'rest.port'
Database configuration option 'serverTimezone' is set but is obsolete, please use 'connectionTimeZone' instead
```

这些 warning 不是根因。

真正根因是：

- Doris JSON serializer 没有按 Doris 物理 schema 生成 JSON key
- 它错误依赖了输入 schema 名称或 lower-case 旧假设

因此会出现：

- 物理列是 `ID`，JSON 却输出 `id`
- 小写列偶然能落，大写列丢失

## 5.3 `column-name-case=UPPER` 后 DDL 对了，但数据没同步

现场确认过的第二类问题是：

- `column-name-case=UPPER` 后，下游 Doris 自动建表 DDL 的字段名已经变成大写
- 但数据依然未能正确同步

这说明：

- DDL 层 schema case 规则本身是生效的
- 真正损坏的是 data event 到 JSON payload 的字段名映射

如果 Doris 物理列已经是：

```text
ID
DEPLOY_MODE
```

serializer 还在输出：

```json
{"id":1,"deploy_mode":2}
```

那么 DDL 看起来是对的，数据仍然会错。

## 5.4 `schema evolution + json + 晚到事件` 下 streaming sink 丢值

本次补强过程中定位到的真实现象：

```text
Expected:
1 | Alice | null
2 | Bob   | 18
3 | Carol | null

Actual:
1 | Alice | null
2 | Bob   | null
3 | Carol | null
```

这说明：

- 晚到旧 schema 事件可以补 `null`
- 但新增列已经存在的那条新 schema 事件，在 streaming sink 里仍然丢值

根因不是 schema change 没有发生，而是：

- `FlushEvent` 触发了 `copySinkWriter.flush(false)`
- Doris 原生 `DorisWriter.flush(false)` 在 JSON streaming 路径下只 flush serializer
- active stream load 没有真正结束
- schema change 前后数据还在同一 stream load 会话里提交

## 5.5 Oracle DATE / LONG / NUMBER / TIMESTAMP 类型语义漂移

Oracle 这条线的问题，不完全表现为单一异常栈，更多体现为“类型映射不一致”和“上下游语义不对齐”：

- source connector、pipeline connector、DDL parser 之间映射不一致
- variable-scale `NUMBER` 存在 schema/value mismatch
- DATE / LONG 的实际行为与原有 IT baseline 漂移

Oracle 侧在验证中暴露出的典型基线漂移包括：

- `PRODUCTS.ID` 预期从 `BIGINT` 修正为 `INT`
- `COLS27` 的 plain `NUMBER` add-column 预期从 `BIGINT` 修正为 `DECIMAL(38, 0)`
- LONG / DATE 路径的 current baseline 与旧断言不一致

它的实质不是“测试脆弱”，而是类型语义确实需要重新对齐。

## 6. 本次明确收敛的最终语义

这次 PR 之后，团队内部应统一接受以下不变量：

### 6.1 runtime null 优先级高于 schema `NOT NULL`

不能再把 schema 声明当作 runtime row 的绝对真相。

### 6.2 cached schema 是 hint，不是 authority

cached `CreateTableEvent` schema 只有兼容时才能用，不兼容就必须退回 value inference。

### 6.3 Doris JSON key 必须等于 Doris 当前物理列名

不是 source schema 名，也不是“一律小写”。

### 6.4 `projection` 显式 alias 优先，`pipeline.column-name-case` 只做兜底

最终确认的语义示例：

```yaml
transform:
  - source-table: testdb.customer
    projection: id as my_id, NAME as name, age as AGE

pipeline:
  column-name-case: UPPER
```

最终下游列名必须是：

```text
my_id,name,AGE
```

解释：

- `my_id` 是显式 alias，原样保留
- `name` 是显式 alias，原样保留
- `AGE` 是显式 alias，原样保留
- `UPPER` 只作用于 projection 没有显式改名的字段

### 6.5 `FlushEvent` 必须是 Doris streaming JSON 路径上的真实 schema barrier

schema change 前的数据必须在旧 stream load 完整提交，schema change 后的数据必须进入新的 stream load 会话。

## 7. 主要代码改动

## 7.1 Common / Runtime 层

主要目标：

- 补 runtime null-safety
- 补 decimal fail-fast
- 避免 schema `NOT NULL` 压制 runtime null-bit

代表性改动：

- `flink-cdc-common/.../RecordData.java`
- `flink-cdc-common/.../DecimalData.java`
- `flink-cdc-common/.../binary/BinaryRecordData.java`
- `flink-cdc-common/.../binary/BinaryArrayData.java`
- `flink-cdc-runtime/.../PreTransformOperatorTest.java`
- `flink-cdc-runtime/.../PostTransformOperatorRuntimeNullRegressionTest.java`

结果：

- typed getter 读取前统一检查 runtime null-bit
- empty decimal bytes 不再以 JVM 底层异常形式泄漏

## 7.2 Debezium / Base Source / MySQL / Postgres schema cache 层

主要目标：

- cached schema 自动同步
- stale cached schema 显式兼容性判断
- `includeSchemaChanges=false` 下也同步内部 cache

代表性改动：

- `flink-connector-debezium/.../DebeziumEventDeserializationSchema.java`
- `flink-cdc-base/.../IncrementalSourceRecordEmitter.java`
- `flink-connector-mysql-cdc/.../MySqlRecordEmitter.java`
- `flink-cdc-pipeline-connector-postgres/.../PostgresPipelineRecordEmitter.java`

结果：

- cached schema 不再盲目信任
- hidden schema-change 模式不再导致 cache drift
- Postgres hidden schema-change 不再走额外重型 JDBC 推断

## 7.3 Oracle source / pipeline / parser 层

主要目标：

- 对齐 Oracle DATE / LONG / NUMBER / TIMESTAMP 语义
- 修复 variable-scale `NUMBER` schema/value mismatch
- 对齐 source connector、pipeline connector、DDL parser 的映射规则

代表性改动：

- `flink-connector-oracle-cdc/.../OracleTypeUtils.java`
- `flink-cdc-pipeline-connector-oracle/.../OracleTypeUtils.java`
- `flink-cdc-pipeline-connector-oracle/.../OracleEventDeserializer.java`
- `flink-cdc-pipeline-connector-oracle/.../ColumnDefinitionParserListener.java`
- `flink-cdc-pipeline-connector-oracle/.../OraclePipelineRecordEmitter.java`

结果：

- Oracle 类型语义在 source / pipeline / parser 三层重新统一
- Oracle IT baseline 与当前真实行为重新对齐

## 7.4 MySQL mixed-case schema / identity 层

主要目标：

- 保留 MySQL mixed-case table / column identity
- 修复 schema 生成和 DDL 事件中的大小写丢失

代表性改动：

- `flink-cdc-pipeline-connector-mysql/.../MySqlEventDeserializer.java`
- `flink-cdc-pipeline-connector-mysql/.../MySqlPipelineRecordEmitter.java`
- `flink-cdc-pipeline-connector-mysql/.../CustomAlterTableParserListener.java`
- `flink-cdc-pipeline-connector-mysql/.../MySqlSchemaUtils.java`

结果：

- mixed-case 元数据能从源端一路保留到下游 schema/DDL 语义

## 7.5 Doris sink / serializer / metadata 层

主要目标：

- JSON key 与 Doris 物理 schema 对齐
- schema evolution 场景下让 streaming barrier 成真
- 修复 add-column 位置与 schema cache 行为

代表性改动：

- `flink-cdc-pipeline-connector-doris/.../DorisEventSerializer.java`
- `flink-cdc-pipeline-connector-doris/.../DorisMetadataApplier.java`
- `flink-cdc-pipeline-connector-doris/.../DorisSchemaChangeManager.java`
- `flink-cdc-pipeline-connector-doris/.../DorisSchemaUtils.java`

与 Doris connector 联动的真实修复点：

- JSON streaming flush 时真正结束 active stream load
- 2PC stream-load label 引入 sequence 规则
- recovery / abort 路径和新 label 规则保持一致

结果：

- mixed-case / upper-case Doris 物理列在 JSON 模式下可正确写入
- `FlushEvent` 在 streaming JSON 路径下成为真实 schema barrier

## 7.6 Runtime transform 语义层

主要目标：

- 固化 `projection alias` 与 `column-name-case` 的优先级

代表性改动：

- `flink-cdc-runtime/.../SchemaColumnCaseFormatter.java`
- `flink-cdc-runtime/.../PostTransformOperatorProjectionKeysRegressionTest.java`

结果：

- 显式 alias 原样保留
- `column-name-case` 只处理 projection 未显式命名的字段

## 8. 解决了哪些功能和行为问题

从产品/能力层面看，这次 PR 实际交付了以下能力：

- Oracle 类型映射在 source、pipeline、parser 三层统一
- cached-schema 解码能力可保留，同时不会因为 stale schema 造成 silent corruption 或 runtime crash
- `includeSchemaChanges=false` 不再牺牲 deserializer 的 cache 正确性
- MySQL mixed-case 表名、字段名能稳定穿透 schema 生成和 DDL 事件
- Doris `format=json` 下，JSON key 与 Doris 物理列严格对齐
- `column-name-case` 与 projection alias 的最终语义被固定下来
- `schema evolution + json + late-event` 下，Doris streaming sink 能稳定切换批次并保留新增列值

## 9. 测试与验证情况

## 9.1 Oracle 相关验证

根据现有设计文档和验证记录：

- `OraclePipelineITCase#testAlterAddAllColumnTypeStatement` 通过
- `OraclePipelineITCase#testParseAlterStatement` 通过
- `OracleFullTypesITCase` 通过
- `OraclePipelineRecordEmitterITCase` 通过
- `OracleMetadataAccessorITCase` 在刷新过期 baseline 后通过

说明：

- Oracle DATE 路径通过
- Oracle LONG 路径通过
- Oracle LONG NULL 路径通过
- Oracle common-types metadata baseline 已与当前真实行为重新对齐

## 9.2 Common / Runtime / Debezium / MySQL / Postgres 回归

文档记录中已通过的关键测试包括：

- `RecordDataTest`
- `DecimalDataTest`
- `BinaryRecordDataExtractorTest`
- `PreTransformOperatorTest`
- `PostTransformOperatorRuntimeNullRegressionTest`
- `DebeziumEventDeserializationSchemaTest`
- `MySqlRecordEmitterTest`
- `PostgresPipelineRecordEmitterTest`

这些回归主要覆盖：

- runtime null 读取
- decimal empty bytes fail-fast
- cached schema fallback
- hidden schema-change cache sync
- MySQL mixed-case schema / DDL

## 9.3 MySQL -> Doris 定向与流式 IT

根据已沉淀文档，以下验证已完成并通过：

- `PostTransformOperatorProjectionKeysRegressionTest`
- `DorisMetadataApplierITCase#testDorisJsonFormatHandlesLateEventsAfterAddColumn`
- MySQL -> Doris JSON column-case 定向 E2E
- projection + `column-name-case` 语义回归

其中 `DorisMetadataApplierITCase#testDorisJsonFormatHandlesLateEventsAfterAddColumn` 已确认：

- `batchMode=true` 通过
- `batchMode=false` 通过

说明 streaming schema barrier 修复闭环完成。

## 9.4 最新完整 Doris E2E 矩阵结果

在当前 `release-3.6` 分支代码上，`2026-04-09` 已使用 `JDK 11` 完整运行：

- `MySqlToDorisE2eITCase`

最终结果：

- `Tests run: 9`
- `Failures: 0`
- `Errors: 0`
- `Skipped: 0`
- `BUILD SUCCESS`

覆盖的 9 个用例为：

- `testSyncWholeDatabase`
- `testSyncMysqlColumnCaseTablesWithJsonFormat`
- `testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase`
- `testSyncMysqlProjectionTransformToDorisWithJsonFormat`
- `testSyncMysqlProjectionTransformToDorisWithJsonFormatAndUpperColumnNameCase`
- `testSyncWholeDatabaseInBatchMode`
- `testComplexDataTypes`
- `testComplexDataTypesInBatchMode`
- `testSchemaEvolution`

需要特别说明：

- E2E 日志中的 `Executing ... didn't get expected results.` 是轮询等待过程中的中间日志，不代表最终失败
- 最终以 Surefire 汇总和 Maven 退出状态为准

## 10. 当前代码状态与 9ae400fd 的关系

`9ae400fd` 是这次大 PR 的主集成 commit。

当前分支 `HEAD` 还包含一个紧随其后的 Doris follow-up：

- `b001ed2f2a0f91579eb140b7ec121ae12a5b623c`
- 标题：`[BUG] doris resolveAddColumnPosition bug fixed.`

这个 follow-up 的定位应理解为：

- 它不是另一条独立产品线
- 它是对 Doris add-column 位置计算的后续收口
- 最新完整 Doris E2E 绿色结果是在包含该 follow-up 的当前分支上得到的

因此，从维护视角最合理的表述是：

- `9ae400fd` 定义了这次大 PR 的主逻辑范围
- 当前 `release-3.6` 分支已经在此基础上补齐 Doris add-column positioning 的收尾修复

## 11. 影响范围评估

这次改动的影响是“横跨多个共享抽象，但逻辑上高度同源”的中等偏大修复，不是无边界扩散。

正向收益：

- 修复线上 crash 风险
- 修复 silent data loss 风险
- 修复 mixed-case / JSON / schema evolution 下的数据正确性问题
- 修复 Oracle 类型语义漂移
- 固化 projection alias 和 column-name-case 的最终规则

需要理解的行为变化：

- runtime null 现在会严格先于 schema `NOT NULL`
- cached schema 不再被无条件信任
- Doris JSON key 会按 Doris 物理 schema 输出
- projection alias 不再被 pipeline case 规则二次改写

对业务最重要的影响：

- 这是数据正确性和系统稳定性增强，不是表面配置项小修小补

## 12. 仍需持续关注的边界

当前主故障已经闭环，但仍有几个后续增强方向值得继续跟进：

- `schema evolution + late-event` 目前主要覆盖了 `AddColumnEvent` 场景
- 如果未来出现“列数不变但类型变化”的极端晚到事件，当前 serializer 仍缺少显式 schema version
- Oracle source 侧更重的新增表、failover、长链路 IT 还可以继续按计划扩张
- cached-schema fallback 与 hidden schema refresh 的线上可观测性还可以继续增强

这些项不构成当前修复的 blocker，但适合作为下一阶段的增强目标。

## 13. 给项目管理和团队维护的最终结论

如果只用一句话概括，这次大 PR 的本质是：

它把 `release-3.6` 上 Oracle 类型语义、Debezium cached-schema 解码、runtime null/decimal 安全、MySQL mixed-case 元数据、Doris JSON 物理列匹配、以及 schema barrier 的 streaming 行为，统一收敛到了同一套一致性约束下。

对团队维护最重要的结论是：

- 以后不要再把 cached schema 当绝对真相
- 以后不要再把 schema `NOT NULL` 当 runtime row 的绝对真相
- 以后不要再假设 Doris JSON key 可以直接等于 source 字段名或统一 lower-case
- 以后不要再让 `projection alias` 被 `column-name-case` 二次改写
- 以后不要再把 `FlushEvent` 当“逻辑 barrier 但物理不切批”

从当前代码与验证结果看，这次 PR 已经把主问题和主要回归面收拢到一个可发布、可维护的状态。
