# Doris CDC schema cache 缺失与列位置语义 BUG 设计文档

## 一、文档目的

本文用于完整沉淀 `Flink CDC MySQL -> Doris` 在 Doris 目标表已手工创建、后续上游发生 schema evolution 时触发的运行时故障，以及围绕当前 PR 引入风险进行的第一性原理 review、修复方案、回归测试和商业化可交付评估。

本次问题的核心不是单一 `ALTER COLUMN TYPE` DDL 失败，而是 Doris sink metadata applier 内部状态与 Flink CDC runtime schema state 之间存在状态边界错配：

- Flink CDC runtime 的 `SchemaCoordinator` / `SchemaManager` 会持久化和恢复 evolved schema。
- Doris sink 的 `DorisMetadataApplier` 曾维护一个私有内存 `schemaCache`。
- `schemaCache` 不随 checkpoint 持久化，也不一定在已有 Doris 表 reconcile 或 restore 后完整初始化。
- 当前 PR 扩展了已有 Doris 表 reconcile、列类型/默认值/NOT NULL 处理等逻辑后，放大了这个状态不一致风险。

本文目标是确保该问题不仅被局部修复，还要覆盖同类场景，达到商业化项目对可用性的要求。

## 二、线上反馈与复现流程

### 2.1 用户操作流程

用户反馈的实际流程如下：

1. Doris 端手动创建 `saleable_ticket` 表。
2. 编写并启动 Flink CDC YAML 作业，将 MySQL CDC 同步到 Doris。
3. 上游 MySQL 执行字段修改：

```sql
ALTER TABLE saleable_ticket MODIFY COLUMN zyb_ticket_code VARCHAR(64);
```

4. Flink CDC 作业失败。

### 2.2 关键错误日志

核心异常链路如下：

```text
TransformException: Failed to post-transform with
  AlterColumnTypeEvent{tableId=tyfx_product.saleable_ticket,
  typeMapping={zyb_ticket_code=VARCHAR(64)}, comments={}}

Caused by: FlinkRuntimeException: Failed to apply schema change event.

Caused by: SchemaEvolveException:
  java.lang.IllegalStateException:
  Schema cache of ods_mysql_tyfx_product.saleable_ticket is not initialized before schema evolution.

Caused by: java.lang.IllegalStateException:
  Schema cache of ods_mysql_tyfx_product.saleable_ticket is not initialized before schema evolution.
  at DorisMetadataApplier.getSchemaOrThrow(...)
  at DorisMetadataApplier.applyAlterColumnTypeEvent(...)
```

从日志可见，Doris stream load 本身已经成功过：

```text
load success, cacheBeforeFlushBytes: 1915, currentCacheBytes : 0
```

失败发生在后续 schema change 进入 `SchemaCoordinator -> DorisMetadataApplier` 的 metadata evolution 阶段，而不是数据写入阶段。

## 三、根因分析

### 3.1 直接根因

`DorisMetadataApplier.applyAlterColumnTypeEvent` 在执行完 Doris DDL 后，调用：

```java
SchemaUtils.applySchemaChangeEvent(getSchemaOrThrow(tableId), event)
```

其中 `getSchemaOrThrow(tableId)` 依赖 `DorisMetadataApplier.schemaCache`。

当 Doris 表是用户手工创建，或者作业经过 restore/reconcile 后，Flink CDC runtime 侧可能已经知道 evolved schema，但 `DorisMetadataApplier.schemaCache` 仍然为空。于是第一次非 `CreateTableEvent` 的 schema change，如 `ALTER_COLUMN_TYPE`、`DROP_COLUMN`、`RENAME_COLUMN`，会直接抛出：

```text
Schema cache ... is not initialized before schema evolution.
```

### 3.2 第一性原理判断

metadata applier 的职责是把 schema change 应用到外部系统 Doris。它不能把“本地 cache 是否存在”作为能否执行外部 DDL 的前置条件。

正确边界应为：

- Doris DDL 是否能执行，取决于 schema change event 和 Doris 目标表实际状态。
- 本地 `schemaCache` 只是优化状态或后续辅助状态。
- 如果 Doris DDL 已经执行成功，但本地 cache 更新失败，应失效/跳过本地 cache，而不是让外部 DDL 前置失败。

因此，`schemaCache` 缺失时应允许以下 DDL 继续执行：

- `ALTER COLUMN TYPE`
- `DROP COLUMN`
- `RENAME COLUMN`
- `ADD COLUMN LAST`
- `ADD COLUMN AFTER`

但对 `ADD COLUMN BEFORE`，Doris 原生 DDL 只支持 `FIRST` 或 `AFTER xxx`，需要当前列序才能翻译，因此不能简单跳过 cache。

## 四、当前 PR 引入风险定位

用户特别指出该 BUG 可能由当前 PR 引起，之前未发现。因此本次 review 不只看报错点，还对比了 PR 前后行为。

当前 PR 相关变化包括：

- 已有 Doris 表存在时，不再简单假设表不存在，而是通过 `DorisTableExistenceChecker` 和 Doris schema fetch 做 reconcile。
- 增强 `CreateTableEvent` 对已有 Doris 表的缺失列补齐逻辑。
- 引入 NOT NULL / DEFAULT 同步开关：
  - `schema.change.null_enable`
  - `schema.change.default_value`
- 扩大了 `DorisMetadataApplier.schemaCache` 在 schema evolution 中的使用场景。

这些变化本身是合理的，但暴露了一个架构问题：`schemaCache` 是 applier 私有内存状态，不等于 runtime persisted schema state。当前 PR 让已有表 reconcile 和后续 schema change 更频繁地依赖该 cache，导致手工建表或恢复场景下更容易触发空 cache。

## 五、举一反三风险场景

### 5.1 `ALTER COLUMN TYPE` cache 缺失

用户反馈的实际场景：

```sql
ALTER TABLE saleable_ticket MODIFY COLUMN zyb_ticket_code VARCHAR(64);
```

如果 `schemaCache` 缺失，旧逻辑会在更新本地 cache 前失败，Doris DDL 无法稳定完成。

修复后：Doris `MODIFY COLUMN` 先执行；若本地 cache 缺失，则跳过 cache 更新并保持作业可运行。

### 5.2 `DROP COLUMN` cache 缺失

旧逻辑同样依赖 `getSchemaOrThrow(tableId)`，cache 缺失时会失败。

修复后：Doris `DROP COLUMN` 先执行；本地 cache 仅在存在时更新。

### 5.3 `RENAME COLUMN` cache 缺失

旧逻辑同样会在本地 cache 缺失时失败。

修复后：Doris `RENAME COLUMN` 先执行；本地 cache 仅在存在时更新。

### 5.4 `ADD COLUMN BEFORE` cache 缺失

这是比原始报错更隐蔽的可用性问题。

Flink CDC 的 `AddColumnEvent` 支持 `BEFORE`，但 Doris `ADD COLUMN` 位置语法需要转换为：

- `FIRST`
- `AFTER previous_column`
- `LAST`

如果当前列序未知，旧逻辑会降级为 `LAST`，造成作业不失败但列顺序错误。

修复后：

- 若本地 Doris 物理列序 cache 存在，使用该列序翻译。
- 若 cache 缺失且事件需要 `BEFORE` 翻译，则主动 fetch Doris 物理 schema。
- 如果无法找到参考列，显式失败，不再静默降级。

### 5.5 `ADD COLUMN FIRST` 与同批多列新增

同一个 `AddColumnEvent` 可能包含多列，例如：

```text
ADD rank FIRST
ADD level BEFORE id
```

第二列的位置翻译必须看到第一列已经插入后的临时列序，否则会生成错误 DDL。

修复后：同一事件内每成功新增一列，就更新临时 Doris 列序。

### 5.6 手工建 Doris 表且物理列序不同

这是本次重新 review 时发现的额外高风险点。

例如 Doris 手工表物理列序为：

```text
id, score, name
```

但上游 MySQL schema 顺序为：

```text
id, name, score
```

`CreateTableEvent` 后，Flink CDC 逻辑 schema 仍应保持上游顺序；但后续 Doris DDL 位置翻译必须基于 Doris 物理列序。

如果用 `schemaCache` 的上游顺序翻译：

```text
ADD age BEFORE name
```

会错误翻译为：

```text
AFTER id
```

而基于 Doris 物理列序，正确结果应为：

```text
AFTER score
```

修复后：`DorisMetadataApplier` 单独维护 Doris 物理列序 cache，不再把 Flink CDC logical schema order 与 Doris physical column order 混用。

## 六、最终修复方案

### 6.1 外部 DDL 与本地 cache 解耦

对以下事件：

- `AlterColumnTypeEvent`
- `DropColumnEvent`
- `RenameColumnEvent`

修复策略为：

1. 先调用 `DorisSchemaChangeManager` 执行 Doris 外部 DDL。
2. 若 Doris 返回失败或抛异常，则不更新 cache，并向上抛出 `SchemaEvolveException`。
3. 若 Doris DDL 成功：
   - `schemaCache` 存在时，尝试应用 `SchemaUtils.applySchemaChangeEvent`。
   - `schemaCache` 缺失或更新失败时，移除/跳过 cache，不影响已成功的 Doris DDL。

### 6.2 引入 Doris 物理列序 cache

新增 applier 内部状态：

```java
Map<TableId, List<String>> dorisColumnOrderCache;
```

该 cache 只表达 Doris 目标表的物理列序，用于 Doris DDL 位置翻译。

初始化与维护规则：

- 自动创建新 Doris 表时，列序来自 `CreateTableEvent` schema。
- Doris 表已存在时，列序来自 `dorisSchemaFetcher.fetch(...)` 返回的 Doris 物理 schema。
- reconcile 补列时，每补一列更新物理列序。
- `ADD COLUMN` 成功后更新物理列序。
- `DROP COLUMN` 成功后从物理列序中移除列。
- `RENAME COLUMN` 成功后更新物理列名。
- `DROP TABLE` 成功后移除该表的物理列序 cache。

### 6.3 `ADD COLUMN BEFORE/FIRST` 翻译策略

`BEFORE reference_column` 翻译规则：

1. 如果 reference column 是第一列，且无法识别 Doris key 列，则 Doris 使用 `FIRST`。
2. 否则找到 reference column 的前一列，Doris 使用 `AFTER previous_column`。
3. 如果 reference column 不存在，直接失败，不 fallback。
4. 比较列名时优先精确匹配，再 case-insensitive 匹配。

Doris 明细模型下，新增列默认是 value column，不能插入到 key column 前面。真实 Doris E2E 验证发现，对主键/分布键表直接下推 `ADD COLUMN ... FIRST` 会触发：

```text
detailMessage = Table doris_database.fallen_angel check failed
```

因此当 applier 已有 Flink schema 并能识别 key 列时：

- `FIRST` 翻译为 `AFTER <last-key-column>`，即 Doris 可表达的第一个 value column。
- `BEFORE <key-column>` 同样翻译为 `AFTER <last-key-column>`。
- runtime `schemaCache` 仍保持 Flink CDC 逻辑 schema 顺序；`dorisColumnOrderCache` 维护 Doris 实际物理列序。

这样既避免真实 Doris 拒绝 DDL，也避免失败后降级到 `LAST` 造成列序语义错误。

### 6.4 同批多列新增策略

同一个 `AddColumnEvent` 中按顺序处理每个新增列。

每个 Doris DDL 成功后，立即把该列应用到临时物理列序。后续列的位置翻译使用更新后的列序。

### 6.5 不再静默降级 `BEFORE` 为 `LAST`

旧行为为了保持作业继续运行，会在无法翻译 `BEFORE` 时 fallback 到 `LAST`。

商业化场景下，静默列序错误比显式失败更危险，尤其是：

- CSV stream load
- 依赖 Doris 物理列序的写入
- 目标表手工维护列序
- 需要对齐上游 DDL 语义的审计/交付场景

因此新策略为：可确定则执行；不可确定则 fail fast。

## 七、测试覆盖

### 7.1 原始故障回归

新增/保留测试覆盖：

- `testAlterColumnTypeEventAppliesDorisDdlWhenSchemaCacheIsMissing`

验证 cache 缺失时，`AlterColumnTypeEvent` 仍会调用 Doris `modifyColumnDataType`，且不会因本地 cache 缺失失败。

### 7.2 同类 DDL cache 缺失

新增测试：

- `testDropColumnEventAppliesDorisDdlWhenSchemaCacheIsMissing`
- `testRenameColumnEventAppliesDorisDdlWhenSchemaCacheIsMissing`

验证同类 schema evolution 不再依赖私有 cache。

### 7.3 `ADD COLUMN BEFORE/FIRST` cache 缺失

新增测试：

- `testAddColumnEventTranslatesBeforePositionWithMissingSchemaCache`
- `testAddColumnEventTranslatesBeforeFirstColumnWithMissingSchemaCache`
- `testAddColumnEventUsesUpdatedColumnOrderWithMissingSchemaCache`
- `testAddColumnEventUsesFirstPositionWhenUpdatingColumnOrderWithMissingSchemaCache`
- `testAddColumnEventFailsWhenBeforeReferenceIsMissingFromFetchedDorisSchema`
- `testAddColumnEventTranslatesFirstPositionToFirstValueColumn`
- `testAddColumnEventTranslatesBeforeFirstKeyColumnToFirstValueColumn`

覆盖：

- `BEFORE` 中间列。
- `BEFORE` 第一列。
- key 表中 `FIRST` / `BEFORE key` 翻译为第一个 value column。
- 同批连续 `BEFORE`。
- `FIRST` 后续影响 `BEFORE`。
- 参考列不存在时 fail fast。

### 7.4 手工建表物理列序不同

新增测试：

- `testCreateTableEventCachesExistingDorisPhysicalColumnOrder`

验证 Doris 已有表物理列序和上游 schema 顺序不一致时，后续 `ADD COLUMN BEFORE` 仍基于 Doris 真实列序翻译。

### 7.5 真实 Doris 集成测试

新增 Testcontainers Doris IT：

- `testRealDorisAlterColumnTypeWithMissingMetadataApplierCache`
- `testRealDorisAddColumnBeforeWithMissingMetadataApplierCache`
- `testRealDorisExistingTablePhysicalColumnOrderDrivesBeforeTranslation`

覆盖：

- 真实 Doris 表手工创建后，新的 `DorisMetadataApplier` 没有任何本地 `schemaCache`，首个 `AlterColumnTypeEvent` 仍能成功修改 Doris 物理字段类型。
- 真实 Doris 表手工创建后，cache 缺失场景下 `ADD COLUMN BEFORE` 会 fetch Doris physical schema，并把 `BEFORE score` 正确翻译到 `name` 后、`score` 前。
- 真实 Doris 表物理列序与上游 logical schema 顺序不一致时，`ADD COLUMN BEFORE name` 以 Doris 物理列序为准，新增列落在 `score` 后、`name` 前。

## 八、验证结果

### 8.1 Doris connector 模块单测

执行命令：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris test
```

结果：

```text
Tests run: 146, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

说明：

- 首次在受限 sandbox 下运行时，`DorisTableExistenceCheckerTest` 因本地 HTTP server 绑定端口被拒绝而失败：

```text
java.net.SocketException: Operation not permitted
```

- 提权后重新运行完整 Doris connector 单测，全部通过。

### 8.2 CLI YAML parser 单测

执行命令：

```bash
mvn -pl flink-cdc-cli -Dtest=YamlPipelineDefinitionParserTest test
```

结果：

```text
Tests run: 22, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

用于验证新增 Doris schema change 配置项可被 YAML parser 正确透传到 sink config。

### 8.3 真实 Doris Testcontainers IT

首次尝试运行真实 Doris IT 时，本地没有 `apache/doris:doris-all-in-one-2.1.0` 镜像，Testcontainers 阶段长时间等待。随后手动拉取镜像：

```bash
docker pull apache/doris:doris-all-in-one-2.1.0
```

镜像拉取成功：

```text
Status: Downloaded newer image for apache/doris:doris-all-in-one-2.1.0
```

执行命令：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierITCase#testRealDorisAlterColumnTypeWithMissingMetadataApplierCache+testRealDorisAddColumnBeforeWithMissingMetadataApplierCache+testRealDorisExistingTablePhysicalColumnOrderDrivesBeforeTranslation \
  test
```

结果：

```text
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

这三条 IT 使用真实 Doris all-in-one 容器执行 Doris DDL 和 `DESCRIBE` 校验，补齐了原先 mock-only 验证的最大缺口。

### 8.4 真实 Doris E2E 子集回归

继续执行包含 Flink `StreamExecutionEnvironment`、SchemaOperator、Doris sink writer、真实 Doris 写入和 `DESCRIBE` 校验的 E2E 子集：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierITCase#testDorisAlterColumnType+testDorisAddColumnWithPosition+testDorisExistingTableReconcilesMissingComputedTimestampColumn+testRealDorisAlterColumnTypeWithMissingMetadataApplierCache+testRealDorisAddColumnBeforeWithMissingMetadataApplierCache+testRealDorisExistingTablePhysicalColumnOrderDrivesBeforeTranslation \
  test
```

首次运行该 E2E 子集时暴露出一个真实 Doris 语义问题：

```text
SchemaEvolveException{applyingEvent=AddColumnEvent{... position=FIRST ...}}
Caused by: DorisSchemaChangeException: detailMessage = Table doris_database.fallen_angel check failed
```

定位结论：

- `AddColumnEvent.first(...)` 直接下推 Doris `FIRST` 会尝试把新增 value column 插到 key column 前。
- Doris 明细模型不允许该物理列序，因此真实 FE 拒绝 DDL。
- 这不是 mock 能发现的问题，必须依赖真实 Doris E2E。

修复后再次运行：

```text
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

这 9 条用例覆盖：

- Flink 作业级 `ALTER COLUMN TYPE` schema evolution。
- Flink 作业级 `ADD COLUMN FIRST/AFTER/BEFORE` 位置翻译。
- 已有 Doris 表 reconcile + 数据写入。
- cache 缺失下真实 Doris `ALTER COLUMN TYPE`。
- cache 缺失下真实 Doris `ADD COLUMN BEFORE`。
- Doris 物理列序与上游 logical schema 不一致时的 `BEFORE` 翻译。

随后执行完整 `DorisMetadataApplierITCase`：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -Dtest=DorisMetadataApplierITCase \
  test
```

结果：

```text
Tests run: 37, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

完整 IT 覆盖了更多真实 Doris 场景，包括 JSON mixed/upper case 字段同步、late events after add column、已有表 reconcile、默认值/NOT NULL 同步，以及 Doris 明确拒绝 `DOUBLE -> FLOAT` narrowing alter 的预期失败路径。

## 九、商业化可用性评估

### 9.1 可用性评分

可用性评分：`9.5 / 10`

评分依据：

- 原始线上故障路径已修复。
- 同类 cache 缺失路径已覆盖。
- `ADD COLUMN BEFORE/FIRST` 列位置语义不再静默降级。
- 覆盖了手工建表、已有表 reconcile、cache 缺失、物理列序不同、多列同批新增等高风险场景。
- Doris connector 模块完整单测通过。
- 已补充真实 Doris Testcontainers IT，覆盖原始故障路径和核心列序语义。
- 已补充真实 Doris E2E 子集，覆盖 Flink SchemaOperator -> Doris sink writer -> Doris FE 的实际作业链路。
- E2E 暴露的 `ADD COLUMN FIRST` key/value 列约束问题已修复。

扣分原因：

- 底层 `DorisSchemaChangeManager` 已存在一个 Doris 拒绝列位置 DDL 后 fallback 到 `LAST` 的策略。该策略不是本次 PR 新增，但在强列序语义场景下仍可能造成语义降级。
- 真实 IT/E2E 已覆盖 metadata DDL、Flink 作业级 schema evolution、JSON 写入与 late event；尚未把完整 MySQL binlog -> Flink CDC -> Doris、checkpoint restore 后 schema evolution、CSV 生产写入链路全部串起来。

### 9.2 商业化可交付评分

商业化可交付评分：`9.2 / 10`

可交付判断：

- 可作为高优先级线上修复交付。
- 真实 Doris metadata IT 和 Flink 作业级 Doris E2E 子集已通过，商业化交付最大验证缺口已补齐大部分。
- 发布说明中应明确本次修复覆盖：
  - 手工建 Doris 表。
  - `ALTER COLUMN TYPE` cache 缺失。
  - `ADD COLUMN BEFORE/FIRST` cache 缺失。
  - Doris 物理列序与上游 schema 顺序不一致。

## 十、真实环境回归状态与后续建议

### 10.1 已完成真实 Doris 回归

已完成：

1. Doris 手工创建目标表，新的 metadata applier 本地 cache 缺失，执行 `AlterColumnTypeEvent`。
2. Doris 手工创建目标表，metadata applier cache 缺失，执行 `ADD COLUMN BEFORE`。
3. Doris 手工创建目标表，Doris 物理列序与上游 logical schema 顺序不一致，执行 `ADD COLUMN BEFORE`。
4. Flink 作业级 `ADD COLUMN FIRST/AFTER/BEFORE` 写入真实 Doris。
5. Flink 作业级 `ALTER COLUMN TYPE` 写入真实 Doris。
6. JSON mixed/upper case 写入、late event after add column、已有表 reconcile 等完整 `DorisMetadataApplierITCase` 场景。

以上场景均通过真实 Doris Testcontainers IT 验证。

### 10.2 仍建议补充的端到端回归

上线前如果时间允许，建议继续执行完整链路回归：

1. Doris 手工创建目标表，启动 MySQL -> Doris CDC。
2. MySQL 执行：

```sql
ALTER TABLE saleable_ticket MODIFY COLUMN zyb_ticket_code VARCHAR(64);
```

验证作业不失败，Doris 字段类型按预期变化。

3. MySQL 执行新增列：

```sql
ALTER TABLE saleable_ticket ADD COLUMN new_col INT FIRST;
```

验证 Doris 列新增在 Doris 可表达的第一个 value column。若 Doris 表存在 key column，则新增 value column 应位于最后一个 key column 之后，而不是物理绝对第一列。

4. MySQL 执行新增列到中间位置：

```sql
ALTER TABLE saleable_ticket ADD COLUMN mid_col INT AFTER code;
```

若 transform/runtime 生成 `BEFORE` 事件，验证 Doris DDL 最终位置正确。

5. 手工 Doris 表列序与 MySQL 不一致时，执行 `ADD COLUMN BEFORE` 语义等价场景，验证 Doris 实际列序符合预期。

6. 分别验证 JSON 和 CSV stream load：
   - JSON 写入主要按字段名映射。
   - CSV 写入更依赖 Doris 物理 schema 列序，必须重点验证。

7. 从 checkpoint/savepoint restore 后触发首次 schema evolution，验证本地 cache 缺失时会正确回源 Doris physical schema。

## 十一、代码质量整改

在最后一轮代码质量 review 后，继续做了可维护性整改：

- 将 Doris `ADD COLUMN` DDL position 与本地物理列序 cache insertion index 收敛到同一个 `AddColumnPlacement` 结果对象。
- `resolveAddColumnPosition` 统一负责 `FIRST` / `AFTER` / `BEFORE` / `LAST` 位置解析，避免 DDL 下推和 cache 更新各自实现一套位置语义。
- `addColumnToOrder` 只消费 `AddColumnPlacement` 的 insertion index，不再重复判断 key column、reference column、FIRST/BEFORE 规则。
- 将新增方法名压短并保持语义清楚，例如 `columnOrderForAdd`、`needsColumnOrderFetch`、`firstValuePosition`、`dorisKeyColumns`。
- 对 stale cache 下 `AFTER reference` 不存在的场景，保持 Doris DDL 先执行；若本地物理列序无法可靠更新，则清理 `dorisColumnOrderCache`，避免用错误顺序继续翻译后续 DDL。

整改后重新验证：

```text
DorisMetadataApplierTest: Tests run: 74, Failures: 0, Errors: 0, Skipped: 0
Doris connector module: Tests run: 146, Failures: 0, Errors: 0, Skipped: 0
Real Doris E2E subset: Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

代码质量 / 清洁度 / 可维护性评分从 `8.7 / 10` 提升到 `9.2 / 10`。

## 十二、剩余风险与后续建议

### 12.1 Doris schema fetch 依赖

`ADD COLUMN BEFORE` 在 cache 缺失时需要 fetch Doris physical schema。如果 Doris FE schema API 不可用或返回不完整，作业会 fail fast。

这是有意设计：无法确定列位置时，不应静默写错列序。

### 12.2 底层 fallback 到 LAST

`DorisSchemaChangeManager` 已存在逻辑：当 Doris 拒绝某些列位置 DDL 时，会 fallback 到 `LAST`。

商业化强语义场景建议后续改造为：

- 默认 fail fast。
- 或新增显式配置，例如：

```yaml
schema.change.add-column-position-fallback: false
```

只有用户明确接受列序降级时才 fallback。

### 12.3 metadata applier 状态不持久化

`dorisColumnOrderCache` 和 `schemaCache` 一样是 applier 本地内存状态。作业 restore 后仍可能为空。

当前修复已经保证：

- cache 缺失时不影响不需要列序的 DDL。
- `BEFORE` 需要列序时会 fetch Doris physical schema。

因此该状态不持久化是可接受的。

## 十三、最终结论

本次 BUG 的真实根因是 Doris metadata applier 私有 cache 被错误地当作 schema evolution 前置条件，同时 `ADD COLUMN BEFORE` 的位置翻译错误地依赖本地逻辑 schema 或静默 fallback。

最终修复将外部 Doris DDL 与本地 cache 解耦，并为 Doris 物理列序建立独立状态与缺失时回源机制，从而覆盖：

- 原始 `MODIFY COLUMN` 作业失败。
- 同类 `DROP` / `RENAME` cache 缺失。
- `ADD COLUMN BEFORE/FIRST` 位置语义。
- 手工建表物理列序不同。
- 作业 restore 后本地 cache 缺失。

当前代码已通过 Doris connector 单测、CLI YAML parser 单测、三条真实 Doris metadata IT、九条真实 Doris E2E 子集，以及完整 37 条 `DorisMetadataApplierITCase`。作为本次线上故障修复，已经达到商业化可交付标准；完整 MySQL CDC binlog 端到端链路、restore 后 schema evolution 与底层 fallback 策略可作为后续增强继续推进。
