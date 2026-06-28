# Schema Change 列名归一化与字段注释同步设计文档

## 一、文档目的

本文用于完整沉淀本次 PR 的改动背景、线上 BUG、根因分析、设计原则、模块级实现方案、测试覆盖、review 修复结论，以及最终回归验收清单。

本次改动触及 Flink CDC schema evolution 的核心链路：

- `flink-cdc-common`：schema change event 语义、schema apply、schema diff、冗余判断。
- `flink-cdc-runtime`：schema derivation、pre/post transform schema event 过滤与改写。
- `flink-cdc-pipeline-connector-doris`：Doris schema DDL 下发、本地 schema cache 更新。
- `flink-cdc-pipeline-connector-paimon`：仅评估字段 comment removal 兼容性；不属于 `column-name-case` 主修复路径，保留与否取决于补充测试。

因此本文不只记录单点修复，而是将 `ADD / ALTER / DROP / RENAME / COMMENT` 全部 schema change 纳入统一设计和验收范围。

最新设计结论：

- `column-name-case` 的主转换必须在 Runtime/PostTransform 阶段完成，sink 收到的 schema event 应已经使用下游物理列名。
- `SchemaUtils.transformSchemaChangeEvent` 只承担 projection 过滤和 `AddColumnEvent` 位置修正，不承担 lower/upper 字段名转换。
- Doris connector 改动只承担两类职责：历史/异常事件的 schema cache 兜底解析，以及 comment update/removal 的真实 DDL correctness。
- Paimon connector 改动不解决字段大小写问题；在没有独立测试证明 `null` comment 语义前，不应作为本 PR 的必需改动保留。

## 二、需求背景

### 2.1 线上问题一：Doris alter column type 列名大小写不一致

用户在 `MySQL CDC -> Doris` 作业重新打包运行后，schema evolution 仍失败。

关键日志：

```text
AlterColumnTypeEvent{tableId=test.student, typeMapping={JOB=VARCHAR(255)}, comments={}}
```

Doris sink 侧日志：

```text
The column JOB is not exists in table student, can not modify it's type
Doris schema change returned false for alter column type JOB on test.student.
Will not update local schema cache.
```

Flink CDC runtime 随后触发全局失败：

```text
Failed to apply schema change event.
Global failure triggered by OperatorCoordinator for 'Transform:Data -> SchemaOperator -> PrePartition'
```

典型状态：

- source / schema event 中列名为 `JOB`。
- transform 或 `column-name-case` 后 evolved schema 中列名为 `job`。
- Doris 物理表真实列名为 `job`。
- Doris sink 按 event 原始列名 `JOB` 下发 DDL，导致 Doris 判断列不存在。

### 2.2 线上问题二：上游 MySQL drop column 继续失败

修复 alter type 后，上游 MySQL 删除字段又触发新失败。最新日志显示，真正失败点不是 Doris
端 DDL 猜错字段名，而是 runtime/coordinator 在 schema event 下发前就把 drop 事件吞掉：

```text
DropColumnEvent{tableId=test.student, droppedColumnNames=[JOB_NAME]}
is redundant for current schema ... job_name ... just skip it.
```

随后 writer 侧继续使用旧 schema，而数据记录已经少了一个字段：

```text
Unable to resolve input schema for test.student.
record arity=8, current schema columns=9
```

这说明问题不是 Doris alter type 单点缺陷，而是所有引用“已存在列”的 schema change 都存在相同风险：

- `AddColumnEvent` 的 `BEFORE` / `AFTER` 参考列。
- `AlterColumnTypeEvent` 的 `typeMapping`、`oldTypeMapping`、`comments`。
- `DropColumnEvent` 的删除列。
- `RenameColumnEvent` 的旧列名。

### 2.3 需求约束

用户明确要求：

- 不能猜问题，要用日志和代码路径精确定位。
- 不能发现一个修一个，要举一反三覆盖所有 schema change。
- 代码质量需要达到 Apache 顶级项目水平。
- 本次 PR 改动较大，必须给出完整设计文档和回归验收清单。

## 三、根因分析

### 3.1 直接根因

schema change event 在跨越 `source -> transform -> SchemaCoordinator -> sink` 后，仍可能携带 source 侧或 pre-transform 侧列名。

下游 sink 需要操作的是 evolved schema / post-transform schema / 目标系统物理表中的真实列名。如果事件中的列名没有按当前 schema 归一化，就会出现：

- Doris 找不到列。
- Drop/Rename 作用到错误列名。
- schema cache 更新失败或与外部系统状态分裂。
- transform projection 后的数据 schema 与 schema event 不一致。

对于配置了 `column-name-case: lower/upper` 的作业，列名大小写转换不是 sink 端自由猜测：

- `column-name-case` 定义的是上游字段同步到下游物理表时的最终列名规则。
- Runtime/PostTransform 必须优先按该规则生成下游 schema 与 schema change event。
- Sink 端只基于自身 schema cache 做 exact match / 唯一 case-insensitive match 的兼容兜底。

例子：

```yaml
pipeline:
  column-name-case: lower
```

上游：

```sql
ALTER TABLE t_user CHANGE COLUMN JOB_NAME JOB_NAME_123 VARCHAR(255);
ALTER TABLE t_user DROP COLUMN JOB_NAME;
```

下游 schema event 应分别是：

```text
RenameColumnEvent{JOB_NAME -> JOB_NAME_123}  ->  RenameColumnEvent{job_name -> job_name_123}
DropColumnEvent{JOB_NAME}                    ->  DropColumnEvent{job_name}
```

即 DDL 应面向下游真实列名 `job_name` / `job_name_123`，而不是先在 Doris 端猜测 `JOB_NAME` 是否等价于 `job_name`。

### 3.2 系统性根因

1. **已有列引用缺少统一解析规则**

   旧逻辑在多个模块中分散处理大小写兼容，部分路径 exact match，部分路径没有处理 transform 后列名。

2. **post-transform schema event rewrite 不完整**

   wildcard projection、显式 projection、`column-name-case: lower/upper` 下，数据列已经变成 post schema 名称，但 schema change event 未完全改写。

3. **`AlterColumnTypeEvent` 只表达类型变化，comment 语义不完整**

   旧逻辑中 comment-only alter 会在 copy、trim、schema diff、transform 过滤等路径被丢弃。

4. **字段注释删除没有明确语义**

   comment 新增/修改可以用非空 string 表达，但 comment 删除需要一个独立语义。本 PR 引入 `comments[columnName] = null` 表达“删除字段注释”。

5. **Doris applier 依赖 event 原始列名下发 DDL**

   Doris sink 原先直接使用 event column name，没有先通过 schema cache 解析到 Doris 真实列名。

6. **多模块重复逻辑导致行为漂移**

   `SchemaUtils`、`SchemaDerivator`、`PostTransformOperator`、Doris/Paimon applier 中都有相似但不完全一致的列名处理逻辑。

## 四、设计目标

### 4.1 功能目标

- 所有已有列引用统一解析为当前 schema 中的真实列名。
- 支持 exact match 优先，唯一 case-insensitive match 兜底。
- 支持 `AlterColumnTypeEvent` 同时表达：
  - type-only change
  - comment-only add/update
  - comment removal
  - type + comment change
- transform 后传给 sink 的 schema event 必须面向 post-transform schema。
- Doris sink 能正确应用 comment update / removal。
- Paimon comment removal 不作为本 PR 的确认交付范围；若保留改动，必须补充独立测试证明 catalog 行为。
- schema cache 只在外部 DDL 成功后更新。

### 4.2 非目标

- 不改变各 source connector 解析 DDL 的方式。
- 不引入新的 schema change event 类型，仍复用 `AlterColumnTypeEvent` 承载 column comment change。
- 不在本 PR 中完成所有 sink 的 comment 能力矩阵；Paimon 及其他 sink 需要后续逐个确认。

## 五、核心设计

### 5.1 统一列名解析

新增公共解析能力：

```java
SchemaUtils.resolveExistingColumnName(Schema schema, String columnName)
SchemaUtils.resolveExistingColumnNameMap(Schema schema, Map<String, T> columnNameMap)
```

解析规则：

1. 如果 schema 中存在 exact match，返回原始列名。
2. 如果 exact match 不存在，查找 case-insensitive match。
3. 只有唯一匹配时返回 schema 中真实列名。
4. 如果没有匹配或匹配不唯一，保留原始列名，避免误改写。

适用事件：

- `AddColumnEvent`：解析 `BEFORE` / `AFTER` 参考列。
- `AlterColumnTypeEvent`：解析 type / old type / comments 的 key。
- `DropColumnEvent`：解析 dropped columns。
- `RenameColumnEvent`：解析 old column name。

### 5.2 comment 语义

`AlterColumnTypeEvent.comments` 的语义定义为：

- key：字段名。
- value 非空：设置字段注释为该值。
- value 为 null：删除字段注释。
- key 不存在：不修改该字段注释。

该设计要求所有处理 `comments` 的路径都必须保留 null value，不能用会拒绝 null value 的 `Collectors.toMap`。

### 5.3 schema diff

`SchemaMergingUtils.getSchemaDifference()` 需要基于 before/after schema 生成最小 schema change：

- 类型变化：生成 `typeMapping` / `oldTypeMapping`。
- comment 新增/修改：生成 `comments[column] = newComment`。
- comment 删除：生成 `comments[column] = null`。
- 类型变化且 comment 非空：携带 comment，避免下游 `MODIFY COLUMN` 丢注释。
- 类型变化且 comment 为空：不额外 emit comment，保持原有行为。

### 5.4 schema apply

`SchemaUtils.applyAlterColumnTypeEvent()` 需要同时处理类型和 comment：

- 类型变化时 copy column type。
- comment key 存在时 copy column comment，包括 null。
- 保留 `PhysicalColumn.defaultValueExpression`。
- 保留 `MetadataColumn.metadataKey`。

### 5.5 transform rewrite

`PostTransformOperator` 需要将 schema change event 改写到 post schema：

- wildcard projection 下也不能直接透传 source event。
- `AddColumnEvent` 基于 prev/next post schema diff 生成新增列事件。
- `DropColumnEvent` 基于 prev/next post schema diff 生成删除列事件。
- `AlterColumnTypeEvent` 基于 prev/next post schema diff 生成 type/comment 变化。
- `RenameColumnEvent` 优先基于 prev/next post schema 同位置 diff 生成 rename event；只有无法证明时才回退到 pre/post lineage map。
- 对列级 schema change，如果 prev/next post schema 完全一致，说明下游无物理 DDL 变化，直接跳过，不再 fallback 透传 source event。
- `column-name-case` 必须在 Runtime/PostTransform 阶段体现到 sink 侧 schema event 中。

该策略避免了两个问题：

- 不把 `JOB_NAME` 这种上游物理名泄漏给下游 sink。
- 不依赖 lineage 只能一对一映射的限制；例如 `JOB, JOB AS job_copy` 时，上游 `JOB` 类型变化应生成 `job` 和 `job_copy` 两个下游列的 alter event。

### 5.6 `SchemaUtils.transformSchemaChangeEvent` 边界

`SchemaUtils.transformSchemaChangeEvent(boolean hasAsterisk, List<String> referencedColumns, SchemaChangeEvent event)`
不是字段名大小写转换入口。

它的职责只有：

- `hasAsterisk = true` 时，透传大部分 schema change。
- 对 `AddColumnEvent` 的 `FIRST` / `LAST` 改写为相对 `referencedColumns` 的 `BEFORE` / `AFTER`，避免 transform 增加计算列后位置语义错误。
- `hasAsterisk = false` 时，过滤掉不会影响固定 projection 输出列集合的事件。
- 对 `AlterColumnTypeEvent`，只按 `referencedColumns.contains(columnName)` 精确匹配保留 type/comment change。

它明确不负责：

- `JOB_NAME -> job_name` 的 lower/upper 转换。
- source column 到 post schema column 的映射。
- alias 映射。
- case-insensitive resolve。
- drop/rename/alter 的下游物理列名改写。

因此它只能作为 projection filtering fallback，不能作为 `column-name-case` 的主实现。

### 5.7 Doris sink

Doris applier 应遵循：

- 主链路上，Doris 应收到 Runtime/PostTransform 已经生成好的下游物理列名。
- 对 `DROP / RENAME / ALTER` 再通过 schema cache 解析真实列名，作为兼容历史 state、外部大小写差异和异常链路的兜底；这不是 `column-name-case` 的主实现。
- Doris DDL 成功后再更新 schema cache。
- Doris DDL 返回 false 或抛异常时不得更新 cache。
- type change 使用 Doris connector 的 `modifyColumnDataType`。
- comment update/removal 使用 Doris connector 的 `modifyColumnComment`。
- comment removal 以空字符串落到 Doris DDL，避免 `modifyColumnDataType` 在 comment 为空时回填旧注释。
- type + comment change 拆分执行：
  - type DDL 使用当前 cache 中的 comment，避免类型修改时意外清空注释。
  - comment DDL 单独执行，只在 comment 真实变化时下发。
  - 任一步失败都不能更新 cache。

该设计与 Doris Flink Connector 的真实能力一致：

- `SchemaChangeManager.modifyColumnDataType(...)` 会在 comment 为空时回填 Doris 当前 comment，适合类型变化但不适合删除 comment。
- `SchemaChangeManager.modifyColumnComment(...)` 是独立修改 comment 的正确接口。

### 5.8 Paimon sink

Paimon 改动不解决 `column-name-case`。当前 diff 中的 Paimon 改动意图是让 `comments[column] = null`
不再被 `comment != null` 判断吞掉，从而支持 comment removal。

客观评估：

- 收益：如果 Paimon catalog 支持 `SchemaChange.updateColumnComment(columnName, null)` 表示清空 comment，则该改动能补齐 comment removal 语义。
- 风险：当前没有独立测试证明 Paimon catalog 对 null comment 的执行语义；字节码仅能证明对象可构造，不能证明 alterTable 后行为正确。
- 额外风险：`SchemaChange.UpdateColumnComment.equals()` 对 null description 存在 NPE 风险，测试和后续比较逻辑需要覆盖。

结论：

- Paimon 改动不是字段大小写修复的必要条件。
- 若本 PR 聚焦 `column-name-case` 和 Doris correctness，应移除 Paimon 改动。
- 若保留 Paimon 改动，必须补充独立单测或集成测试证明 comment removal 行为。

## 六、模块级改动摘要

### 6.1 `flink-cdc-common`

涉及文件：

- `AlterColumnTypeEvent.java`
- `SchemaUtils.java`
- `SchemaMergingUtils.java`
- `SchemaUtilsTest.java`
- `SchemaMergingUtilsTest.java`

改动摘要：

- `AlterColumnTypeEvent.copy(TableId)` 保留 comments。
- `AlterColumnTypeEvent.trimRedundantChanges()` 保留 comment-only event。
- `AlterColumnTypeEvent.addColumnComment()` 允许 null 表示删除注释。
- `SchemaUtils` 新增统一列名解析方法。
- schema apply 覆盖 ADD / ALTER / DROP / RENAME 的大小写归一化。
- `SchemaUtils.applyAlterColumnTypeEvent()` 支持 comment update/removal。
- `SchemaMergingUtils.getSchemaDifference()` 支持 comment-only diff 和 comment removal diff。

### 6.2 `flink-cdc-runtime`

涉及文件：

- `SchemaDerivator.java`
- `PostTransformOperator.java`
- `SchemaDerivatorTest.java`
- `PostTransformOperatorProjectionKeysRegressionTest.java`

改动摘要：

- `SchemaDerivator.normalizeSchemaChangeEvents()` 在 schema behavior rewrite 前归一化已有列引用。
- 归一化日志使用 DEBUG，避免 INFO 被高频 schema event 淹没。
- `PostTransformOperator` 对 wildcard schema events 做 post schema rewrite。
- 覆盖 RENAME / ALTER / ADD / DROP。
- 增加 `column-name-case` 场景回归。
- 增加 comment removal 在 post-transform rewrite 中保留 null value 的测试。

### 6.3 `flink-cdc-pipeline-connector-doris`

涉及文件：

- `DorisMetadataApplier.java`
- `DorisMetadataApplierTest.java`

改动摘要：

- DROP / RENAME 通过 schema cache 解析真实列名。
- ALTER 通过 schema cache 解析 typeMapping/comments。
- comment-only alter 使用独立 `modifyColumnComment` DDL。
- type + comment alter 拆分为 `modifyColumnDataType` 与 `modifyColumnComment`，避免类型 DDL 回填旧 comment 导致 comment removal 失效。
- Doris DDL 失败时不更新 schema cache。
- 增加大小写归一化、cache 保持、comment removal 相关测试。

### 6.4 `flink-cdc-pipeline-connector-paimon`

涉及文件：

- `PaimonMetadataApplier.java`

当前 diff 中的改动摘要：

- comment update 判断从 `comment != null` 改为 `comments.containsKey(columnName)`。
- 允许 `SchemaChange.updateColumnComment(columnName, null)` 表示删除注释。

当前评估结论：

- 该改动与 `column-name-case` 无关。
- 当前缺少 Paimon comment removal 测试，不应计入本 PR 已确认修复。
- 保留条件：补充 Paimon 独立测试，证明 null comment 能正确清空字段注释。
- 否则建议从本 PR 中移除，避免扩大核心链路改动范围。

## 七、已修复问题与剩余风险

### 7.1 已修复：显式 projection 下 comment removal NPE

`SchemaUtils.transformSchemaChangeEvent(false, referencedColumns, event)` 原先用 `Collectors.toMap`
收集 `AlterColumnTypeEvent.comments`。当 `comments[column] = null` 表示删除注释时，JDK
collector 会抛 `NullPointerException`。

当前实现已改为显式 `LinkedHashMap` 循环，按 key 是否存在判断事件语义，保留 null value。

覆盖测试：

- `SchemaUtilsTest.testTransformSchemaChangeEventKeepsProjectedCommentRemovalAlterColumnEvent`
- `SchemaUtilsTest.testAlterColumnTypeEventAddColumnCommentAcceptsNull`

### 7.2 已修复：Doris comment removal 物理表与 cache 分裂

旧路径把 type/comment 统一走 `modifyColumnDataType`。Doris connector 在 comment 为空时会回填原始
comment，导致 `comments[column] = null` 不能真实清空 Doris comment，但 Flink CDC cache 已经被清空。

当前实现已拆分：

- type change 走 `modifyColumnDataType`。
- comment update/removal 走 `modifyColumnComment`。
- comment removal 传空字符串，使 Doris 物理表注释被清空。
- type + comment change 拆成 type DDL + comment DDL，全部成功后才更新 cache。

覆盖测试：

- `DorisMetadataApplierTest` 中 comment-only alter、comment removal、type+comment split DDL、DDL 失败不更新 cache 相关用例。

### 7.3 已修复：drop column 被 redundant 判断吞掉

最新日志定位到 drop 失败的直接原因：

```text
DropColumnEvent{droppedColumnNames=[JOB_NAME]} is redundant for current schema ... job_name ... just skip it.
```

这会导致 sink writer 仍持有 9 列 schema，而后续数据 record 已经是 8 列。

当前修复点：

- Runtime/PostTransform 优先按 prev/next post schema diff 生成 `DropColumnEvent(job_name)`。
- `SchemaUtils.isSchemaChangeEventRedundant` 覆盖 case-insensitive 但不把“将要删除的列仍存在”误判为 redundant。
- 新增 `JOB_NAME -> job_name` drop 回归。

### 7.4 剩余风险

- Paimon 及其他非 Doris sink 是否完整支持 comment removal，需要后续按 sink 能力矩阵逐个确认。
- 端到端 MySQL -> Doris 真实集群 DDL 矩阵仍需在发布前执行。
- savepoint/restore 后的历史 state 中可能携带旧版本未归一化事件，Doris sink 的 schema cache 解析作为兼容兜底保留。

## 八、执行方案

### 8.1 第一阶段：统一语义

1. 定义 `comments[column] = null` 表示删除 comment。
2. 所有 map copy/filter 逻辑必须保留 null value。
3. 所有 schema apply/diff/rewrite 路径必须把“key 存在”和“value 非空”区分开。

### 8.2 第二阶段：统一列名归一化

1. `SchemaUtils` 提供公共 resolver。
2. `SchemaDerivator` 在 behavior rewrite 前统一归一化。
3. `PostTransformOperator` 基于 prev/next post schema diff 生成下游 schema event，确保 `column-name-case` 在 runtime 阶段生效。
4. 只有 schema diff 无法证明 rename 映射时，才回退到 lineage map。
5. Doris sink 在落外部系统前再次基于 sink cache 解析，作为兼容兜底；其他 sink 不在本 PR 中承担字段大小写兜底职责。

### 8.3 第三阶段：sink 语义落地

1. Doris：
   - type change 使用 `modifyColumnDataType`。
   - comment update/removal 使用 comment DDL。
   - type + comment change 需要保证两类 DDL 的成功顺序和 cache 一致性。

2. Paimon：
   - 当前不作为必需修复保留。
   - 若保留，必须用测试证明 `comments.containsKey(columnName)` 与 null value 的 comment removal 语义。

### 8.4 第四阶段：回归验证

按第十节清单执行自动化和端到端验收。

## 九、已执行自动化验证

所有命令均使用 JDK 11：

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home
```

已执行以下测试：

```bash
mvn -pl flink-cdc-common -Dtest=SchemaUtilsTest,SchemaMergingUtilsTest test
```

结果：

```text
Tests run: 24, Failures: 0, Errors: 0, Skipped: 0
```

```bash
mvn -pl flink-cdc-runtime -am \
  -Dtest=SchemaDerivatorTest,PostTransformOperatorProjectionKeysRegressionTest,AlterColumnTypeEventSerializerTest \
  -DfailIfNoTests=false test
```

结果：

```text
Tests run: 47, Failures: 0, Errors: 0, Skipped: 0
```

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -am -Dtest=DorisMetadataApplierTest,DorisEventSerializerTest \
  -DfailIfNoTests=false test
```

结果：

```text
Tests run: 116, Failures: 0, Errors: 0, Skipped: 0
```

此外，`git diff --check` 已通过，无 whitespace error。

## 十、完整回归验收清单

### 10.1 自动化测试

- [x] `SchemaUtilsTest` 覆盖 case-insensitive apply / redundancy。
- [x] `SchemaUtilsTest` 覆盖 comment update / removal。
- [x] `SchemaUtilsTest` 覆盖 explicit projection comment removal，且无 NPE。
- [x] `SchemaMergingUtilsTest` 覆盖 comment-only diff。
- [x] `SchemaMergingUtilsTest` 覆盖 comment removal diff。
- [x] `SchemaDerivatorTest` 覆盖 ADD / ALTER / DROP / RENAME 归一化。
- [x] `SchemaDerivatorTest` 覆盖 comment removal 归一化。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖 wildcard lower/upper case。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖 explicit projection。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖 `column-name-case: lower` 下 `JOB_NAME -> JOB_NAME_123` rename。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖 `column-name-case: lower` 下 drop `JOB_NAME` 生成 drop `job_name`。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖 case-only rename 跳过下游 DDL。
- [x] `PostTransformOperatorProjectionKeysRegressionTest` 覆盖同一上游字段投影到多个下游字段时基于 post schema diff 生成 alter event。
- [x] `DorisMetadataApplierTest` 覆盖 alter type 大小写归一化。
- [x] `DorisMetadataApplierTest` 覆盖 drop/rename 大小写归一化。
- [x] `DorisMetadataApplierTest` 覆盖 comment update。
- [x] `DorisMetadataApplierTest` 覆盖 comment removal 使用 comment DDL。
- [x] `DorisMetadataApplierTest` 覆盖 type + comment 拆分 DDL。
- [x] `DorisMetadataApplierTest` 覆盖 DDL 失败不更新 cache。
- [ ] `PaimonMetadataApplier` 独立单测覆盖 comment removal。
- [x] `AlterColumnTypeEventSerializerTest` 覆盖 comments null value 序列化。

### 10.2 MySQL -> Doris 端到端验收

基础表：

```sql
CREATE TABLE student (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  job VARCHAR(64) COMMENT 'old job',
  create_time TIMESTAMP,
  age INT DEFAULT 30,
  flag INT DEFAULT 1
);
```

DDL 矩阵：

```sql
ALTER TABLE student MODIFY COLUMN job VARCHAR(255);
ALTER TABLE student MODIFY COLUMN job VARCHAR(255) COMMENT 'new job';
ALTER TABLE student MODIFY COLUMN job VARCHAR(255);
ALTER TABLE student ADD COLUMN score INT AFTER name;
ALTER TABLE student ADD COLUMN level INT FIRST;
ALTER TABLE student ADD COLUMN address VARCHAR(128) BEFORE create_time;
ALTER TABLE student CHANGE COLUMN job occupation VARCHAR(255) COMMENT 'occupation';
ALTER TABLE student DROP COLUMN flag;
```

大小写矩阵：

```sql
ALTER TABLE student MODIFY COLUMN JOB VARCHAR(512);
ALTER TABLE student DROP COLUMN AGE;
```

验收点：

- [ ] Doris 表结构与 Flink CDC evolved schema 一致。
- [ ] 字段类型变化成功。
- [ ] 字段 comment 新增/修改成功。
- [ ] 字段 comment 删除成功。
- [ ] drop column 成功。
- [ ] rename column 成功。
- [ ] add column 位置符合预期。
- [ ] schema cache 与 Doris 物理表一致。

### 10.3 Transform 验收

- [ ] wildcard projection 下 ADD / ALTER / DROP / RENAME 均 rewrite 到 post schema。
- [ ] explicit projection 下只输出投影字段相关 schema event。
- [ ] `column-name-case: lower` 下 sink event 使用小写列名。
- [ ] `column-name-case: upper` 下 sink event 使用大写列名。
- [ ] comment removal 在 pre-transform 和 post-transform 都不丢失。
- [ ] projection alias 后，上游原始列 schema change 能映射到 alias 后列。

### 10.4 Savepoint / restore 验收

- [ ] 作业运行到 checkpoint 成功。
- [ ] 从 savepoint 恢复后继续处理 schema change。
- [ ] 恢复后执行 alter type 成功。
- [ ] 恢复后执行 drop column 成功。
- [ ] 恢复后执行 rename column 成功。
- [ ] 恢复后执行 comment update/removal 成功。
- [ ] schema manager 状态、sink cache、Doris 物理表三者一致。

### 10.5 失败路径验收

- [ ] Doris DDL 返回 false 时，作业失败信息包含 table、column、schema change 类型。
- [ ] Doris DDL 返回 false 时，不更新 schema cache。
- [ ] type + comment 双 DDL 中任一步失败时，不产生错误 cache。
- [ ] reference column 不存在时显式失败，不静默降级。
- [ ] 大小写不敏感匹配不唯一时不误改写。

## 十一、质量评估

### 11.1 当前状态

当前 PR 已系统覆盖 ADD / ALTER / DROP / RENAME / COMMENT 在 common/runtime/Doris 链路上的主路径，并将 `column-name-case`
从 sink 端猜测提升为 Runtime/PostTransform 生成下游物理列名的主策略。

已修复原始 Doris alter type 列不存在问题、drop column redundant 误判导致 writer schema
滞后问题、comment removal NPE 和 Doris 物理表/cache 分裂问题。

Paimon 改动当前仍处于待决策状态：补测试后可保留，否则建议从本 PR 中移除。

### 11.2 当前评分

- common/runtime/Doris 主链路可用性：9.1 / 10
- common/runtime/Doris 主链路代码质量：9.1 / 10
- 整体 PR 商业可交付：8.8 / 10

整体 PR 尚未给 9+ 的原因：

- Paimon 改动未补独立测试，保留与否未决。
- MySQL -> Doris 真实 E2E DDL 矩阵尚未执行。
- savepoint/restore 后 schema change 仍需实测。

### 11.3 达到 9+ 的必要条件

- 在真实 MySQL -> Doris 环境执行第十节 E2E DDL 矩阵。
- 移除 Paimon 改动，或补齐 Paimon comment removal 独立单测。
- 做一次 savepoint/restore 后 schema change 回归。

## 十二、发布前 Gate

合并前必须满足：

- [x] 第七节已识别问题全部修复。
- [x] `git diff --check` 无输出。
- [x] common/runtime/doris 相关单测全部通过。
- [x] comment removal 在 explicit projection、post-transform、Doris sink 均通过。
- [ ] Paimon 改动已移除，或已补齐独立 comment removal 测试。
- [ ] MySQL -> Doris DDL 矩阵通过。
- [ ] savepoint restore 后 schema change 通过。
- [ ] 失败路径确认不会污染 schema cache。
