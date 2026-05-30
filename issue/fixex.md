# Doris 已存在表接管与类型校验最终修复说明

## 一、结论

本次客户现场失败的直接原因是 Doris sink 在处理 `CreateTableEvent` 时，对已存在 Doris 表做了过强的类型校验。

现场报错包括：

```text
Doris table test.student already has column name with incompatible type.
desired=VARCHAR(192), actual=VARCHAR.
Existing-table reconcile only supports additive missing columns.
```

以及：

```text
Doris table test.student already has column create_time with incompatible type.
desired=DATETIME(3), actual=DATETIME(0).
Existing-table reconcile only supports additive missing columns.
```

最终修复策略：

1. `CreateTableEvent` 遇到已存在 Doris 表时，不再以类型差异阻断作业。
2. 已存在列按 Doris 物理表现状接管，只记录 `WARN`。
3. 只对缺失列执行 additive reconciliation。
4. Doris DDL 返回 `false` 时立即失败，且不更新本地 schema cache。
5. `ADD COLUMN` 继续按 Doris connector 现有 DDL 语义执行；本地 schema cache 保留 CDC 期望 schema，不把 Doris 物理 DDL 结果反向写入 CDC schema。

这是从可用性和稳定性优先出发的架构修复，不再继续补单个类型兼容规则。

## 二、为什么不能继续强校验类型

上游 Flink CDC 的 Doris connector 不做“已存在表接管时的类型校验”，核心原因是上游没有承担“根据 Doris 物理表 schema 反向证明它与 CDC schema 完全兼容”的职责。

当前分支新增了已存在表接管能力后，如果继续做强类型校验，会遇到几个无法稳定解决的问题：

1. Doris FE `_schema` 返回的类型信息并不总是保留完整精度，例如 `VARCHAR` 可能不带 length，`DATETIME` 可能返回 scale 为 `0`。
2. Flink CDC 类型到 Doris 类型不是一一映射，例如 `STRING`、`VARCHAR(n)`、`TEXT`，`TIMESTAMP(3)`、`DATETIME(0)`、`DATETIMEV2` 都可能在不同链路中表现不同。
3. 预建表是客户现场常态，目标表通常包含分区、主键、bucket、默认值列、审计列和手动定义的精度，不能假设它与 CDC schema 字符串完全一致。
4. 错误拒绝会直接导致作业启动失败，影响全链路可用性；错误放行最多暴露为后续 Doris 写入阶段的数据/类型问题，定位边界更清晰。

因此，在 `CreateTableEvent` 的已存在表接管阶段，类型校验不能作为硬门禁。该阶段唯一稳定职责是确认表存在、补齐缺失列、建立本地 schema 认知。

## 三、最终架构

`CreateTableEvent` 的处理边界调整为：

```text
fetch Doris physical schema
  |
  +-- Doris 表不存在:
  |      create table
  |      createTable 返回 true 后更新 schema cache
  |
  +-- Doris 表已存在:
         对已存在列做类型差异 WARN
         根据列名找出 CDC schema 中缺失的列
         逐个 ADD COLUMN
         所有 ADD COLUMN 成功后更新 schema cache
```

关键原则：

1. 已存在列不改类型、不拒绝、不自动 ALTER。
2. 缺失列只做 additive add column。
3. 本地 schema cache 只能在 Doris DDL 确认成功后更新。
4. Doris schema 查询异常不能被当成“表不存在”吞掉，只有明确的 404 / missing schema 才走 create table。

## 四、代码修改点

主文件：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplier.java
```

核心变化：

1. 移除 `validateExistingColumnCompatibility(...)` 的硬失败逻辑。
2. 改为 `logExistingColumnTypeDifferences(...)`，仅输出 `WARN`。
3. 移除脆弱的 Doris 类型兼容推断方法，包括字符类型、decimal、datetime 参数比较等。
4. 新增 `ensureSchemaChangeSucceeded(...)`，所有 boolean-returning DDL 都必须检查返回值。
5. `createTable`、`addColumn`、`dropColumn`、`renameColumn`、`modifyColumnDataType` 返回 `false` 时不更新 schema cache。
6. 抽出 `DorisTypeUtils` 和 `DorisExistingColumnTypeAnalyzer`，统一 DDL 类型字符串构造和非阻断类型诊断。

保留的行为：

1. Doris schema 查询失败仍会中止，避免把网络、权限、FE 异常误判为表不存在。
2. `truncateTable`、`dropTable`、`alterTableComment` 仍按异常驱动处理，因为这些方法不是 boolean-returning DDL。

## 五、字段类型验证改动完整总结

### 5.1 改动前的问题

改动前，已存在 Doris 表接管流程中存在一套强类型兼容判断：

```text
CreateTableEvent desired schema
  vs
Doris FE _schema physical fields
```

只要同名字段的 Doris 类型字符串和 Flink CDC 转换后的 Doris 类型字符串不满足内部兼容规则，就直接抛出 `SchemaEvolveException`，导致 SchemaCoordinator 触发全局失败。

该逻辑的问题不是“检查类型”这个目标本身错误，而是检查依据不可靠：

1. Doris FE `_schema` 的类型信息可能缺少 length、precision、scale。
2. Doris 类型存在多种别名和版本形态，例如 `TEXT` / `STRING`、`DATETIME` / `DATETIMEV2`、`DECIMAL` / `DECIMALV3`。
3. Flink CDC 的 logical type 到 Doris physical type 不是严格双射。
4. 客户预建表通常经过人工设计，类型字符串不完全一致不代表不能写入。
5. 一旦误判，作业在启动或恢复阶段直接失败，客户现场不可用。

因此，旧方案实际是在用不完整元数据做强一致性判断，误杀概率高。

### 5.2 改动后的验证边界

本次不是完全删除类型相关处理，而是重新划定字段类型验证的职责边界。

`CreateTableEvent` 接管已存在表时：

```text
已存在字段:
  不做硬校验
  不自动 ALTER
  不阻断作业
  只记录 WARN

缺失字段:
  仍按 Flink CDC schema 构造 Doris FieldSchema
  通过 ADD COLUMN 补齐
  Doris DDL 返回 false 时失败

后续 AlterColumnTypeEvent:
  仍会调用 Doris modifyColumnDataType
  返回 false 时失败且不更新 cache
```

也就是说，类型验证从“启动阶段硬门禁”改为“接管阶段诊断信息”。真正需要改变字段类型时，仍由显式的 `AlterColumnTypeEvent` 路径处理。

### 5.3 删除的强兼容规则

本次移除的逻辑包括：

```text
validateExistingColumnCompatibility
areDorisTypesCompatible
isDorisCharacterStringCompatible
isDorisDecimalCompatible
isDorisDatetimeCompatible
extractDorisTypeRoot
extractDorisTypeParameters
hasDorisTypeParameters
isDorisCharacterStringType
```

这些方法的共同问题是试图在 connector 侧重新实现一套 Doris 类型等价系统，但输入信息来自 FE `_schema` 的简化结果，无法覆盖 Doris 版本差异、类型别名、精度缺省和预建表人工定义。

保留的类型处理仅用于日志展示：

```text
buildExistingFieldType
normalizeDorisTypeDefinition
```

它们不再参与是否失败的判断，只用于输出 `WARN`，帮助定位潜在类型漂移。

### 5.4 当前 WARN 的含义

当已有列类型不同，当前只输出类似日志：

```text
Doris table test.student already has column create_time with physical type DATETIME(0),
while the incoming CreateTableEvent desires DATETIME(3).
Existing-table reconcile will not alter existing columns and will only add missing columns.
```

该 WARN 表达三个事实：

1. Doris 表中已有该列。
2. CDC schema 期望类型和 Doris 物理类型字符串不同。
3. 本次接管不会修改已有列，只会补缺失列。

它不是错误，也不会阻断作业。

### 5.5 为什么不能改成“更完整的类型兼容表”

从第一性原理看，要在这里做硬校验，必须同时满足：

1. Doris FE 返回的元数据完整可信。
2. connector 能准确表达 Doris 当前版本的所有类型等价关系。
3. connector 能判断每种类型差异对写入语义是否安全。
4. 误判成本可接受。

当前四个条件都不成立。特别是在客户现场，误判成本是作业不可用和赔付风险，因此不能继续用补规则的方式止血。

例如：

```text
VARCHAR(64) vs VARCHAR
VARCHAR(64) vs VARCHAR(192)
TIMESTAMP(3) vs DATETIME(0)
DECIMAL(17,7) vs DECIMAL
BOOLEAN vs TINYINT
STRING vs TEXT
```

这些差异有些是 Doris 元数据缺省，有些是类型别名，有些是人工预建表的容量差异。connector 无法只靠字符串比较稳定判断“必然不可写”。

### 5.6 与上游 release-3.6 的关系

对比上游 release-3.6，原生 Doris connector 没有在 `CreateTableEvent` 阶段承担“已存在 Doris 物理表类型兼容证明”的职责。

本分支新增了已存在表接管和缺失列调和能力，这是上游之外的增强。既然新增能力的目标是让客户预建表可被接管，那么接管阶段必须保持宽松，不能用比上游更严格、且依据不完整的类型判断破坏可用性。

最终策略是：

```text
上游没有做的强类型证明，本分支也不再补做。
本分支只额外做已有表接管、缺失列补齐和 WARN 诊断。
```

### 5.7 保留的安全防线

虽然取消了已有列强类型拦截，但仍保留以下防线：

1. Doris schema 查询异常不会被吞掉，非 missing table 错误直接失败。
2. 只有明确表不存在时才执行 create table。
3. 缺失列 ADD COLUMN 必须返回 true，否则失败。
4. create table 返回 false 时失败。
5. drop column、rename column、alter column type 返回 false 时失败。
6. DDL 失败时不更新 schema cache。
7. 已有列类型差异记录 WARN，便于客户排查后续写入问题。

这保证了“可用性优先”不等于“静默吞掉 DDL 失败”。

### 5.8 可用性评分

按客户现场已交付、失败会造成赔付风险的标准评估：

```text
旧强校验方案:
  可用性: 4/10
  稳定性: 5/10
  问题: 误杀概率高，恢复/启动阶段直接失败。

继续补类型规则方案:
  可用性: 6/10
  稳定性: 5/10
  问题: 每补一个规则只能覆盖一个表现形态，仍会被 Doris 版本、精度缺省、类型别名击穿。

当前 WARN + additive reconcile 方案:
  可用性: 9/10
  稳定性: 8/10
  问题: 真实不可写类型会延后到 Doris 写入阶段暴露，但不会在接管阶段误杀。
```

在当前业务风险下，第三种方案是最稳妥的交付方案。

### 5.9 Review 关注项评估

Cursor review 提到的风险基本成立，但不能因此回到强类型校验。逐项评估如下。

高风险项：类型漂移从启动失败变为 WARN。

结论：接受该行为变化，并在 PR / issue 中明确说明这是可用性优先的行为变化。原因是旧的启动失败大量来自误杀，而不是确定的数据不可写。为降低“静默 WARN”风险，代码已把日志改成结构化字段形式，便于日志告警接入，但仍不阻断作业：

```text
table=<tableId>, column=<column>, physicalType=<dorisType>, desiredType=<cdcType>, typeDefinitionDrift=true
table=<tableId>, column=<column>, physicalType=<dorisType>, lowConfidencePhysicalMetadata=true
table=<tableId>, column=<column>, physicalType=<dorisType>, desiredType=<cdcType>, capacityRisk=true
table=<tableId>, column=<column>, physicalType=<dorisType>, desiredType=<cdcType>, familyMismatch=true
```

后续 backlog：增加 opt-in 严格模式，例如 `sink.doris.strict-schema-reconcile`。该模式只能作为用户显式开启的诊断/治理能力，不能作为本次 hotfix 默认行为。

中风险项：Doris 物理 DDL 与 schema cache 不完全一致。

结论：接受该设计。schema cache 表示 Flink CDC 期望 schema，用于后续 schema evolution 和序列化推断；Doris connector 的 `FieldSchema` / `SchemaChangeHelper.buildAddColumnDDL` 当前只表达 type/default/comment，不表达 nullable，因此不能把本地 cache 当成 Doris 物理 DDL 的完全镜像。该差异需要在 PR 描述中说明。

中风险项：`TINYINT` 与 `BOOLEAN` 的别名处理。

结论：只接受单向的 Doris FE 元数据别名：CDC 期望 `BOOLEAN`，Doris `_schema` 返回 `TINYINT` 且 `precision=0` 时，不标记 `familyMismatch`，但仍保留 `physicalType=TINYINT` 和 `typeDefinitionDrift=true` 诊断。不能在类型字符串构造阶段把 `TINYINT` 改写成 `BOOLEAN`，也不能把 CDC `TINYINT` 写入 Doris `BOOLEAN` 视为兼容；后者数值域不同，应记录 family mismatch 风险。

低风险项：`ensureSchemaChangeSucceeded` 没覆盖 `truncateTable` / `dropTable` / `alterTableComment`。

结论：当前不改。这三个方法在现有 `DorisSchemaChangeManager` 中不是 boolean-returning DDL，仍按异常路径处理。若未来 API 改为 boolean 返回，需要补同样的 cache 防线。

低风险项：CSV 和 JSON 行为不对称。

结论：本次不扩大修复范围。Doris-only 默认值列问题当前修复的是 JSON Stream Load 路径；CSV 物理 schema resolver 仍要求 Doris 物理列能在 CDC schema 中找到。原因是 CSV 按列序输出，Doris-only 列的省略语义与 JSON key-value 不同，不能直接套用 JSON 的跳过策略。PR 描述必须明确本次修复范围是 JSON 路径。

流程项：commit message 和文档是否进入上游 PR。

结论：建议拆分或至少在 commit message 中明确两个主题：

```text
[hotfix][pipeline-connector][doris] Skip Doris-only columns in JSON stream load
[improve][pipeline-connector][doris] Relax existing-table reconcile and harden DDL cache
```

`issue/*.md` 更适合作为内部复盘材料，不建议直接进入 ASF 上游 PR，除非转写为开发文档或 PR 描述。

## 六、现场失败场景覆盖

客户现场 schema：

```text
CreateTableEvent:
id BIGINT NOT NULL
name VARCHAR(64) NOT NULL
create_time TIMESTAMP(3)
_db_ STRING NOT NULL
_tb_ STRING NOT NULL
_op_ STRING NOT NULL
```

Doris 物理表：

```text
id BIGINT
name VARCHAR(192)
create_time DATETIME(0)
_db_ STRING
_tb_ STRING
_op_ STRING
```

修复前：

```text
DATETIME(3) vs DATETIME(0) 被判定 incompatible type
SchemaCoordinator 触发全局失败
作业无法启动
```

修复后：

```text
create_time 类型差异只输出 WARN
所有字段已存在，不执行 ADD COLUMN
schema cache 更新为当前 CDC schema
作业继续运行
```

## 七、测试覆盖

关键测试文件：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplierTest.java
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisExistingColumnTypeAnalyzerTest.java
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisEventSerializerTest.java
```

新增/调整的关键用例：

```text
testCreateTableEventAcceptsExistingColumnTypeDrift
testCreateTableEventAcceptsExistingDecimalWithoutPrecisionMetadata
testCreateTableEventDoesNotRejectExistingVarcharWithCapacityRisk
testCreateTableEventAcceptsExistingCharForVarcharColumn
testCreateTableEventAcceptsExistingTinyintWithPrecisionForBooleanColumn
testCreateTableEventDoesNotRejectExistingDecimalWithCapacityRisk
testCreateTableEventAcceptsExistingDatetimeZeroScaleForTimestampThree
testDoesNotFlagCapacityRiskWhenDecimalMetadataHasLowConfidence
testParsesDatetimeScaleFromTypeDefinition
testParsesDecimalCapacityFromTypeDefinition
testJsonSerializationSkipsNullInputFieldMappings
testCreateTableEventFailsAndDoesNotCacheSchemaWhenCreateTableReturnsFalse
testCreateTableEventFailsAndDoesNotCacheSchemaWhenReconcileAddColumnReturnsFalse
testAddColumnEventFailsAndKeepsPreviousCacheWhenAddColumnReturnsFalse
testAddColumnEventKeepsCdcNullabilityInCache
testAlterColumnTypeEventFailsAndKeepsPreviousCacheWhenDorisReturnsFalse
```

其中 `testCreateTableEventAcceptsExistingDatetimeZeroScaleForTimestampThree` 精确覆盖客户现场：

```text
desired=DATETIME(3)
actual=DATETIME(0)
```

## 八、已执行验证

目标单测已通过：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -DskipITs -DskipE2e -Dcheckstyle.skip -Drat.skip \
  -Dspotless.check.skip=true \
  -Dtest=DorisMetadataApplierTest,DorisExistingColumnTypeAnalyzerTest,DorisEventSerializerTest \
  clean test
```

结果：

```text
Tests run: 87, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

格式检查已通过：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris \
  -DskipTests -DskipITs -DskipE2e -Drat.skip -Dcheckstyle.skip spotless:check
```

结果：

```text
BUILD SUCCESS
```

静态检查确认以下旧强校验内容已不存在：

```text
incompatible type
Existing-table reconcile only supports
validateExistingColumnCompatibility
areDorisTypesCompatible
isDorisCharacterStringCompatible
isDorisDecimalCompatible
isDorisDatetimeCompatible
```

## 九、风险与取舍

本次取舍明确偏向可用性和稳定性。

不再强校验已有列类型，意味着系统不会在启动阶段替客户判断 Doris 预建表是否“类型完全等价”。这是有意为之，因为当前阶段没有可靠、完备、跨 Doris 版本稳定的类型等价判断依据。

剩余风险：

1. 如果客户预建表字段类型确实无法承载 CDC 数据，错误会在 Doris 写入阶段暴露。
2. 已存在列不会自动 ALTER，需要客户或运维侧显式调整 Doris 表结构。
3. WARN 日志会提示存在物理类型差异，便于排查潜在写入问题。

收益：

1. 避免因为 Doris 元数据精度缺失或类型别名导致作业误失败。
2. 兼容客户现场常见预建表。
3. 与上游 Flink CDC 的宽松接管语义保持一致。
4. DDL 失败不再污染本地 schema cache，降低后续状态不一致风险。

## 十、客户侧解释口径

这是我们在 Doris 已存在表接管逻辑中引入的过强类型校验导致的可用性问题。Doris 返回的物理类型元数据和 Flink CDC 期望类型在字符串层面不完全一致，但这不应阻断已有表接管。

修复后，已存在字段不再因为类型字符串差异导致作业失败；系统只补齐缺失字段，并对类型差异记录 WARN。这样可以保证客户已预建 Doris 表的同步任务优先可用，同时保留问题诊断信息。
