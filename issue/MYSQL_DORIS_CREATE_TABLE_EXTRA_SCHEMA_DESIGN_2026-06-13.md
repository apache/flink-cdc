# MySQL CDC -> Doris 自动建表追加 extra schema 设计文档

## 一、文档目的

本文用于沉淀 `Flink CDC MySQL -> Doris` 自动建表时追加自定义 Doris schema 片段的完整需求、架构判断、参数语义、实现方案和验证计划。

本需求的核心目标是：

- 上游 MySQL schema 仍然作为自动建表的主体来源。
- 用户可以通过 Doris sink 参数，在 `CREATE TABLE (...)` 字段/索引定义列表末尾追加 Doris 专属 schema 片段。
- 追加片段只在 Doris 目标表不存在、自动执行 `CREATE TABLE` 时生效。
- 如果追加片段中声明的列与上游 schema event 中的列同名，必须以上游 schema 为准，追加片段中的同名列定义被忽略。

本文不包含实际代码提交记录，只定义推荐设计与执行计划。

## 二、需求背景

当前 Flink CDC pipeline 会根据上游 schema event 自动在 Doris 下游创建表。例如上游 MySQL 表：

```sql
CREATE TABLE `ods_mysql_test`.`sequence` (
  `uid` BIGINT NOT NULL,
  `name` VARCHAR(384)
)
```

默认自动建表时，下游 Doris 表主要由上游字段推导得到：

```sql
CREATE TABLE IF NOT EXISTS `ods_mysql_test`.`sequence` (
  `uid` BIGINT NOT NULL,
  `name` VARCHAR(384)
)
ENGINE=OLAP
UNIQUE KEY(`uid`)
DISTRIBUTED BY HASH(`uid`) BUCKETS 4;
```

业务希望在自动建表时额外补充 Doris 侧字段和索引，例如：

```sql
`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
`_db_` STRING NOT NULL,
`_tb_` STRING NOT NULL,
`_op_` STRING NOT NULL,
INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

最终 Doris 建表语句期望是：

```sql
CREATE TABLE IF NOT EXISTS `ods_mysql_test`.`sequence` (
  `uid` BIGINT NOT NULL,
  `name` VARCHAR(384),
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
)
ENGINE=OLAP
UNIQUE KEY(`uid`)
DISTRIBUTED BY HASH(`uid`) BUCKETS 4;
```

这个能力本质上不是通用 CDC schema evolution，也不是 transform projection，而是 Doris sink 在自动建表时对 Doris 目标表结构的补充能力。

## 三、最终需求语义

经过需求确认，本次能力只覆盖以下语义：

1. 只在 `CREATE TABLE` 时生效。
2. 只在 Doris 目标表不存在、自动创建新表时生效。
3. 不对已存在 Doris 表执行补列、补索引或任何 reconcile 扩展。
4. 不改变上游 CDC schema。
5. 不改变数据写入 payload。
6. 不负责给额外字段填充值。
7. `table.create.extra-schema` 是补充，不是覆盖；同名列以上游 schema event 为准。

因此，如果 extra schema 中包含如下无默认值的 `NOT NULL` 字段：

```sql
`_db_` STRING NOT NULL,
`_tb_` STRING NOT NULL,
`_op_` STRING NOT NULL
```

那么后续写入是否成功，取决于数据流 payload 是否真的包含这些字段，或 Doris 侧是否允许缺省写入。这不属于本次建表扩展能力的职责范围。

## 四、参数命名

推荐新增参数：

```yaml
table.create.extra-schema: |
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

选择 `table.create.extra-schema` 的原因：

- `table.create` 与现有 Doris 建表参数前缀保持一致。
- `extra-schema` 比 `extra-ddl` 更准确，因为用户配置的不是完整 DDL，而是 `CREATE TABLE (...)` 内部的 schema 片段。
- `extra-schema` 能覆盖列定义、索引定义等 Doris table schema element。
- 不使用 `extra-columns`，因为示例中包含 `INDEX ... USING INVERTED`，不只是列。
- 不使用 `extra-table-elements`，虽然 SQL 语义更准确，但对用户不如 `extra-schema` 直观。

## 五、当前架构分析

### 5.1 Flink CDC pipeline Doris sink

Flink CDC 侧 Doris connector 的建表入口在：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/
  src/main/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplier.java
```

关键链路：

```text
CreateTableEvent
  -> DorisMetadataApplier.applyCreateTableEvent(...)
  -> DorisMetadataApplier.buildTableSchema(...)
  -> DorisSchemaChangeManager.createTable(TableSchema)
```

`DorisMetadataApplier.buildTableSchema(...)` 当前负责把 Flink CDC 的 `Schema` 转成 Doris connector 的 `TableSchema`：

- 设置 database/table
- 设置 DataModel
- 设置 fields
- 设置 keys
- 设置 distribute keys
- 设置 table comment
- 设置 table create properties
- 设置 table buckets
- 设置 auto partition 信息

### 5.2 Doris connector 建表 SQL 生成

真正生成 Doris `CREATE TABLE` SQL 的位置在 Doris connector 仓库：

```text
/Users/benjobs/Github/doris-flink-connector/flink-doris-connector/
  src/main/java/org/apache/doris/flink/catalog/doris/DorisSchemaFactory.java
```

关键链路：

```text
SchemaChangeManager.createTable(TableSchema)
  -> DorisSystem.buildCreateTableDDL(TableSchema)
  -> DorisSchemaFactory.generateCreateTableDDL(TableSchema)
```

其中 `DorisSchemaFactory.generateCreateTableDDL(...)` 会生成：

- `CREATE TABLE IF NOT EXISTS db.table (...)`
- key columns
- value columns
- `UNIQUE KEY(...)`
- comment
- auto partition
- `DISTRIBUTED BY HASH(...)`
- buckets
- properties

### 5.3 为什么核心能力应放在 Doris connector

`table.create.extra-schema` 是 Doris 建表 SQL 语义，应该由 Doris connector 的 `TableSchema -> CREATE TABLE DDL` 层支持。

原因：

1. Flink CDC `Schema` 是跨 sink 的通用模型，不应该携带 Doris 专属 raw SQL 片段。
2. Doris connector 自身 CDC tools 和 Flink CDC pipeline Doris sink 都会构造 `TableSchema` 并走 Doris connector 建表逻辑。
3. 如果只在 Flink CDC Doris adapter 层拼字符串，会绕过 Doris connector 的统一建表能力，后续复用和测试都会变差。
4. raw Doris schema 片段中可能包含 `INDEX`、Doris 类型、Doris 函数和 Doris 专属语法，只适合留在 Doris connector 边界内。

推荐分工：

```text
doris-flink-connector:
  定义 TableSchema 扩展字段
  解析/过滤 extra schema
  生成最终 CREATE TABLE DDL
  支持 Doris CDC tools 的 table-conf 参数

apache-flink-cdc:
  定义 Doris pipeline sink 参数 table.create.extra-schema
  读取配置
  透传到 Doris connector TableSchema
  保持 CreateTableEvent / DataChangeEvent / schema evolution 不变
```

## 六、架构设计

### 6.1 数据结构扩展

在 Doris connector 的 `TableSchema` 中增加字段：

```java
private List<String> extraTableSchemaElements;
```

语义：

- 保存已经切分并过滤后的 extra schema 片段。
- 每个元素是一个 `CREATE TABLE (...)` 内部的完整 table element。
- 元素可能是列定义，也可能是 index/constraint 定义。

示例：

```java
[
  "`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)",
  "`_db_` STRING NOT NULL",
  "`_tb_` STRING NOT NULL",
  "`_op_` STRING NOT NULL",
  "INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED"
]
```

### 6.2 建表 SQL 拼接位置

在 `DorisSchemaFactory.generateCreateTableDDL(TableSchema schema)` 中追加 extra schema。

推荐拼接位置：

```text
CREATE TABLE IF NOT EXISTS db.table (
  上游 key columns,
  上游 value columns,
  extra schema elements
)
UNIQUE KEY(...)
DISTRIBUTED BY HASH(...)
...
```

也就是：

- 在所有自动推导列之后追加。
- 在删除最后一个逗号之前追加。
- 在 `UNIQUE KEY`、`DISTRIBUTED BY`、`PROPERTIES` 之前追加。

### 6.3 配置透传

Flink CDC Doris pipeline connector 新增参数：

```java
public static final ConfigOption<String> TABLE_CREATE_EXTRA_SCHEMA =
        ConfigOptions.key("table.create.extra-schema")
                .stringType()
                .noDefaultValue()
                .withDescription(...);
```

在 `DorisDataSinkFactory.validateExcept(...)` 中继续放行 `table.create.*` 类型配置或显式加入该 option。

在 `DorisMetadataApplier.buildTableSchema(...)` 中读取：

```java
config.getOptional(TABLE_CREATE_EXTRA_SCHEMA)
        .ifPresent(extraSchema -> tableSchema.setExtraTableSchemaElements(...));
```

实际解析和过滤逻辑建议放在 Doris connector 侧，Flink CDC 侧只透传 raw string 或调用 Doris connector 提供的工具方法。

## 七、参数规则

### 7.1 参数格式

`table.create.extra-schema` 是一段 Doris `CREATE TABLE (...)` 内部 schema 片段。

用户可以写单行：

```yaml
table.create.extra-schema: "`_db_` STRING NOT NULL, `_tb_` STRING NOT NULL"
```

也可以写多行：

```yaml
table.create.extra-schema: |
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

允许用户在每个片段末尾带逗号或不带逗号，但内部实现应统一处理，避免生成重复逗号。

### 7.2 生效时机

只在新建表时生效：

```text
Doris 表不存在:
  CreateTableEvent -> createTable -> 拼接 table.create.extra-schema

Doris 表已存在:
  CreateTableEvent -> reconcileExistingTable -> 忽略 table.create.extra-schema
```

这里的 `忽略` 是显式设计，不是待实现能力。

### 7.3 上游 schema 优先

`table.create.extra-schema` 是补充，不是覆盖。

如果上游 schema event 中已经包含某个列，则 extra schema 中同名列定义必须被过滤掉。

例如上游已经包含：

```sql
`confluent__last_updated` DATETIME(0)
```

配置：

```yaml
table.create.extra-schema: |
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

最终追加内容应该是：

```sql
`_db_` STRING NOT NULL,
`_tb_` STRING NOT NULL,
`_op_` STRING NOT NULL,
INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

也就是说：

- extra schema 中的 `` `confluent__last_updated` ... `` 列定义被丢弃。
- index 仍然保留，因为它不是列定义，而是对 Doris 表结构的补充。

### 7.4 列名比较规则

同名列判断应使用规范化列名：

1. 去掉首尾空白。
2. 去掉首尾反引号。
3. 大小写不敏感。

以下都视为同一个列：

```text
confluent__last_updated
`confluent__last_updated`
CONFLUENT__LAST_UPDATED
`CONFLUENT__LAST_UPDATED`
```

### 7.5 哪些片段参与同名列过滤

extra schema 片段默认按列定义处理，并参与同名列过滤。只有能明确识别为 table element
语法的片段才视为非列定义并原样保留，例如：

```text
INDEX name (...)
KEY name (...)
UNIQUE KEY name (...)
UNIQUE (...)
PRIMARY KEY (...)
FOREIGN KEY (...)
CHECK (...)
CONSTRAINT name CHECK (...)
CONSTRAINT name UNIQUE KEY (...)
```

不能仅根据第一个 token 判断片段类型。例如 `check INT NOT NULL`、`key STRING`、
`index VARCHAR(20)` 这类无反引号关键字列名仍应按列定义处理，如果上游 schema 已有
同名列，则必须过滤 extra schema 中的该列定义。

示例：

```sql
INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

这是 index 定义，不按 `confluent__last_updated` 列名过滤。

### 7.6 解析边界：不代替 Doris 做完整 DDL 校验

`table.create.extra-schema` 是 Doris 原生 schema 片段。connector 可以做有明确正收益的轻量解析，
但不应代替 Doris FE 判断整段 DDL 是否语法合法。

推荐策略：

- connector 做轻量词法切分，用于把 extra schema 分成顶层 table element。
- connector 识别列定义的首个 identifier，用于同名列过滤。
- connector 识别明确的 `INDEX`、`KEY`、`UNIQUE`、`PRIMARY KEY`、`FOREIGN KEY`、
  `CHECK`、`CONSTRAINT` 等 table element，用于避免这些片段被当成列定义过滤。
- connector 对明显误用 fail-fast，例如完整 `CREATE/ALTER/DROP TABLE` DDL、顶层分号、
  未闭合引号、反引号、括号或尖括号。
- connector 不校验 Doris 类型、默认值、函数、索引类型、索引引用字段是否存在。
- 最终语法合法性由 Doris FE 在执行 `CREATE TABLE` 时校验。

如果配置片段本身不是合法 Doris SQL，建表失败并返回 Doris 原始错误。

## 八、extra schema 解析设计

### 8.1 为什么不能简单 split(",")

extra schema 中可能出现括号和字符串：

```sql
`price` DECIMAL(10,2) NOT NULL
`ts` DATETIME(0) DEFAULT CURRENT_TIMESTAMP(0)
COMMENT "a,b,c"
```

简单 `split(",")` 会错误切开：

- `DECIMAL(10,2)`
- 字符串注释里的逗号
- 函数参数里的逗号

因此需要按 SQL 顶层逗号切分。

### 8.2 顶层逗号切分规则

切分时维护以下状态：

- 当前括号深度 `parenDepth`
- 是否在单引号字符串中
- 是否在双引号字符串中
- 是否在反引号 identifier 中
- 是否遇到转义字符
- 顶层尖括号深度 `angleDepth`，用于支持 `MAP<STRING,INT>`、`CUSTOM_TYPE<A,B>` 这类
  片段中的逗号，不维护 Doris 类型白名单

只有满足以下条件时，逗号才是分隔符：

```text
字符是 ','
并且 parenDepth == 0
并且 angleDepth == 0
并且不在单引号字符串中
并且不在双引号字符串中
并且不在反引号 identifier 中
```

### 8.3 片段规范化

每个切分后的片段需要：

1. trim。
2. 去掉末尾多余逗号。
3. 空片段丢弃。
4. 保留原始大小写和原始 Doris SQL 内容。

不要重写用户片段中的类型、默认值、函数、注释和索引语法。

### 8.4 第一个 token 提取

用于判断列定义或关键字。

示例：

```text
`confluent__last_updated` DATETIME(0) ... -> confluent__last_updated
INDEX idx_name (`col`) USING INVERTED       -> INDEX
PRIMARY KEY (`id`)                          -> PRIMARY
```

提取规则：

- 跳过前导空白。
- 如果第一个字符是反引号，读取到下一个反引号。
- 否则读取到第一个空白或 `(`。
- 返回 token 原文和规范化后的 token。

## 九、生成 DDL 示例

### 9.1 上游不包含额外列

上游：

```sql
CREATE TABLE `ods_mysql_test`.`sequence` (
  `uid` BIGINT NOT NULL,
  `name` VARCHAR(384)
)
```

配置：

```yaml
table.create.extra-schema: |
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

最终 Doris：

```sql
CREATE TABLE IF NOT EXISTS `ods_mysql_test`.`sequence`(
  `uid` BIGINT COMMENT '',
  `name` VARCHAR(384) COMMENT '',
  `confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `_db_` STRING NOT NULL,
  `_tb_` STRING NOT NULL,
  `_op_` STRING NOT NULL,
  INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
) UNIQUE KEY(`uid`)
DISTRIBUTED BY HASH(`uid`) BUCKETS 4
```

实际字段类型、comment、key 语法以当前 Doris connector 生成逻辑为准，extra schema 片段保持用户原文。

### 9.2 上游已经包含同名列

上游 schema event 包含：

```sql
`confluent__last_updated` DATETIME(0)
```

extra schema 也包含：

```sql
`confluent__last_updated` DATETIME(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)
```

最终结果：

- 上游的 `confluent__last_updated` 保留。
- extra schema 中的同名列定义丢弃。
- extra schema 中的 index 保留。

最终追加：

```sql
`_db_` STRING NOT NULL,
`_tb_` STRING NOT NULL,
`_op_` STRING NOT NULL,
INDEX idx_confluent_last_updated (`confluent__last_updated`) USING INVERTED
```

### 9.3 Doris 表已存在

如果 Doris 表已经存在：

```text
applyCreateTableEvent
  -> fetchExistingDorisSchema != null
  -> reconcileExistingTable(...)
```

此时 `table.create.extra-schema` 不参与处理。

不会执行：

- `ALTER TABLE ADD COLUMN`
- `ALTER TABLE ADD INDEX`
- 字段类型覆盖
- 默认值覆盖

## 十、执行方案

### 10.1 Doris connector 改动

仓库：

```text
/Users/benjobs/Github/doris-flink-connector
```

建议改动：

1. `TableSchema`
   - 新增 `extraTableSchemaElements` 字段。
   - 增加 getter/setter。
   - 更新 `toString()`。

2. `DorisSchemaFactory`
   - 在 `generateCreateTableDDL(...)` 中追加 extra schema elements。
   - 增加轻量工具方法：
     - `parseExtraSchemaElements(String extraSchema)`
     - `filterExtraSchemaElements(String extraSchema, Set<String> upstreamColumns)`
     - `splitTopLevelElements(String extraSchema)`
     - `isColumnDefinitionElement(String element)`
     - `extractFirstToken(String element)`
     - `normalizeIdentifier(String identifier)`

3. `DorisTableConfig`
   - 新增常量：
     ```java
     public static final String TABLE_CREATE_EXTRA_SCHEMA = "table.create.extra-schema";
     ```
   - 从 `table-conf` 中解析并移除该配置。
   - 暴露 getter。
   - 在 `DorisSchemaFactory.createTableSchema(...)` 中设置到 `TableSchema`。

4. `SQLParserSchemaManager`
   - 如果 Debezium create table 解析路径也会构造 `TableSchema`，确保 `DorisTableConfig` 中的 extra schema 同样进入 `TableSchema`。

### 10.2 Flink CDC 改动

仓库：

```text
/Users/benjobs/Github/apache-flink-cdc
```

建议改动：

1. `DorisDataSinkOptions`
   - 新增：
     ```java
     TABLE_CREATE_EXTRA_SCHEMA = ConfigOptions.key("table.create.extra-schema")
     ```

2. `DorisDataSinkFactory`
   - 在 optional options 或 validateExcept 中接入新参数。
   - 保持 `table.create.properties.*` 和 auto partition 参数现有行为不变。

3. `DorisMetadataApplier`
   - 在 `buildTableSchema(...)` 中读取 `table.create.extra-schema`。
   - 使用上游 `schema.getColumnNames()` 作为过滤基准。
   - 将过滤后的 extra schema elements 设置到 `TableSchema`。
   - `reconcileExistingTable(...)` 不读取、不使用该参数。

## 十一、测试计划

### 11.1 Doris connector 单元测试

建议新增或扩展：

```text
flink-doris-connector/src/test/java/org/apache/doris/flink/catalog/doris/DorisSchemaFactoryTest.java
```

覆盖：

1. extra schema 正常拼接到 `CREATE TABLE (...)` 内部。
2. extra schema 支持多行输入。
3. extra schema 支持最后一个片段不带逗号。
4. extra schema 支持所有片段都带逗号。
5. `CURRENT_TIMESTAMP(0)` 不被错误切分。
6. `DECIMAL(10,2)` 不被错误切分。
7. 字符串中的逗号不被错误切分。
8. extra schema 中同名列被过滤。
9. 同名列过滤大小写不敏感。
10. 同名列过滤支持反引号。
11. `INDEX ...` 不因引用同名列而被过滤。
12. `PRIMARY KEY`、`UNIQUE KEY`、`CONSTRAINT` 等非列定义不参与列名去重。

### 11.2 Flink CDC Doris connector 单元测试

建议扩展：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris/
  src/test/java/org/apache/flink/cdc/connectors/doris/sink/DorisMetadataApplierTest.java
```

覆盖：

1. `CreateTableEvent` 新建表路径会把 `table.create.extra-schema` 设置到 `TableSchema`。
2. 上游 schema 已包含同名列时，extra schema 同名列不会进入 `TableSchema`。
3. index 片段会保留。
4. Doris 表已存在时，`reconcileExistingTable(...)` 不使用 extra schema。

### 11.3 集成测试建议

如果环境允许，补充 Doris IT：

1. MySQL 表只有 `uid/name`。
2. Doris sink 配置 `table.create.extra-schema`。
3. 启动作业并自动建表。
4. 使用 `SHOW CREATE TABLE` 或 information schema 验证：
   - `confluent__last_updated` 存在。
   - `_db_/_tb_/_op_` 存在。
   - inverted index 存在。
5. 上游表本身包含 `confluent__last_updated` 时，验证 Doris 表中没有重复列。

## 十二、兼容性与风险

### 12.1 兼容性

默认不配置 `table.create.extra-schema` 时，行为完全不变。

配置后只影响 Doris 自动创建新表：

- 不影响 MySQL source。
- 不影响 CDC schema event。
- 不影响 transform。
- 不影响 runtime schema operator。
- 不影响 DataChangeEvent。
- 不影响 Doris serializer 输出字段集合。
- 不影响已有表 reconcile 行为。

### 12.2 主要风险

1. extra schema 片段语法错误
   - 明显无法安全切分或明显误用的配置由 connector 提前报错。
   - 其他 Doris 语义级错误由 Doris FE 在建表时返回。
   - connector 不做 Doris 类型、默认值、函数、索引等完整 DDL 校验。

2. extra schema 中包含无默认值的 `NOT NULL` 字段
   - 建表可以成功。
   - 后续写入可能失败，取决于 Stream Load payload 是否提供字段值。
   - 这是用户配置语义问题，不由本参数自动解决。

3. CSV 写入模式
   - 当前 Doris pipeline sink 默认是 JSON。
   - 如果用户显式使用 CSV 且未配置 `sink.properties.columns`，Doris 物理表多出 CDC schema 没有的列时，CSV 输出列解析可能要求物理列都存在于 CDC schema。
   - 推荐在文档中说明：`table.create.extra-schema` 更适合 JSON Stream Load；CSV 模式需要用户显式配置 columns 或后续单独增强。

4. index 片段依赖 Doris 版本
   - 例如 inverted index 语法与 Doris 版本相关。
   - connector 只透传，最终由 Doris FE 校验。

### 12.3 安全边界

`table.create.extra-schema` 是 raw SQL 片段，会进入 Doris `CREATE TABLE`。

需要明确：

- 只允许配置在受信任的 pipeline job 配置中。
- 不从数据事件、上游表名、字段值中拼接该参数。
- 不在日志中过度改写或隐藏该参数，因为建表失败时需要排查完整 SQL。

## 十三、文档与用户说明建议

用户文档中建议写成：

```text
table.create.extra-schema

Optional. A Doris CREATE TABLE schema fragment appended to the generated column/index definition list.
It is applied only when the Doris sink automatically creates a new table.
If an extra column has the same name as a column from the upstream CDC schema, the upstream column definition wins and the extra column definition is ignored.
Indexes and constraints in extra-schema are kept as-is.
```

中文说明：

```text
table.create.extra-schema 用于在 Doris sink 自动建表时，向生成的 CREATE TABLE 字段/索引定义列表末尾追加 Doris 原生 schema 片段。
该参数仅在新建表时生效。
如果 extra-schema 中的列名与上游 schema event 中的列名相同，则以上游 schema 为准，忽略 extra-schema 中的同名列定义。
INDEX/KEY/CONSTRAINT 等非列定义片段会原样保留。
```

## 十四、推荐实施顺序

1. 先在 Doris connector 实现 `TableSchema` 扩展和 DDL 生成能力。
2. 补齐 Doris connector 单元测试，确保拼接和同名列过滤稳定。
3. 在 Flink CDC Doris pipeline connector 增加参数和透传。
4. 补齐 Flink CDC 侧单元测试。
5. 视环境补 Doris IT 或 E2E。
6. 更新用户文档。

## 十五、最终结论

本需求应实现为 Doris 自动建表能力扩展，而不是 Flink CDC 通用 schema 能力扩展。

推荐参数为：

```text
table.create.extra-schema
```

最终语义：

1. 在自动生成的 Doris `CREATE TABLE (...)` 中追加用户配置的 Doris schema 片段。
2. 只在目标表不存在、自动建表时生效。
3. 上游 schema event 字段优先，extra schema 中同名列定义自动过滤。
4. index/constraint 等非列定义片段原样保留。
5. 不影响已有表 reconcile。
6. 不影响数据写入字段集合。
7. 不负责填充额外字段值。

这个设计能满足当前建表补充字段和 inverted index 的需求，同时把行为边界控制在 Doris sink 自动建表阶段，避免污染 Flink CDC 的通用 schema/event 语义。
