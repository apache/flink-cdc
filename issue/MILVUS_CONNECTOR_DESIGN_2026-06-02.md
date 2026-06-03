# Flink CDC Pipeline Milvus Connector 设计文档

## 1. 文档目的

本文用于沉淀 Flink CDC Pipeline 支持写入 Milvus 向量数据库的设计方案与当前实现状态。

本文覆盖：

- Milvus 写入规范与实现注意事项
- Flink CDC Pipeline sink 接入方式
- Milvus connector 的配置项设计
- Flink CDC 类型到 Milvus 类型的映射策略
- connector 类结构与核心职责
- schema evolution 策略
- 测试用例清单
- 后续实现顺序
- 当前实现与验证结果

## 2. 背景与目标

AI 应用爆发后，实时数据同步框架需要支持把业务数据、特征数据和向量数据同步到向量数据库。Flink CDC Pipeline 作为实时数据同步框架，需要新增 Milvus sink connector，使用户可以将上游数据库中的实体数据和已有向量字段实时同步到 Milvus collection。

首版 connector 的定位是：

- 同步源表中已经存在的向量字段
- 不负责调用 embedding 模型生成向量
- 以主键幂等 upsert 为核心写入语义
- 支持基础标量字段和 FloatVector 向量字段
- 支持自动创建 Milvus collection 和基础向量索引

## 3. 非目标

首版不覆盖以下能力：

- 自动生成 embedding
- 无主键表的 update/delete 语义
- exactly-once 写入语义
- 修改 Milvus 向量字段维度
- drop/rename/alter column 的自动 schema evolution
- 多向量字段的复杂索引调优
- Milvus 原生 partition key schema 路由
- 全量支持所有 Milvus vector data type

这些能力可以在首版稳定后按需求增量实现。

## 4. Milvus 使用规范与注意事项

### 4.1 主键规范

Milvus collection 必须有主键字段。用于 CDC 同步时，collection 应设置 `autoID=false`，并使用源表主键作为 Milvus 主键。

原因：

- `UPDATE` 需要按同一主键覆盖旧实体
- `DELETE` 需要按主键删除实体
- 如果启用 Milvus 自动 ID，则 CDC delete/update 无法稳定定位实体

首版建议只支持单字段主键：

- `BIGINT` -> Milvus `Int64`
- `VARCHAR/CHAR` -> Milvus `VarChar`

复合主键首版建议拒绝。后续可以考虑提供 synthetic primary key 策略，例如将多个主键字段拼接成一个 VarChar，但这会引入编码、转义和兼容问题，不适合作为首版默认行为。

### 4.2 写入语义

Milvus connector 不能默认使用普通 `insert` 处理 CDC 数据。普通 insert 在 CDC update/replay 场景下容易产生重复实体。

推荐事件映射：

| Flink CDC 事件 | Milvus 操作 | 数据来源 | 说明 |
| --- | --- | --- | --- |
| `INSERT` | `upsert` | `after` | 插入或覆盖同主键实体 |
| `UPDATE` | `upsert` | `after` | 用新值覆盖旧实体 |
| `REPLACE` | `upsert` | `after` | 与 update 一致 |
| `DELETE` | `delete(ids)` | `before` | 按主键删除实体 |
| `CreateTableEvent` | create collection / cache schema | schema | 创建或校验 collection |
| `AddColumnEvent` | add field / dynamic field | schema | 仅支持安全新增字段 |
| `Drop/Rename/Alter` | reject by default | schema | 避免破坏 Milvus schema |

写入语义承诺：

- 基于主键幂等的 at-least-once
- 不承诺 exactly-once
- checkpoint 时可以 flush writer buffer
- 不建议每次 checkpoint 强制调用 Milvus server-side flush

### 4.3 向量字段规范

Milvus 向量字段需要固定类型和固定维度。首版建议只支持：

```text
FloatVector(dim)
```

约束：

- 向量字段必须通过配置显式声明
- 写入时向量长度必须等于配置维度
- 不自动截断
- 不自动补零
- 不自动归一化
- null 向量字段的处理需要与 Milvus schema nullable 能力对齐；首版建议默认拒绝必填向量字段为 null

向量字段输入可以支持两种形式：

- Flink CDC `ARRAY<FLOAT>` / `ARRAY<DOUBLE>`
- JSON 数组字符串，例如 `[0.1,0.2,0.3]`

### 4.4 Collection 命名规范

Milvus collection、field、partition、index 名称存在字符限制。`TableId` 的字符串形式通常可能包含点号，例如 `db.schema.table`，不能直接无脑作为 Milvus collection 名。

设计上需要提供两种策略：

1. 用户显式配置 `collection.mapping`
2. 默认规范化表名，例如把非法字符替换为 `_`

默认建议：

- 开启名称规范化
- 保留原始 `TableId -> collection` 映射缓存
- 对冲突规范化结果做启动期校验，避免两张表落到同一个 collection

### 4.5 Schema evolution 规范

Milvus schema evolution 能力有限。首版应采取保守策略：

| Schema 事件 | 首版行为 |
| --- | --- |
| `CreateTableEvent` | 创建 collection 或校验已有 collection |
| `AddColumnEvent` | 仅支持新增标量字段；dynamic field 开启时也可进入动态字段 |
| `DropColumnEvent` | 拒绝 |
| `RenameColumnEvent` | 拒绝 |
| `AlterColumnTypeEvent` | 拒绝 |
| `TruncateTableEvent` | 拒绝 |
| `DropTableEvent` | 拒绝 |

拒绝危险 schema evolution 的原因：

- 删除字段不等价于删除 Milvus 已有 field
- 重命名字段需要迁移已有实体数据
- 修改类型和修改向量维度会破坏已有 collection schema
- truncate/drop table 如果直接映射到 Milvus 可能造成不可恢复的数据删除

## 5. 当前仓库接入点评估

Flink CDC Pipeline sink 的核心 SPI 是 `DataSink`：

- `getEventSinkProvider()` 用于提供实际数据写入 sink
- `getMetadataApplier()` 用于处理 schema change event
- `getDataChangeEventHashFunctionProvider()` 可用于数据按主键分区

Milvus connector 应按现有 pipeline connector 风格新增独立模块：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus
```

并在父模块中增加：

```xml
<module>flink-cdc-pipeline-connector-milvus</module>
```

Factory identifier：

```text
milvus
```

设计上不应依赖 pgvector 模块作为稳定基线。pgvector 的配置思路可以参考，例如向量字段配置前缀、flush 参数、delete 开关等，但 Milvus connector 需要独立完成 writer、metadata applier、type mapping 和 serializer。

## 6. 模块结构设计

建议新增包结构：

```text
org.apache.flink.cdc.connectors.milvus.factory
  MilvusDataSinkFactory

org.apache.flink.cdc.connectors.milvus.sink
  MilvusDataSink
  MilvusDataSinkOptions
  MilvusDataSinkConfig
  MilvusEventSink
  MilvusEventWriter
  MilvusMetadataApplier
  MilvusWriteBuffer
  MilvusOperation

org.apache.flink.cdc.connectors.milvus.serde
  MilvusEventSerializer
  MilvusRowConverter
  MilvusVectorConverter
  MilvusPrimaryKeyExtractor

org.apache.flink.cdc.connectors.milvus.utils
  MilvusSchemaUtils
  MilvusTypeUtils
  MilvusCollectionUtils
  MilvusNameUtils
  MilvusRetryUtils
```

核心类职责：

| 类 | 职责 |
| --- | --- |
| `MilvusDataSinkFactory` | 解析配置、校验参数、创建 `MilvusDataSink` |
| `MilvusDataSinkOptions` | 声明 connector 配置项 |
| `MilvusDataSinkConfig` | 保存可序列化运行时配置 |
| `MilvusDataSink` | 实现 Flink CDC `DataSink` SPI |
| `MilvusEventSink` | 实现 Flink `Sink<Event>` |
| `MilvusEventWriter` | 管理 Milvus client、buffer、flush、retry、close |
| `MilvusMetadataApplier` | 处理 create collection、add field、schema reconcile |
| `MilvusWriteBuffer` | 按 collection 聚合 upsert rows 和 delete ids |
| `MilvusEventSerializer` | 将 CDC event 转为 Milvus 写入操作 |
| `MilvusRowConverter` | 将 `RecordData` 转为 Milvus row `JsonObject` |
| `MilvusVectorConverter` | 解析向量字段并校验维度 |
| `MilvusPrimaryKeyExtractor` | 从 before/after row 提取 Milvus 主键 |
| `MilvusTypeUtils` | Flink CDC type 到 Milvus type 的映射 |
| `MilvusNameUtils` | collection/field/index 名称规范化 |

## 7. 配置项设计

### 7.1 连接配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `uri` | string | none | 是 | Milvus endpoint，例如 `http://localhost:19530` |
| `token` | string | `""` | 否 | Milvus token |
| `database-name` | string | `default` | 否 | 目标 Milvus database |

### 7.2 Collection 配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `collection.mapping` | string | `""` | 否 | 表到 collection 映射，如 `db.tbl:c1;db.tbl2:c2` |
| `collection.name-normalize.enabled` | boolean | `true` | 否 | 是否规范化非法 collection 名称 |
| `create-collection.enabled` | boolean | `true` | 否 | collection 不存在时是否自动创建 |
| `enable-dynamic-field` | boolean | `false` | 否 | 是否启用 Milvus dynamic field |
| `load-collection.enabled` | boolean | `false` | 否 | schema/index setup 后是否同步 load collection |
| `consistency-level` | string | Milvus default | 否 | collection consistency level |
| `partition.names` | string | `""` | 否 | metadata 阶段预创建的 Milvus 命名 partition 列表，如 `manual,auto` |
| `partition.field` | string | `""` | 否 | 从源字段值提取 Milvus partition name，空值写入默认 partition |
| `partition.auto-create.enabled` | boolean | `false` | 否 | `partition.field` 路由时是否在写入阶段创建缺失 partition |

### 7.3 向量字段配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `vector.fields` | string | none | 是 | 全局向量字段定义，如 `embedding:FloatVector(1536)` |
| `vector.fields.<table>` | string | none | 否 | 表级向量字段定义，优先级高于全局配置 |

配置示例：

```yaml
sink:
  type: milvus
  uri: http://localhost:19530
  database-name: default
  vector.fields: embedding:FloatVector(1536)
```

多字段示例：

```yaml
sink:
  type: milvus
  uri: http://localhost:19530
  vector.fields: title_embedding:FloatVector(768),content_embedding:FloatVector(1536)
```

表级覆盖示例：

```yaml
sink:
  type: milvus
  uri: http://localhost:19530
  vector.fields: embedding:FloatVector(1536)
  vector.fields.app_db.article: article_vector:FloatVector(768)
```

### 7.4 主键配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `primary-key.field` | string | `""` | 否 | 显式指定 Milvus 主键字段；默认使用源表单主键 |
| `sink.allow-no-primary-key` | boolean | `false` | 否 | 是否允许无主键 append-only；首版建议保持 false |

### 7.5 Index 配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `create-index.enabled` | boolean | `true` | 否 | 创建 collection 时是否创建向量索引 |
| `index.type` | string | `AUTOINDEX` | 否 | 向量索引类型 |
| `index.metric-type` | string | `COSINE` | 否 | 向量距离度量，例如 `COSINE`、`L2`、`IP` |
| `index.params.*` | string | empty | 否 | 透传 index 参数 |

### 7.6 写入配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `sink.flush.max-rows` | int | `500` | 否 | 批量写入最大行数 |
| `sink.flush.interval` | duration | `1s` | 否 | 定时 flush 间隔 |
| `sink.max-retries` | int | `3` | 否 | 写入失败最大重试次数 |
| `sink.retry.backoff` | duration | `200ms` | 否 | 写入失败重试间隔 |
| `sink.adaptive-batch-split.enabled` | boolean | `true` | 否 | 遇到限流或消息过大错误时是否拆分批次重试 |
| `sink.adaptive-batch-split.min-rows` | int | `10` | 否 | 自适应拆分最小批大小 |
| `sink.delete.enabled` | boolean | `true` | 否 | 是否应用 delete event |

### 7.7 类型降级配置

| 配置项 | 类型 | 默认值 | 必填 | 说明 |
| --- | --- | --- | --- | --- |
| `varchar.max-length.default` | int | `65535` | 否 | Milvus VarChar 默认最大长度 |
| `unsupported-type.handling` | string | `fail` | 否 | 可选 `fail` / `string` / `dynamic` |

首版建议默认 `fail`，避免复杂类型被静默写成不可预期格式。

## 8. 类型映射设计

### 8.1 标量字段映射

| Flink CDC 类型 | Milvus 类型 | 首版策略 |
| --- | --- | --- |
| `BOOLEAN` | `Bool` | 支持 |
| `TINYINT` | `Int32` | 支持 |
| `SMALLINT` | `Int32` | 支持 |
| `INTEGER` | `Int32` | 支持 |
| `BIGINT` | `Int64` | 支持 |
| `FLOAT` | `Float` | 支持 |
| `DOUBLE` | `Double` | 支持 |
| `CHAR` | `VarChar` | 支持 |
| `VARCHAR` | `VarChar` | 支持 |
| `DECIMAL` | `VarChar` | 首版转字符串或按配置拒绝 |
| `DATE` | `VarChar` | ISO 字符串 |
| `TIME` | `VarChar` | 字符串 |
| `TIMESTAMP` | `VarChar` | 字符串 |
| `TIMESTAMP_WITH_TIME_ZONE` | `VarChar` | 字符串 |
| `TIMESTAMP_WITH_LOCAL_TIME_ZONE` | `VarChar` | 按 pipeline timezone 转字符串 |
| `BINARY` | unsupported | 默认拒绝 |
| `VARBINARY` | unsupported | 默认拒绝 |
| `MAP` | dynamic/string | 取决于配置 |
| `ROW` | dynamic/string | 取决于配置 |
| `VARIANT` | dynamic/string | 取决于配置 |

### 8.2 向量字段映射

| 源字段形态 | Milvus 类型 | 首版策略 |
| --- | --- | --- |
| `ARRAY<FLOAT>` | `FloatVector` | 支持 |
| `ARRAY<DOUBLE>` | `FloatVector` | 转 float，需检查精度风险 |
| JSON 数组字符串 | `FloatVector` | 支持解析 |
| `BINARY` | `BinaryVector` | 首版不支持 |
| sparse vector JSON | `SparseFloatVector` | 首版不支持 |

向量转换规则：

- 维度必须等于配置维度
- 元素必须是数字
- JSON 字符串必须是数组
- 对 `DOUBLE` 转 `FLOAT` 应有明确转换逻辑
- 非法向量值抛出异常并让任务失败

## 9. Collection Schema 构造

### 9.1 主键字段

源表单主键映射为 Milvus primary field。

规则：

- `BIGINT` -> `Int64 primary key`
- `CHAR/VARCHAR` -> `VarChar primary key`
- 不允许 nullable primary key
- 不允许复合主键
- 不允许无主键，除非显式开启 append-only 模式

### 9.2 向量字段

每个配置的向量字段构造为 Milvus vector field。

首版至少要求一个向量字段。原因是 Milvus 作为向量数据库，collection 没有向量字段不符合该 connector 的主要用途。

### 9.3 标量字段

源表中非主键、非向量字段映射为 Milvus scalar field。

如果字段类型不支持：

- `unsupported-type.handling=fail`：抛异常
- `unsupported-type.handling=string`：转 VarChar
- `unsupported-type.handling=dynamic`：写入 dynamic field

首版默认 `fail`。

## 10. 写入流程设计

### 10.1 Writer 初始化

`MilvusEventWriter` open 阶段：

1. 根据配置创建 `MilvusClientV2`
2. 初始化 collection name resolver
3. 初始化 schema cache
4. 初始化 write buffer
5. 初始化 retry 策略

### 10.2 Schema event 处理

writer 侧也需要缓存 schema，用于将 `RecordData` 转为 Milvus row。

`CreateTableEvent`：

- 缓存 schema
- 准备 row converter
- metadata applier 会负责创建或校验 collection

其他 schema change event：

- 使用 `SchemaUtils.applySchemaChangeEvent` 更新本地 schema cache
- 对 unsupported schema evolution 抛出明确异常

### 10.3 Data event 处理

`INSERT/UPDATE/REPLACE`：

1. 找到 table schema
2. 将 `after` 转换为 Milvus row
3. 校验主键和向量字段
4. 放入 collection 对应 upsert buffer

`DELETE`：

1. 如果 `sink.delete.enabled=false`，忽略 delete
2. 从 `before` 提取主键
3. 放入 collection 对应 delete buffer

### 10.4 Flush 触发

flush 触发条件：

- buffer rows 达到 `sink.flush.max-rows`
- 距离上次 flush 超过 `sink.flush.interval`
- checkpoint snapshot
- writer close

flush 顺序建议：

1. 先执行 upsert buffer
2. 再执行 delete buffer

但同一主键如果在同一 buffer 周期内同时出现 upsert 和 delete，需要按事件到达顺序保证最终语义。更稳妥的实现是 buffer 中保留 operation sequence，flush 时按 collection 分组但不打乱同 collection 内的操作顺序。

首版可以采用更简单且安全的策略：

- 每个 collection 维护有序 operation list
- flush 时按顺序切分批次
- 连续 upsert 合并为一次批量 upsert
- 连续 delete 合并为一次批量 delete

## 11. Retry 策略

写入失败时按 `sink.max-retries` 重试。

建议：

- 对临时网络异常重试
- 对 schema mismatch、向量维度错误、主键缺失等数据错误不重试
- 重试间隔可以使用固定 sleep 或指数退避
- 超过最大重试次数后抛异常，让 Flink failover

错误分类可以在首版先做基础区分：

| 错误类型 | 是否重试 |
| --- | --- |
| 网络超时 | 是 |
| Milvus transient status | 是 |
| collection 不存在且禁止自动创建 | 否 |
| field 不存在 | 否 |
| vector dimension mismatch | 否 |
| primary key missing | 否 |
| unsupported type | 否 |

## 12. MetadataApplier 设计

### 12.1 CreateTableEvent

处理步骤：

1. 解析目标 collection 名
2. 检查 collection 是否存在
3. 如果不存在且 `create-collection.enabled=true`，创建 collection
4. 如果不存在且禁止自动创建，抛异常
5. 如果已存在，校验主键字段、向量字段、标量字段类型
6. 如果 `create-index.enabled=true`，创建向量索引
7. 缓存 schema

### 12.2 AddColumnEvent

处理策略：

- 新增标量字段：允许
- 新增向量字段：首版建议拒绝
- dynamic field 开启：可以不改 collection schema，后续数据进入 dynamic field
- 字段非 nullable 且无默认值：拒绝

### 12.3 危险 Schema Event

以下事件默认拒绝：

- `DropColumnEvent`
- `RenameColumnEvent`
- `AlterColumnTypeEvent`
- `TruncateTableEvent`
- `DropTableEvent`

拒绝时应给出明确错误信息，提示用户 Milvus connector 首版不支持该 schema evolution。

## 13. DataChangeEvent 分区策略

Milvus upsert/delete 都以主键为核心。为了降低同一主键乱序风险，建议实现自定义 `HashFunctionProvider<DataChangeEvent>`：

- 有主键时按主键 hash
- 无主键且 append-only 模式时按 table hash 或默认策略

这样可以尽量保证同一主键事件进入同一 sink subtask。

需要注意：

- Flink CDC pipeline 上游是否已经按主键分区取决于 runtime 规划
- connector 只能提供 hash function provider，最终执行策略由 pipeline runtime 使用
- 主键变更 update 会跨旧 key 和新 key，严格顺序语义比普通同主键 update 更复杂；当前 writer 内会将主键变更转换为 delete old key + upsert new key，并保持单 writer 内操作顺序。

## 13A. SeaTunnel Milvus Connector 借鉴评估

参考路径：

```text
/Users/benjobs/Github/seatunnel/seatunnel-connectors-v2/connector-milvus
```

SeaTunnel Milvus connector 覆盖 source、sink、catalog 三部分。与 Flink CDC 当前 Milvus sink 最相关的是 `MilvusSinkOptions`、`MilvusBufferBatchWriter`、`MilvusSinkConverter`、`MilvusCatalog` 和对应单元测试。

| SeaTunnel 能力 | 是否建议借鉴 | 评估 |
| --- | --- | --- |
| `url/token/database/collection` 基础配置 | 已部分借鉴 | Flink CDC 已采用 `uri/token/database-name/collection.mapping`；CDC 多表场景需要 mapping，不宜只用单 collection 覆盖。 |
| `collection_name` fallback alias | 可借鉴 | 对迁移友好，可考虑为单表场景增加 `collection` 或 `collection-name` 兼容别名。 |
| `enable_upsert` | 不建议照搬 | CDC 必须以 upsert 为核心语义，普通 insert 会导致 replay/update 重复实体。 |
| `enable_auto_id` | 不建议照搬 | CDC delete/update 需要源主键定位，`autoID=true` 会破坏幂等语义；当前实现已拒绝 autoID。 |
| `rate_limit` | 可改造后借鉴 | SeaTunnel 会修改 collection insert/upsert rate。Flink CDC 可做显式配置，但不应默认修改全局 collection 属性。 |
| `load_collection` | 已借鉴 | 已增加 `load-collection.enabled`，在 collection schema/index setup 后按需同步 load collection，IT 已验证 load state。 |
| 写入失败按半批拆分 | 已借鉴 | 已实现 adaptive split batch，对 `rate limit exceeded`、`received message larger than max` 递归半批拆分，并受 `sink.adaptive-batch-split.min-rows` 约束。 |
| partition 写入和自动创建 partition | 已改造后借鉴 | 已支持 `partition.field` 显式命名 partition 路由、`partition.auto-create.enabled` 运行时创建、`partition.names` metadata 阶段预创建。 |
| 静态 partitionNames | 已改造后借鉴 | SeaTunnel catalog 中的静态 partition 创建已迁移为 `partition.names`，适合生产上预声明分区并关闭写入阶段 auto-create。 |
| collection description | 可借鉴 | 低风险增强，可支持按 collection 配置描述。 |
| schema/data save mode | 不建议照搬 | `DROP_DATA`、`ERROR_WHEN_DATA_EXISTS` 更适合批同步，CDC sink 不应在启动时 drop collection 或清数据。 |
| 更多向量类型 | 后续扩展借鉴 | SeaTunnel 支持 `BinaryVector`、`Float16Vector`、`BFloat16Vector`、`SparseFloatVector`；Flink CDC 首版只支持 `FloatVector` 是合理收敛。 |
| JSON/Array/dynamic field | 后续扩展借鉴 | 当前 Flink CDC 首版对复杂类型偏保守；后续可支持 JSON field、Array scalar 和 dynamic field merge。 |
| 异常 error code 分类 | 可借鉴 | 当前 Flink CDC 使用通用异常信息。后续可引入结构化错误码，提升生产排障质量。 |

建议后续优先吸收：

1. JSON/Array scalar：参考 SeaTunnel 的 JSON/Array 映射，但默认仍应 fail fast。
2. structured error code：引入 connector 内部错误码，提升日志和测试断言质量。
3. collection description：低风险增强，可按 collection 配置描述。
4. 原生 partition key：需要单独设计 schema 创建、已有 collection 校验和 delete 定位语义，不应与当前命名 partition 路由混用。

## 14. 测试用例清单

### 14.1 Factory 与配置解析测试

| 用例 | 期望 |
| --- | --- |
| 缺少 `uri` | 抛出配置校验异常 |
| 缺少 `vector.fields` | 抛出配置校验异常 |
| `sink.flush.max-rows <= 0` | 抛出配置校验异常 |
| `sink.max-retries < 0` | 抛出配置校验异常 |
| 非法 vector field 格式 | 抛出配置校验异常 |
| 表级 vector fields 覆盖全局配置 | 解析结果符合预期 |
| collection mapping 解析 | 表到 collection 映射正确 |
| index params 解析 | 前缀参数透传正确 |
| `partition.names` 解析 | 预创建 partition 名称列表正确 |
| `_default` partition 配置 | 拒绝保留 partition 名 |

### 14.2 类型映射测试

| 用例 | 期望 |
| --- | --- |
| boolean/int/bigint/float/double/string | 映射到对应 Milvus scalar type |
| decimal/date/time/timestamp | 按配置转 VarChar 或拒绝 |
| binary/varbinary | 默认拒绝 |
| map/row/variant | 按配置 fail/string/dynamic |
| nullable 字段 | Milvus field nullable 语义正确 |
| non-null 字段 | 写入 null 时失败 |

### 14.3 主键测试

| 用例 | 期望 |
| --- | --- |
| 单 BIGINT 主键 | Milvus Int64 primary key |
| 单 VARCHAR 主键 | Milvus VarChar primary key |
| 复合主键 | 默认拒绝 |
| 无主键 | 默认拒绝 |
| 无主键且 append-only 开启 | 仅允许 insert/upsert，不允许 delete/update |
| delete before row 主键为 null | 抛异常 |

### 14.4 向量转换测试

| 用例 | 期望 |
| --- | --- |
| `ARRAY<FLOAT>` 维度正确 | 转为 FloatVector |
| `ARRAY<DOUBLE>` 维度正确 | 转为 FloatVector |
| JSON 数组字符串维度正确 | 转为 FloatVector |
| 向量维度小于配置 | 抛异常 |
| 向量维度大于配置 | 抛异常 |
| JSON 字符串不是数组 | 抛异常 |
| 向量元素非数字 | 抛异常 |
| 必填向量字段为 null | 抛异常 |

### 14.5 Event serializer 测试

| 用例 | 期望 |
| --- | --- |
| `INSERT` | 生成 upsert operation |
| `UPDATE` | 使用 after row 生成 upsert operation |
| `REPLACE` | 使用 after row 生成 upsert operation |
| partition 字段写入 | operation 携带规范化后的 partition name |
| partition 字段为空 | operation 不带 partition name，写入默认 partition |
| partition 变化 update | 先 delete 旧 partition，再 upsert 新 partition |
| default 与命名 partition 互转 | 先从旧位置 delete，再向新位置 upsert |
| `DELETE` | 使用 before row 生成 delete operation |
| schema 不存在时 data event 到达 | 抛异常 |
| schema change 后 converter 更新 | 后续数据按新 schema 序列化 |

### 14.6 MetadataApplier 测试

| 用例 | 期望 |
| --- | --- |
| collection 不存在且允许创建 | 创建 collection |
| collection 不存在且禁止创建 | 抛异常 |
| 已有 collection schema 完全一致 | 通过 |
| 已有 collection 主键不一致 | 抛异常 |
| 已有 collection 向量维度不一致 | 抛异常 |
| add scalar column | 成功或进入 dynamic field |
| add vector column | 首版拒绝 |
| drop/rename/alter/truncate/drop table | 拒绝 |

### 14.7 Buffer 与 flush 测试

| 用例 | 期望 |
| --- | --- |
| 达到 max rows | 触发 flush |
| 达到 flush interval | 触发 flush |
| checkpoint snapshot | 触发 flush |
| close writer | flush remaining records |
| 同 collection 连续 upsert | 合并批量 upsert |
| 同 collection 连续 delete | 合并批量 delete |
| upsert/delete 交错 | 保持事件顺序 |

### 14.8 Retry 测试

| 用例 | 期望 |
| --- | --- |
| transient exception | 按 max retries 重试 |
| 重试后成功 | 不再抛异常 |
| 超过 max retries | 抛异常 |
| vector dimension mismatch | 不重试，直接失败 |
| primary key missing | 不重试，直接失败 |

### 14.9 集成测试

可选使用 Testcontainers Milvus 做 IT：

| 场景 | 期望 |
| --- | --- |
| insert 两条数据 | Milvus 查询到两条实体 |
| update 同一主键 | Milvus 中实体被覆盖 |
| delete 一条数据 | Milvus 中实体被删除 |
| job failover replay | 基于主键 upsert 后最终结果正确 |
| add scalar column | 新字段可写入或进入 dynamic field |

## 15. 示例 Pipeline

示例：

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: password
  tables: app_db.article

sink:
  type: milvus
  uri: http://localhost:19530
  database-name: default
  collection.mapping: app_db.article:article_vectors
  vector.fields: embedding:FloatVector(1536)
  index.type: AUTOINDEX
  index.metric-type: COSINE
  sink.flush.max-rows: 500
  sink.flush.interval: 1s

pipeline:
  parallelism: 2
```

源表示例：

```sql
CREATE TABLE article (
  id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  content TEXT,
  embedding JSON,
  updated_at TIMESTAMP
);
```

其中 `embedding` 需要是 JSON 数组字符串：

```json
[0.1, 0.2, 0.3]
```

实际维度必须等于 `FloatVector(1536)` 中配置的维度。

## 16. 实现顺序

建议按以下顺序实现：

1. 新增 `flink-cdc-pipeline-connector-milvus` 模块、POM、Factory SPI 文件。
2. 实现 `MilvusDataSinkOptions` 和 `MilvusDataSinkConfig`。
3. 实现配置解析、vector fields 解析、collection mapping 解析。
4. 实现 `MilvusNameUtils`，处理 collection/field/index 名称规范化。
5. 实现 `MilvusTypeUtils` 和基础类型映射测试。
6. 实现 `MilvusVectorConverter` 和向量维度校验测试。
7. 实现 `MilvusPrimaryKeyExtractor`。
8. 实现 `MilvusEventSerializer`。
9. 实现 `MilvusMetadataApplier` 的 create collection 和 schema reconcile。
10. 实现 `MilvusEventSink` / `MilvusEventWriter` / `MilvusWriteBuffer`。
11. 补齐 factory、serializer、metadata、buffer、retry 单元测试。
12. 视环境补充 Milvus Testcontainers IT。
13. 运行新模块编译和测试。
14. 补充 connector 使用文档和示例 pipeline。

## 17. 风险与决策

### 17.1 必须默认使用 upsert

CDC update/replay 场景下，普通 insert 容易产生重复实体。首版默认应使用 upsert。

### 17.2 无主键默认拒绝

无主键表无法可靠表达 update/delete。首版默认拒绝，避免用户静默写入错误数据。

### 17.3 向量维度必须 fail fast

向量维度错误属于数据质量或配置错误。connector 不应自动截断或补零。

### 17.4 Schema evolution 必须保守

Milvus schema 变更能力有限。首版只支持 create collection 和安全 add scalar field，其余 schema evolution 默认拒绝。

### 17.5 不承诺 exactly-once

Milvus connector 首版以主键幂等和 at-least-once 为目标。Flink checkpoint flush 只能降低重复窗口，不能等价于 exactly-once transaction。

## 18. 后续扩展方向

首版完成后，可以按优先级扩展：

1. 支持 `Float16Vector`、`BFloat16Vector`、`BinaryVector`。
2. 支持 sparse vector。
3. 支持复合主键 synthetic key。
4. 支持 Milvus 原生 partition key schema 路由。
5. 支持更丰富的 index 参数配置。
6. 支持 dynamic field 的更完整测试矩阵。
7. 支持 embedding generation operator，但应放在 transform 或独立算子中，不建议混入 sink。

## 19. 当前实现状态

当前实现已落在以下模块：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus
```

父模块已增加：

```xml
<module>flink-cdc-pipeline-connector-milvus</module>
```

已实现的主要能力：

| 能力 | 状态 |
| --- | --- |
| Factory SPI `milvus` | 已实现 |
| 连接配置、collection mapping、vector fields、index params 解析 | 已实现 |
| 主键 upsert/delete 写入 | 已实现 |
| 有序批量 flush 与 retry | 已实现 |
| 向量维度、向量源类型、主键类型校验 | 已实现 |
| 自动创建 collection 和向量索引 | 已实现 |
| `load-collection.enabled` | 已实现，schema/index setup 后按需同步 load collection |
| 已有 collection schema 兼容性校验 | 已实现，覆盖主键、向量字段、标量字段类型和 VarChar 长度 |
| 新增 nullable scalar field | 已实现 |
| 新增 vector field、drop/rename/alter/truncate/drop table | 默认拒绝 |
| DataChangeEvent 按 collection + primary key hash routing | 已实现 |
| `partition.field` 显式命名 partition 路由 | 已实现，字段值规范化为空值时写入默认 partition |
| `partition.names` metadata 预创建 partition | 已实现 |
| `partition.auto-create.enabled` 写入阶段创建缺失 partition | 已实现 |
| partition 变化 update | 已实现，先 delete 旧 partition 后 upsert 新 partition |
| adaptive split batch | 已实现，覆盖限流和消息过大错误 |
| Docker/Testcontainers Milvus IT | 已实现 |

生产化约束已在代码中显式执行：

- `sink.allow-no-primary-key=true` 当前直接拒绝，避免无主键 CDC 误写。
- `vector.fields` 不允许重复字段。
- `collection.mapping` 不允许重复 table entry。
- `index.params.` 不允许空参数名。
- `partition.names` 不允许 `_default`，避免用户显式创建 Milvus 保留 partition。
- Milvus field/collection/index 名称做合法性校验或规范化。
- `partition.field` 字段值会规范化为合法 Milvus partition name；null 写入默认 partition。
- `partition.auto-create.enabled=false` 时，未知 partition 交由 Milvus 拒绝，适合生产上只允许预创建分区。
- vector 字段必须存在、不能是主键、必须非 nullable。
- vector 源字段只允许 `ARRAY<FLOAT>` 或 JSON 数组字符串。
- 已有 collection 必须满足主键类型、`autoID=false`、向量类型、向量维度、标量字段类型和 VarChar 长度一致。

## 20. 测试与验证

### 20.1 单元测试

当前单元测试覆盖：

| 测试类 | 覆盖重点 |
| --- | --- |
| `MilvusDataSinkFactoryTest` | 配置解析、非法配置拒绝、表级 vector fields、mapping、index params |
| `MilvusNameUtilsTest` | Milvus 名称规范化与合法性 |
| `MilvusCollectionUtilsTest` | create collection schema、已有 collection 校验、add field 约束 |
| `MilvusVectorConverterTest` | ARRAY/JSON 向量转换和维度校验 |
| `MilvusEventSerializerTest` | CDC event 到 Milvus operation 转换、partition 路由和 partition 变化 update |
| `MilvusEventWriterTest` | flush 顺序、retry、metrics、维度错误 fail fast、partition batching、auto-create、adaptive split |
| `MilvusHashFunctionProviderTest` | 按 collection + primary key 做 data event hash routing |

验证命令：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus test
```

本地验证结果：

```text
Tests run: 67, Failures: 0, Errors: 0, Skipped: 0
```

### 20.2 Docker 集成测试

集成测试类：

```text
org.apache.flink.cdc.connectors.milvus.sink.MilvusSinkITCase
```

覆盖场景：

| 场景 | 覆盖 |
| --- | --- |
| Docker standalone Milvus 启动 | Testcontainers 启动 `milvusdb/milvus:v2.6.0` |
| insert/upsert | 写入后可按主键读取 |
| update | 同主键覆盖 |
| primary key change update | 删除旧主键并写入新主键 |
| delete | 按主键删除 |
| add scalar column | 新增字段后可写入 |
| existing collection idempotent validation | 重复 create table 不失败 |
| vector dimension mismatch | 在调用 Milvus 前失败 |
| string primary key + JSON string vector | 支持 VarChar 主键和 JSON 数组向量 |
| partition pre-create + partition field write | metadata 阶段创建 partition，写入阶段按字段写入命名 partition |
| load collection option | `load-collection.enabled=true` 后 collection load state 为 true |

默认 Docker IT 命令：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus integration-test
```

本地验证结果：

```text
MilvusSinkITCase: Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Milvus 2.6 standalone Docker 注意事项：

- 需要设置 `ETCD_USE_EMBED=true`。
- 需要设置 `DEPLOY_MODE=STANDALONE`，否则 embedded etcd 会被 Milvus 判定为 distributed mode 并启动失败。
- 当前 IT 使用 `COMMON_STORAGETYPE=local`，避免依赖 MinIO/S3。
- 当前 IT 使用 `QUOTAANDLIMITS_FLUSHRATE_ENABLED=false`，避免小数据高频验证触发默认 collection 级 flush 限流。
- 容器健康检查使用 `9091/healthz`，客户端连接使用 `19530`。
- Milvus 查询前 collection 必须已 load；load 又要求向量字段存在索引，因此 IT 开启 `createIndexEnabled=true`。
- IT 已减少轮询期间的 flush/load，只在每次 await 前做一次 flush + load。

使用外部手动安装的 Milvus：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus \
  -Dmilvus.it.uri=http://127.0.0.1:19530 \
  integration-test
```

如果 Milvus 开启认证：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus \
  -Dmilvus.it.uri=http://127.0.0.1:19530 \
  -Dmilvus.it.token='root:Milvus' \
  integration-test
```

也可以使用环境变量：

```bash
export MILVUS_IT_URI=http://127.0.0.1:19530
export MILVUS_IT_TOKEN='root:Milvus'
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus integration-test
```

## 21. 参考资料

- Milvus Java SDK insert API: https://milvus.io/api-reference/java/v2.6.x/v2/Vector/insert.md
- Milvus Java SDK upsert API: https://milvus.io/api-reference/java/v2.6.x/v2/Vector/upsert.md
- Milvus Java SDK delete API: https://milvus.io/api-reference/java/v2.6.x/v2/Vector/delete.md
- Milvus create collection API: https://milvus.io/api-reference/java/v2.6.x/v2/Collections/createCollection.md
- Milvus add collection field API: https://milvus.io/api-reference/java/v2.6.x/v2/Collections/addCollectionField.md
- Milvus schema 文档: https://milvus.io/docs/v2.6.x/schema.md
- Milvus dynamic field 文档: https://milvus.io/docs/v2.6.x/enable-dynamic-field.md
- Milvus standalone Docker 安装脚本: https://milvus.io/docs/install_standalone-docker.md
