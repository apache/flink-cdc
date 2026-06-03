# Flink CDC Pipeline Connector Milvus

Milvus Pipeline Connector 是 Flink CDC Pipeline 的 Milvus Sink，用于将
CDC 变更事件实时写入 Milvus collection。Connector identifier 为
`milvus`。

该 Connector 面向 CDC 同步语义设计：依赖源表中的确定性主键，将
`INSERT`、`UPDATE`、`REPLACE` 转换为 Milvus `upsert`，将 `DELETE` 转换为
Milvus `delete`。重复消费或故障恢复后的重放可以收敛到同一条 Milvus entity。

Connector 不负责生成 embedding。向量字段必须已经存在于源表记录中，可以是
`ARRAY<FLOAT>`、`ARRAY<DOUBLE>`，也可以是 JSON array string。

## 前置要求

运行 Pipeline 前，请确认：

* Flink TaskManager 可以访问 Milvus endpoint，例如 `http://milvus:19530`。
* 源表必须有且只有一个主键，或显式配置 `primary-key.field` 指定一个确定性字段。
* 目标 Milvus collection 的主键字段必须使用 `autoID=false`。
* 必须配置全局 `vector.fields`；指定表需要不同向量字段时，可用 `vector.fields.<table>` 覆盖。
* 源数据中的向量值不能为 null，且维度必须等于配置的 `FloatVector(dim)`。
* 如果开启认证，需要配置 `token`，例如 `root:Milvus`。

当前集成测试覆盖 Milvus standalone `2.6.0`。其他 Milvus 版本建议在上线前用
实际部署环境执行集成验证。

## 模块信息

模块路径：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus
```

Maven artifact：

```text
flink-cdc-pipeline-connector-milvus
```

Pipeline 中的 Sink 类型：

```yaml
sink:
  type: milvus
```

## 快速开始

最小配置：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  database-name: default
  vector.fields: embedding:FloatVector(1536)
```

这会把每张源表写入一个按 `TableId` 自动推导出的 Milvus collection。默认会：

* 自动创建不存在的 collection。
* 自动创建向量索引。
* 使用源表单主键作为 Milvus primary key。
* 将源表名归一化为合法 Milvus collection 名称。
* 使用 upsert/delete 执行 CDC 写入。

例如源表 `ai.docs` 默认会映射为 collection `ai_docs`。

## 完整示例

```yaml
source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: root
  tables: ai.docs
  server-id: 5401-5404

sink:
  type: milvus
  uri: http://127.0.0.1:19530
  token: root:Milvus
  connect.timeout: 10s
  rpc.deadline: 60s
  database-name: default

  collection.mapping: ai.docs:docs_collection
  collection.collision-check.tables: ai.docs
  collection.name-normalize.enabled: true
  create-collection.enabled: true
  create-index.enabled: true
  load-collection.enabled: true
  enable-dynamic-field: false
  consistency-level: strong

  vector.fields: embedding:FloatVector(1536)
  primary-key.field: id
  sink.primary-key-change.mode: reject

  partition.names: manual,auto
  partition.field: category
  partition.auto-create.enabled: false
  partition.auto-create.max-count: 1024

  index.type: AUTOINDEX
  index.metric-type: COSINE

  sink.flush.max-rows: 500
  sink.flush.interval: 1s
  sink.max-retries: 3
  sink.retry.backoff: 200ms
  sink.adaptive-batch-split.enabled: true
  sink.adaptive-batch-split.min-rows: 10
  sink.delete.enabled: true

pipeline:
  name: mysql-to-milvus
  parallelism: 2
```

## 配置项

### 连接配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `uri` | 是 | 无 | Milvus endpoint，例如 `http://localhost:19530`。 |
| `token` | 否 | 空字符串 | Milvus token。未开启认证时保持为空。 |
| `connect.timeout` | 否 | `10s` | Milvus 客户端连接超时，必须大于 `0`。 |
| `rpc.deadline` | 否 | `60s` | Milvus RPC 请求 deadline，必须大于 `0`。用于 upsert/delete/load 等 RPC。 |
| `database-name` | 否 | `default` | 目标 Milvus database。 |

### Collection 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `collection.mapping` | 否 | 空字符串 | 源表到 Milvus collection 的显式映射，格式为 `db.tbl:collection1;db.tbl2:collection2`。 |
| `collection.collision-check.tables` | 否 | 空字符串 | 启动期校验归一化 collection 名碰撞的源表列表，逗号分隔，例如 `db.t1,db.t2`。多表同步且未完整配置 `collection.mapping` 时建议填写。 |
| `collection.name-normalize.enabled` | 否 | `true` | 是否将源表 ID 自动归一化为合法 Milvus collection 名。 |
| `create-collection.enabled` | 否 | `true` | collection 不存在时是否自动创建。 |
| `create-index.enabled` | 否 | `true` | 是否创建向量索引。新建 collection 和校验已存在 collection 时都会使用该配置。 |
| `enable-dynamic-field` | 否 | `false` | 新建 collection 时是否启用 Milvus dynamic field。 |
| `load-collection.enabled` | 否 | `false` | schema 和 index 处理完成后是否同步 load collection。 |
| `consistency-level` | 否 | 空字符串 | 新建 collection 的一致性级别。空字符串表示使用 Milvus 默认值。支持值依赖 Milvus Java SDK enum，常用值包括 `strong`、`bounded`、`session`、`eventually`。 |

`collection.mapping` 推荐用于生产环境，因为显式名称更稳定，也能避免表名归一化后的碰撞风险：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: embedding:FloatVector(1536)
  collection.mapping: ai.docs:docs_collection;ai.tags:tags_collection
```

### 向量字段配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `vector.fields` | 是 | 无 | 全局向量字段声明，例如 `embedding:FloatVector(1536)`。 |
| `vector.fields.<table>` | 否 | 无 | 针对指定源表的向量字段声明，会覆盖全局 `vector.fields`。 |

`vector.fields` 支持逗号分隔的多个字段：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: title_embedding:FloatVector(768),content_embedding:FloatVector(1536)
```

表级覆盖示例：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: embedding:FloatVector(1536)
  vector.fields.ai.article: article_vector:FloatVector(768)
```

当前只支持 `FloatVector(dim)`：

* `dim` 必须大于 `0`。
* 字段名必须是合法 Milvus identifier。
* 源字段类型必须是 `ARRAY<FLOAT>`、`ARRAY<DOUBLE>`、`CHAR` 或 `VARCHAR`。
* 字符串向量必须是 JSON array，例如 `"[0.1,0.2,0.3]"`。

### 主键配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `primary-key.field` | 否 | 空字符串 | 显式指定 Milvus 主键字段。为空时使用源表的单主键。 |
| `sink.primary-key-change.mode` | 否 | `reject` | `UPDATE` 事件发生主键变化时的处理方式。支持 `reject` 和 `allow`。 |
| `sink.allow-no-primary-key` | 否 | `false` | 当前即使配置为 `true` 也会被拒绝，因为 CDC 写入需要确定性主键。 |

支持的主键类型：

| Flink CDC 类型 | Milvus 类型 |
| --- | --- |
| `BIGINT` | `Int64` |
| `CHAR` | `VarChar` |
| `VARCHAR` | `VarChar` |

当前不支持复合主键，也不支持无主键表写入 Milvus。

生产默认拒绝主键变更，因为主键变更需要先删除旧 key 再写入新 key。并行写入时如果按主键路由，旧 key 删除和新 key 写入可能落到不同 writer，无法证明严格顺序。确实需要支持主键变更时，可以显式配置：

```yaml
sink:
  type: milvus
  sink.primary-key-change.mode: allow
```

开启 `allow` 后，Connector 会按 collection 路由数据变化事件，以保证同一 collection 内主键变更的 delete/upsert 顺序，代价是该 collection 的写入并行度降低。

### 分区配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `partition.names` | 否 | 空字符串 | 元数据初始化阶段预创建的 Milvus partition 名称，逗号分隔。 |
| `partition.field` | 否 | 空字符串 | 使用源字段值作为写入时的 Milvus partition name。 |
| `partition.auto-create.enabled` | 否 | `false` | 配置 `partition.field` 后，写入阶段是否自动创建不存在的 partition。 |
| `partition.auto-create.max-count` | 否 | `1024` | 每个 writer 最多自动创建的 partition 数量，必须大于 `0`。 |

行为说明：

* `partition.names` 用于预创建一组受控 partition。
* `partition.names` 不能包含 Milvus 保留分区 `_default`。
* `partition.field` 的值会归一化为合法 Milvus partition name。
* `partition.field` 值为 null 时写入 Milvus 默认分区。
* `partition.auto-create.enabled=false` 时，写入未知 partition 会由 Milvus 返回失败。
* UPDATE 导致分区变化时，Connector 会先从旧 partition 删除旧 entity，再向新 partition upsert 新 entity。

生产环境建议提前规划 partition，并关闭运行时自动创建：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: embedding:FloatVector(1536)
  partition.names: manual,auto
  partition.field: category
  partition.auto-create.enabled: false
```

开发或低风险场景可以开启运行时创建：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: embedding:FloatVector(1536)
  partition.field: category
  partition.auto-create.enabled: true
  partition.auto-create.max-count: 128
```

注意：如果 `partition.field` 基数不可控，开启自动创建可能造成大量 partition。

### 索引配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `index.type` | 否 | `AUTOINDEX` | Milvus 向量索引类型，必须匹配 Milvus Java SDK `IndexParam.IndexType`。 |
| `index.metric-type` | 否 | `COSINE` | 向量距离度量，常用值包括 `COSINE`、`L2`、`IP`。 |
| `index.params.*` | 否 | 空 | 透传给 Milvus 的额外索引参数。值会被解析为 boolean、integer、double 或 string。 |

HNSW 示例：

```yaml
sink:
  type: milvus
  uri: http://127.0.0.1:19530
  vector.fields: embedding:FloatVector(768)
  index.type: HNSW
  index.metric-type: L2
  index.params.M: 16
  index.params.efConstruction: 200
```

Connector 会在启动时校验 `index.type` 和 `index.metric-type` 是否是 Milvus Java
SDK 支持的 enum 值。

### 写入配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sink.flush.max-rows` | 否 | `500` | 缓冲多少条操作后触发 flush，必须大于 `0`。 |
| `sink.flush.interval` | 否 | `1s` | 缓冲数据最长等待多久后触发 flush，必须大于 `0`。 |
| `sink.max-retries` | 否 | `3` | Milvus 写入失败后的最大重试次数，必须大于等于 `0`。 |
| `sink.retry.backoff` | 否 | `200ms` | 写入重试间隔，必须大于等于 `0`。 |
| `sink.adaptive-batch-split.enabled` | 否 | `true` | 写入失败时是否自动拆分 batch 后重试，当前仅针对限流和消息过大这类 batch-size 相关错误。 |
| `sink.adaptive-batch-split.min-rows` | 否 | `10` | 自适应拆分的最小 batch 行数，必须大于 `0`。 |
| `sink.delete.enabled` | 否 | `true` | 是否执行 CDC `DELETE` 事件。 |
| `sink.primary-key-change.mode` | 否 | `reject` | 主键变更处理模式。生产默认拒绝；显式 `allow` 时按 collection 路由以保证顺序。 |

自适应拆分会识别以下错误片段：

* `rate limit exceeded`
* `received message larger than max`

触发后，Connector 会递归将失败 batch 对半拆分，直到写入成功或达到
`sink.adaptive-batch-split.min-rows`。拆分过程中已成功写入的子 batch 会从
pending 缓冲区裁剪，避免 checkpoint 重放时重复 upsert。

连接中断、超时、服务不可用等错误会触发重试和客户端重连，但不会拆分 batch。

### 类型配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `varchar.max-length.default` | 否 | `65535` | Milvus `VarChar` 默认最大长度，必须大于 `0`。 |

对于 Flink CDC `VARCHAR`，如果源类型长度不是无限长，则使用
`min(source_length, varchar.max-length.default)`；`CHAR` 使用源类型长度；其他映射为
`VarChar` 的类型使用 `varchar.max-length.default`。

## 类型映射

### 标量字段

| Flink CDC 类型 | Milvus 类型 | 写入转换 |
| --- | --- | --- |
| `BOOLEAN` | `Bool` | 保持 boolean。 |
| `TINYINT` | `Int32` | 转为 int。 |
| `SMALLINT` | `Int32` | 转为 int。 |
| `INTEGER` | `Int32` | 转为 int。 |
| `BIGINT` | `Int64` | 转为 long。 |
| `FLOAT` | `Float` | 转为 float。 |
| `DOUBLE` | `Double` | 转为 double。 |
| `CHAR` | `VarChar` | 转为 string。 |
| `VARCHAR` | `VarChar` | 转为 string。 |
| `DECIMAL` | `VarChar` | 转为 string。 |
| `DATE` | `VarChar` | 转为 ISO string。 |
| `TIME` | `VarChar` | 转为 string。 |
| `TIMESTAMP` | `VarChar` | 转为 string。 |
| `TIMESTAMP WITH TIME ZONE` | `VarChar` | 转为 string。 |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `VarChar` | 转为 string。 |
| 非向量 `ARRAY` | 不支持 | 启动或写入阶段 fail fast。 |
| 其他复杂类型 | 不支持 | fail fast。 |

### 向量字段

| 源值形态 | Milvus 类型 | 说明 |
| --- | --- | --- |
| `ARRAY<FLOAT>` | `FloatVector` | 支持，维度必须匹配配置。 |
| `ARRAY<DOUBLE>` | `FloatVector` | 支持，写入前转为 float。 |
| JSON array string | `FloatVector` | 支持，例如 `"[0.1,0.2,0.3]"`。 |

向量转换规则：

* 向量字段不能为 null。
* 向量长度必须严格等于配置维度。
* 向量元素必须是有限数值。
* `NaN`、`Infinity`、`-Infinity` 会被拒绝。
* Connector 不会自动截断、补齐或归一化向量。

## Collection 命名

未配置 `collection.mapping` 时，Connector 会从源表 `TableId` 推导 collection 名。

当 `collection.name-normalize.enabled=true` 时，非法 identifier 字符会被替换为
`_`：

```text
ai.docs -> ai_docs
```

Milvus collection、field、partition、index 名称必须满足：

* 以字母或 `_` 开头。
* 只能包含字母、数字和 `_`。
* 最大长度为 `255`。

如果关闭自动归一化，源表名必须天然满足 Milvus identifier 规则，否则启动校验会失败。

## 写入语义

Connector 提供 at-least-once 写入语义，不提供与 Milvus 之间的 exactly-once 事务。

CDC 事件映射：

| Flink CDC 事件 | Milvus 操作 |
| --- | --- |
| `INSERT` | `upsert(after)` |
| `UPDATE` | `upsert(after)` |
| `REPLACE` | `upsert(after)` |
| `DELETE` | `delete(before primary key)`，仅当 `sink.delete.enabled=true` |

主键变化时，默认行为是拒绝该 `UPDATE` 并 fail fast。配置
`sink.primary-key-change.mode=allow` 后才会执行：

```text
delete(old primary key)
upsert(new row)
```

分区变化时：

```text
delete(old primary key from old partition)
upsert(new row into new partition)
```

只要 Milvus collection 使用相同的确定性主键并保持 `autoID=false`，重放后的最终结果
可以保持确定。

## Schema Evolution

支持的 schema change：

| 事件 | 行为 |
| --- | --- |
| `CREATE_TABLE` | 创建 collection，或校验已存在 collection。 |
| `ADD_COLUMN` | 添加可映射到 Milvus 的 nullable 标量字段；字段已存在且兼容时会跳过，支持 schema event 重放。 |
| `ALTER_TABLE_COMMENT` | no-op，Milvus collection comment 不随 CDC 事件演进。 |

不支持的 schema change：

| 事件 | 原因 |
| --- | --- |
| 添加向量字段 | Milvus 向量字段增加涉及索引和历史数据，当前不支持。 |
| `DROP_COLUMN` | 破坏已有 Milvus schema。 |
| `RENAME_COLUMN` | 需要数据迁移。 |
| `ALTER_COLUMN_TYPE` | 可能破坏 Milvus 字段类型或向量维度。 |
| `TRUNCATE_TABLE` | 破坏性操作。 |
| `DROP_TABLE` | 破坏性操作。 |

当 collection 已存在时，Connector 会校验：

* 主键字段名称和类型。
* 主键字段 `autoID=false`。
* 向量字段存在、类型匹配、维度匹配。
* 标量字段存在且类型匹配。
* `VarChar` 最大长度不小于源字段要求。
* `partition.field` 存在，并且不是向量字段。

## 生产建议

推荐生产基线配置：

```yaml
sink:
  type: milvus
  uri: http://milvus:19530
  connect.timeout: 10s
  rpc.deadline: 120s
  vector.fields: embedding:FloatVector(1536)
  collection.mapping: db.t1:c1;db.t2:c2
  collection.collision-check.tables: db.t1,db.t2
  create-collection.enabled: true
  create-index.enabled: true
  load-collection.enabled: false
  sink.flush.max-rows: 500
  sink.flush.interval: 1s
  sink.max-retries: 5
  sink.retry.backoff: 500ms
  sink.adaptive-batch-split.enabled: true
  sink.delete.enabled: true
```

生产分区写入建议：

```yaml
sink:
  type: milvus
  uri: http://milvus:19530
  vector.fields: embedding:FloatVector(1536)
  partition.names: tenant_a,tenant_b
  partition.field: tenant
  partition.auto-create.enabled: false
```

建议：

* 保持 `sink.allow-no-primary-key=false`，当前 Connector 不支持无主键 CDC 写入。
* 目标 collection 不要开启 Milvus `autoID`。
* 生产环境优先配置 `collection.mapping`，避免自动命名碰撞。
* 分区集合可控时使用 `partition.names` 预创建，并关闭运行时自动创建。
* 只有需要 Connector 在启动阶段把 collection 变成 query-ready 时才开启 `load-collection.enabled`。
* 根据向量维度、Milvus gRPC 消息大小、写入吞吐调整 `sink.flush.max-rows`。
* 保持 `sink.adaptive-batch-split.enabled=true`，除非已经有严格 batch 大小控制。
* 上线前用真实 Milvus 集群执行集成测试，覆盖 upsert、delete、主键变化、分区变化、schema 校验和 checkpoint 恢复后的数据收敛。

## 限制

当前不支持：

* Exactly-once 写入。
* Milvus `autoID=true`。
* 复合主键。
* 无主键 CDC 表。
* Milvus native partition-key schema routing。
* `BinaryVector`、`Float16Vector`、`BFloat16Vector`、Sparse Vector。
* 通过 schema evolution 添加向量字段。
* 删除、重命名或修改已有字段类型。
* 自动生成 embedding。
* 非向量数组、map、row、variant 等复杂类型。

## 指标

Connector 通过 `MilvusSinkMetrics` 注册 Sink Writer 指标：

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `numRecordsSend` | Counter | 成功 flush 到 Milvus 的行数。 |
| `numRecordsOutErrors` | Counter | 重试耗尽后最终失败的行数。 |
| `currentSendTime` | Gauge | 最近一次成功 flush 的耗时，单位毫秒。 |
| `milvusFlushCount` | Counter | 成功 flush 次数。 |
| `milvusFlushFailureCount` | Counter | flush 失败次数。 |
| `milvusRetryCount` | Counter | 写入重试次数。 |
| `milvusReconnectCount` | Counter | Milvus 客户端重连次数。 |
| `milvusPendingRows` | Gauge | 当前缓冲区中等待 flush 的操作数。 |
| `milvusLastFlushDurationMs` | Gauge | 最近一次成功 flush 的耗时，单位毫秒。 |

其中 `numRecordsSend`、`numRecordsOutErrors`、`currentSendTime` 使用 Flink
`SinkWriterMetricGroup` 的标准指标。

## 测试

### 单元测试

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus test
```

### 使用 Testcontainers 启动 Milvus

未配置外部 Milvus 地址时，集成测试会通过 Testcontainers 启动
`milvusdb/milvus:v2.6.0`：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus integration-test
```

Testcontainers 启动参数包括：

* `ETCD_USE_EMBED=true`
* `DEPLOY_MODE=STANDALONE`
* `COMMON_STORAGETYPE=local`
* `QUOTAANDLIMITS_FLUSHRATE_ENABLED=false`

### 使用已安装的 Milvus

如果本机或测试环境已经安装 Milvus，可以通过系统属性指定地址：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-milvus \
  -Dmilvus.it.uri=http://127.0.0.1:19530 \
  integration-test
```

认证环境：

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

集成测试会覆盖：

* collection 创建和已存在 collection 幂等校验。
* 向量 upsert、update、主键变化和 delete。
* 添加标量字段后继续写入。
* 向量维度错误在写入 Milvus 前失败。
* 字符串主键和 JSON string vector。
* 预创建 partition 和按字段写入 partition。
* `load-collection.enabled` 同步 load collection。
* Flink checkpoint 完成后故障重启，验证 schema replay 幂等以及最终 entity 收敛。

## 排障

### `vector.fields must not be empty`

必须至少配置一个向量字段：

```yaml
sink:
  vector.fields: embedding:FloatVector(1536)
```

### `Vector field embedding dimension mismatch`

源数据中的向量长度与 `FloatVector(dim)` 不一致。需要修复源数据，或调整
`vector.fields` 中的维度配置。

### `Milvus sink requires exactly one primary key`

源表没有主键或使用了复合主键。请为源表配置单主键，或用 `primary-key.field`
选择一个确定性字段。

### `autoID` 校验失败

目标 collection 使用了 `autoID=true`。CDC upsert/delete 需要源端确定性主键，
请重建 collection 并设置 `autoID=false`。

### partition 写入失败

当 `partition.auto-create.enabled=false` 时，`partition.field` 产生的每个非空
partition name 都必须已经存在。可以通过 `partition.names` 预创建，或在启动前
手动创建。

### 启动阶段较慢

如果配置了 `load-collection.enabled=true`，Connector 会同步 load collection。
不需要由写入任务负责 query-ready 时，建议关闭该选项。

### 写入 batch 过大或 Milvus 限流

可以降低：

```yaml
sink.flush.max-rows: 100
```

并保持：

```yaml
sink.adaptive-batch-split.enabled: true
```

对于高维向量，建议先用较小 batch 压测，再逐步调高。
