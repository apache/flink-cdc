# Flink CDC Pipeline Connector pgvector

pgvector pipeline connector 用于将 Flink CDC pipeline 事件写入启用了 pgvector
扩展的 PostgreSQL 表。该连接器是 sink connector，标识符为 `pgvector`。

连接器通过 PostgreSQL JDBC 写入数据，并将向量值绑定为 PostgreSQL object。当前实现
不依赖额外的 pgvector Java client。

## 前置条件

运行 pipeline 前，需要确认：

* 所有 Flink TaskManager 都可以访问目标 PostgreSQL。
* PostgreSQL 服务端已经安装 pgvector 扩展。
* 当 `create-extension.enabled` 为 `false` 时，目标数据库需要提前启用 pgvector 扩展：

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

当 `create-extension.enabled` 设置为 `true` 时，连接器可以在处理 `CREATE_TABLE` 事件时
自动执行该语句。
不过创建 extension 通常需要较高数据库权限，生产环境建议提前由 DBA 或初始化脚本完成。

## 模块信息

模块名：

```text
flink-cdc-pipeline-connector-pgvector
```

Factory 标识符：

```yaml
sink:
  type: pgvector
```

## 参数说明

| 参数 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `jdbc-url` | 是 | 无 | PostgreSQL JDBC URL，必须以 `jdbc:postgresql:` 开头，例如 `jdbc:postgresql://127.0.0.1:5432/vector_db`。 |
| `username` | 是 | 无 | PostgreSQL 用户名。 |
| `password` | 否 | 空字符串 | PostgreSQL 密码。 |
| `schema` | 否 | `public` | 当输入表 id 不包含 schema 时使用的默认 PostgreSQL schema。 |
| `create-schema.enabled` | 否 | `true` | 是否通过 `CREATE SCHEMA IF NOT EXISTS` 自动创建缺失的目标 schema。 |
| `create-table.enabled` | 否 | `true` | 是否根据输入的 `CREATE_TABLE` 事件自动创建目标表。 |
| `create-extension.enabled` | 否 | `false` | 建表时是否执行 `CREATE EXTENSION IF NOT EXISTS vector`。 |
| `sink.flush.max-rows` | 否 | `500` | JDBC batch flush 前最多缓存的行数，必须大于 `0`。 |
| `sink.flush.interval` | 否 | `1s` | JDBC batch flush 前最多等待的时间。 |
| `sink.max-retries` | 否 | `3` | JDBC batch 执行失败后的最大重试次数，必须大于等于 `0`。 |
| `sink.retry.backoff` | 否 | `1s` | 每次 JDBC batch 失败后、下一次重试前等待的时间，必须大于等于 `0`。 |
| `sink.delete.enabled` | 否 | `true` | 是否处理输入的 `DELETE` 事件。设置为 `false` 时会忽略 delete 事件。 |
| `sink.allow-no-primary-key` | 否 | `false` | 是否允许向无主键表写入 append-only 数据。无主键表只支持 `INSERT` 和 `REPLACE`，不支持 `UPDATE` 和 `DELETE`。 |
| `vector.columns.<key>` | 否 | 无 | 将目标列声明为 pgvector 类型。详见 [向量列参数](#向量列参数)。 |
| `table.create.properties.<key>` | 否 | 无 | 为自动生成的 `CREATE TABLE` SQL 增加 PostgreSQL `WITH` 表存储参数，例如 `table.create.properties.fillfactor: 90`。 |

## 向量列参数

Flink CDC 的 array、string、binary 或 map 字段不会被自动推断为 pgvector 列。需要通过
`vector.columns.` 前缀显式声明每一个向量列：

```yaml
sink:
  type: pgvector
  jdbc-url: jdbc:postgresql://127.0.0.1:5432/vector_db
  username: postgres
  password: postgres
  vector.columns.public.docs.embedding: vector(1536)
```

`vector.columns.` 后面的 key 支持两种形式：

* 完整目标列 key：`<schema>.<table>.<column>`。
* 仅列名 key：`<column>`。当多个表中同名列使用相同向量类型时，可作为 fallback。

示例：

```yaml
sink:
  type: pgvector
  jdbc-url: jdbc:postgresql://127.0.0.1:5432/vector_db
  username: postgres
  password: postgres
  vector.columns.public.docs.embedding: vector(1536)
  vector.columns.public.docs.summary_embedding: halfvec(768)
  vector.columns.public.docs.sparse_embedding: sparsevec(1000)
  vector.columns.public.docs.hash_bits: bit(1024)
  vector.columns.common_embedding: vector(384)
```

支持的 pgvector 声明：

| 声明 | 目标 PostgreSQL 类型 | 支持的输入值形态 |
| --- | --- | --- |
| `vector(n)` | `vector(n)` | 长度必须等于 `n` 的稠密向量，元素必须是有限数字。 |
| `halfvec(n)` | `halfvec(n)` | 长度必须等于 `n` 的稠密向量，元素必须是有限数字。 |
| `sparsevec(n)` | `sparsevec(n)` | 使用 1-based index 的稀疏向量 map，或可转换为稠密向量 literal 的 list/array/string 输入。 |
| `bit(n)` | `bit(n)` | 只包含 `0` 和 `1` 的字符串，或展开后长度正好为 `n` 的 `byte[]`。 |

稠密向量可以来自 array/list，也可以来自类似 `[0.1,0.2,0.3]` 的字符串 literal。连接器
会在绑定 JDBC 参数前校验向量维度。

## Pipeline 示例

下面示例将 MySQL 表同步到 PostgreSQL，并把 `embedding` 列映射为 pgvector
`vector(1536)`：

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
  type: pgvector
  jdbc-url: jdbc:postgresql://127.0.0.1:5432/vector_db
  username: postgres
  password: postgres
  schema: public
  create-schema.enabled: true
  create-table.enabled: true
  create-extension.enabled: false
  sink.flush.max-rows: 500
  sink.flush.interval: 1s
  sink.max-retries: 3
  sink.retry.backoff: 1s
  sink.delete.enabled: true
  vector.columns.public.docs.embedding: vector(1536)
  table.create.properties.fillfactor: 90

route:
  - source-table: ai.docs
    sink-table: public.docs

pipeline:
  name: mysql-to-pgvector
  parallelism: 2
```

如果目标数据库已经启用 extension，且目标表已经提前创建，可以关闭自动 DDL：

```yaml
sink:
  type: pgvector
  jdbc-url: jdbc:postgresql://127.0.0.1:5432/vector_db
  username: postgres
  password: postgres
  create-schema.enabled: false
  create-table.enabled: false
  vector.columns.public.docs.embedding: vector(1536)
```

## 自动建表类型映射

当 `create-table.enabled` 为 `true` 时，连接器会根据输入的 Flink CDC schema 创建
PostgreSQL 表。

| Flink CDC 类型 | PostgreSQL 类型 |
| --- | --- |
| `CHAR(n)` | `CHAR(n)` |
| `VARCHAR(n)` | `VARCHAR(n)` |
| 无界 `STRING` / max `VARCHAR` | `TEXT` |
| `BOOLEAN` | `BOOLEAN` |
| `BINARY`, `VARBINARY` | `BYTEA` |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `TINYINT`, `SMALLINT` | `SMALLINT` |
| `INTEGER` | `INTEGER` |
| `BIGINT` | `BIGINT` |
| `FLOAT` | `REAL` |
| `DOUBLE` | `DOUBLE PRECISION` |
| `DATE` | `DATE` |
| `TIME` | `TIME(p)`，精度最高保留到微秒 |
| `TIMESTAMP` | `TIMESTAMP` |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMPTZ` |
| `ARRAY<T>`（非向量列） | PostgreSQL array，元素类型按映射后的 PostgreSQL 类型生成，写入时使用 JDBC `createArrayOf` 绑定 |
| `MAP`, `ROW`, `VARIANT` | `TEXT` |
| 通过 `vector.columns.*` 声明的列 | `vector(n)`、`halfvec(n)`、`sparsevec(n)` 或 `bit(n)` |

不支持的 Flink CDC 类型会在建表或类型转换阶段直接失败，例如 `JSON`/`JSONB`、`UUID`、`INTERVAL`、`ENUM` 等。

## 写入语义

连接器提供 **at-least-once** 写入语义，不提供 exactly-once 保证。故障恢复后可能重复写入
`INSERT`/`UPDATE`/`REPLACE` 事件；有主键表通过 upsert 可保证最终一致。`DELETE` 重复执行
通常幂等，但如果目标表存在触发器、审计或级联逻辑，仍需要按可能重复执行来评估。
checkpoint 时 writer 会 flush 缓冲 batch，未 flush 的数据在
Task 失败时会随 checkpoint 回退由上游重放。

对于有主键表，`INSERT`、`UPDATE` 和 `REPLACE` 事件会通过 PostgreSQL upsert SQL 写入：

```sql
INSERT INTO ... VALUES (...) ON CONFLICT (<primary keys>) DO UPDATE SET ...
```

对于 `DELETE` 事件，当 `sink.delete.enabled` 为 `true` 时，连接器会按主键删除目标行。

对于无主键表：

* 默认拒绝写入。
* 设置 `sink.allow-no-primary-key: true` 后允许 append-only 写入。
* 仅支持 `INSERT` 和 `REPLACE` 事件。
* 不支持 `UPDATE` 和 `DELETE`，因为无主键时无法稳定匹配目标行。

## Schema Evolution

MetadataApplier 支持以下 schema change 事件：

* `CREATE_TABLE`
* `ADD_COLUMN`
* `ALTER_COLUMN_TYPE`
* `DROP_COLUMN`
* `RENAME_COLUMN`
* `TRUNCATE_TABLE`
* `DROP_TABLE`

表注释变更会被忽略。

对于 `ALTER_COLUMN_TYPE`，向量列会继续使用 `vector.columns.*` 中配置的 pgvector
类型；普通列会按 Flink CDC 到 PostgreSQL 的常规类型映射处理。

`RENAME_COLUMN` 后需要手动更新 `vector.columns.*` 中的列 key，连接器不会自动迁移向量列
配置。

## 使用注意事项

* pgvector 的维度是 PostgreSQL 列类型的一部分。如果 embedding model 的维度发生变化，
  需要同步更新 `vector.columns.*`，并执行兼容的 schema evolution 或重建目标表。
* 连接器只校验向量声明维度必须大于 `0`，以及写入值长度必须匹配声明维度。不同 pgvector
  版本、类型和索引方式对最大维度或非零元素数量的限制可能不同，应以目标 PostgreSQL 上安装
  的 pgvector 版本和实际索引 DDL 为准。
* 向量值长度必须严格等于配置维度。维度不匹配会在写入 JDBC 参数前失败。
* 稠密向量元素必须是有限数字，`NaN` 和无限值会被拒绝。
* `sparsevec(n)` map 输入使用 1-based index，与 pgvector literal 保持一致。
* `bit(n)` 字符串输入可以包含空白字符，校验前会先移除空白字符。
* 连接器使用普通 PostgreSQL SQL 创建表和执行 DDL，需要确保配置的数据库用户具备相应
  权限。
* JDBC 连接在 batch flush 失败且检测到连接类错误（SQLState `08xxx`、`57P01` 等）时会
  自动重连并重建 prepared statement。
* Writer 在处理 schema change 时要求先收到对应表的 `CREATE_TABLE` 事件；否则会 fail-fast
  而不是静默忽略。
* `DROP_TABLE` 事件会从 writer 内存 schema 中移除目标表；后续写入该表的数据变更事件会失败。
* HNSW、IVFFlat 等向量索引建议在目标表创建完成后由外部 DDL 创建。索引类型、operator class
  与参数依赖具体查询、向量类型和数据分布，不应从 CDC 事件中自动推断。

## 运行指标

连接器会注册 Flink sink writer 标准指标，并额外注册 pgvector 专用指标：

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `numRecordsSend` | Counter | 成功 flush 到 PostgreSQL 的行数。 |
| `numRecordsOutErrors` | Counter | 最终失败、无法成功 flush 的行数。短暂失败但重试成功不会计入该指标。 |
| `currentSendTime` | Gauge | 最近一次成功 flush 的耗时，单位毫秒。 |
| `pgvectorPendingRows` | Gauge | 当前 writer 内待 flush 的行数。 |
| `pgvectorLastFlushDurationMs` | Gauge | 最近一次成功 flush 的耗时，单位毫秒。 |
| `pgvectorFlushCount` | Counter | 成功 flush 的次数。 |
| `pgvectorFlushFailureCount` | Counter | JDBC batch flush 失败尝试次数。一次 batch 多次重试失败会累加多次。 |
| `pgvectorRetryCount` | Counter | 已执行的重试次数。 |
| `pgvectorReconnectCount` | Counter | 因连接不可用或连接类 SQLState 触发的重连次数。 |

生产环境建议至少对 `numRecordsOutErrors`、`pgvectorFlushFailureCount`、
`pgvectorRetryCount` 和 `pgvectorPendingRows` 设置告警。`pgvectorFlushFailureCount`
短时间内升高但 `numRecordsOutErrors` 不变，通常表示存在可恢复的数据库抖动；两者同时升高
则表示有 batch 最终失败，需要排查目标库、权限、表结构或向量数据质量。

## 测试

模块单测：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-pgvector \
  -Dspotless.check.skip=true test
```

代码格式检查：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-pgvector \
  spotless:check
```

Docker 集成测试使用 `pgvector/pgvector:pg16` 镜像：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-pgvector \
  -Dspotless.check.skip=true -Dtest=PgVectorSinkITCase test
```
