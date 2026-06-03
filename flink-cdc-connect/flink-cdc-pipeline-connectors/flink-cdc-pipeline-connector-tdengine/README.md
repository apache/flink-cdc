# Flink CDC Pipeline Connector TDengine

TDengine Pipeline Connector 是 Flink CDC Pipeline 的 TDengine Sink，用于将
CDC 变更事件实时写入 TDengine supertable 和 subtable。Connector identifier 为
`tdengine`。

该 Connector 面向 TDengine 的时序模型设计：源记录必须提供一个时间戳字段、一个
子表字段和至少一个 tag 字段。写入时会生成 `INSERT INTO child USING stable TAGS ...`
语句，自动按源记录中的子表值写入或创建对应 subtable。

## 前置要求

运行 Pipeline 前，请确认：

* Flink TaskManager 可以访问 TDengine taosAdapter WebSocket 端口，默认 `6041`。
* 使用 WebSocket JDBC URL，例如 `jdbc:TAOS-WS://tdengine:6041`。
* 源表中存在 `timestamp.field`、`subtable.field` 和所有 `tag.fields`。
* `timestamp.field` 写入 TDengine 主时间戳列，值不能为 null。
* `subtable.field` 写入 TDengine 子表名，值不能为 null 或空字符串。
* `tag.fields` 写入 TDengine tags，至少配置一个 tag。
* TDengine 不支持单行 DELETE，默认会拒绝 DELETE 事件。

## 模块信息

模块路径：

```text
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-tdengine
```

Maven artifact：

```text
flink-cdc-pipeline-connector-tdengine
```

Pipeline 中的 Sink 类型：

```yaml
sink:
  type: tdengine
```

## 快速开始

最小配置：

```yaml
sink:
  type: tdengine
  url: jdbc:TAOS-WS://127.0.0.1:6041
  username: root
  password: taosdata
  database-name: power
  stable.name: meters
  timestamp.field: ts
  subtable.field: meter_id
  tag.fields: location,group_id
```

示例源表：

```sql
CREATE TABLE meter_source (
  ts BIGINT,
  meter_id VARCHAR(64),
  location VARCHAR(64),
  group_id INT,
  current DOUBLE,
  voltage DOUBLE,
  phase DOUBLE
);
```

目标 TDengine supertable 等价于：

```sql
CREATE DATABASE IF NOT EXISTS `power`;

CREATE STABLE IF NOT EXISTS `power`.`meters` (
  `ts` TIMESTAMP,
  `current` DOUBLE,
  `voltage` DOUBLE,
  `phase` DOUBLE
) TAGS (
  `location` NCHAR(64),
  `group_id` INT
);
```

写入记录会生成类似：

```sql
INSERT INTO `power`.`d1001`
USING `power`.`meters`
TAGS ('California.SanFrancisco', 2)
(`ts`, `current`, `voltage`, `phase`)
VALUES (1717400000000, 10.3, 219.7, 0.31);
```

## 完整示例

```yaml
source:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: root
  password: root
  tables: iot.meter_source
  server-id: 5401-5404

sink:
  type: tdengine
  url: jdbc:TAOS-WS://127.0.0.1:6041
  username: root
  password: taosdata
  database-name: power

  stable.name: meters
  stable.mapping: iot.meter_source:meters
  name-normalize.enabled: true

  timestamp.field: ts
  subtable.field: meter_id
  tag.fields: location,group_id

  create-database.enabled: true
  create-stable.enabled: true
  string.type: NCHAR
  varchar.max-length.default: 256
  decimal.mapping: DOUBLE
  timezone: UTC

  sink.flush.max-rows: 1000
  sink.max-sql-bytes: 900000
  sink.flush.interval: 1s
  sink.max-retries: 3
  sink.retry.backoff: 200ms

  sink.delete.mode: reject
  sink.update.mode: upsert
  sink.timestamp-change.mode: reject
  sink.subtable-change.mode: reject

pipeline:
  name: mysql-to-tdengine
  parallelism: 2
```

## 配置项

### 连接配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `url` | 是 | 无 | TDengine WebSocket JDBC URL，必须以 `jdbc:TAOS-WS://` 开头。 |
| `username` | 否 | `root` | TDengine 用户名。 |
| `password` | 否 | `taosdata` | TDengine 密码。 |
| `database-name` | 是 | 无 | 目标 TDengine database。 |
| `connection.properties.*` | 否 | 无 | 透传给 TDengine JDBC driver 的连接属性，例如 `connection.properties.charset: UTF-8`。 |

生产建议使用 WebSocket JDBC。Native 和 REST 连接方式不作为默认支持路径。

### 表模型配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `stable.name` | 否 | 空字符串 | 默认目标 supertable 名。为空时按源 `TableId` 推导。 |
| `stable.mapping` | 否 | 空字符串 | 源表到 supertable 的显式映射，格式为 `db.tbl:stable1;db.tbl2:stable2`。 |
| `name-normalize.enabled` | 否 | `true` | 是否将 database、stable、subtable 名归一化为 TDengine identifier。 |
| `timestamp.field` | 是 | 无 | 源字段，写入 TDengine 主时间戳列。 |
| `subtable.field` | 是 | 无 | 源字段，写入 TDengine 子表名，不会写入 metric column。 |
| `tag.fields` | 是 | 无 | 逗号分隔的 tag 字段列表，至少一个。 |
| `create-database.enabled` | 否 | `true` | 是否自动执行 `CREATE DATABASE IF NOT EXISTS`。 |
| `create-stable.enabled` | 否 | `true` | 是否自动执行 `CREATE STABLE IF NOT EXISTS`。 |
| `stable-schema-validation.enabled` | 否 | `true` | 是否在 `CREATE_TABLE` 时校验目标 supertable schema 与源 schema 一致。 |

`stable-schema-validation.enabled` 会在建表阶段执行 `DESCRIBE stable` 并比对列名和类型，
用于尽早发现预建表 schema 漂移。若目标 stable 由 DBA 预先创建，建议保持开启；
仅在明确接受 schema 差异时才关闭。

`timestamp.field`、`subtable.field`、`tag.fields` 必须互不重叠。生产环境建议配置
`stable.mapping`，避免源表名变化或归一化碰撞影响目标表名。

### 类型配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `varchar.max-length.default` | 否 | `16374` | 字符串和二进制字段默认长度上限。源 schema 有明确长度时会取 `min(sourceLength, default)`。 |
| `string.type` | 否 | `NCHAR` | 字符串字段映射类型，支持 `NCHAR` 或 `VARCHAR`。 |
| `decimal.mapping` | 否 | `DOUBLE` | `DECIMAL` 映射方式，支持 `DOUBLE` 或 `NCHAR`。 |
| `timezone` | 否 | `UTC` | 格式化 DATE/TIME/TIMESTAMP 值时使用的时区。 |

类型映射：

| Flink CDC 类型 | TDengine 类型 |
| --- | --- |
| `BOOLEAN` | `BOOL` |
| `TINYINT` | `TINYINT` |
| `SMALLINT` | `SMALLINT` |
| `INTEGER` | `INT` |
| `BIGINT` | `BIGINT`，作为 `timestamp.field` 时为 `TIMESTAMP` |
| `FLOAT` | `FLOAT` |
| `DOUBLE` | `DOUBLE` |
| `DECIMAL` | `DOUBLE` 或 `NCHAR(n)` |
| `CHAR` / `VARCHAR` | `NCHAR(n)` 或 `VARCHAR(n)`；作为 `timestamp.field` 时为 `TIMESTAMP` |
| `DATE` / `TIME` / `TIMESTAMP` | 普通列映射为字符串；作为 `timestamp.field` 时为 `TIMESTAMP` |
| `BINARY` / `VARBINARY` | `VARBINARY(n)` |

### 写入配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sink.flush.max-rows` | 否 | `1000` | 累积多少行后触发 flush，必须大于 `0`。 |
| `sink.max-sql-bytes` | 否 | `900000` | 单条 `INSERT` SQL 渲染字节数上限，超过后拆分执行。 |
| `sink.flush.interval` | 否 | `1s` | 最大 flush 间隔，必须大于 `0`。 |
| `sink.max-retries` | 否 | `3` | 瞬时 JDBC 失败最大重试次数，必须大于等于 `0`。 |
| `sink.retry.backoff` | 否 | `200ms` | 重试等待时间，必须大于等于 `0`。 |

Connector 会按目标 stable 分组，并在单个 stable 内按原始事件顺序生成多子表
`INSERT` 语句。`sink.max-sql-bytes` 用于防止批量 SQL 过大。

如果同一次 flush 被切分成多条 SQL，Connector 会在每条 SQL 执行成功后从待写队列中移除
对应行。后续 SQL 失败并触发重试时，只会重放尚未确认成功的剩余行。

### CDC 语义配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sink.delete.mode` | 否 | `reject` | DELETE 处理方式，支持 `reject`、`ignore`。 |
| `sink.update.mode` | 否 | `upsert` | UPDATE 处理方式，支持 `upsert`、`reject`。 |
| `sink.timestamp-change.mode` | 否 | `reject` | UPDATE 修改 `timestamp.field` 时的处理方式，支持 `reject`、`allow`。 |
| `sink.subtable-change.mode` | 否 | `reject` | UPDATE 修改 `subtable.field` 时的处理方式，支持 `reject`、`allow`。 |

语义说明：

* `INSERT` 和 `REPLACE` 写入 after 记录。
* `UPDATE` 默认写入 after 记录。TDengine 中相同 subtable + timestamp 会覆盖同一时间点数据。
* `DELETE` 默认拒绝，因为 TDengine 不支持常规单行 DELETE 语义。
* UPDATE 修改 `timestamp.field` 默认拒绝，因为旧时间点不会被删除。
* UPDATE 修改 `subtable.field` 默认拒绝，因为旧子表中的旧时间点不会被删除。
* Connector 不提供 exactly-once。故障恢复后可能重放，只有相同 subtable + timestamp 的写入具备幂等收敛属性。
* tag 值应是 subtable 级静态维度。Connector 不在运行时维护每个 subtable 的历史 tag 快照；同一 subtable 后续记录携带不同 tag 值时，TDengine 端行为取决于服务端约束和版本。

## Schema Evolution

当前支持：

* `CREATE_TABLE`
* `ADD_COLUMN`
* `ALTER_TABLE_COMMENT`，忽略

当前拒绝：

* 添加 `timestamp.field`
* 添加 `subtable.field`
* 添加 tag 字段
* 删除列、重命名列、修改列类型、删除表、清空表

新增普通 metric column 会执行：

```sql
ALTER STABLE `db`.`stable` ADD COLUMN `new_metric` DOUBLE;
```

## 生产使用建议

* 显式配置 `stable.mapping`，不要依赖自动从源表名推导。
* 保持 `stable-schema-validation.enabled=true`，避免预建 supertable schema 与源表 silently 漂移。
* 保持 `sink.delete.mode=reject`，除非业务明确接受忽略 DELETE。
* 保持 `sink.timestamp-change.mode=reject` 和 `sink.subtable-change.mode=reject`，除非业务明确接受旧数据残留。
* 为 `subtable.field` 选择稳定、低基数且符合 TDengine 子表命名预期的设备标识。
* `tag.fields` 应选择设备静态维度字段，不要使用高频变化字段。
* 根据 TDengine 集群和 taosAdapter 能力调小或调大 `sink.flush.max-rows` 和 `sink.max-sql-bytes`。
* 上线前使用实际 TDengine 版本执行端到端验证，覆盖建库、建 stable、写入、故障重放和 schema evolution。

## 测试

运行 TDengine connector 单元测试：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-tdengine test
```

当前单元测试覆盖：

* Factory 服务发现和配置校验
* TDengine 类型映射
* identifier 归一化和校验
* DDL 和 INSERT SQL 构造
* row converter 字段拆分和语义校验
* writer 自动 flush、SQL 大小切分、跨 stable 部分 flush 重试、CDC 事件拒绝策略
* supertable schema 校验
* MetadataApplier schema evolution
* HashFunctionProvider 子表归一化路由稳定性

集成测试（需要 Docker）：

```bash
mvn -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-tdengine test -Dtest=TDengineSinkITCase
```

`TDengineSinkITCase` 使用 Testcontainers 启动 `tdengine/tsdb`，覆盖建库建表、写入查询、
schema 漂移拒绝和 ADD COLUMN。

真实 TDengine 环境验证建议：

1. 启动 TDengine，并确认 WebSocket 端口 `6041` 可访问。
2. 使用上面的完整 Pipeline 示例写入测试数据。
3. 在 TDengine 中查询：

```sql
SHOW DATABASES;
USE power;
SHOW STABLES;
SELECT * FROM meters;
```

4. 重启 Flink job 后重放同一批数据，确认相同 subtable + timestamp 的数据可收敛。
