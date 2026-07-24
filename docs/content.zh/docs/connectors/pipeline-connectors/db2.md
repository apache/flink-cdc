---
title: "DB2"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/db2
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DB2 Connector

DB2 CDC Pipeline 连接器支持从 DB2 数据库读取快照数据和增量变更数据。该连接器基于 DB2 CDC Source 连接器，并向 Flink CDC Pipeline 输出变更事件。

## 依赖

IBM DB2 JDBC driver 使用 IBM IPLA 协议，该协议与 Flink CDC 项目不兼容。
Flink CDC 不会将该驱动打包进 DB2 Pipeline 连接器 jar。
提交 YAML Pipeline 作业时，需要手动配置以下依赖，并通过 Flink CDC CLI 的 `--jar` 参数传入。

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">依赖名称</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4">com.ibm.db2.jcc:db2jcc:db2jcc4</a></td>
        <td>用于连接 DB2 数据库。</td>
      </tr>
    </tbody>
</table>
</div>

## 前置条件

使用 DB2 Pipeline 连接器前，请确认：

1. DB2 已配置 CDC 采集，目标表已启用 DB2 change capture。
2. Flink 集群可以通过 TCP 访问 DB2 服务。
3. 配置的用户具备读取目标表和 DB2 CDC 元数据表的权限。
4. 同一个 Pipeline Source 中的所有表属于同一个 DB2 数据库，`tables` 选项使用 `database.schema.table` 格式。

### 在 DB2 侧开启 CDC

DB2 Pipeline 连接器不会自动创建 DB2 SQL Replication 相关对象。提交 Pipeline 作业前，需要由 DB2 实例用户或数据库管理员先完成 DB2 侧 CDC 配置。完整上游流程可参考 [Debezium DB2 connector 文档中的 Setting up Db2](https://debezium.io/documentation/reference/stable/connectors/db2.html#setting-up-db2)。

1. 为源数据库启用 DB2 SQL Replication，并开启归档日志或日志保留。容器化 DB2 环境通常需要在创建数据库前完成该配置，例如连接器测试镜像通过 `ARCHIVE_LOGS=true` 启动 DB2。修改日志模式后，需要执行一次完整数据库备份，并重启或激活数据库，确保 ASN capture 服务有可用的日志起点。
2. 安装 Debezium DB2 管理 UDF，或创建等价的 DB2 SQL Replication 控制对象。UDF 安装流程会编译 `asncdc`，将其安装到 DB2 function 目录，执行 `db2 bind db2schema.bnd blocking all grant public sqlerror continue` 绑定 catalog 访问权限，并创建 `ASNCDC` schema、元数据表，以及 `ASNCDC.ADDTABLE`、`ASNCDC.REMOVETABLE`、`ASNCDC.ASNCDCSERVICES` 等辅助例程。
3. 启动 ASN capture 服务并确认服务运行正常：

   ```sql
   VALUES ASNCDC.ASNCDCSERVICES('start','asncdc');
   VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');
   ```

4. 将每张源表加入 capture mode，然后重新初始化 ASN 服务。请将示例中的 schema 和表名替换为实际源表标识：

   ```sql
   CALL ASNCDC.ADDTABLE('DB2INST1', 'ORDERS');
   VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
   ```

5. 确认源表已经注册，并且存在对应的 change table：

   ```sql
   SELECT SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE, STATE
   FROM ASNCDC.IBMSNAP_REGISTER
   WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = 'ORDERS';
   ```

6. 为 Flink CDC 运行用户授予源表、ASNCDC 元数据表和对应 change table 的读取权限：

   ```sql
   GRANT SELECT ON TABLE DB2INST1.ORDERS TO USER FLINK_USER;
   GRANT SELECT ON TABLE ASNCDC.IBMSNAP_REGISTER TO USER FLINK_USER;
   GRANT SELECT ON TABLE ASNCDC.CDC_DB2INST1_ORDERS TO USER FLINK_USER;
   ```

如果 DB2 标识符创建时未使用引号，DB2 会以大写形式保存 schema 和表名。`ASNCDC.ADDTABLE` 与 Pipeline `tables` 选项都应使用 DB2 中保存的名称，例如 `TESTDB.DB2INST1.ORDERS`。

DB2 SQL Replication 不支持捕获包含 `BOOLEAN` 列的表。这类表可以读取快照，但无法获得增量 CDC 记录；如需增量同步，应将 `BOOLEAN` 替换为 SQL Replication 支持的类型。生产环境启用前，请先确认 IBM 与 Debezium 对 SQL Replication 的许可要求。

## 示例

从 DB2 读取数据同步到 Fluss 的 Pipeline 可以定义如下：

```yaml
source:
  type: db2
  name: DB2 Source
  hostname: 127.0.0.1
  port: 50000
  username: db2inst1
  password: pass
  tables: TESTDB.DB2INST1.ORDERS
  schema-change.enabled: true
  metadata.list: op_ts,table_name,database_name,schema_name

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123

pipeline:
  name: DB2 to Fluss Pipeline
  parallelism: 4
```

## 连接器配置项

| Option | Required | Default | Type | Description |
|--------|----------|---------|------|-------------|
| hostname | required | (none) | String | DB2 数据库服务器的 IP 地址或主机名。 |
| port | optional | 50000 | Integer | DB2 数据库服务器端口号。 |
| username | required | (none) | String | 连接 DB2 数据库服务器时使用的用户名。 |
| password | required | (none) | String | 连接 DB2 数据库服务器时使用的密码。 |
| tables | required | (none) | String | 需要监控的 DB2 表，支持正则表达式。表名格式为 `database.schema.table`。点号会作为数据库、Schema 和表名分隔符；如需在正则表达式中使用点号匹配任意字符，请使用反斜杠转义。 |
| tables.exclude | optional | (none) | String | 在 `tables` 匹配结果中排除的 DB2 表，格式与 `tables` 相同。 |
| schema-change.enabled | optional | true | Boolean | 是否发送 Schema 变更事件，以便下游 Sink 响应表结构变更。 |
| server-time-zone | optional | UTC | String | 数据库服务器会话时区。 |
| scan.startup.mode | optional | initial | String | DB2 CDC 消费者的启动模式。有效值为 `initial` 和 `latest-offset`。 |
| scan.incremental.snapshot.chunk.size | optional | 8096 | Integer | 快照读取阶段切分表数据时使用的分片行数。 |
| scan.snapshot.fetch.size | optional | 1024 | Integer | 读取表快照时每次拉取的最大行数。 |
| scan.incremental.snapshot.chunk.key-column | optional | (none) | String | 表快照分片使用的 chunk key 列。默认使用第一个主键列。 |
| scan.incremental.snapshot.backfill.skip | optional | true | Boolean | 是否跳过快照读取阶段的 backfill。跳过 backfill 后，快照期间发生的变更可能在增量阶段回放，下游需要正确处理 at-least-once 事件。 |
| scan.incremental.close-idle-reader.enabled | optional | false | Boolean | 是否在快照阶段结束后关闭空闲 reader。该功能依赖任务结束后的 checkpoint 支持。 |
| connect.timeout | optional | 30s | Duration | 建立 DB2 连接时的最大等待时间。 |
| connect.max-retries | optional | 3 | Integer | 建立 DB2 数据库连接的最大重试次数。 |
| connection.pool.size | optional | 20 | Integer | 连接池大小。 |
| metadata.list | optional | (none) | String | 从 SourceRecord 读取并传递给下游、可在 transform 表达式中使用的元数据字段列表，多个字段用逗号分隔。可用字段包括：`op_ts`、`table_name`、`database_name` 和 `schema_name`。 |
| jdbc.properties.* | optional | (none) | String | 传递给 DB2 JDBC 连接的自定义 JDBC URL 属性。 |
| debezium.* | optional | (none) | String | 传递给内嵌 DB2 Debezium connector 的 Debezium 参数。 |

## 启动模式

`scan.startup.mode` 选项指定 DB2 CDC 从哪里开始读取：

- `initial`（默认）：先对监控表执行初始快照，然后继续读取 DB2 变更流。
- `latest-offset`：跳过快照阶段，直接从 DB2 变更流末尾开始读取。

## 支持的元数据列

DB2 CDC Pipeline 连接器支持从源记录中读取元数据列。这些元数据列可以在 transform 操作中使用，也可以传递给下游 Sink。

部分元数据信息也可以通过 Transform 表达式获取，例如 `__namespace_name__`、`__schema_name__` 和 `__table_name__`。`op_ts` 只能通过 `metadata.list` 获取，表示数据库中的操作时间戳。

```yaml
source:
  type: db2
  # ... 其他配置
  metadata.list: op_ts,table_name,database_name,schema_name
```

| 元数据列 | 数据类型 | 描述 |
|----------|----------|------|
| op_ts | BIGINT NOT NULL | 变更事件在数据库中发生的时间戳，单位为从 epoch 开始的毫秒数。对于快照记录，该值为 0。 |
| table_name | STRING NOT NULL | 包含变更行的表名。 |
| database_name | STRING NOT NULL | 包含变更行的数据库名。 |
| schema_name | STRING NOT NULL | 包含变更行的 Schema 名称。 |

## 可用的指标

指标可以帮助了解分片分发进度，支持的 Flink 指标如下：

| Group | Name | Type | Description |
|-------|------|------|-------------|
| namespace.schema.table | isSnapshotting | Gauge | 表是否处于快照读取阶段 |
| namespace.schema.table | isStreamReading | Gauge | 表是否处于增量读取阶段 |
| namespace.schema.table | numTablesSnapshotted | Gauge | 已完成快照读取的表数量 |
| namespace.schema.table | numTablesRemaining | Gauge | 尚未完成快照读取的表数量 |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | 正在处理的快照分片数量 |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | 尚未处理的快照分片数量 |
| namespace.schema.table | numSnapshotSplitsFinished | Gauge | 已处理完成的快照分片数量 |
| namespace.schema.table | snapshotStartTime | Gauge | 快照读取阶段开始时间 |
| namespace.schema.table | snapshotEndTime | Gauge | 快照读取阶段结束时间 |

Group 名称是 `namespace.schema.table`，其中 `namespace` 是 DB2 数据库名，`schema` 是 DB2 Schema 名称，`table` 是 DB2 表名。

{{< top >}}
