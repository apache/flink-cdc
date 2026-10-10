---
title: "Postgres"
weight: 5
type: docs
aliases:
- /connectors/flink-sources/postgres-cdc
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

# Postgres CDC 连接器

Postgres CDC 连接器允许从 PostgreSQL 数据库读取快照数据和增量数据。本文描述了如何设置 Postgres CDC 连接器来对 PostgreSQL 数据库运行 SQL 查询。

## 依赖

为了设置 Postgres CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-postgres-cdc >}}

### SQL Client JAR

```下载链接仅适用于稳定版本。```

下载 [flink-sql-connector-postgres-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-postgres-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-postgres-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-postgres-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

## 如何创建 Postgres CDC 表

Postgres CDC 表可以定义如下：

```sql
-- register a PostgreSQL table 'shipments' in Flink SQL
CREATE TABLE shipments (
  shipment_id INT,
  order_id INT,
  origin STRING,
  destination STRING,
  is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink',
   -- experimental feature: incremental snapshot (default off)
  'scan.incremental.snapshot.enabled' = 'true'
);

-- read snapshot and binlogs from shipments table
SELECT * FROM shipments;
```

## 连接器配置项

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器, 这里应该是 <code>'postgres-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>PostgreSQL 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 PostgreSQL 数据库服务器时要使用的 PostgreSQL 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 PostgreSQL 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 PostgreSQL 服务器的数据库名称。</td>
    </tr>
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 PostgreSQL 数据库的 Schema 名称。Schema 名称还支持正则表达式，以监视满足正则表达式的多个 schema。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 PostgreSQL 数据库的表名。表名还支持正则表达式，以监视满足正则表达式的多个表。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>PostgreSQL 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>slot.name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>为从特定插件以流式传输方式获取某个数据库/模式的变更数据，所创建的 Postgres 逻辑解码槽（logical decoding slot）的名称。服务器使用这个槽（slot）将事件流式传输给你要配置的连接器（connector）。
          <br/>复制槽名称必须符合 <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL 复制槽的命名规则</a>, 其规则如下: "Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character."</td>
    </tr> 
    <tr>
      <td>decoding.plugin.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">decoderbufs</td>
      <td>String</td>
      <td>安装在服务器上的 Postgres 逻辑解码插件的名称。
          支持的值为 decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming 和 pgoutput。</td>
    </tr>
    <tr>
      <td>changelog-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">all</td>
      <td>String</td>
      <td>用于编码流式变更的 changelog 模式。支持的值为 <code>all</code>（使用所有 RowKind 将变更编码为回撤流）和 <code>upsert</code>（将变更编码为描述针对某个键的幂等更新的 upsert 流）。
          <br/> 当副本标识（replica identity）无法设置为 <code>FULL</code> 时，<code>upsert</code> 模式可用于有主键的表。使用 <code>upsert</code> 模式必须设置主键。</td>
    </tr>
    <tr>
      <td>heartbeat.interval.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>用于跟踪最新可用复制槽位点的发送心跳事件的间隔。</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 Postgres 服务器捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-connector-properties"> Debezium 的  Postgres 连接器属性</a></td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>如果你遇到表中数据量很大，而你不需要所有历史数据的情况，可以尝试在 debezium 中指定底层配置来选择你想要快照的数据范围。该参数只影响快照，不影响后续的数据读取消费。
        <br/> 注意: PostgreSQL 必须使用 schema 名称和表名。
        <br/> 例如: <code>'debezium.snapshot.select.statement.overrides' = 'schema.table'</code>.
        <br/> 指定上述属性后，你还必须添加以下属性:
        <code> debezium.snapshot.select.statement.overrides.[schema].[table] </code>
      </td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides.[schema].[table]</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>你可以指定 SQL 语句来限制快照的数据范围。
        <br/> 注意1: SQL 语句中需要指定 schema 和表，且 SQL 应符合数据源的语法。
        <br/> 例如: <code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where 1 != 1'</code>.
        <br/> 注意2: Flink SQL 客户端提交的任务不支持内容中带单引号的函数。
        <br/> 例如: <code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where to_char(rq, 'yyyy-MM-dd')'</code>.
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">false</td>
          <td>Boolean</td>
          <td>增量快照是一种读取表快照的新机制，与旧的快照机制相比，
              增量快照有许多优点，包括：
                <br/>（1）在快照读取期间，Source 支持并发读取，
                <br/>（2）在快照读取期间，Source 支持进行 chunk 粒度的 checkpoint，
                <br/>（3）在快照读取之前，Source 不需要获取全局读锁（FLUSH TABLES WITH READ LOCK）。
              <br/>请查阅 <a href="#增量快照读取实验性">增量快照读取</a> 章节了解更多详细信息。
          </td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束后关闭空闲的 Reader。 <br>
          当 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 设置为 true 时，要求 flink 版本大于等于 1.14。<br>
          如果 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 的默认值已变更为 true，
          因此不需要显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.metadata.release.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在 source 进入增量阶段后，释放 source coordinator 持有的快照分片元数据（已分配的分片、已完成分片的位点以及表结构），以降低快照分片数量非常大的作业的 JobManager 内存占用。默认关闭。与 scan.newly-added-table.enabled 不兼容：同时开启两者会导致作业启动失败，且已释放元数据的作业无法再开启动态加表功能。仅在成功完成一次 checkpoint 后才会释放；若未开启 checkpoint 或没有 checkpoint 完成，则会保留该元数据，因此该配置项在未开启 checkpoint 时不生效。开启该配置项后生成的 checkpoint 或 savepoint，无法在降级到 Flink CDC 3.6.0 及更早版本后用于恢复作业。</td>
    </tr>
    <tr>
      <td>scan.lsn-commit.checkpoints-num-delay</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>在开始提交 LSN 位点之前，允许延迟的 checkpoint 次数。 <br>
          checkpoint 的 LSN 位点将以滚动方式提交，最早的那个 checkpoint 标识符将首先从延迟的 checkpoint 中被提交。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
        快照读取阶段是否先分配 UnboundedChunk。<br>
        这可能有助于降低 TaskManager 在对最大的 UnboundedChunk 执行快照时出现内存溢出 (OOM) 错误的风险。<br> 
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br> 
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 changelog 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 changelog 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 changelog 事件应进行特殊处理。
      </td>
    </tr>
    <tr>
      <td>scan.read-changelog-as-append-only.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否将 changelog 数据流转换为 append-only 数据流。<br>
        仅在需要保存上游表删除消息等特殊场景下开启使用，比如在逻辑删除场景下，用户不允许物理删除下游消息，此时使用该特性，并配合 row_kind 元数据字段，下游可以先保存所有明细数据，再通过 row_kind 字段判断是否进行逻辑删除。<br>
        参数取值如下：<br>
          <li>true：所有类型的消息（包括INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER）都会转换成 INSERT 类型的消息。</li>
          <li>false（默认）：所有类型的消息都保持原样下发。</li>
      </td>
    </tr>
    <tr>
      <td>scan.include-partitioned-tables.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否启用通过分区根读取分区表。<br>
        如果启用：
          （1）必须事先创建 PUBLICATION，并带有参数 publish_via_partition_root=true
          （2）表列表（正则表达式或预定义列表）应当只匹配父表名，如果表列表同时匹配父表和子表，快照数据将被读取两次。
      </td>
    </tr>
    <tr>
      <td>scan.pre-epoch-timestamp.wall-clock-conversion.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否对 1970-01-01（epoch）之前的 PostgreSQL "timestamp without time zone" 列值，按照数据库中存储的日期时间（wall clock）进行转换。<br>
        开启后，该值不会受运行任务的 JVM 时区影响，这在存在历史时区偏移的时区下很重要：当 JVM 时区为 Asia/Shanghai 时，
        数据库中存储的 "1900-01-01 00:00:00.123" 否则会被读取为 "1900-01-01 00:05:43.123"。<br>
        关闭（默认值）时保持原有的转换行为。1970-01-01 及之后的值无论是否开启该选项都会被同样地转换，
        且该选项不会改变列的数据类型。<br>
        该选项仅在 <code>scan.incremental.snapshot.enabled</code> 为 true 时生效，因为非增量快照源的数据值由 Debezium 自身转换。<br>
        使用 DataStream API 时，可以通过在 Debezium 参数中添加 "scan.pre-epoch-timestamp.wall-clock-conversion.enabled" 来开启该行为。
      </td>
    </tr>
    <tr>
      <td>records.per.second</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Double</td>
      <td>
        每秒发出的最大记录数，默认值为 -1，表示不进行速率限制。（仅适用于 flink 2.x）<br>
        警告：增量/二进制日志阶段：速率限制可能导致连接器落后于上游变更流，在连接器赶上之前，二进制日志/WAL 可能会被清除（MySQL 数据丢失，PostgreSQL 复制槽问题）。
      </td>
    </tr>
    </tbody>
    </table>
</div>
<div>

### 注意事项

#### `slot.name` 选项

建议为不同的表设置不同的 `slot.name`，以避免潜在的 `PSQLException: ERROR: replication slot "flink" is active for PID 974` 错误。更多信息请参阅 [这里](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-property-slot-name)。

#### `scan.lsn-commit.checkpoints-num-delay` 选项

在消费 PostgreSQL 日志时，必须提交 LSN 位点以触发对应复制槽的日志数据清理。然而，一旦 LSN 位点被提交，更早的位点就会失效。为了确保作业恢复时能够访问更早的 LSN 位点，我们将 LSN 的提交延迟 `scan.lsn-commit.checkpoints-num-delay`（默认值为 `3`）个 checkpoint。该特性在配置选项 `scan.incremental.snapshot.enabled` 设置为 true 时可用。

### 增量快照选项

以下选项仅在 `scan.incremental.snapshot.enabled=true` 时可用：

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>表快照的分片大小（行数），读取表的快照时，捕获的表被拆分为多个分片。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Postgres CDC 消费者可选的启动模式，合法的模式为 "initial"，"latest-offset"，"committed-offset" 和 "snapshot"。
           请查阅 <a href="#启动模式">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>分片元数据的分组大小，如果元数据大小超过分组大小，则元数据将被划分为多个分组。</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>连接器在尝试连接到 PostgreSQL 数据库服务器后超时前应等待的最长时间。</td>
    </tr>
    <tr>
          <td>connect.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30</td>
          <td>Integer</td>
          <td>连接池大小。</td>
    </tr>
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>连接器应重试以建立 PostgreSQL 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>表快照的分片键，在读取表的快照时，被捕获的表会按分片键拆分为多个分片。
            默认情况下，分片键是主键的第一列。可以使用非主键列作为分片键，但这可能会导致查询性能下降。
          <br>
            <b>警告：</b> 使用非主键列作为分片键可能会导致数据不一致。请参阅 <a href="#警告">警告</a> 了解详细信息。
          </td>
    </tr>
    <tr>
          <td>chunk-key.even-distribution.factor.lower-bound</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">0.05d</td>
          <td>Double</td>
          <td>分片键分布因子的下界。分布因子用于判断表的数据分布是否均匀。
              当数据分布均匀时，表分片将使用均匀计算优化，当数据分布不均匀时，将通过查询进行拆分。
              分布因子可以通过 (MAX(id) - MIN(id) + 1) / rowCount 计算得出。</td>
    </tr>
    <tr>
          <td>chunk-key.even-distribution.factor.upper-bound</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1000.0d</td>
          <td>Double</td>
          <td>分片键分布因子的上界。分布因子用于判断表的数据分布是否均匀。
              当数据分布均匀时，表分片将使用均匀计算优化，当数据分布不均匀时，将通过查询进行拆分。
              分布因子可以通过 (MAX(id) - MIN(id) + 1) / rowCount 计算得出。</td>
    </tr>
    </tbody>
</table>
</div>

## 可用的元数据

下表中的元数据可以在 DDL 中作为只读（虚拟）meta 列声明。

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的表名称。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的 schema 名称。</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是更改流读取记录，该值将始终为0。</td>
    </tr>
    <tr>
      <td>row_kind</td>
      <td>STRING NOT NULL</td>
      <td>当前记录的变更类型。<br>
         注意：如果 Source 算子选择为每条记录输出 row_kind 列，则下游 SQL 操作符在处理回撤时可能会由于此新添加的列而无法比较。建议仅在简单的同步作业中使用此元数据列。<br>
         '+I' 表示 INSERT 消息，'-D' 表示 DELETE 消息，'-U' 表示 UPDATE_BEFORE 消息，'+U' 表示 UPDATE_AFTER 消息。</td>
    </tr>
  </tbody>
</table>

## 限制

### 增量快照关闭时，无法在扫描表快照期间执行 checkpoint

当 `scan.incremental.snapshot.enabled=false` 时，存在以下限制。

在扫描数据库表的快照期间，由于没有可恢复的位点，我们无法执行 checkpoint。为了不执行 checkpoint，Postgres CDC source 会让 checkpoint 一直等待直至超时。超时的 checkpoint 将被认定为失败的 checkpoint，默认情况下，这会触发 Flink 作业的故障转移。因此，如果数据库表很大，建议添加以下 Flink 配置，以避免因 checkpoint 超时而触发故障转移：

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    shipment_id INT,
    order_id INT,
    origin STRING,
    destination STRING,
    is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink'
);
```

## 特性

### 增量快照读取（实验性）

增量快照读取是一种读取表快照的新机制。与旧的快照机制相比，增量快照具有许多优点，包括：
* （1）在快照读取期间，PostgreSQL CDC Source 支持并发读取
* （2）在快照读取期间，PostgreSQL CDC Source 支持进行 chunk 粒度的 checkpoint
* （3）在快照读取之前，PostgreSQL CDC Source 不需要获取全局读锁

在增量快照读取过程中，PostgreSQL CDC Source 首先会根据用户指定的表分片键将快照切分为多个分片（splits），
然后 PostgreSQL CDC Source 将这些分片分配给多个 reader，以读取快照分片的数据。

### Exactly-Once 处理

Postgres CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后继续读取 binlog，即使在处理时出现故障，也能**准确读取数据**。请参阅 [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#how-the-postgresql-connector-works)。

### 启动模式

配置选项`scan.startup.mode`指定 PostgreSQL CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取复制槽。
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从复制槽的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
- `committed-offset`：跳过快照阶段，从复制槽的 `confirmed_flush_lsn` 位点开始读取事件。
- `snapshot`：仅执行快照阶段，并在快照阶段读取完成后退出。

### 动态加表

**注意:** 该功能从 Flink CDC 3.1.0 版本开始支持。

动态加表功能使你可以为正在运行的作业添加新表进行监控。新添加的表将首先读取其快照数据,然后自动读取其 WAL (Write-Ahead Log) 日志 或者 replication slot changes 复制槽。

想象一下这个场景:一开始,Flink 作业监控表 `[product, user, address]`,但几天后,我们希望这个作业还可以监控表 `[order, custom]`,这些表包含历史数据,我们需要作业仍然可以复用作业的已有状态。动态加表功能可以优雅地解决此问题。

以下操作显示了如何启用此功能来解决上述场景。使用现有的 PostgreSQL CDC Source 作业,如下:

```java
    JdbcIncrementalSource<String> postgresSource =
            PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                .hostname("yourHostname")
                .port(5432)
                .database("postgres") // 设置捕获的数据库
                .schemaList("inventory") // 设置捕获的 schema
                .tableList("inventory.product", "inventory.user", "inventory.address") // 设置捕获的表
                .username("yourUsername")
                .password("yourPassword")
                .slotName("flink")
                .scanNewlyAddedTableEnabled(true) // 启用扫描新添加的表功能
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
   // 你的业务代码
```

如果我们想添加新表 `[inventory.order, inventory.custom]` 到现有的 Flink 作业,只需更新作业的 `tableList()` 将新增表 `[inventory.order, inventory.custom]` 加入并从已有的 savepoint 恢复作业。

_Step 1_: 使用 savepoint 停止现有的 Flink 作业。
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_Step 2_: 更新现有 Flink 作业的表列表选项。
1. 更新 `tableList()` 参数。
2. 编译更新后的作业,示例如下:
```java
    JdbcIncrementalSource<String> postgresSource =
            PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                .hostname("yourHostname")
                .port(5432)
                .database("postgres")
                .schemaList("inventory")
                .tableList("inventory.product", "inventory.user", "inventory.address", "inventory.order", "inventory.custom") // 设置捕获的表 [product, user, address, order, custom]
                .username("yourUsername")
                .password("yourPassword")
                .slotName("flink")
                .scanNewlyAddedTableEnabled(true)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
   // 你的业务代码
```
_Step 3_: 从 savepoint 还原更新后的 Flink 作业。
```shell
$ ./bin/flink run \
      --detached \
      --from-savepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**注意:** 请参考文档 [Restore the job from previous savepoint](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/#command-line-interface) 了解更多详细信息。

### DataStream Source

Postgres CDC 连接器也可以是一个数据流源。DataStream 源有两种模式：

- 基于增量快照，允许并行读取
- 基于 SourceFunction，仅支持单线程读取

#### 基于增量快照的 DataStream（实验性）

```java
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresParallelSourceExample {

    public static void main(String[] args) throws Exception {

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("inventory")
                        .tableList("inventory.products")
                        .username("postgres")
                        .password("postgres")
                        .slotName("flink")
                        .decodingPluginName("decoderbufs") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserializer)
                        .splitSize(2) // the split size of each snapshot split
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();

        env.execute("Output Postgres Snapshot");
    }
}
```

#### 基于 SourceFunction 的 DataStream

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;

public class PostgreSQLSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
      .hostname("localhost")
      .port(5432)
      .database("postgres") // monitor postgres database
      .schemaList("inventory")  // monitor inventory schema
      .tableList("inventory.products") // monitor products table
      .username("flinkuser")
      .password("flinkpw")
      .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute();
  }
}
```

### 可用的指标

指标系统能够帮助了解分片分发的进展， 下面列举出了支持的 Flink 指标 [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description    |
|------------------------|----------------------------|-------|----------------|
| namespace.schema.table | isSnapshotting             | Gauge | 表是否在快照读取阶段     |     
| namespace.schema.table | isStreamReading            | Gauge | 表是否在增量读取阶段     |
| namespace.schema.table | numTablesSnapshotted       | Gauge | 已经被快照读取完成的表的数量 |
| namespace.schema.table | numTablesRemaining         | Gauge | 还没有被快照读取的表的数据  |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | 正在处理的分片的数量     |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | 还没有被处理的分片的数量   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | 已经处理完成的分片的数据   |
| namespace.schema.table | snapshotStartTime          | Gauge | 快照读取阶段开始的时间    |
| namespace.schema.table | snapshotEndTime            | Gauge | 快照读取阶段结束的时间    |

注意:
1. Group 名称是 `namespace.schema.table`，这里的 `namespace` 是实际的数据库名称， `schema` 是实际的 schema 名称， `table` 是实际的表名称。
2. 对于 PostgreSQL，Group 的名称会类似于 `test_database.test_schema.test_table`。

### 关于无主键表

从3.4.0 版本开始支持无主键表，使用无主键表必须设置 `scan.incremental.snapshot.chunk.key-column`，且只能选择非空类型的一个字段。

在使用无主键表时，需要注意以下两种情况。

1. 配置 `scan.incremental.snapshot.chunk.key-column` 时，如果表中存在索引，请尽量使用索引中的列来加快 select 速度。
2. 无主键表的处理语义由 `scan.incremental.snapshot.chunk.key-column` 指定的列的行为决定：
* 如果指定的列不存在更新操作，此时可以保证 Exactly once 语义。
* 如果指定的列存在更新操作，此时只能保证 At least once 语义。但可以结合下游，通过指定下游主键，结合幂等性操作来保证数据的正确性。

#### 警告

在 Postgres 表中，若使用 **非主键列** 作为有主键表的 `scan.incremental.snapshot.chunk.key-column`，可能导致**数据不一致**。以下为可能出现的问题及其缓解方案。

#### 问题场景

- **表结构：**
    - **主键：** `id`
    - **分片键列 ：** `pid`（非主键）

- **快照分片 ：**
    - **分片 0:** `1 < pid <= 3`
    - **分片 1:** `3 < pid <= 5`

- **操作 ：**
    - 两个子任务并行读取 **分片 0** 和 **分片 1**。
    - 在读取过程中，发生了一次 **更新** 操作，使 `id=0` 的 `pid` 从 `2` 变为 `4`，在两个分片的**高低水位**间都包含此次变更，导致该更新操作在增量阶段不会被处理。

- **结果 ：**
    - **分片 0:** 记录 `[id=0, pid=2]`
    - **分片 1:** 记录 `[id=0, pid=4]`

由于**处理顺序**无法保证，最终 `id=0` 的 `pid` 可能为 `2` 或 `4`，从而导致数据不一致。

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        INT2<br>
        SMALLSERIAL<br>
        SERIAL2</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INTEGER<br>
        SERIAL</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        BIGSERIAL</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td></td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>
        REAL<br>
        FLOAT4</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        FLOAT8<br>
        DOUBLE PRECISION</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [ (p) ] WITH TIME ZONE</td>
      <td>TIMESTAMP_LTZ(p)</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        CHARACTER(n)<br>
        VARCHAR(n)<br>
        CHARACTER VARYING(n)<br>
        UUID<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BYTEA</td>
      <td>BYTES</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
