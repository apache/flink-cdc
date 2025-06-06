---
title: "MySQL"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/mysql
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

# Postgres Connector

Postgres CDC Pipeline 连接器允许从 Postgres 数据库读取快照数据和增量数据，并提供端到端的整库数据同步能力。 本文描述了如何设置 Postgres CDC Pipeline 连接器。

## 示例

从 Postgres 读取数据同步到 Doris 的 Pipeline 可以定义如下：

```yaml
source:
   type: posgtres
   name: Postgres Source
   hostname: 127.0.0.1
   port: 3306
   username: admin
   password: pass
   tables: adb.\.*.\.*, bdb.user_schema_[0-9].user_table_[0-9]+, [app|web].schema_\.*.order_\.*
   decoding.plugin.name:  pgoutput
   slot.name: pgtest

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
   name: Postgres to Doris Pipeline
   parallelism: 4
```

## 连接器配置项

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> Postgres 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td>Postgres 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 Postgres 数据库服务器时要使用的 Postgres 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 Postgres 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 Postgres 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。<br>
          需要注意的是，点号（.）被视为数据库和表名的分隔符。 如果需要在正则表达式中使用点（.）来匹配任何字符，必须使用反斜杠对点进行转义。<br>
          例如，adb.\.*.\.*, bdb.user_schema_[0-9].user_table_[0-9]+, [app|web].schema_\.*.order_\.*</td>
    </tr>
    <tr>
      <td>slot.name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>为从特定插件以流式传输方式获取某个数据库/模式的变更数据，所创建的 Postgre 逻辑解码槽（logical decoding slot）的名称。服务器使用这个槽（slot）将事件流式传输给你要配置的连接器（connector）。
          <br/>复制槽名称必须符合 <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL 复制插槽的命名规则</a>, 其规则如下: "Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character."</td>
    </tr> 
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要排除的 Postgres 数据库的表名，参数会在tables参数后发生排除作用。表名支持正则表达式，以排除满足正则表达式的多个表。<br>
          用法和tables参数相同</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送模式更改事件，下游 sink 可以响应模式变更事件实现表结构同步，默认为true。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> Postgres CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"latest-offset"，"committed-offset"和 ""snapshot"。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-events</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的事件数量。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的数据行数量。</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>连接器在尝试连接到 Postgres 数据库服务器后超时前应等待的最长时间。该时长不能少于250毫秒。</td>
    </tr>    
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>连接器应重试以建立 Postgres 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>String</td>
      <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
      <td>heartbeat.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>用于跟踪最新可用 binlog 偏移的发送心跳事件的间隔。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 MySQL 服务器捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties"> Debezium 的  MySQL 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。<br>
          若 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 默认值变更为 true，可以不用显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = true。</td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否启用动态加表特性，默认关闭。 此配置项只有作业从savepoint/checkpoint启动时才生效。</td>
    </tr>
    <tr>
      <td>scan.binlog.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>在 binlog 读取阶段，是否读取新增表的表结构变更和数据变更，默认值是 false。 <br>
          scan.newly-added-table.enabled 和 scan.binlog.newly-added-table.enabled 参数的不同在于: <br>
          scan.newly-added-table.enabled: 在作业重启后，对新增表的全量和增量数据进行读取; <br>
          scan.binlog.newly-added-table.enabled: 只在 binlog 读取阶段读取新增表的增量数据。
      </td>
    </tr>
    <tr>
      <td>scan.parse.online.schema.changes.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否尝试解析由 <a href="https://github.com/github/gh-ost">gh-ost</a> 或 <a href="https://docs.percona.com/percona-toolkit/pt-online-schema-change.html">pt-osc</a> 工具生成的表结构变更事件。
        这些工具会在变更表结构时，将变更语句应用到“影子表”之上，并稍后将其与主表进行交换，以达到表结构变更的目的。
        <br>
        这是一项实验性功能。
      </td>
    </tr>
    <tr>
      <td>include-comments.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否启用同步表、字段注释特性，默认关闭。注意：开启此特性将会对内存使用产生影响。</td>
    </tr>
    <tr>
      <td>treat-tinyint1-as-boolean.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否将TINYINT(1)类型当做Boolean类型处理，默认true。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        快照读取阶段是否先分配 UnboundedChunk。<br>
        这有助于降低 TaskManager 在快照阶段同步最后一个chunk时遇到内存溢出 (OOM) 的风险。<br> 
        这是一项实验特性，默认为 false。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br>
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 binlog 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 binlog 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 binlog 事件应进行特殊处理。
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td>
        可额外读取的SourceRecord中元数据的列表，后续可直接使用在transform模块，英文逗号 `,` 分割。目前可用值包含：op_ts。
      </td>
    </tr>
    </tbody>
</table>
</div>

## 启动模式

配置选项`scan.startup.mode`指定 Postgres CDC 使用者的启动模式。有效枚举包括：
- `initial` （默认）：在连接器首次启动时，会对被监控的数据库表执行一次初始快照（snapshot），然后继续读取复制插槽中的变更事件。。
- `latest-offset`：在首次启动时不执行快照操作，而是直接从复制插槽的末尾开始读取，即只获取连接器启动之后发生的变更。
- `committed-offset`：跳过快照阶段，从复制插槽中已确认的 confirmed_flush_lsn 偏移量开始读取事件，即从上次提交的位置继续读取变更。
- `snapshot`: 只进行快照阶段，跳过增量阶段，快照阶段读取结束后退出。

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

## 数据类型映射


<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
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
      <td>
        CHAR(n)<br>
        CHARACTER(n)<br>
        VARCHAR(n)<br>
        CHARACTER VARYING(n)<br>
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

### 空间数据类型映射
暂不支持

{{< top >}}