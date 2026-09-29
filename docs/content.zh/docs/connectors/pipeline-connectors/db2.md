---
title: "DB2"
weight: 3
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

DB2 CDC Pipeline 连接器允许从 DB2 数据库读取快照数据和增量数据，并提供端到端的表同步能力。 本文描述了如何设置 DB2 CDC Pipeline 连接器。

## 前置条件

- DB2 CDC 基于 DB2 的 SQL Replication 功能。必须在 DB2 服务器上启用 SQL Replication 捕获功能，并为被监控的表配置捕获表。
- 配置的 DB2 用户能够连接到服务器并读取被捕获的表。
- 在 `tables` 中，每个表匹配模式使用 `schema.table` 格式，且 `database` 选项必须是单个固定的数据库名。

## 示例

从 DB2 读取数据同步到 Doris 的 Pipeline 可以定义如下：

```yaml
source:
   type: db2
   name: DB2 Source
   hostname: 127.0.0.1
   port: 50000
   username: db2inst1
   password: 123456
   database: testdb
   # Every table pattern uses the format "schema.table".
   tables: DB2INST1.\.*
   schema-change.enabled: true

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: 123456

pipeline:
   name: DB2 to Doris Pipeline
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
      <td>DB2 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>DB2 数据库服务器的端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 DB2 数据库服务器时使用的 DB2 用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 DB2 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监控的 DB2 数据库服务器的数据库名。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监控的 DB2 表名。每个表匹配模式使用 "schema.table" 格式，并支持使用正则表达式匹配多个对象。<br>
          示例：DB2INST1.\.*、DB2INST1.user_table_[0-9]+、DB2INST1.(APP|WEB)_ORDER_\.*</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在应用 `tables` 之后要排除的 DB2 表名。每个排除模式使用 "schema.table" 格式，并支持使用正则表达式匹配多个对象。<br>
          示例：DB2INST1.audit_\.*、DB2INST1.tmp_[0-9]+</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送表结构变更事件，以便下游 Sink 同步表结构变更。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.key-column</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>表快照的分块键。默认情况下，分块键是主键的第一列。该列必须是主键列。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的分块大小（行数）。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次拉取的最大行数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>DB2 CDC 消费者的启动模式。有效值为 "initial" 和 "latest-offset"。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否跳过快照读取阶段的回填。跳过回填可能导致变更日志事件被重放，仅提供 at-least-once 语义。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否在快照读取阶段优先分配无界分块。这有助于降低 TaskManager 在对最大的无界分块做快照时出现内存溢出（OOM）错误的风险。</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>尝试连接 DB2 数据库服务器后，连接器等待的最大超时时间。</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>构建 DB2 数据库服务器连接失败后的最大重试次数。</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>从 SourceRecord 传递到下游的可读元数据列表，以逗号分隔。可用的元数据键为：database_name、schema_name、table_name、op_ts。</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束后关闭空闲的读取器。该功能依赖 FLIP-147。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>透传给 Debezium Embedded Engine 的 Debezium 属性。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>透传自定义 JDBC URL 属性。例如：<code>jdbc.properties.encrypt=false</code>。</td>
    </tr>
    </tbody>
</table>
</div>

## 可用元数据

通过在 `metadata.list` 中配置，以下元数据可以传递到下游。

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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的数据库名。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的 Schema 名。</td>
    </tr>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的表名。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>数据库中发生变更的时间。对于快照记录，该值始终为 0。</td>
    </tr>
  </tbody>
</table>

## 启动模式

配置项 `scan.startup.mode` 指定了 DB2 CDC 消费者的启动模式。有效值为：

- `initial`：对被监控的表执行初始快照，并继续读取最新变更。
- `latest-offset`：从最新的变更日志位点开始读取。

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">DB2 type<a href="https://www.ibm.com/docs/en/db2-for-zos/12?topic=language-built-in-data-types"></a></th>
        <th class="text-left" style="width:20%;">CDC type</th>
        <th class="text-left" style="width:50%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>REAL</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECFLOAT(16)</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECFLOAT(34)</td>
      <td>DECIMAL(34, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)<br>NUMERIC(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td>当精度信息不可用时，回退为 <code>DECIMAL(38, s)</code>。</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>CLOB<br>DBCLOB</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>XML</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>CHARACTER VARYING<br>GRAPHIC<br>VARGRAPHIC</td>
      <td>STRING</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>BINARY<br>VARBINARY<br>BLOB</td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME(p)</td>
      <td>TIME(p)</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP(p)</td>
      <td>TIMESTAMP(p)</td>
      <td>未指定精度时默认为 6。</td>
    </tr>
    </tbody>
</table>
</div>

## 限制

### 单数据库

`database` 选项必须是单个固定的数据库名，不支持跨数据库的数据捕获。在 `tables` 中，每个表匹配模式使用 `schema.table` 格式，schema 和 table 支持使用正则表达式匹配多个对象。

{{< top >}}
