---
title: "DB2"
slug: "/docs/connectors/pipeline-connectors/db2"
sidebar_position: 3
hide_title: true
displayed_sidebar: docs
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
- 每个被捕获表必须设置 `CHG_UPD_TO_DEL_INS='Y'`。不支持直接写入的 `IBMSNAP_OPERATION='U'` 记录，详见下文更新数据的前置要求。
- 在 `tables` 中，每个表匹配模式使用 `schema.table` 格式，且 `database` 选项必须是单个固定的数据库名。

## 依赖安装

IBM DB2 JDBC 驱动的 IPLA 许可证不符合 Apache Flink CDC 的分发要求，因此连接器 JAR 不包含该驱动。请按照适用的 IBM 许可条款单独获取 [db2jcc4](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4)。

将 DB2 Pipeline 连接器 JAR 和所需的 Sink 连接器 JAR 放入 `<FLINK_CDC_HOME>/lib/`。将 DB2 JDBC 驱动放入提交客户端以及每个 JobManager、TaskManager 的 `<FLINK_HOME>/lib/`，安装后重启 Flink 集群。使用容器部署时，这些进程使用的 Flink 镜像都需要包含驱动。

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
      <td>是否转发底层源报告的表结构变更事件。此选项不会开启 DB2 的运行期 DDL 发现。初始表结构仍通过 CreateTableEvent 事件发送，参见「限制」一节。</td>
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
      <td>通过 Debezium 的 database 配置透传 IBM DB2 JDBC 属性。例如：<code>jdbc.properties.currentSchema=DB2INST1</code>。显式配置的 <code>debezium.database.*</code> 优先于对应的 <code>jdbc.properties.*</code> 别名。</td>
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
      <td>CHARACTER VARYING<br>GRAPHIC<br>VARGRAPHIC</td>
      <td>STRING</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>CHAR(n) FOR BIT DATA<br>VARCHAR(n) FOR BIT DATA</td>
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

### 不支持的列类型

此 Pipeline 连接器不支持 `DECFLOAT`、`XML`、`CLOB`、`DBCLOB` 和 `BLOB` 列。当前捕获和反序列化链路尚未端到端支持这些类型的结构与值转换。例如，Debezium 会忽略 JDBC 类型为 `OTHER` 的 `DECFLOAT` 列，导致数据字段与下发的表结构不一致。连接器会拒绝这些类型，避免输出不完整或转换错误的数据。

因此，当表包含这类列时，源在读取表结构阶段就会失败，并在错误信息中指明不支持的类型：

```text
Doesn't support Db2 type 'DECFLOAT', JDBC type '1111' yet.
```

### 运行期间无法发现表结构变更

DB2 源用于发现新增或重新捕获的 change table 的查询被 `AND 1=0` 禁用，因此不支持运行期 DDL 同步，包括 ALTER 后重新捕获的场景。设置 `schema-change.enabled=true` 也不会开启这一发现能力；初始 `CreateTableEvent` 仍会正常发送。

不能通过从已有 checkpoint 或 savepoint 重启来刷新 DDL 后的表结构。即使重新读取了捕获注册信息，恢复过程仍可能保留旧表结构。表结构变更需要单独规划重新初始化、新快照和下游一致性处理；此连接器不提供跨表结构变更的自动迁移保障。

### 更新数据要求 CHG_UPD_TO_DEL_INS=Y

每个被捕获表的 SQL Replication 注册必须设置 `CHG_UPD_TO_DEL_INS='Y'`，这样一次更新会被写成同一事务中相邻的 `D` 和 `I` 两条记录，连接器由这一对记录还原出更新事件。连接器不会识别直接写成 `IBMSNAP_OPERATION='U'` 的记录（即 `CHG_UPD_TO_DEL_INS='N'` 产生的记录），这类记录不会作为更新事件下发。

部署前请确认捕获注册：

```sql
SELECT CHG_UPD_TO_DEL_INS FROM ASNCDC.IBMSNAP_REGISTER
WHERE SOURCE_OWNER = 'SCHEMA_NAME' AND SOURCE_TABLE = 'TABLE_NAME';
```
