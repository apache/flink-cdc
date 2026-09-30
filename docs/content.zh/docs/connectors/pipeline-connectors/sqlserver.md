---
title: "SQL Server"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/sqlserver
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

# SQL Server Connector

SQL Server CDC Pipeline 连接器支持从 SQL Server 数据库读取快照数据和增量数据，并提供端到端的表数据同步能力。本文描述如何设置 SQL Server CDC Pipeline 连接器。

## 前置条件

- SQL Server Agent 正在运行。
- 已为数据库和需要捕获的表开启 Change Data Capture。
- 配置的 SQL Server 用户可以连接服务器并读取被捕获的表。
- 当前连接器的 `tables` 配置中，数据库只支持单个固定库名，schema 和 table 支持正则表达式匹配多个。

为数据库和表启用 CDC：

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_enable_db;
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'MyTable',
@role_name     = NULL,
@supports_net_changes = 0;
GO
```

检查表是否已启用 CDC：

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture;
GO
```

## 示例

从 SQL Server 读取数据同步到 Doris 的 Pipeline 可以定义如下：

```yaml
source:
   type: sqlserver
   name: SQL Server Source
   hostname: 127.0.0.1
   port: 1433
   username: root
   password: 123456
   # 数据库只支持单个固定库名，schema 和 table 支持正则匹配多个。
   tables: inventory.dbo.\.*
   schema-change.enabled: true

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: 123456

pipeline:
   name: SQL Server to Doris Pipeline
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
      <td>SQL Server 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>SQL Server 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 SQL Server 数据库服务器时使用的 SQL Server 用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 SQL Server 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监控的 SQL Server 表名。database只支持单个固定库名，schema 和 table 支持正则表达式匹配多个；不支持数据库级别正则表达式或跨数据库匹配。点号（.）会被视为 database、schema 和 table 名的分隔符。如果需要在正则表达式中使用点号（.）匹配任意字符，必须使用反斜杠转义。<br>
          示例：inventory.dbo.\.*、inventory.dbo.user_table_[0-9]+、inventory.dbo.(app|web)_order_\.*</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在应用 `tables` 后需要排除的 SQL Server 表名。schema 和 table 支持正则表达式匹配多个。所有排除规则必须与 `tables` 选项使用同一个固定库名。<br>
          示例：inventory.dbo.audit_\.*、inventory.dbo.tmp_[0-9]+</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送 schema change 事件，使下游 sink 可以同步表结构变更。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 确定服务器时区。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.key-column</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>表快照的切分键。默认使用主键的第一列，该列必须是主键列。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的 chunk 大小，单位为行数。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次轮询的最大读取条数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>SQL Server CDC 消费者可选启动模式。合法值为 "initial"、"latest-offset"、"snapshot" 和 "timestamp"。</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>当 `scan.startup.mode` 为 "timestamp" 时使用的启动时间戳。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照读取阶段跳过 backfill。跳过 backfill 可能导致部分 change log 事件以 at-least-once 语义被重放。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否在快照读取阶段优先分配无上界 chunk。这可能有助于降低对最大的无上界 chunk 执行快照时 TaskManager 出现内存溢出（OOM）的风险。</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>连接器尝试连接 SQL Server 数据库服务器后的最长等待时间。</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>连接器建立 SQL Server 数据库服务器连接的最大重试次数。</td>
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
      <td>从 SourceRecord 中读取并传递给下游的元数据列表，使用英文逗号分隔。可用元数据包括：database_name、schema_name、table_name、op_ts。</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束时关闭空闲 reader。此特性依赖 FLIP-147。</td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否扫描新增表。该选项仅在作业从 savepoint 或 checkpoint 启动时有用。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>传递给 Debezium Embedded Engine 的 Debezium 属性。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>传递自定义 JDBC URL 属性。例如：<code>jdbc.properties.encrypt=false</code>。</td>
    </tr>
    </tbody>
</table>
</div>

> 兼容性说明：如果希望保持旧版本 Pipeline 行为，可以显式配置 `scan.incremental.snapshot.backfill.skip: true` 和 `scan.incremental.snapshot.unbounded-chunk-first.enabled: false`。

## 可用 Metadata

配置 `metadata.list` 后，以下 metadata 可以传递到下游。

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
      <td>包含该行的数据库名称。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的 schema 名称。</td>
    </tr>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的表名。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>该变更在数据库中发生的时间。对于快照记录，该值始终为 0。</td>
    </tr>
  </tbody>
</table>

## 启动读取位置

配置项 `scan.startup.mode` 指定 SQL Server CDC 消费者的启动模式。有效值包括：

- `initial`：先对被监控表执行初始快照，然后继续读取最新变更。
- `latest-offset`：从最新 change log offset 开始读取。
- `snapshot`：只读取快照。
- `timestamp`：从 `scan.startup.timestamp-millis` 指定的时间戳开始读取。

## 数据类型映射

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">SQL Server type<a href="https://learn.microsoft.com/zh-cn/sql/t-sql/data-types/data-types-transact-sql"></a></th>
        <th class="text-left" style="width:20%;">CDC type</th>
        <th class="text-left" style="width:50%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>BIT</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>SMALLINT</td>
      <td>SQL Server 中的 <code>TINYINT</code> 为无符号类型（取值 0-255），因此映射为范围更大的 <code>SMALLINT</code>。</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
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
      <td>FLOAT</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DECIMAL(p, s)<br>
        NUMERIC(p, s)
      </td>
      <td>DECIMAL(p, s)</td>
      <td>当精度大于 Flink 支持的上限 38 时，会回退为 <code>DECIMAL(38, 0)</code>。</td>
    </tr>
    <tr>
      <td>MONEY</td>
      <td>DECIMAL(19, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLMONEY</td>
      <td>DECIMAL(10, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        NCHAR(n)
      </td>
      <td>CHAR(n)</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)<br>
        NVARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td>当长度信息不可用时，映射为 <code>STRING</code>。</td>
    </tr>
    <tr>
      <td>
        TEXT<br>
        NTEXT
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY<br>
        VARBINARY<br>
        IMAGE
      </td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIMESTAMP<br>
        ROWVERSION
      </td>
      <td>BYTES</td>
      <td>SQL Server 中的 <code>TIMESTAMP</code>/<code>ROWVERSION</code> 是行版本二进制值，并非日期时间类型。</td>
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
      <td>SMALLDATETIME</td>
      <td>TIMESTAMP(0)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME</td>
      <td>TIMESTAMP(3)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME2(p)</td>
      <td>TIMESTAMP(p)</td>
      <td>未显式指定精度时，默认精度为 7。</td>
    </tr>
    <tr>
      <td>DATETIMEOFFSET(p)</td>
      <td>TIMESTAMP_LTZ(p)</td>
      <td>未显式指定精度时，默认精度为 7。</td>
    </tr>
    <tr>
      <td>
        UNIQUEIDENTIFIER<br>
        XML<br>
        SQL_VARIANT<br>
        HIERARCHYID<br>
        GEOMETRY<br>
        GEOGRAPHY
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

## 限制

### 单数据库

`tables` 中的所有条目必须属于同一个数据库。数据库只支持单个固定库名，schema 和 table 支持正则表达式匹配多个。

{{< top >}}
