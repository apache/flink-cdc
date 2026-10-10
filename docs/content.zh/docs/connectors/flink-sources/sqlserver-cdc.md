---
title: "SQL Server"
weight: 4
type: docs
aliases:
- /connectors/flink-sources/sqlserver-cdc
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

# SQLServer CDC 连接器

SQLServer CDC 连接器允许从 SQLServer 数据库读取快照数据和增量数据。本文描述了如何设置 SQLServer CDC 连接器来对 SQLServer 数据库运行 SQL 查询。

依赖
------------

为了设置 SQLServer CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-sqlserver-cdc >}}

### SQL Client JAR

```下载链接仅适用于稳定版本。```

下载 [flink-sql-connector-sqlserver-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-sqlserver-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-sqlserver-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-sqlserver-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

设置 SQLServer 数据库
----------------
SQL Server 管理员必须在你想要捕获的源表上启用变更数据捕获（CDC）。数据库必须已经启用了 CDC。要在表上启用 CDC，SQL Server 管理员需要为该表运行存储过程 ```sys.sp_cdc_enable_table```。

**前置条件：**
* SQL Server 数据库已启用 CDC。
* SQL Server Agent 正在运行。
* 你是该数据库 db_owner 固定数据库角色的成员。

**步骤：**
* 通过数据库管理工具（database management studio）连接到 SQL Server 数据库。
* 运行以下 SQL 语句，在表上启用 CDC。
```sql
USE MyDB
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',     -- Specifies the schema of the source table.
@source_name   = N'MyTable', -- Specifies the name of the table that you want to capture.
@role_name     = N'MyRole',  -- Specifies a role MyRole to which you can add users to whom you want to grant SELECT permission on the captured columns of the source table. Users in the sysadmin or db_owner role also have access to the specified change tables. Set the value of @role_name to NULL, to allow only members in the sysadmin or db_owner to have full access to captured information.
@filegroup_name = N'MyDB_CT',-- Specifies the filegroup where SQL Server places the change table for the captured table. The named filegroup must already exist. It is best not to locate change tables in the same filegroup that you use for source tables.
@supports_net_changes = 0
GO
```
* 验证用户是否有权访问 CDC 表
```sql
--The following example runs the stored procedure sys.sp_cdc_help_change_data_capture on the database MyDB:
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture
GO
```
该查询会针对数据库中已启用 CDC 且包含调用者有权访问的变更数据的每个表返回配置信息。如果结果为空，请验证用户是否有权访问捕获实例和 CDC 表。

如何创建 SQLServer CDC 表
----------------

SqlServer CDC 表可以定义如下：

```sql
-- register a SqlServer table 'orders' in Flink SQL
CREATE TABLE orders (
    id INT,
    order_date DATE,
    purchaser INT,
    quantity INT,
    product_id INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'table-name' = 'dob.orders'
);

-- read snapshot and binlogs from orders table
SELECT * FROM orders;
```

连接器配置项
----------------

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
      <td>指定要使用的连接器, 这里应该是 <code>'sqlserver-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>SQLServer 数据库的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 SQLServer 数据库时要使用的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 SQLServer 数据库时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 SQLServer 数据库名称。</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 SQLServer 数据库的表名，例如: "db1.table1"</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>SQLServer 数据库的整数端口号。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">UTC</td>
      <td>String</td>
      <td>数据库服务器中的会话时区， 例如： "Asia/Shanghai"。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否启用并行快照。</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>分片元数据的分组大小，如果元数据大小超过分组大小，则元数据将被划分为多个分组。</td>
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
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 SQLServer 捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'initial_only'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html#sqlserver-required-connector-configuration-properties">Debezium 的 SQLServer 连接器属性</a></td> 
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
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 change log 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 change log 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 change log 事件应进行特殊处理。
      </td>
    </tr>
    </tbody>
</table>    
</div>

可用的元数据
----------------

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
  </tbody>
</table>

限制
--------

### 扫描表快照期间无法执行 checkpoint

*注意：此限制仅在未启用增量快照框架（即 `scan.incremental.snapshot.enabled` 设置为 `false`）时适用。*

在扫描数据库表快照期间，由于没有可恢复的位点，我们无法执行 checkpoint。为了不执行 checkpoint，SqlServer CDC source 会让 checkpoint 一直等待直至超时。超时的 checkpoint 会被视为失败的 checkpoint，默认情况下，这将触发 Flink 作业的故障转移。因此，如果数据库表很大，建议添加以下 Flink 配置，以避免因 checkpoint 超时而触发故障转移：

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    schema_name STRING METADATA  FROM 'schema_name' VIRTUAL,
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    id INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3)
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'table-name' = 'dbo.products'
);
```

特性
--------

### Exactly-Once 处理

SQLServer CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后继续读取变更事件，即使在处理时出现故障，也能**准确读取数据**。请参阅 [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html#how-the-sqlserver-connector-works)。

### 启动模式

配置选项`scan.startup.mode`指定 SQLServer CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：对捕获表的结构和数据执行快照；适用于需要使用捕获表数据的完整表示来填充主题的场景。
- `latest-offset`：仅对捕获表的结构执行快照；适用于只需要将从现在开始发生的变更传播到主题的场景。

_注意：`scan.startup.mode` 选项的机制依赖于 Debezium 的 `snapshot.mode` 配置，因此请不要同时使用它们。如果在表 DDL 中同时指定 `scan.startup.mode` 和 `debezium.snapshot.mode` 选项，可能会导致 `scan.startup.mode` 失效。_

### 单线程读取

SQLServer CDC source 无法进行并行读取，因为只有一个任务可以接收变更事件。

### DataStream Source

SQLServer CDC 连接器也可以是一个数据流源。 你可以创建 SourceFunction，如下所示：

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;

public class SqlServerSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
      .hostname("localhost")
      .port(1433)
      .database("sqlserver") // monitor sqlserver database
      .tableList("dbo.products") // monitor products table
      .username("sa")
      .password("Password!")
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

SQLServer CDC 增量连接器（2.4.0 之后）可以使用，如下所示：
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class SqlServerIncrementalSourceExample {
    public static void main(String[] args) throws Exception {
        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder()
                        .hostname("localhost")
                        .port(1433)
                        .databaseList("inventory")
                        .tableList("dbo.products")
                        .username("sa")
                        .password("Password!")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(
                        sqlServerSource,
                        WatermarkStrategy.noWatermarks(),
                        "SqlServerIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print SqlServer Snapshot + Change Stream");
    }
}
```

### 关于无主键表

从3.4.0 版本开始支持无主键表，使用无主键表必须设置 `scan.incremental.snapshot.chunk.key-column`，且只能选择非空类型的一个字段。

在使用无主键表时，需要注意以下两种情况。

1. 配置 `scan.incremental.snapshot.chunk.key-column` 时，如果表中存在索引，请尽量使用索引中的列来加快 select 速度。
2. 无主键表的处理语义由 `scan.incremental.snapshot.chunk.key-column` 指定的列的行为决定：
* 如果指定的列不存在更新操作，此时可以保证 Exactly once 语义。
* 如果指定的列存在更新操作，此时只能保证 At least once 语义。但可以结合下游，通过指定下游主键，结合幂等性操作来保证数据的正确性。

#### 警告

在 SQLServer 表中，若使用 **非主键列** 作为有主键表的 `scan.incremental.snapshot.chunk.key-column`，可能导致**数据不一致**。以下为可能出现的问题及其缓解方案。

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
2. 对于 SQLServer，Group 的名称会类似于 `test_database.test_schema.test_table`。

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">SQLServer type<a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>char(n)</td>
      <td>CHAR(n)</td>
    </tr>
    <tr>
      <td>
        varchar(n)<br>
        nvarchar(n)<br>
        nchar(n)
      </td>
      <td>VARCHAR(n)</td>
    </tr>
    <tr>
      <td>
        text<br>
        ntext<br>
        xml
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>
        decimal(p, s)<br>
        money<br>
        smallmoney
      </td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>numeric(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>
        float<br>
        real
      </td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>bit</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>int</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>tinyint</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>smallint</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>bigint</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>date</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>time(n)</td>
      <td>TIME(n)</td>
    </tr>
    <tr>
      <td>
        datetime2<br>
        datetime<br>
        smalldatetime
      </td>
      <td>TIMESTAMP(n)</td>
    </tr>
    <tr>
      <td>datetimeoffset</td>
      <td>TIMESTAMP_LTZ(3)</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
