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

# SQLServer CDC Connector

The SQLServer CDC connector allows for reading snapshot data and incremental data from SQLServer database. This document describes how to setup the SQLServer CDC connector to run SQL queries against SQLServer databases.

Dependencies
------------

In order to setup the SQLServer CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

{{< artifact flink-connector-sqlserver-cdc >}}

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-sqlserver-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-sqlserver-cdc/{{< param Version >}}/flink-sql-connector-sqlserver-cdc-{{< param Version >}}.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to [flink-sql-connector-sqlserver-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-sqlserver-cdc), more released versions will be available in the Maven central warehouse.

Setup SQLServer Database
----------------
A SQL Server administrator must enable change data capture on the source tables that you want to capture. The database must already be enabled for CDC. To enable CDC on a table, a SQL Server administrator runs the stored procedure ```sys.sp_cdc_enable_table``` for the table.

**Prerequisites:**
* CDC is enabled on the SQL Server database.
* The SQL Server Agent is running.
* You are a member of the db_owner fixed database role for the database.

**Procedure:**
* Connect to the SQL Server database by database management studio.
* Run the following SQL statement to enable CDC on the table.
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
* Verifying that the user has access to the CDC table
```sql
--The following example runs the stored procedure sys.sp_cdc_help_change_data_capture on the database MyDB:
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture
GO
```
The query returns configuration information for each table in the database that is enabled for CDC and that contains change data that the caller is authorized to access. If the result is empty, verify that the user has privileges to access both the capture instance and the CDC tables.

How to create a SQLServer CDC table
----------------

The SqlServer CDC table can be defined as following:

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

Connector Options
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
      <td>Specify what connector to use, here should be <code>'sqlserver-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the SQLServer database.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Username to use when connecting to the SQLServer database.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the SQLServer database.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the SQLServer database to monitor.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the SQLServer database to monitor, e.g.: "db1.table1"</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>Integer port number of the SQLServer database.</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">UTC</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai".</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether enable parallelism snapshot.</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.</td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.lower-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0.05d</td>
      <td>Double</td>
      <td>The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the table is evenly distribution or not. 
          The table chunks would use evenly calculation optimization when the data distribution is even, and the query for splitting would happen when it is uneven. 
          The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.</td>
    </tr> 
    <tr>
      <td>chunk-key.even-distribution.factor.upper-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000.0d</td>
      <td>Double</td>
      <td>The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the table is evenly distribution or not. 
          The table chunks would use evenly calculation optimization when the data distribution is even, and the query for splitting would happen when it is uneven. 
          The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from SQLServer.
          For example: <code>'debezium.snapshot.mode' = 'initial_only'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html#sqlserver-required-connector-configuration-properties">Debezium's SQLServer Connector properties</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. <br>
          The flink version is required to be greater than or equal to 1.14 when 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' is set to true.<br>
          If the flink version is greater than or equal to 1.15, the default value of 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' has been changed to true,
          so it does not need to be explicitly configured 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table.
            By default, the chunk key is the first column of the primary key. This column must be a column of the primary key.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        Whether to assign the unbounded chunks first during snapshot reading phase.<br>
        This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk.<br> 
        Experimental option, defaults to false.
      </td>
    </tr>
    </tbody>
</table>    
</div>

Available Metadata
----------------

The following format metadata can be exposed as read-only (VIRTUAL) columns in a table definition.

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
      <td>Name of the table that contain the row.</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the schema that contain the row.</td>
    </tr>    
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contain the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>It indicates the time that the change was made in the database. <br>If the record is read from snapshot of the table instead of the change stream, the value is always 0.</td>
    </tr>
  </tbody>
</table>

Limitation
--------

### Can't perform checkpoint during scanning snapshot of tables
During scanning snapshot of database tables, since there is no recoverable position, we can't perform checkpoints. In order to not perform checkpoints, SqlServer CDC source will keep the checkpoint waiting to timeout. The timeout checkpoint will be recognized as failed checkpoint, by default, this will trigger a failover for the Flink job. So if the database table is large, it is recommended to add following Flink configurations to avoid failover because of the timeout checkpoints:

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
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

Features
--------

### Exactly-Once Processing

The SQLServer CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **exactly-once processing** even failures happen. Please read [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html#how-the-sqlserver-connector-works).

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for SQLServer CDC consumer. The valid enumerations are:

- `initial` (default): Takes a snapshot of structure and data of captured tables; useful if topics should be populated with a complete representation of the data from the captured tables.
- `initial-only`: Takes a snapshot of structure and data like initial but instead does not transition into streaming changes once the snapshot has completed.
- `latest-offset`: Takes a snapshot of the structure of captured tables only; useful if only changes happening from now onwards should be propagated to topics.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not use them together. If you specific both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### Single Thread Reading

The SQLServer CDC source can't work in parallel reading, because there is only one task can receive change events.

### DataStream Source

The SQLServer CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

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

The SQLServer CDC incremental connector (after 2.4.0) can be used as the following shows:
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

Data Type Mapping
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
      <td>numeric</td>
      <td>NUMERIC</td>
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
