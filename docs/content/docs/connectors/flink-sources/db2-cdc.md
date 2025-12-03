---
title: "Db2"
weight: 7
type: docs
aliases:
- /connectors/flink-sources/db2-cdc
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

# Db2 CDC Connector

The Db2 CDC connector allows for reading snapshot data and incremental data from Db2 database. This document 
describes how to setup the db2 CDC connector to run SQL queries against Db2 databases.


## Supported Databases

| Connector | Database                                           | Driver               |
|-----------|----------------------------------------------------|----------------------|
| Db2-cdc   | <li> [Db2](https://www.ibm.com/products/db2): 11.5 | Db2 Driver: 11.5.0.0 |

Dependencies
------------

In order to set up the Db2 CDC connector, the following table provides dependency information for both projects 
using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

{{< artifact flink-connector-db2-cdc >}}

### SQL Client JAR

Download [flink-sql-connector-db2-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-db2-cdc/{{< param Version >}}/flink-sql-connector-db2-cdc-{{< param Version >}}.jar) and 
put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to
[flink-sql-connector-db2-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-db2-cdc),
more released versions will be available in the Maven central warehouse.

Since Db2 Connector's IPLA license is incompatible with Flink CDC project, we can't provide Db2 connector in prebuilt connector jar packages. 
You may need to configure the following dependencies manually.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Dependency Item</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4">com.ibm.db2.jcc:db2jcc:db2jcc4</a></td>
        <td>Used for connecting to Db2 database.</td>
      </tr>
    </tbody>
</table>
</div>

Setup Db2 server
----------------

Follow the steps in the [Debezium Db2 Connector](https://debezium.io/documentation/reference/1.9/connectors/db2.html#setting-up-db2).


Notes
----------------

###  Not support BOOLEAN type in SQL Replication on Db2

Only snapshots can be taken from tables with BOOLEAN type columns. Currently, SQL Replication on Db2 does not support BOOLEAN, so Debezium can not perform CDC on those tables.
Consider using another type to replace BOOLEAN type.


How to create a Db2 CDC table
----------------

The Db2 CDC table can be defined as following:

```sql
-- checkpoint every 3 seconds                     
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a Db2 table 'products' in Flink SQL
Flink SQL> CREATE TABLE products (
     ID INT NOT NULL,
     NAME STRING,
     DESCRIPTION STRING,
     WEIGHT DECIMAL(10,3),
     PRIMARY KEY(ID) NOT ENFORCED
     ) WITH (
     'connector' = 'db2-cdc',
     'hostname' = 'localhost',
     'port' = '50000',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'table-name' = 'myschema.products');
  
-- read snapshot and redo logs from products table
Flink SQL> SELECT * FROM products;
```

Connector Options
----------------

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
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'db2-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the Db2 database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Db2 database to use when connecting to the Db2 database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the Db2 database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the Db2 server to monitor.</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Db2 database to monitor, e.g.: "db1.table1"</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>Integer port number of the Db2 database server.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for Db2 CDC consumer, valid enumerations are "initial"
           and "latest-offset". Please see <a href="#startup-reading-position">Startup Reading Position</a> section 
for more detailed information.</td>
    </tr> 
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai". 
          It controls how the TIMESTAMP type in Db2 converted to STRING.
          See more <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-temporal-types">here</a>.
          If not set, then ZoneId.systemDefault() is used to determine the server time zone.
      </td>
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
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table.
            By default, the chunk key is the first column of the primary key. A column that is not part of the primary key can be used as a chunk key, but this may lead to slower query performance.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from 
Db2 server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-connector-properties">Debezium's Db2 Connector properties</a></td> 
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
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
        Whether to assign the unbounded chunks first during snapshot reading phase.<br>
        This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk.<br> 
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

Features
--------
### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for DB2 CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest redo logs.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the redo logs which means only have the changes since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not using them together. If you speicifying both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### DataStream Source

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class Db2SourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> db2Source =
            Db2Source.<String>builder()
                    .hostname("yourHostname")
                    .port(50000)
                    .database("yourDatabaseName") // set captured database
                    .tableList("yourSchemaName.yourTableName") // set captured table
                    .username("yourUsername")
                    .password("yourPassword")
                    .deserializer(
                            new JsonDebeziumDeserializationSchema()) // converts SourceRecord to
                    // JSON String
                    .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    env.addSource(db2Source)
            .print()
            .setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print Db2 Snapshot + Change Stream");
  }
}
```

The DB2 CDC incremental connector (since 3.1.0) can be used as the following shows:
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder.Db2IncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class Db2ParallelSourceExample {

  public static void main(String[] args) throws Exception {

    Db2IncrementalSource<String> sqlServerSource =
            new Db2SourceBuilder()
                    .hostname("localhost")
                    .port(50000)
                    .databaseList("TESTDB")
                    .tableList("DB2INST1.CUSTOMERS")
                    .username("flink")
                    .password("flinkpw")
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .startupOptions(StartupOptions.initial())
                    .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // enable checkpoint
    env.enableCheckpointing(3000);
    // set the source parallelism to 2
    env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "Db2IncrementalSource")
            .setParallelism(2)
            .print()
            .setParallelism(1);

    env.execute("Print DB2 Snapshot + Change Stream");
  }
}
```

### Available Source metrics

Metrics can help understand the progress of assignments, and the following are the supported [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description                                         |
|------------------------|----------------------------|-------|-----------------------------------------------------|
| namespace.schema.table | isSnapshotting             | Gauge | Weather the table is snapshotting or not            |
| namespace.schema.table | isStreamReading            | Gauge | Weather the table is stream reading or not          |
| namespace.schema.table | numTablesSnapshotted       | Gauge | The number of tables that have been snapshotted     |
| namespace.schema.table | numTablesRemaining         | Gauge | The number of tables that have not been snapshotted |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | The number of splits that is being processed        |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | The number of splits that have not been processed   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | The number of splits that have been processed       |
| namespace.schema.table | snapshotStartTime          | Gauge | The time when the snapshot started                  |
| namespace.schema.table | snapshotEndTime            | Gauge | The time when the snapshot ended                    |

Notice:
1. The group name is `namespace.schema.table`, where `namespace` is the actual database name, `schema` is the actual schema name, and `table` is the actual table name.
2. For DB2, the group name will be like `test_database.test_schema.test_table`.

### Tables Without primary keys

Starting from version 3.4.0, DB2 CDC support tables that do not have a primary key. To use a table without primary keys, you must configure the `scan.incremental.snapshot.chunk.key-column` option and specify one non-null field.

There are two places that need to be taken care of.

1. If there is an index in the table, try to use a column which is contained in the index in `scan.incremental.snapshot.chunk.key-column`. This will increase the speed of select statement.
2. The processing semantics of a DB2 CDC table without primary keys is determined based on the behavior of the column that are specified by the `scan.incremental.snapshot.chunk.key-column`.
* If no update operation is performed on the specified column, the exactly-once semantics is ensured.
* If the update operation is performed on the specified column, only the at-least-once semantics is ensured. However, you can specify primary keys at downstream and perform the idempotence operation to ensure data correctness.

#### Warning

Using a **non-primary key column** as the `scan.incremental.snapshot.chunk.key-column` for a DB2 table with primary keys may lead to data inconsistencies. Below is a scenario illustrating this issue and recommendations to mitigate potential problems.

#### Problem Scenario

- **Table Structure:**
  - **Primary Key:** `id`
  - **Chunk Key Column:** `pid` (Not a primary key)

- **Snapshot Splits:**
  - **Split 0:** `1 < pid <= 3`
  - **Split 1:** `3 < pid <= 5`

- **Operation:**
  - Two different subtasks are reading Split 0 and Split 1 concurrently.
  - An update operation changes `pid` from `2` to `4` for `id=0` while both splits are being read. This update occurs between the low and high watermark of both splits.

- **Result:**
  - **Split 0:** Contains the record `[id=0, pid=2]`
  - **Split 1:** Contains the record `[id=0, pid=4]`

Since the order of processing these records cannot be guaranteed, the final value of `pid` for `id=0` may end up being either `2` or `4`, leading to potential data inconsistencies.



Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;"><a href="https://www.ibm.com/docs/en/db2/11.5?topic=elements-data-types">Db2 type</a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        SMALLINT<br>
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INTEGER
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TIME</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHARACTER(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>VARBINARY(N)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BLOB<br>
        CLOB<br>
        DBCLOB<br>
      </td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARGRAPHIC<br>
        XML
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
