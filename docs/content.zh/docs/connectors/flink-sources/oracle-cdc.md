---
title: "Oracle"
weight: 3
type: docs
aliases:
- /connectors/flink-sources/oracle-cdc
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

# Oracle CDC Connector

The Oracle CDC connector allows for reading snapshot data and incremental data from Oracle database. This document describes how to setup the Oracle CDC connector to run SQL queries against Oracle databases.

Dependencies
------------

In order to setup the Oracle CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

{{< artifact flink-connector-oracle-cdc >}}

### SQL Client JAR

**Download link is available only for stable releases.**

Download [flink-sql-connector-oracle-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oracle-cdc/{{< param Version >}}/flink-sql-connector-oracle-cdc-{{< param Version >}}.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to [flink-sql-connector-oracle-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oracle-cdc), more released versions will be available in the Maven central warehouse.

由于 Oracle Connector 采用的 FUTC 协议与 Flink CDC 项目不兼容，我们无法在 jar 包中提供 Oracle 连接器。
您可能需要手动配置以下依赖：

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
        <td><a href="https://mvnrepository.com/artifact/com.oracle.ojdbc/ojdbc8/19.3.0.0">com.oracle.ojdbc:ojdbc8:19.3.0.0</a></td>
        <td>用于连接到 Oracle 数据库。</td>
      </tr>
    </tbody>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oracle.database.xml/xdb/19.3.0.0">com.oracle.database.xml:xdb:19.3.0.0</a></td>
        <td>用于存储 XML 文件。</td>
      </tr>
    </tbody>
</table>
</div>

Setup Oracle
----------------
You have to enable log archiving for Oracle database and define an Oracle user with appropriate permissions on all databases that the Debezium Oracle connector monitors.

### For Non-CDB database

1. Enable log archiving 

   (1.1). Connect to the database as DBA
    ```sql
    ORACLE_SID=SID
    export ORACLE_SID
    sqlplus /nolog
      CONNECT sys/password AS SYSDBA
    ```

   (1.2). Enable log archiving
    ```sql
    alter system set db_recovery_file_dest_size = 10G;
    alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
    shutdown immediate;
    startup mount;
    alter database archivelog;
    alter database open;
   ```
    **Note:**

    - Enable log archiving requires database restart, pay attention when try to do it
    - The archived logs will occupy a large amount of disk space, so consider clean the expired logs the periodically
   
    (1.3). Check whether log archiving is enabled
    ```sql
    -- Should now "Database log mode: Archive Mode"
    archive log list;
    ```
   **Note:**
   
   Supplemental logging must be enabled for captured tables or the database in order for data changes to capture the <em>before</em> state of changed database rows.
   The following illustrates how to configure this on the table/database level.
   ```sql
   -- Enable supplemental logging for a specific table:
   ALTER TABLE inventory.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   ```
   ```sql
   -- Enable supplemental logging for database
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ```

2. Create an Oracle user with permissions

   (2.1). Create Tablespace
   ```sql
   sqlplus sys/password@host:port/SID AS SYSDBA;
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/SID/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit;
   ```

   (2.2). Create a user and grant permissions
   ```sql
   sqlplus sys/password@host:port/SID AS SYSDBA;
     CREATE USER flinkuser IDENTIFIED BY flinkpw DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
     GRANT CREATE SESSION TO flinkuser;
     GRANT SET CONTAINER TO flinkuser;
     GRANT SELECT ON V_$DATABASE to flinkuser;
     GRANT FLASHBACK ANY TABLE TO flinkuser;
     GRANT SELECT ANY TABLE TO flinkuser;
     GRANT SELECT_CATALOG_ROLE TO flinkuser;
     GRANT EXECUTE_CATALOG_ROLE TO flinkuser;
     GRANT SELECT ANY TRANSACTION TO flinkuser;
     GRANT LOGMINING TO flinkuser;
     GRANT ANALYZE ANY TO flinkuser;

     GRANT CREATE TABLE TO flinkuser;
     -- need not to execute if set scan.incremental.snapshot.enabled=true(default)
     GRANT LOCK ANY TABLE TO flinkuser;
     GRANT ALTER ANY TABLE TO flinkuser;
     GRANT CREATE SEQUENCE TO flinkuser;

     GRANT EXECUTE ON DBMS_LOGMNR TO flinkuser;
     GRANT EXECUTE ON DBMS_LOGMNR_D TO flinkuser;

     GRANT SELECT ON V_$LOG TO flinkuser;
     GRANT SELECT ON V_$LOG_HISTORY TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_LOGS TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_CONTENTS TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_PARAMETERS TO flinkuser;
     GRANT SELECT ON V_$LOGFILE TO flinkuser;
     GRANT SELECT ON V_$ARCHIVED_LOG TO flinkuser;
     GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO flinkuser;
     exit;
   ```
   
### For CDB database

Overall, the steps for configuring CDB database is quite similar to non-CDB database, but the commands may be different.
1. Enable log archiving
   ```sql
   ORACLE_SID=ORCLCDB
   export ORACLE_SID
   sqlplus /nolog
     CONNECT sys/password AS SYSDBA
     alter system set db_recovery_file_dest_size = 10G;
     -- should exist
     alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
     shutdown immediate
     startup mount
     alter database archivelog;
     alter database open;
     -- Should show "Database log mode: Archive Mode"
     archive log list
     exit;
   ```
   **Note:**
   You can also use the following commands to enable supplemental logging:
   ```sql
   -- Enable supplemental logging for a specific table:
   ALTER TABLE inventory.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   -- Enable supplemental logging for database
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ```

2. Create an Oracle user with permissions
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLCDB as sysdba
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit
   ```
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLPDB1 as sysdba
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit
   ```
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLCDB as sysdba
     CREATE USER flinkuser IDENTIFIED BY flinkpw DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
     GRANT CREATE SESSION TO flinkuser CONTAINER=ALL;
     GRANT SET CONTAINER TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$DATABASE to flinkuser CONTAINER=ALL;
     GRANT FLASHBACK ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT_CATALOG_ROLE TO flinkuser CONTAINER=ALL;
     GRANT EXECUTE_CATALOG_ROLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ANY TRANSACTION TO flinkuser CONTAINER=ALL;
     GRANT LOGMINING TO flinkuser CONTAINER=ALL;
     GRANT CREATE TABLE TO flinkuser CONTAINER=ALL;
     -- need not to execute if set scan.incremental.snapshot.enabled=true(default)
     GRANT LOCK ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT CREATE SEQUENCE TO flinkuser CONTAINER=ALL;

     GRANT EXECUTE ON DBMS_LOGMNR TO flinkuser CONTAINER=ALL;
     GRANT EXECUTE ON DBMS_LOGMNR_D TO flinkuser CONTAINER=ALL;

     GRANT SELECT ON V_$LOG TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOG_HISTORY TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_LOGS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_CONTENTS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_PARAMETERS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGFILE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$ARCHIVED_LOG TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO flinkuser CONTAINER=ALL;
     exit
   ```
   
See more about the [Setting up Oracle](https://debezium.io/documentation/reference/1.9/connectors/oracle.html#setting-up-oracle)

How to create an Oracle CDC table
----------------

The Oracle CDC table can be defined as following:

```sql 
-- register an Oracle table 'products' in Flink SQL
Flink SQL> CREATE TABLE products (
     ID INT NOT NULL,
     NAME STRING,
     DESCRIPTION STRING,
     WEIGHT DECIMAL(10, 3),
     PRIMARY KEY(id) NOT ENFORCED
     ) WITH (
     'connector' = 'oracle-cdc',
     'hostname' = 'localhost',
     'port' = '1521',
     'username' = 'flinkuser',
     'password' = 'flinkpw',
     'database-name' = 'ORCLCDB',
     'schema-name' = 'inventory',
     'table-name' = 'products');
  
-- read snapshot and redo logs from products table
Flink SQL> SELECT * FROM products;
```
**Note:**
When working with the CDB + PDB model, you are expected to add an extra option `'debezium.database.pdb.name' = 'xxx'` in Flink DDL to specific the name of the PDB to connect to.

**Note:**
While the connector might work with a variety of Oracle versions and editions, only Oracle 9i, 10g, 11g and 12c have been tested.

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
      <td>Specify what connector to use, here should be <code>'oracle-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the Oracle database server. If the url is not empty, hostname may not be configured, otherwise hostname can not be empty</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Oracle database to use when connecting to the Oracle database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the Oracle database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the Oracle server to monitor.</td>
    </tr>
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name of the Oracle database to monitor.</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Oracle database to monitor.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1521</td>
      <td>Integer</td>
      <td>Integer port number of the Oracle database server.</td>
    </tr>
  <tr>
      <td>url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">jdbc:oracle:thin:@{hostname}:{port}:{database-name}</td>
      <td>String</td>
      <td>JdbcUrl of the oracle database server . If the hostname and port parameter is configured, the URL is concatenated by hostname port database-name in SID format by default. Otherwise, you need to configure the URL parameter</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for Oracle CDC consumer, valid enumerations are "initial"
           and "latest-offset". 
           Please see <a href="#startup-reading-position">Startup Reading Position</a> section for more detailed information.</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">true</td>
          <td>Boolean</td>
          <td>Incremental snapshot is a new mechanism to read snapshot of a table. Compared to the old snapshot mechanism,
              the incremental snapshot has many advantages, including:
                (1) source can be parallel during snapshot reading, 
                (2) source can perform checkpoints in the chunk granularity during snapshot reading, 
                (3) source doesn't need to acquire ROW SHARE MODE lock before snapshot reading.
          </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>The maximum fetch size for per poll when read table snapshot.</td>
    </tr>
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>The max retry times that the connector should retry to build Oracle database server connection.</td>
    </tr>
    <tr>
          <td>connection.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>Integer</td>
          <td>The connection pool size.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from Oracle server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/oracle.html#oracle-connector-properties">Debezium's Oracle Connector properties</a></td> 
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
            By default, the chunk key is 'ROWID'. A column that is not part of the primary key can be used as a chunk key, but this may lead to slower query performance.
            <br>
            <b>Warning:</b> Using a non-primary key column as a chunk key may lead to data inconsistencies. Please see <a href="#warning">Warning</a> for details.
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
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        Whether to skip backfill in snapshot reading phase.<br> 
        If backfill is skipped, changes on captured tables during snapshot phase will be consumed later in change log reading phase instead of being merged into the snapshot.<br>
        WARNING: Skipping backfill might lead to data inconsistency because some change log events happened within the snapshot phase might be replayed (only at-least-once semantic is promised).
        For example updating an already updated value in snapshot, or deleting an already deleted entry in snapshot. These replayed change log events should be handled specially.
      </td>
    </tr>
    </tbody>
</table>    
</div>

Limitation
--------

### Can't perform checkpoint during scanning snapshot of tables
During scanning snapshot of database tables, since there is no recoverable position, we can't perform checkpoints. In order to not perform checkpoints, Oracle CDC source will keep the checkpoint waiting to timeout. The timeout checkpoint will be recognized as failed checkpoint, by default, this will trigger a failover for the Flink job. So if the database table is large, it is recommended to add following Flink configurations to avoid failover because of the timeout checkpoints:

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

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

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    schema_name STRING METADATA FROM 'schema_name' VIRTUAL, 
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    ID INT NOT NULL,
    NAME STRING,
    DESCRIPTION STRING,
    WEIGHT DECIMAL(10, 3),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'oracle-cdc',
    'hostname' = 'localhost',
    'port' = '1521',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'ORCLCDB',
    'schema-name' = 'inventory',
    'table-name' = 'products',
    'debezium.log.mining.strategy' = 'online_catalog',
    'debezium.log.mining.continuous.mine' = 'true'
);
```

**Note** : The Oracle dialect is case-sensitive, it converts field name to uppercase if the field name is not quoted, Flink SQL doesn't convert the field name. Thus for physical columns from oracle database, we should use its converted field name in Oracle when define an `oracle-cdc` table in Flink SQL.

Features
--------

### Exactly-Once Processing

The Oracle CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **exactly-once processing** even failures happen. Please read [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/oracle.html#how-the-oracle-connector-works).

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for Oracle CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest redo log.
- `latest-offset`: Never to perform a snapshot on the monitored database tables upon first startup, just read from
  the change since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not use them together. If you specific both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### Single Thread Reading

The Oracle CDC source can't work in parallel reading, because there is only one task can receive change events.

### DataStream Source

The Oracle CDC connector can also be a DataStream source. There are two modes for the DataStream source:

- incremental snapshot based, which allows parallel reading
- SourceFunction based, which only supports single thread reading

#### Incremental Snapshot based DataStream (Experimental)

```java
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class OracleParallelSourceExample {

    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");

        JdbcIncrementalSource<String> oracleChangeEventSource =
                new OracleSourceBuilder()
                        .hostname("host")
                        .port(1521)
                        .databaseList("ORCLCDB")
                        .schemaList("DEBEZIUM")
                        .tableList("DEBEZIUM.PRODUCTS")
                        .username("username")
                        .password("password")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .splitSize(2)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000L);
        // set the source parallelism to 4
        env.fromSource(
                        oracleChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "OracleParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);
        env.execute("Print Oracle Snapshot + RedoLog");
    }
}
```

#### SourceFunction-based DataStream

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.oracle.OracleSource;

public class OracleSourceExample {
  public static void main(String[] args) throws Exception {
     SourceFunction<String> sourceFunction = OracleSource.<String>builder()
             .url("jdbc:oracle:thin:@{hostname}:{port}:{database}")
             .port(1521)
             .database("ORCLCDB") // monitor XE database
             .schemaList("inventory") // monitor inventory schema
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
2. 对于 Oracle，Group 的名称会类似于 `test_database.test_schema.test_table`。

### Tables Without primary keys

Starting from version 3.4.0, Oracle CDC support tables that do not have a primary key. To use a table without primary keys, you must configure the `scan.incremental.snapshot.chunk.key-column` option and specify one non-null field.

There are two places that need to be taken care of.

1. If there is an index in the table, try to use a column which is contained in the index in `scan.incremental.snapshot.chunk.key-column`. This will increase the speed of select statement.
2. The processing semantics of an Oracle CDC table without primary keys is determined based on the behavior of the column that are specified by the `scan.incremental.snapshot.chunk.key-column`.
* If no update operation is performed on the specified column, the exactly-once semantics is ensured.
* If the update operation is performed on the specified column, only the at-least-once semantics is ensured. However, you can specify primary keys at downstream and perform the idempotence operation to ensure data correctness.

#### Warning

Using a **non-primary key column** as the `scan.incremental.snapshot.chunk.key-column` for an oracle table with primary keys may lead to data inconsistencies. Below is a scenario illustrating this issue and recommendations to mitigate potential problems.

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
        <th class="text-left"><a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html">Oracle type</a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 3
      </td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 5
      </td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 10
      </td>
      <td>INT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 19
      </td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), 19 <= p - s <= 38 <br>
      </td>
      <td>DECIMAL(p - s, 0)</td>
    </tr>
    <tr>
      <td>NUMBER(p, s > 0)
      </td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s > 38
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td> 
        FLOAT<br>
        BINARY_FLOAT
      </td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        DOUBLE PRECISION<br>
        BINARY_DOUBLE
      </td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>NUMBER(1)</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        DATE<br>
        TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] WITH TIME ZONE</td>
      <td>TIMESTAMP [(p)] WITH TIME ZONE</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] WITH LOCAL TIME ZONE</td>
      <td>TIMESTAMP_LTZ [(p)]</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        NCHAR(n)<br>
        NVARCHAR2(n)<br>
        VARCHAR(n)<br>
        VARCHAR2(n)<br>
        CLOB<br>
        NCLOB<br>
        XMLType<br>
        SYS.XMLTYPE
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BLOB<br>
      ROWID
      </td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>
      INTERVAL DAY TO SECOND<br>
      INTERVAL YEAR TO MONTH
      </td>
      <td>BIGINT</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
