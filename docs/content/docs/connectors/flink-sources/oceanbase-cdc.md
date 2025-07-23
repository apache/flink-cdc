---
title: "OceanBase"
weight: 9
type: docs
aliases:
- /connectors/flink-sources/oceanbase-cdc
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

# OceanBase CDC Connector

The OceanBase CDC connector allows for reading snapshot data and incremental data from OceanBase. This document describes how to set up the OceanBase CDC connector to run SQL queries against OceanBase.

### OceanBase CDC Solutions

Glossary:

- *OceanBase CE*: OceanBase Community Edition. It's compatible with MySQL and has been open sourced at https://github.com/oceanbase/oceanbase.
- *OceanBase EE*: OceanBase Enterprise Edition. It supports two compatibility modes: MySQL and Oracle. See https://en.oceanbase.com.
- *OceanBase Cloud*: OceanBase Enterprise Edition on Cloud. See https://en.oceanbase.com/product/cloud.
- *Binlog Service CE*: OceanBase Binlog Service Community Edition. It is a solution of OceanBase CE that is compatible with the MySQL replication protocol. See the docs of [OceanBase Binlog Service](https://en.oceanbase.com/docs/common-ocp-10000000002168919) for details.
- *Binlog Service EE*: OceanBase Binlog Service Enterprise Edition. It is a solution of OceanBase EE MySQL compatible mode that is compatible with the MySQL replication protocol, and it's only available for users of Alibaba Cloud, see [User Guide](https://www.alibabacloud.com/help/en/apsaradb-for-oceanbase/latest/binlog-overview).
- *MySQL Driver*: `mysql-connector-java` which can be used with OceanBase CE and OceanBase EE MySQL compatible mode.
- *OceanBase Driver*: The JDBC driver for OceanBase, which supports both MySQL compatible mode and Oracle compatible mode of all OceanBase versions. It's open sourced at https://github.com/oceanbase/obconnector-j.

CDC Source Solutions for OceanBase:

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">Database</th>
                <th class="text-left">Supported Driver</th>
                <th class="text-left">CDC Source Connector</th>
                <th class="text-left">Other Required Components</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td rowspan="2">OceanBase CE</td>
                <td>
                    MySQL Driver: 8.0.x <br>
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Binlog Service CE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service CE</td>
            </tr>
            <tr>
                <td rowspan="2">OceanBase EE (MySQL Compatible Mode)</td>
                <td>
                    MySQL Driver: 8.0.x <br>
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Binlog Service EE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service EE</td>
            </tr>
            <tr>
                <td>OceanBase EE (Oracle Compatible Mode)</td>
                <td></td>
                <td>The incremental data subscription service for OceanBase Oracle Compatibility Mode is not currently supported. Please contact Enterprise Support for assistance.</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>

**Important Notes**: The oceanbase-cdc connector has undergone significant changes starting from version 3.5:
- The previous connector implementation based on the OceanBase Log Proxy service has been officially removed. The current version now only supports connecting to the OceanBase Binlog Service.
- The current oceanbase-cdc connector is rebuilt based on the mysql-cdc connector, offering enhanced compatibility with the OceanBase Binlog Service, including critical bug fixes. Upgrading to this version is strongly recommended.
- Since the OceanBase Binlog Service is compatible with MySQL replication protocols, you may still use the MySQL CDC connector to connect to the OceanBase Binlog Service.
- The incremental data subscription service for OceanBase Oracle Compatibility Mode is not currently supported. Please contact Enterprise Support for assistance.

Dependencies
------------

In order to set up the OceanBase CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

{{< artifact flink-connector-oceanbase-cdc >}}

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oceanbase-cdc/{{< param Version >}}/flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to [flink-sql-connector-oceanbase-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oceanbase-cdc), more released versions will be available in the Maven central warehouse.

Since the licenses of MySQL Driver and OceanBase Driver are incompatible with Flink CDC project, we can't provide them in prebuilt connector jar packages. You may need to configure the following dependencies manually.

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
        <td><a href="https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.27">mysql:mysql-connector-java:8.0.27</a></td>
        <td>Used for connecting to MySQL tenant of OceanBase.</td>
      </tr>
    </tbody>
</table>
</div>

Deploy OceanBase database and Binlog service
----------------------

1. Deploy the OceanBase database according to the [document](https://en.oceanbase.com/docs/common-oceanbase-database-10000000001970931).
2. Deploy the OceanBase Binlog service according to the [document](https://en.oceanbase.com/docs/common-ocp-10000000002168919).

Usage Documentation
----------------

**Notes**：
- The current version of the **oceanbase-cdc connector** is built on top of the **mysql-cdc connector**, with **only minor internal implementation changes**.
   - **External interfaces** and **configuration parameters** are **fully consistent** with those of the mysql-cdc connector.
   - For detailed usage instructions, refer to the [**MySQL CDC Usage Documentation**](mysql-cdc.md).

How to create a OceanBase CDC table
----------------

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a OceanBase table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'oceanbase-cdc',
     'hostname' = 'localhost',
     'port' = '2881',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'table-name' = 'orders');
  
-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
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
      <td>Specify what connector to use, here should be <code>'oceanbase-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the OceanBase database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the OceanBase database to use when connecting to the OceanBase database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the OceanBase database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the OceanBase server to monitor. The database-name also supports regular expressions to monitor multiple tables matches the regular expression.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
         Table name of the OceanBase database to monitor. The table-name also supports regular expressions to monitor multiple tables that satisfy the regular expressions. Note: When the OceanBase CDC connector regularly matches the table name, it will concat the database-name and table-name filled in by the user through the string `\\.` to form a full-path regular expression, and then use the regular expression to match the fully qualified name of the table in the OceanBase database.
      </td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2881</td>
      <td>Integer</td>
      <td>Integer port number of the OceanBase database server.</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like '5400', 
          the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is recommended when 'scan.incremental.snapshot.enabled' enabled.
          Every ID must be unique across all currently-running database processes in the OceanBase cluster. This connector joins the OceanBase cluster
          as another server (with this unique ID) so it can read the binlog. By default, a random number is generated between 5400 and 6400,
          though we recommend setting an explicit value.
      </td>
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
                (3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.
              If you would like the source run in parallel, each parallel reader should have an unique server id, so 
              the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism.
              Please see <a href="./mysql-cdc.md#incremental-snapshot-reading">Incremental Snapshot Reading</a>section for more detailed information.
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
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table.
            By default, the chunk key is the first column of the primary key. A column that is not part of the primary key can be used as a chunk key, but this may lead to slower query performance.
            <br>
            <b>Warning:</b> Using a non-primary key column as a chunk key may lead to data inconsistencies. Please see <a href="./mysql-cdc.md#warning">Warning</a> for details.
          </td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for OceanBase CDC consumer, valid enumerations are "initial", "earliest-offset", "latest-offset", "specific-offset", "timestamp" and "snapshot".
           Please see <a href="./mysql-cdc.md#startup-reading-position">Startup Reading Position</a> section for more detailed information.</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.file</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Optional binlog file name used in case of "specific-offset" startup mode</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.pos</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional binlog file position used in case of "specific-offset" startup mode</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.gtid-set</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Optional GTID set used in case of "specific-offset" startup mode</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional millisecond timestamp used in case of "timestamp" startup mode.</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-events</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional number of events to skip after the specific starting offset</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional number of rows to skip after the specific starting offset</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai". 
          It controls how the TIMESTAMP type in OceanBase converted to STRING.
          See more <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types">here</a>.
          If not set, then ZoneId.systemDefault() is used to determine the server time zone.
      </td>
    </tr>
    <tr>
      <td>debezium.min.row.count.to.stream.result</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>	
During a snapshot operation, the connector will query each included table to produce a read event for all rows in that table. This parameter determines whether the OceanBase connection will pull all results for a table into memory (which is fast but requires large amounts of memory), or whether the results will instead be streamed (can be slower, but will work for very large tables). The value specifies the minimum number of rows a table must contain before the connector will stream results, and defaults to 1,000. Set this parameter to '0' to skip all table size checks and always stream all results during a snapshot.</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>The maximum time that the connector should wait after trying to connect to the OceanBase database server before timing out.
              This value cannot be less than 250ms.</td>
    </tr>    
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>The max retry times that the connector should retry to build OceanBase database server connection.</td>
    </tr>
    <tr>
          <td>connection.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>Integer</td>
          <td>The connection pool size.</td>
    </tr>
    <tr>
          <td>jdbc.properties.*</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>String</td>
          <td>Option to pass custom JDBC URL properties. User can pass custom properties like 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
          <td>heartbeat.interval</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>The interval of sending heartbeat event for tracing the latest available binlog offsets.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from OceanBase server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties">Debezium's MySQL Connector properties</a></td> 
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
      <td>debezium.binary.handling.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>debezium.binary.handling.mode can be set to one of the following values:
          none: No processing is performed, and the binary data type is transmitted as a byte array (byte array).
          base64: The binary data type is converted to a Base64-encoded string and transmitted.
          hex: The binary data type is converted to a hexadecimal string and transmitted.
      The default value is none. Depending on your requirements and data types, you can choose the appropriate processing mode. If your database contains a large number of binary data types, it is recommended to use base64 or hex mode to make it easier to handle during transmission.</td> 
    </tr>
    <tr>
      <td>scan.parse.online.schema.changes.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        Whether to parse "online" schema changes generated by <a href="https://github.com/github/gh-ost">gh-ost</a> or <a href="https://docs.percona.com/percona-toolkit/pt-online-schema-change.html">pt-osc</a>.
        Schema change events are applied to a "shadow" table and then swapped with the original table later.
        <br>
        This is an experimental feature, and subject to change in the future.
      </td>
    </tr>
    <tr>
      <td>use.legacy.json.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to use legacy JSON format to cast JSON type data in binlog. <br>
          It determines whether to use the legacy JSON format when retrieving JSON type data in binlog. 
          If the user configures 'use.legacy.json.format' = 'true', whitespace before values and after commas in the JSON type data is removed. For example,
          JSON type data {"key1": "value1", "key2": "value2"} in binlog would be converted to {"key1":"value1","key2":"value2"}.
          When 'use.legacy.json.format' = 'false', the data would be converted to {"key1": "value1", "key2": "value2"}, with whitespace before values and after commas preserved.
      </td>
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
    <tr>
      <td>scan.read-changelog-as-append-only.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        Whether to convert the changelog stream to an append-only stream.<br>
        This feature is only used in special scenarios where you need to save upstream table deletion messages. For example, in a logical deletion scenario, users are not allowed to physically delete downstream messages. In this case, this feature is used in conjunction with the row_kind metadata field. Therefore, the downstream can save all detailed data at first, and then use the row_kind field to determine whether to perform logical deletion.<br>
        The option values are as follows:<br>
          <li>true: All types of messages (including INSERT, DELETE, UPDATE_BEFORE, and UPDATE_AFTER) will be converted into INSERT messages.</li>
          <li>false (default): All types of messages are sent as is.</li>
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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contain the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>It indicates the time that the change was made in the database. <br>If the record is read from snapshot of the table instead of the binlog, the value is always 0.</td>
    </tr>
    <tr>
      <td>row_kind</td>
      <td>STRING NOT NULL</td>
      <td>It indicates the row kind of the changelog,Note: The downstream SQL operator may fail to compare due to this new added column when processing the row retraction if 
the source operator chooses to output the 'row_kind' column for each record. It is recommended to use this metadata column only in simple synchronization jobs.
<br>'+I' means INSERT message, '-D' means DELETE message, '-U' means UPDATE_BEFORE message and '+U' means UPDATE_AFTER message.</td>
    </tr>
  </tbody>
</table>

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE products
(
    db_name       STRING METADATA FROM 'database_name' VIRTUAL,
    table_name    STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts  TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation     STRING METADATA FROM 'row_kind' VIRTUAL,
    order_id      INT,
    order_date    TIMESTAMP(0),
    customer_name STRING,
    price         DECIMAL(10, 5),
    product_id    INT,
    order_status  BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'oceanbase-cdc',
      'hostname' = 'localhost',
      'port' = '2881',
      'username' = 'root',
      'password' = '123456',
      'database-name' = 'mydb',
      'table-name' = 'orders'
      );
```

The extended CREATE TABLE example demonstrates the usage of regex to match multi-tables:

```sql
CREATE TABLE products
(
    db_name       STRING METADATA FROM 'database_name' VIRTUAL,
    table_name    STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts  TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation     STRING METADATA FROM 'row_kind' VIRTUAL,
    order_id      INT,
    order_date    TIMESTAMP(0),
    customer_name STRING,
    price         DECIMAL(10, 5),
    product_id    INT,
    order_status  BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'oceanbase-cdc',
      'hostname' = 'localhost',
      'port' = '2881',
      'username' = 'root',
      'password' = '123456',
      'database-name' = '(^(test).*|^(tpc).*|txc|.*[p$]|t{2})',
      'table-name' = '(t[5-8]|tt)'
      );
```
<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">example</th>
       <th class="text-left" style="width: 30%">expression</th>
       <th class="text-left" style="width: 55%">description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>prefix match</td>
      <td>^(test).*</td>
      <td>This matches the database name or table name starts with prefix of test, e.g test1、test2. </td>
    </tr>
    <tr>
      <td>suffix match</td>
      <td>.*[p$]</td>
      <td>This matches the database name or table name ends with suffix of p, e.g cdcp、edcp. </td>
    </tr>
    <tr>
      <td>specific match</td>
      <td>txc</td>
      <td>This matches the database name or table name according to a specific name, e.g txc. </td>
    </tr>
  </tbody>
</table>

It will use `database-name\\.table-name` as a pattern to match tables, as above examples using pattern `(^(test).*|^(tpc).*|txc|.*[p$]|t{2})\\.(t[5-8]|tt)` matches txc.tt、test2.test5.


Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">OceanBase type<a href="https://en.oceanbase.com/docs/common-oceanbase-database-10000000001974954"></a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED<br>
        TINYINT UNSIGNED ZEROFILL
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED<br>
        SMALLINT UNSIGNED ZEROFILL
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED<br>
        INT UNSIGNED ZEROFILL<br>
        MEDIUMINT UNSIGNED<br>
        MEDIUMINT UNSIGNED ZEROFILL
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>
        BIGINT UNSIGNED<br>
        BIGINT UNSIGNED ZEROFILL<br>
        SERIAL
      </td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        FLOAT UNSIGNED<br>
        FLOAT UNSIGNED ZEROFILL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        REAL UNSIGNED<br>
        REAL UNSIGNED ZEROFILL<br>
        DOUBLE<br>
        DOUBLE UNSIGNED<br>
        DOUBLE UNSIGNED ZEROFILL<br>
        DOUBLE PRECISION<br>
        DOUBLE PRECISION UNSIGNED<br>
        DOUBLE PRECISION UNSIGNED ZEROFILL
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>The precision for DECIMAL data type is up to 65 in OceanBase compatible mode, but the precision for DECIMAL is limited to 38 in Flink.
  So if you define a decimal column whose precision is greater than 38, you should map it to STRING to avoid precision loss.</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
        TINYINT(1)<br>
        BIT(1)
        </td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]<br>
        DATETIME [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)
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
        BIT(n)
      </td>
      <td>BINARY(⌈(n + 7) / 8⌉)</td>
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
        TINYTEXT<br>
        TEXT<br>
        MEDIUMTEXT<br>
        LONGTEXT<br>
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYBLOB<br>
        BLOB<br>
        MEDIUMBLOB<br>
        LONGBLOB<br>
      </td>
      <td>BYTES</td>
      <td>Currently, for BLOB data type, only the blob whose length isn't greater than 2,147,483,647(2 ** 31 - 1) is supported. </td>
    </tr>
    <tr>
      <td>
        YEAR
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td>The JSON data type  will be converted into STRING with JSON format in Flink.</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>ARRAY&lt;STRING&gt;</td>
      <td>As the SET data type is a string object that can have zero or more values, 
          it should always be mapped to an array of string
      </td>
    </tr>
    <tr>
      <td>
       GEOMETRY<br>
       POINT<br>
       LINESTRING<br>
       POLYGON<br>
       MULTIPOINT<br>
       MULTILINESTRING<br>
       MULTIPOLYGON<br>
       GEOMETRYCOLLECTION<br>
      </td>
      <td>
        STRING
      </td>
      <td>
      The spatial data types will be converted into STRING with a fixed Json format.
      Please see <a href="#patial-data-types-mapping ">Spatial Data Types Mapping</a> section for more detailed information.
      </td>
    </tr>
    </tbody>
</table>
</div>

### Spatial Data Types Mapping
The spatial data types except for `GEOMETRYCOLLECTION` will be converted into Json String with a fixed format like:<br>
```json
{"srid": 0 , "type": "xxx", "coordinates": [0, 0]}
```
The field `srid` identifies the SRS in which the geometry is defined, SRID 0 is the default for new geometry values if no SRID is specified.

The field `type` identifies the spatial data type, such as `POINT`/`LINESTRING`/`POLYGON`.

The field `coordinates` represents the `coordinates` of the spatial data.

For `GEOMETRYCOLLECTION`, it will be converted into Json String with a fixed format like:<br>
```json
{"srid": 0 , "type": "GeometryCollection", "geometries": [{"type":"Point","coordinates":[10,10]}]}
```

The field `geometries` is an array contains all spatial data.

The example for different spatial data types mapping is as follows:
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>POINT(1 1)</td>
        <td>{"coordinates":[1,1],"type":"Point","srid":0}</td>
      </tr>
      <tr>
        <td>LINESTRING(3 0, 3 3, 3 5)</td>
        <td>{"coordinates":[[3,0],[3,3],[3,5]],"type":"LineString","srid":0}</td>
      </tr>
      <tr>
        <td>POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))</td>
        <td>{"coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],"type":"Polygon","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOINT((1 1),(2 2))</td>
        <td>{"coordinates":[[1,1],[2,2]],"type":"MultiPoint","srid":0}</td>
      </tr>
      <tr>
        <td>MultiLineString((1 1,2 2,3 3),(4 4,5 5))</td>
        <td>{"coordinates":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],"type":"MultiLineString","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))</td>
        <td>{"coordinates":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],"type":"MultiPolygon","srid":0}</td>
      </tr>
      <tr>
        <td>GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))</td>
        <td>{"geometries":[{"type":"Point","coordinates":[10,10]},{"type":"Point","coordinates":[30,30]},{"type":"LineString","coordinates":[[15,15],[20,20]]}],"type":"GeometryCollection","srid":0}</td>
      </tr>
    </tbody>
</table>
</div>

{{< top >}}
