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

# MySQL CDC Connector

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document describes how to setup the MySQL CDC connector to run SQL queries against MySQL databases.


## Supported Databases

| Connector                                                | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                  |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [mysql-cdc](mysql-cdc.md)         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.27     |

Dependencies
------------

In order to setup the MySQL CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself. -->
  <version>3.0-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download flink-sql-connector-mysql-cdc-3.0-SNAPSHOT.jar and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-mysql-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-mysql-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-connector-mysql-cdc), the released version will be available in the Maven central warehouse. 

Setup MySQL server
----------------

You have to define a MySQL user with appropriate permissions on all databases that the Debezium MySQL connector monitors.

1. Create the MySQL user:

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. Grant the required permissions to the user:

```sql
mysql> GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
**Note:** The RELOAD permissions is not required any more when `scan.incremental.snapshot.enabled` is enabled (enabled by default).

3. Finalize the user’s permissions:

```sql
mysql> FLUSH PRIVILEGES;
```

See more about the [permission explanation](https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-creating-user).


Notes
----------------

### Set a different SERVER ID for each reader

Every MySQL database client for reading binlog should have a unique id, called server id. MySQL server will use this id to maintain network connection and the binlog position. Therefore, if different jobs share a same server id, it may result to read from wrong binlog position. 
Thus, it is recommended to set different server id for each reader via the [SQL Hints](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/hints.html), 
e.g.  assuming the source parallelism is 4, then we can use `SELECT * FROM source_table /*+ OPTIONS('server-id'='5401-5404') */ ;` to assign unique server id for each of the 4 source readers. 


### Setting up MySQL session timeouts

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring interactive_timeout and wait_timeout in your MySQL configuration file.
- `interactive_timeout`: The number of seconds the server waits for activity on an interactive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: The number of seconds the server waits for activity on a noninteractive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).


How to create a MySQL CDC table
----------------

The MySQL CDC table can be defined as following:

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a MySQL table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'localhost',
     'port' = '3306',
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
      <td>Specify what connector to use, here should be <code>'mysql-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the MySQL database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the MySQL database to use when connecting to the MySQL database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the MySQL database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the MySQL server to monitor. The database-name also supports regular expressions to monitor multiple tables matches the regular expression.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
         Table name of the MySQL database to monitor. The table-name also supports regular expressions to monitor multiple tables that satisfy the regular expressions. Note: When the MySQL CDC connector regularly matches the table name, it will concat the database-name and table-name filled in by the user through the string `\\.` to form a full-path regular expression, and then use the regular expression to match the fully qualified name of the table in the MySQL database.
      </td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td>Integer port number of the MySQL database server.</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like '5400', 
          the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is recommended when 'scan.incremental.snapshot.enabled' enabled.
          Every ID must be unique across all currently-running database processes in the MySQL cluster. This connector joins the MySQL cluster
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
              Please see <a href="#incremental-snapshot-reading ">Incremental Snapshot Reading</a>section for more detailed information.
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
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for MySQL CDC consumer, valid enumerations are "initial", "earliest-offset", "latest-offset", "specific-offset" and "timestamp".
           Please see <a href="#startup-reading-position">Startup Reading Position</a> section for more detailed information.</td>
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
          It controls how the TIMESTAMP type in MYSQL converted to STRING.
          See more <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types">here</a>.
          If not set, then ZoneId.systemDefault() is used to determine the server time zone.
      </td>
    </tr>
    <tr>
      <td>debezium.min.row.
      count.to.stream.result</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>	
During a snapshot operation, the connector will query each included table to produce a read event for all rows in that table. This parameter determines whether the MySQL connection will pull all results for a table into memory (which is fast but requires large amounts of memory), or whether the results will instead be streamed (can be slower, but will work for very large tables). The value specifies the minimum number of rows a table must contain before the connector will stream results, and defaults to 1,000. Set this parameter to '0' to skip all table size checks and always stream all results during a snapshot.</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>The maximum time that the connector should wait after trying to connect to the MySQL database server before timing out.</td>
    </tr>    
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>The max retry times that the connector should retry to build MySQL database server connection.</td>
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
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from MySQL server.
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
      'connector' = 'mysql-cdc',
      'hostname' = 'localhost',
      'port' = '3306',
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
      'connector' = 'mysql-cdc',
      'hostname' = 'localhost',
      'port' = '3306',
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


Features
--------

### Incremental Snapshot Reading

Incremental snapshot reading is a new mechanism to read snapshot of a table. Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:
* (1) MySQL CDC Source can be parallel during snapshot reading
* (2) MySQL CDC Source can perform checkpoints in the chunk granularity during snapshot reading
* (3) MySQL CDC Source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading

If you would like the source run in parallel, each parallel reader should have an unique server id, so the 'server-id' must be a range like '5400-6400', 
and the range must be larger than the parallelism.

During the incremental snapshot reading, the MySQL CDC Source firstly splits snapshot chunks (splits) by primary key of table,
and then MySQL CDC Source assigns the chunks to multiple readers to read the data of snapshot chunk.

#### Controlling Parallelism

Incremental snapshot reading provides the ability to read snapshot data parallelly.
You can control the source parallelism by setting the job parallelism `parallelism.default`. For example, in SQL CLI:

```sql
Flink SQL> SET 'parallelism.default' = 8;
```

#### Checkpoint

Incremental snapshot reading provides the ability to perform checkpoint in chunk level. It resolves the checkpoint timeout problem in previous version with old snapshot reading mechanism.

#### Lock-free

The MySQL CDC source use **incremental snapshot algorithm**, which avoid acquiring global read lock (FLUSH TABLES WITH READ LOCK) and thus doesn't need `RELOAD` permission.

#### MySQL High Availability Support

The ```mysql-cdc``` connector offers high availability of MySQL high available cluster by using the [GTID](https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-concepts.html) information. To obtain the high availability, the MySQL cluster need enable the GTID mode, the GTID mode in your mysql config file should contain following settings:

```yaml
gtid_mode = on
enforce_gtid_consistency = on
```

If the monitored MySQL server address contains slave instance, you need set following settings to the MySQL conf file. The setting ```log-slave-updates = 1``` enables the slave instance to also write the data that synchronized from master to its binlog, this makes sure that the ```mysql-cdc``` connector can consume entire data from the slave instance.

```yaml
gtid_mode = on
enforce_gtid_consistency = on
log-slave-updates = 1
```

After the server you monitored fails in MySQL cluster, you only need to change the monitored server address to other available server and then restart the job from the latest checkpoint/savepoint, the job will restore from the checkpoint/savepoint and won't miss any records.

It's recommended to configure a DNS(Domain Name Service) or VIP(Virtual IP Address) for your MySQL cluster, using the DNS or VIP address for ```mysql-cdc``` connector, the DNS or VIP would automatically route the network request to the active MySQL server. In this way, you don't need to modify the address and restart your pipeline anymore.

#### MySQL Heartbeat Event Support

If the table updates infrequently, the binlog file or GTID set may have been cleaned in its last committed binlog position.
The CDC job may restart fails in this case. So the heartbeat event will help update binlog position. By default heartbeat event is enabled in MySQL CDC source and the interval is set to 30 seconds. You can specify the interval by using table option ```heartbeat.interval```, or set the option to `0s` to disable heartbeat events.

#### How Incremental Snapshot Reading works

When the MySQL CDC source is started, it reads snapshot of table parallelly and then reads binlog of table with single parallelism.

In snapshot phase, the snapshot is cut into multiple snapshot chunks according to primary key of table and the size of table rows.
Snapshot chunks is assigned to multiple snapshot readers. Each snapshot reader reads its received chunks with [chunk reading algorithm](#snapshot-chunk-reading) and send the read data to downstream.
The source manages the process status (finished or not) of chunks, thus the source of snapshot phase can support checkpoint in chunk level.
If a failure happens, the source can be restored and continue to read chunks from last finished chunks.

After all snapshot chunks finished, the source will continue to read binlog in a single task.
In order to guarantee the global data order of snapshot records and binlog records, binlog reader will start to read data
until there is a complete checkpoint after snapshot chunks finished to make sure all snapshot data has been consumed by downstream.
The binlog reader tracks the consumed binlog position in state, thus source of binlog phase can support checkpoint in row level.

Flink performs checkpoints for the source periodically, in case of failover, the job will restart and restore from the last successful checkpoint state and guarantees the exactly once semantic.

##### Snapshot Chunk Splitting

When performing incremental snapshot reading, MySQL CDC source need a criterion which used to split the table.
MySQL CDC Source use a splitting column to split the table to multiple splits (chunks). By default, MySQL CDC source will identify the primary key column of the table and use the first column in primary key as the splitting column.
If there is no primary key in the table, incremental snapshot reading will fail and you can disable `scan.incremental.snapshot.enabled` to fallback to old snapshot reading mechanism.

For numeric and auto incremental splitting column, MySQL CDC Source efficiently splits chunks by fixed step length.
For example, if you had a table with a primary key column of `id` which is auto-incremental BIGINT type, the minimum value was `0` and maximum value was `100`,
and the table option `scan.incremental.snapshot.chunk.size` value is `25`, the table would be split into following chunks:

```
 (-∞, 25),
 [25, 50),
 [50, 75),
 [75, 100),
 [100, +∞)
```

For other primary key column type, MySQL CDC Source executes the statement in the form of `SELECT MAX(STR_ID) AS chunk_high FROM (SELECT * FROM TestTable WHERE STR_ID > 'uuid-001' limit 25)` to get the low and high value for each chunk, 
the splitting chunks set would be like:

 ```
 (-∞, 'uuid-001'),
 ['uuid-001', 'uuid-009'),
 ['uuid-009', 'uuid-abc'),
 ['uuid-abc', 'uuid-def'),
 [uuid-def, +∞).
```

##### Chunk Reading Algorithm

For above example `MyTable`, if the MySQL CDC Source parallelism was set to 4, MySQL CDC Source would run 4 readers which each executes **Offset Signal Algorithm** to 
get a final consistent output of the snapshot chunk. The **Offset Signal Algorithm** simply describes as following:

 * (1) Record current binlog position as `LOW` offset
 * (2) Read and buffer the snapshot chunk records by executing statement `SELECT * FROM MyTable WHERE id > chunk_low AND id <= chunk_high`
 * (3) Record current binlog position as `HIGH` offset
 * (4) Read the binlog records that belong to the snapshot chunk from `LOW` offset to `HIGH` offset
 * (5) Upsert the read binlog records into the buffered chunk records, and emit all records in the buffer as final output (all as INSERT records) of the snapshot chunk
 * (6) Continue to read and emit binlog records belong to the chunk after the `HIGH` offset in *single binlog reader*.

The algorithm is inspired by [DBLog Paper](https://arxiv.org/pdf/2010.12597v1.pdf), please refer it for more detail.
 
**Note:** If the actual values for the primary key are not uniformly distributed across its range, this may lead to unbalanced tasks when incremental snapshot read.

### Exactly-Once Processing

The MySQL CDC connector is a Flink Source connector which will read table snapshot chunks first and then continues to read binlog, 
both snapshot phase and binlog phase, MySQL CDC connector read with **exactly-once processing** even failures happen. 

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for MySQL CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
- `earliest-offset`: Skip snapshot phase and start reading binlog events from the earliest accessible binlog offset.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
the end of the binlog which means only have the changes since the connector was started.
- `specific-offset`: Skip snapshot phase and start reading binlog events from a specific offset. The offset could be
specified with binlog filename and position, or a GTID set if GTID is enabled on server.
- `timestamp`: Skip snapshot phase and start reading binlog events from a specific timestamp.

For example in DataStream API:
```java
MySQLSource.builder()
    .startupOptions(StartupOptions.earliest()) // Start from earliest offset
    .startupOptions(StartupOptions.latest()) // Start from latest offset
    .startupOptions(StartupOptions.specificOffset("mysql-bin.000003", 4L) // Start from binlog file and offset
    .startupOptions(StartupOptions.specificOffset("24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")) // Start from GTID set
    .startupOptions(StartupOptions.timestamp(1667232000000L) // Start from timestamp
    ...
    .build()
```

and with SQL:

```SQL
CREATE TABLE mysql_source (...) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset', -- Start from earliest offset
    'scan.startup.mode' = 'latest-offset', -- Start from latest offset
    'scan.startup.mode' = 'specific-offset', -- Start from specific offset
    'scan.startup.mode' = 'timestamp', -- Start from timestamp
    'scan.startup.specific-offset.file' = 'mysql-bin.000003', -- Binlog filename under specific offset startup mode
    'scan.startup.specific-offset.pos' = '4', -- Binlog position under specific offset mode
    'scan.startup.specific-offset.gtid-set' = '24DA167-0C0C-11E8-8442-00059A3C7B00:1-19', -- GTID set under specific offset startup mode
    'scan.startup.timestamp-millis' = '1667232000000' -- Timestamp under timestamp startup mode
    ...
)
```

**Notes:**
1. MySQL source will print the current binlog position into logs with INFO level on checkpoint, with the prefix
"Binlog offset on checkpoint {checkpoint-id}". It could be useful if you want to restart the job from a specific checkpointed position.
2. If schema of capturing tables was changed previously, starting with earliest offset, specific offset or timestamp
could fail as the Debezium reader keeps the current latest table schema internally and earlier records with unmatched schema cannot be correctly parsed.

### DataStream Source

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .databaseList("yourDatabaseName") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
        .tableList("yourDatabaseName.yourTableName") // set captured table
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
      // set 4 parallel source tasks
      .setParallelism(4)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print MySQL Snapshot + Binlog");
  }
}
```

**Note:** Please refer [Deserialization](../about.html#deserialization) for more details about the JSON deserialization.

### Scan Newly Added Tables

Scan Newly Added Tables feature enables you add new tables to monitor for existing running pipeline, the newly added tables will read theirs snapshot data firstly and then read their changelog automatically.
 
Imaging this scenario: At the beginning, a Flink job monitor tables `[product, user, address]`, but after some days we would like the job can also monitor tables `[order, custom]` which contains history data, and we need the job can still reuse existing state of the job, this feature can resolve this case gracefully.

The following operations show how to enable this feature to resolve above scenario. An existing Flink job which uses CDC Source like:

```java
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .scanNewlyAddedTableEnabled(true) // enable scan the newly added tables feature
        .databaseList("db") // set captured database
        .tableList("db.product, db.user, db.address") // set captured tables [product, user, address]
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .build();
   // your business code
```

If we would like to add new tables `[order, custom]` to an existing Flink job，just need to update the `tableList()` value of the job to include `[order, custom]` and restore the job from previous savepoint.

_Step 1_: Stop the existing Flink job with savepoint.
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_Step 2_: Update the table list option for the existing Flink job .
1. update `tableList()` value.
2. build the jar of updated job.
```java
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .scanNewlyAddedTableEnabled(true) 
        .databaseList("db") 
        .tableList("db.product, db.user, db.address, db.order, db.custom") // set captured tables [product, user, address ,order, custom]
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .build();
   // your business code
```
_Step 3_: Restore the updated Flink job from savepoint.
```shell
$ ./bin/flink run \
      --detached \ 
      --fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**Note:** Please refer the doc [Restore the job from previous savepoint](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/cli/#command-line-interface) for more details.

### Tables Without primary keys

Starting from version 2.4.0, MySQL CDC support tables that do not have a primary key. To use a table without primary keys, you must configure the `scan.incremental.snapshot.chunk.key-column` option and specify one non-null field. 

There are two places that need to be taken care of.

1. If there is an index in the table, try to use a column which is contained in the index in `scan.incremental.snapshot.chunk.key-column`. This will increase the speed of select statement.
2. The processing semantics of a MySQL CDC table without primary keys is determined based on the behavior of the column that are specified by the `scan.incremental.snapshot.chunk.key-column`.
  * If no update operation is performed on the specified column, the exactly-once semantics is ensured.
  * If the update operation is performed on the specified column, only the at-least-once semantics is ensured. However, you can specify primary keys at downstream and perform the idempotence operation to ensure data correctness.

### About converting binary type data to base64 encoded data

```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    binary_data STRING,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'test_db',
    'table-name' = 'test_tb',
    'debezium.binary.handling.mode' = 'base64'
);
```

`binary_data` field in the database is of type VARBINARY(N). In some scenarios, we need to convert binary data to base64 encoded string data. This feature can be enabled by adding the parameter 'debezium.binary.handling.mode'='base64',
By adding this parameter, we can map the binary field type to 'STRING' in Flink SQL, thereby obtaining base64 encoded string data.

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
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
      <td>The precision for DECIMAL data type is up to 65 in MySQL, but the precision for DECIMAL is limited to 38 in Flink.
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
      <td>Currently, for BLOB data type in MySQL, only the blob whose length isn't greater than 2,147,483,647(2 ** 31 - 1) is supported. </td>
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
      <td>As the SET data type in MySQL is a string object that can have zero or more values, 
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
      The spatial data types in MySQL will be converted into STRING with a fixed Json format.
      Please see <a href="#mysql-spatial-data-types-mapping ">MySQL Spatial Data Types Mapping</a> section for more detailed information.
      </td>
    </tr>
    </tbody>
</table>
</div>

### MySQL Spatial Data Types Mapping
The spatial data types except for `GEOMETRYCOLLECTION` in MySQL will be converted into Json String with a fixed format like:<br>
```json
{"srid": 0 , "type": "xxx", "coordinates": [0, 0]}
```
The field `srid` identifies the SRS in which the geometry is defined, SRID 0 is the default for new geometry values if no SRID is specified.
As only MySQL 8+ support to specific SRID when define spatial data type, the field `srid` will always be 0 in MySQL with a lower version.

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
        <th class="text-left">Spatial data in MySQL</th>
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

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
