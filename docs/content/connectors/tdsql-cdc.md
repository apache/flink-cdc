# TDSQL CDC Connector

The TDSQL CDC connector allows for reading snapshot data and incremental data from TDSQL database. This document describes how to setup the TDSQL CDC connector to run SQL queries against TDSQL databases.

Dependencies
------------

In order to setup the TDSQL CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-tdsql-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.3-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-tdsql-cdc-2.3-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-tdsql-cdc/2.3-SNAPSHOT/flink-sql-connector-mysql-cdc-2.3-SNAPSHOT.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-tdsql-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-tdsql-cdc-XXX.jar](https://mvnrepository.com/artifact/com.ververica/flink-connector-mysql-cdc), the released version will be available in the Maven central warehouse.

Setup TDSQL server
----------------

You have to define a TDSQL user with appropriate permissions on all databases that the Debezium MySQL connector monitors.

1. Create the TDSQL user:

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

See more about the [permission explanation](https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-creating-user).


Notes
----------------

### Set a different SERVER ID for each reader

Every MySQL database client for reading binlog should have an unique id, called server id. MySQL server will use this id to maintain network connection and the binlog position. Therefore, if different jobs share a same server id, it may result to read from wrong binlog position.
Thus, it is recommended to set different server id for each reader via the [SQL Hints](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/hints.html),
e.g.  assuming the source parallelism is 4, then we can use `SELECT * FROM source_table /*+ OPTIONS('server-id'='5401-5404') */ ;` to assign unique server id for each of the 4 source readers.


### Setting up MySQL session timeouts

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring interactive_timeout and wait_timeout in your MySQL configuration file.
- `interactive_timeout`: The number of seconds the server waits for activity on an interactive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: The number of seconds the server waits for activity on a noninteractive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).


How to create a TDSQL CDC table
----------------

The TDSQL CDC table can be defined as following:

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a TDSQL table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'tdsql-cdc',
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
      <td>Specify what connector to use, here should be <code>'tdsql-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the TDSQL database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the TDSQL database to use when connecting to the TDSQL database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the TDSQL database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the TDSQL server to monitor. The database-name also supports regular expressions to monitor multiple tables matches the regular expression.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the TDSQL database to monitor. The table-name also supports regular expressions to monitor multiple tables matches the regular expression.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td>Integer port number of the TDSQL database server.</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like '5400', 
          the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is recommended when 'scan.incremental.snapshot.enabled' enabled.
          Every ID must be unique across all currently-running database processes in the TDSQL MySQL cluster. This connector joins the TDSQL MySQL cluster
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
      <td>Optional startup mode for MySQL CDC consumer, valid enumerations are "initial"
           and "latest-offset". 
           Please see <a href="#startup-reading-position">Startup Reading Position</a>section for more detailed information.</td>
    </tr> 
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">UTC</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai". 
          It controls how the TIMESTAMP type in MYSQL converted to STRING.
          See more <a href="https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types">here</a>.</td>
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
          See more about the <a href="https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-connector-properties">Debezium's MySQL Connector properties</a></td> 
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
  </tbody>
</table>

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
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
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'tdsql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);
```

Features
--------

### Incremental Snapshot Reading

Incremental snapshot reading is a new mechanism to read snapshot of a table. Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:
* (1) TDSQL CDC Source can be parallel during snapshot reading
* (2) TDSQL CDC Source can perform checkpoints in the chunk granularity during snapshot reading
* (3) TDSQL CDC Source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading

If you would like the source run in parallel, each parallel reader should have an unique server id, so the 'server-id' must be a range like '5400-6400',
and the range must be larger than the parallelism.

During the incremental snapshot reading, the TDSQL CDC Source firstly splits snapshot chunks (splits) by primary key of table,
and then TDSQL CDC Source assigns the chunks to multiple readers to read the data of snapshot chunk.

**important:** TDSQL CDC don't guarantee each *TDSQL cluster set* global record in order, but guarantee each single *cluster set* record in order.

#### Controlling Parallelism

Incremental snapshot reading provides the ability to read snapshot data parallelly.
You can control the source parallelism by setting the job parallelism `parallelism.default`. For example, in SQL CLI:

```sql
Flink SQL> SET 'parallelism.default' = 8;
```

#### Checkpoint

Incremental snapshot reading provides the ability to perform checkpoint in chunk level. It resolves the checkpoint timeout problem in previous version with old snapshot reading mechanism.

#### Lock-free

The TDSQL CDC source use **incremental snapshot algorithm**, which avoid acquiring global read lock (FLUSH TABLES WITH READ LOCK) and thus doesn't need `RELOAD` permission.

#### TDSQL Heartbeat Event Support

If the table updates infrequently, the binlog file or GTID set may have been cleaned in its last committed binlog position.
The CDC job may restart fails in this case. So the heartbeat event will help update binlog position. By default heartbeat event is enabled in TDSQL CDC source and the interval is set to 30 seconds. You can specify the interval by using table option ```heartbeat.interval```, or set the option to `0s` to disable heartbeat events.

### Exactly-Once Processing

The TDSQL CDC connector is a Flink Source connector which will read table snapshot chunks first and then continues to read binlog,
both snapshot phase and binlog phase, TDSQL CDC connector read with **exactly-once processing** even failures happen.

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for TDSQL CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the binlog which means only have the changes since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not using them together. If you speicifying both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### DataStream Source

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.tdsql.source.TdSqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    TdSqlSource<String> tdSqlSource = TdSqlSource.<String>builder()
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
      .fromSource(tdSqlSource, WatermarkStrategy.noWatermarks(), "tdSql Source")
      // set 4 parallel source tasks
      .setParallelism(4)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print TDSQL Snapshot + Binlog");
  }
}
```

**Note:** Please refer [Deserialization](../about.html#deserialization) for more details about the JSON deserialization.

Data Type Mapping
----------------

TDSQL CDC data type mapping same as [MySQL CDC](./mysql-cdc.md)