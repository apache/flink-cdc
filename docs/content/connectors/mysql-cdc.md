# MySQL CDC Connector

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document describes how to setup the MySQL CDC connector to run SQL queries against MySQL databases.

Dependencies
------------

In order to setup the MySQL CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <version>2.0.2</version>
</dependency>
```

### SQL Client JAR

Download [flink-sql-connector-mysql-cdc-2.0.2.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.0.2/flink-sql-connector-mysql-cdc-2.0.2.jar) and put it under `<FLINK_HOME>/lib/`.

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
      <td>Table name of the MySQL database to monitor. The table-name also supports regular expressions to monitor multiple tables matches the regular expression.</td>
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
      <td>Integer</td>
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
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
the end of the binlog which means only have the changes since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not using them together. If you speicifying both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._


### DataStream Source

The Incremental Snapshot Reading feature of MySQL CDC Source only exposes in SQL currently, if you're using DataStream, please use legacy MySQL Source:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;

public class MySqlBinlogSourceExample {
  public static void main(String[] args) throws Exception {
    Properties debeziumProperties = new Properties();
    debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
    SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .databaseList("yourDatabaseName") // set captured database
        .tableList("yourDatabaseName.yourTableName") // set captured table
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
        .debeziumProperties(debeziumProperties)
        .build();


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    env.enableCheckpointing(3000); // checkpoint every 3000 milliseconds
    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute();
  }
}
```

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        DOUBLE<br>
        DOUBLE PRECISION</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
         DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
         TINYINT(1)</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>DATETIME [(p)]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]</td>
      <td>TIMESTAMP [(p)]<br>
          TIMESTAMP [(p)] WITH LOCAL TIME ZONE
      </td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        VARCHAR(n)<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>
        BINARY<br>
        VARBINARY<br>
        BLOB</td>
      <td>BYTES</td>
    </tr>
    </tbody>
</table>
</div>

FAQ
--------

#### Q1: How to skip snapshot and only read from binlog? 

Please see [Startup Reading Position](#startup-reading-position) section.

#### Q2: How to read a shared database that contains multiple tables, e.g. user_00, user_01, ..., user_99 ?

The `table-name` option supports regular expressions to monitor multiple tables matches the regular expression. So you can set `table-name` to `user_.*` to monitor all the `user_` prefix tables. The same to the `database-name` option. Note that the shared table should be in the same schema.

#### Q3: ConnectException: Received DML '...' for processing, binlog probably contains events generated with statement or mixed based replication format

If there is above exception, please check `binlog_format` is `ROW`, you can check this by running `show variables like '%binlog_format%'` in MySQL client. Please note that even if the `binlog_format` configuration of your database is `ROW`, this configuration can be changed by other sessions, for example, `SET SESSION binlog_format='MIXED'; SET SESSION tx_isolation='REPEATABLE-READ'; COMMIT;`. Please also make sure there are no other session are changing this configuration.

#### Q4: Mysql8.0 Public Key Retrieval is not allowed ?

This is because the MySQL user account uses `sha256_password` authentication which requires transporting password under protection like TLS protocol. A simple way is to enable the MySQL user account use naive password.
```sql
-- MySQL
ALTER USER 'username'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
FLUSH PRIVILEGES;
```
