# MySQL CDC Connector

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document describes how to setup the MySQL CDC connector to run SQL queries against MySQL databases.

Dependencies
------------

In order to setup the MySQL CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.alibaba.ververica</groupId>
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <version>1.4.0</version>
</dependency>
```

### SQL Client JAR

Download [flink-sql-connector-mysql-cdc-1.4.0.jar](https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-mysql-cdc/1.4.0/flink-sql-connector-mysql-cdc-1.4.0.jar) and put it under `<FLINK_HOME>/lib/`.

Setup MySQL server
----------------

You have to define a MySQL user with appropriate permissions on all databases that the Debezium MySQL connector monitors.

1. Create the MySQL user:

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. Grant the required permissions to the user:

```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

3. Finalize the user’s permissions:

```sql
mysql> FLUSH PRIVILEGES;
```

See more about the [permission explanation](https://debezium.io/documentation/reference/1.2/connectors/mysql.html#_permissions_explained).


Notes
----------------

### How MySQL CDC source works

When the MySQL CDC source is started, it grabs a global read lock (`FLUSH TABLES WITH READ LOCK`) that blocks writes by other database. Then it reads current binlog position and the schema of the databases and tables. After that, the global read lock is released. Then it scans the database tables and reads binlog from the previous recorded position. Flink will periodically perform checkpoints to record the binlog position. In case of failover, the job will restart and restore from the checkpointed binlog position. Consequently, it guarantees the exactly once semantic. 

### Grant RELOAD permission for MySQL user
If the MySQL user is not granted the RELOAD permission, the MySQL CDC source will use table-level locks instead and performs a snapshot with this method. This blocks write for a longer time. 

### The global read lock (FLUSH TABLES WITH READ LOCK)
The global read lock is hold during reading binlog position and schema. It may takes seconds which is depending on the numbers of tables. The global read lock blocks writes, so it may still affect online business. If you want to skip the read lock, and can tolerate at-least-once semantic, you can add `'debezium.snapshot.locking.mode' = 'none'` option to skip the lock. 

### Set a differnet SERVER ID for each job
Every MySQL database client for reading binlog should have an unique id, called server id. MySQL server will use this id to maintain network connection and the binlog position. Therefore, if different jobs share a same server id, it may result to reading from wrong binlog position. 
Thus, it is recommended to set different server id for each job via the [SQL Hints](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/hints.html), e.g. `SELECT * FROM source_table /*+ OPTIONS('server-id'='123456') */ ;`. 

Tip: By default the server id is randomed when the TaskManager is started up. If TaskManager is failed down, it may have a different server id when starting up again. But this shouldn't happen frequently (job exception doesn't restart TaskManager), and shouldn't affect MySQL server too much.

### Can't perform checkpoint during scaning database tables
During scanning tables, since there is no recoverable position, we can't perform checkpoints. In order to not perform checkpoints, MySQL CDC source will keep the checkpoint waiting to timeout. The timemout checkpoint will be recognized as failed checkpoint, by default, this will trigger a failover for the Flink job. So if the database table is large, it is recommended to add following Flink configurations to avoid failover because of the timeout checkpoints:

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

### Setting up MySQL session timeouts

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring interactive_timeout and wait_timeout in your MySQL configuration file.
- `interactive_timeout`: The number of seconds the server waits for activity on an interactive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: The number of seconds the server waits for activity on a noninteractive connection before closing it. See [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).



How to create a MySQL CDC table
----------------

The MySQL CDC table can be defined as following:

```sql
-- register a MySQL table 'orders' in Flink SQL
CREATE TABLE orders (
  order_id INT,
  order_date TIMESTAMP(0),
  customer_name STRING,
  price DECIMAL(10, 5),
  product_id INT,
  order_status BOOLEAN
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'mydb',
  'table-name' = 'orders'
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
      <td>A numeric ID of this database client, which must be unique across all currently-running database processes in the MySQL cluster.
          This connector joins the MySQL database cluster as another server (with this unique ID) so it can read the binlog.
          By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.</td>
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
          See more <a href="https://debezium.io/documentation/reference/1.2/connectors/mysql.html#_temporal_values">here</a>.</td>
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
      <td>debezium.snapshot.
      fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Specifies the maximum number of rows that should be read in one go from each table while taking a snapshot. The connector will read the table contents in multiple batches of this size.</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from MySQL server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.2/connectors/mysql.html#mysql-connector-configuration-properties_debezium">Debezium's MySQL Connector properties</a></td> 
    </tr>
    </tbody>
</table>
</div>

Features
--------

### Exactly-Once Processing

The MySQL CDC connector is a Flink Source connector which will read database snapshot first and then continues to read binlogs with **exactly-once processing** even failures happen. Please read [How the connector performs database snapshot](https://debezium.io/documentation/reference/1.2/connectors/mysql.html#how-the-mysql-connector-performs-database-snapshots_debezium). 

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for MySQL CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
the end of the binlog which means only have the changes since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not using them together. If you speicifying both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### Single Thread Reading

The MySQL CDC source can't work in parallel reading, because there is only one task can receive binlog events. 

### DataStream Source

The MySQL CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

public class MySqlBinlogSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
      .hostname("localhost")
      .port(3306)
      .databaseList("inventory") // monitor all tables under inventory database
      .username("flinkuser")
      .password("flinkpw")
      .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
