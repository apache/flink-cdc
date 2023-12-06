# Postgres CDC Connector

The Postgres CDC connector allows for reading snapshot data and incremental data from PostgreSQL database. This document describes how to setup the Postgres CDC connector to run SQL queries against PostgreSQL databases.

Dependencies
------------

In order to setup the Postgres CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-postgres-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself. -->
  <version>2.5-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download flink-sql-connector-postgres-cdc-2.5-SNAPSHOT.jar and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-postgres-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-postgres-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-postgres-cdc), the released version will be available in the Maven central warehouse.

How to create a Postgres CDC table
----------------

The Postgres CDC table can be defined as following:

```sql
-- register a PostgreSQL table 'shipments' in Flink SQL
CREATE TABLE shipments (
  shipment_id INT,
  order_id INT,
  origin STRING,
  destination STRING,
  is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink',
   -- experimental feature: incremental snapshot (default off)
  'scan.incremental.snapshot.enabled' = 'true'
);

-- read snapshot and binlogs from shipments table
SELECT * FROM shipments;
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
      <td>Specify what connector to use, here should be <code>'postgres-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the PostgreSQL server to monitor.</td>
    </tr>
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name of the PostgreSQL database to monitor.</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the PostgreSQL database to monitor.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>Integer port number of the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td>slot.name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the PostgreSQL logical decoding slot that was created for streaming changes from a particular plug-in
          for a particular database/schema. The server uses this slot to stream events to the connector that you are configuring.
          <br/>Slot names must conform to <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL replication slot naming rules</a>, which state: "Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character."</td>
    </tr> 
    <tr>
      <td>decoding.plugin.name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">decoderbufs</td>
      <td>String</td>
      <td>The name of the Postgres logical decoding plug-in installed on the server.
          Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.</td>
    </tr>
    <tr>
      <td>changelog-mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">all</td>
      <td>String</td>
      <td>The changelog mode used for encoding streaming changes. Supported values are <code>all</code> (which encodes changes as retract stream using all RowKinds) and <code>upsert</code> (which encodes changes as upsert stream that describes idempotent updates on a key).
          <br/> <code>upsert</code> mode can be used for tables with primary keys when replica identity <code>FULL</code> is not an option. Primary keys must be set to use <code>upsert</code> mode.</td>
    </tr>
    <tr>
      <td>heartbeat.interval.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>The interval of sending heartbeat event for tracing the latest available replication slot offsets</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from Postgres server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-connector-properties">Debezium's Postgres Connector properties</a></td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>If you encounter a situation where there is a large amount of data in the table and you don't need all the historical data. You can try to specify the underlying configuration in debezium to select the data range you want to snapshot. This parameter only affects snapshots and does not affect subsequent data reading consumption.
        <br/> Note: PostgreSQL must use schema name and table name.
        <br/> For example: <code>'debezium.snapshot.select.statement.overrides' = 'schema.table'</code>.
        <br/> After specifying the above attributes, you must also add the following attributes:
        <code> debezium.snapshot.select.statement.overrides.[schema].[table] </code>
      </td>
    </tr>
    <tr>
      <td>debezium.snapshot.select.statement.overrides.[schema].[table]</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>You can specify SQL statements to limit the data range of snapshot.
        <br/> Note1: Schema and table need to be specified in the SQL statement, and the SQL should conform to the syntax of the data source.Currently.
        <br/> For example: <code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where 1 != 1'</code>.
        <br/> Note2: The Flink SQL client submission task does not support functions with single quotation marks in the content.
        <br/> For example: <code>'debezium.snapshot.select.statement.overrides.schema.table' = 'select * from schema.table where to_char(rq, 'yyyy-MM-dd')'</code>.
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">false</td>
          <td>Boolean</td>
          <td>Incremental snapshot is a new mechanism to read snapshot of a table. Compared to the old snapshot mechanism,
              the incremental snapshot has many advantages, including:
                (1) source can be parallel during snapshot reading,
                (2) source can perform checkpoints in the chunk granularity during snapshot reading,
                (3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.
              Please see <a href="#incremental-snapshot-reading ">Incremental Snapshot Reading</a>section for more detailed information.
          </td>
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
    </tbody>
    </table>
</div>
<div>

Note: `slot.name` is recommended to set for different tables to avoid the potential `PSQLException: ERROR: replication slot "flink" is active for PID 974` error. See more [here](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-property-slot-name).

### Incremental Snapshot Options

The following options is available only when `scan.incremental.snapshot.enabled=true`:

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
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for Postgres CDC consumer, valid enumerations are "initial"
           and "latest-offset".
           Please see <a href="#startup-reading-position">Startup Reading Position</a> section for more detailed information.</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>The maximum time that the connector should wait after trying to connect to the PostgreSQL database server before timing out.</td>
    </tr>
    <tr>
          <td>connect.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30</td>
          <td>Integer</td>
          <td>The connection pool size.</td>
    </tr>
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>The max retry times that the connector should retry to build database server connection.</td>
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
            By default, the chunk key is the first column of the primary key. This column must be a column of the primary key.</td>
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

### Can't perform checkpoint during scanning snapshot of tables when incremental snapshot is disabled

When `scan.incremental.snapshot.enabled=false`, we have the following limitation.

During scanning snapshot of database tables, since there is no recoverable position, we can't perform checkpoints. In order to not perform checkpoints, Postgres CDC source will keep the checkpoint waiting to timeout. The timeout checkpoint will be recognized as failed checkpoint, by default, this will trigger a failover for the Flink job. So if the database table is large, it is recommended to add following Flink configurations to avoid failover because of the timeout checkpoints:

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    shipment_id INT,
    order_id INT,
    origin STRING,
    destination STRING,
    is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments',
  'slot.name' = 'flink'
);
```

Features
--------

### Incremental Snapshot Reading (Experimental)

Incremental snapshot reading is a new mechanism to read snapshot of a table. Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:
* (1) PostgreSQL CDC Source can be parallel during snapshot reading
* (2) PostgreSQL CDC Source can perform checkpoints in the chunk granularity during snapshot reading
* (3) PostgreSQL CDC Source doesn't need to acquire global read lock before snapshot reading

During the incremental snapshot reading, the PostgreSQL CDC Source firstly splits snapshot chunks (splits) by primary key of table,
and then PostgreSQL CDC Source assigns the chunks to multiple readers to read the data of snapshot chunk.

### Exactly-Once Processing

The Postgres CDC connector is a Flink Source connector which will read database snapshot first and then continues to read binlogs with **exactly-once processing** even failures happen. Please read [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#how-the-postgresql-connector-works).

### DataStream Source

The Postgres CDC connector can also be a DataStream source. There are two modes for the DataStream source:

- incremental snapshot based, which allows parallel reading
- SourceFunction based, which only supports single thread reading

#### Incremental Snapshot based DataStream (Experimental)

```java
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresParallelSourceExample {

    public static void main(String[] args) throws Exception {

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("inventory")
                        .tableList("inventory.products")
                        .username("postgres")
                        .password("postgres")
                        .slotName("flink")
                        .decodingPluginName("decoderbufs") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserializer)
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2) // the split size of each snapshot split
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();

        env.execute("Output Postgres Snapshot");
    }
}
```

#### SourceFunction-based DataStream

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;

public class PostgreSQLSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
      .hostname("localhost")
      .port(5432)
      .database("postgres") // monitor postgres database
      .schemaList("inventory")  // monitor inventory schema
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
**Note:** Please refer [Deserialization](../about.html#deserialization) for more details about the JSON deserialization.

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        INT2<br>
        SMALLSERIAL<br>
        SERIAL2</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INTEGER<br>
        SERIAL</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        BIGSERIAL</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td></td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>
        REAL<br>
        FLOAT4</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        FLOAT8<br>
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
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        CHARACTER(n)<br>
        VARCHAR(n)<br>
        CHARACTER VARYING(n)<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BYTEA</td>
      <td>BYTES</td>
    </tr>
    </tbody>
</table>
</div>

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
