# Postgres CDC Connector

* [Dependencies](#dependencies)
  * [Maven dependency](#maven-dependency)
  * [SQL Client JAR](#sql-client-jar)
* [How to create a Postgres CDC table](#how-to-create-a-postgres-cdc-table)
* [Connector Options](#connector-options)
* [Features](#features)
  * [Exactly-Once Processing](#exactly-once-processing)
  * [Single Thread Reading](#single-thread-reading)
  * [DataStream Source](#datastream-source)
* [Data Type Mapping](#data-type-mapping)

The Postgres CDC connector allows for reading snapshot data and incremental data from PostgreSQL database. This document describes how to setup the Postgres CDC connector to run SQL queries against PostgreSQL databases.

Dependencies
------------

In order to setup the Postgres CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.alibaba.ververica</groupId>
  <artifactId>flink-connector-postgres-cdc</artifactId>
  <version>1.1.0</version>
</dependency>
```

### SQL Client JAR

Download [flink-sql-connector-postgres-cdc-1.1.0.jar](https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-postgres-cdc/1.1.0/flink-sql-connector-postgres-cdc-1.1.0.jar) and put it under `<FLINK_HOME>/lib/`.

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
  'table-name' = 'shipments'
);

-- read snapshot and binlogs from shipments table
SELECT * FROM shipments;
```

Connector Options
----------------

<table class="table table-bordered">
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
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'postgres-cdc'</code>.</td>
    </tr>
    <tr>
      <td><h5>hostname</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td><h5>database-name</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the PostgreSQL server to monitor.</td>
    </tr> 
    <tr>
      <td><h5>schema-name</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name of the PostgreSQL database to monitor.</td>
    </tr>
    <tr>
      <td><h5>table-name</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the PostgreSQL database to monitor.</td>
    </tr>
    <tr>
      <td><h5>port</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>Integer port number of the PostgreSQL database server.</td>
    </tr>
    <tr>
      <td><h5>decoding.plugin.name</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">decoderbufs</td>
      <td>String</td>
      <td>The name of the Postgres logical decoding plug-in installed on the server. 
          Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.</td>
    </tr>    
    <tr>
      <td><h5>slot.name</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">flink</td>
      <td>String</td>
      <td>The name of the PostgreSQL logical decoding slot that was created for streaming changes from a particular plug-in
          for a particular database/schema. The server uses this slot to stream events to the connector that you are configuring.
          <br/>Slot names must conform to <a href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL replication slot naming rules</a>, which state: "Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character."</td>
    </tr>  
   <tr>
      <td><h5>debezium.*</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from Postgres server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-connector-properties">Debezium's Postgres Connector properties</a></td> 
    </tr>   
    </tbody>
</table>

Note: `slot.name` is recommended to set for different tables to avoid the potential `PSQLException: ERROR: replication slot "flink" is active for PID 974` error. See more [here](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-property-slot-name).

Features
--------

### Exactly-Once Processing

The Postgres CDC connector is a Flink Source connector which will read database snapshot first and then continues to read binlogs with **exactly-once processing** even failures happen. Please read [How the connector works](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#how-the-postgresql-connector-works). 

### Single Thread Reading

The Postgres CDC source can't work in parallel reading, because there is only one task can receive binlog events. 

### DataStream Source

The Postgres CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;

public class PostgreSQLSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
      .hostname("localhost")
      .port(5432)
      .database("postgres")
      .schemaList("inventory") // monitor all tables under inventory schema
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

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td><code>TINYINT</code></td>
    </tr>
    <tr>
      <td>
        <code>SMALLINT</code><br>
        <code>INT2</code><br>
        <code>SMALLSERIAL</code><br>
        <code>SERIAL2</code></td>
      <td><code>SMALLINT</code></td>
    </tr>
    <tr>
      <td>
        <code>INTEGER</code><br>
        <code>SERIAL</code></td>
      <td><code>INT</code></td>
    </tr>
    <tr>
      <td>
        <code>BIGINT</code><br>
        <code>BIGSERIAL</code></td>
      <td><code>BIGINT</code></td>
    </tr>
   <tr>
      <td></td>
      <td><code>DECIMAL(20, 0)</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td>
        <code>REAL</code><br>
        <code>FLOAT4</code></td>
      <td><code>FLOAT</code></td>
    </tr>
    <tr>
      <td>
        <code>FLOAT8</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td><code>DOUBLE</code></td>
    </tr>
    <tr>
      <td>
        <code>NUMERIC(p, s)</code><br>
        <code>DECIMAL(p, s)</code></td>
      <td><code>DECIMAL(p, s)</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
    </tr>
    <tr>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td>
        <code>CHAR(n)</code><br>
        <code>CHARACTER(n)</code><br>
        <code>VARCHAR(n)</code><br>
        <code>CHARACTER VARYING(n)</code><br>
        <code>TEXT</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td><code>BYTEA</code></td>
      <td><code>BYTES</code></td>
    </tr>
    </tbody>
</table>
