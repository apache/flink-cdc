---
title: "Postgres"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/Postgres
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

# Postgres Connector

Postgres connector allows reading snapshot data and incremental data from Postgres database and provides end-to-end full-database data synchronization capabilities.
This document describes how to setup the Postgres connector.
Note: Since the Postgres WAL log cannot parse table structure change records, Postgres CDC Pipeline Source does not support synchronizing table structure changes currently.

## Example

An example of the pipeline for reading data from Postgres and sink to Fluss can be defined as follows:

```yaml
source:
   type: postgres
   name: Postgres Source
   hostname: 127.0.0.1
   port: 5432
   username: admin
   password: pass
   # make sure all the tables share same database.
   tables: adb.\.*.\.*
   decoding.plugin.name:  pgoutput
   slot.name: pgtest

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123
  # Security-related properties for the Fluss client
  properties.client.security.protocol: sasl
  properties.client.security.sasl.mechanism: PLAIN
  properties.client.security.sasl.username: developer
  properties.client.security.sasl.password: developer-pass

pipeline:
   name: Postgres to Fluss Pipeline
   parallelism: 4
```

## Connector Options

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
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the Postgres database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5432</td>
      <td>Integer</td>
      <td>Integer port number of the Postgres database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Postgres database to use when connecting to the Postgres database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the Postgres database server.</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Postgres database to monitor. The table-name also supports regular expressions to monitor multiple tables that satisfy the regular expressions. <br>
          All the tables are required to share same database.  <br>
          It is important to note that the dot (.) is treated as a delimiter for database, schema and table names.
          If there is a need to use a dot (.) in a regular expression to match any character, it is necessary to escape the dot with a backslash.<br>
          for example:  bdb.user_schema_[0-9].user_table_[0-9]+, bdb.schema_\.*.order_\.*</td>
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
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs and pgoutput.</td>
    </tr> 
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Postgres database to exclude, parameter will have an exclusion effect after the tables parameter. The table-name also supports regular expressions to exclude multiple tables that satisfy the regular expressions. <br>
          The usage is the same as the tables parameter</td>
    </tr>
   <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in the database server, for example: "Asia/Shanghai".It controls how TIMESTAMP values in Postgres are converted to strings.For more information, please refer to <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-data-types"> here</a>.If not set, the system default time zone (ZoneId.systemDefault()) will be used to determine the server time zone.
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
      <td> Optional startup mode for Postgres CDC consumer, valid enumerations are "initial"，"latest-offset"，"committed-offset"or"snapshot"。</td>
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
      <td>scan.lsn-commit.checkpoints-num-delay</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The number of checkpoint delays before starting to commit the LSN offsets. <br>
          The checkpoint LSN offsets will be committed in rolling fashion, the earliest checkpoint identifier will be committed first from the delayed checkpoints.
      </td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>The maximum time that the connector should wait after trying to connect to the Postgres database server before timing out. 
              This value cannot be less than 250ms.</td>
    </tr>    
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times that the connector should retry to build Postgres database server connection.</td>
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
      <td>The interval of sending heartbeat event for tracing the latest available wal log offsets.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from postgres server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/postgresql.html">Debezium's Postgres Connector properties</a></td> 
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>String</td>
      <td>
        The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.
      </td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>String</td>
      <td>
        List of readable metadata from SourceRecord to be passed to downstream and could be used in transform module, split by `,`. Available readable metadata are: op_ts.
      </td>
    </tr>
    <tr>
     <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
     <td>optional</td>
     <td style="word-wrap: break-word;">false</td>
     <td>String</td>
     <td>
        Whether to assign the unbounded chunks first during snapshot reading phase.<br>
        This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk.<br>
        Experimental option, defaults to false.
      </td>
    </tr>
    </tbody>
</table>
</div>


Note:
1. The configuration option tables specifies the tables to be captured by Postgres CDC, in the format db.schema1.table1,db.schema2.table2. All db values must be the same, as the PostgreSQL connection URL requires a single database name. Currently, CDC only supports connecting to one database.

## Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for PostgreSQL CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the replication slot.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the replication which means only have the changes since the connector was started.
- `committed-offset`: Skip snapshot phase and start reading events from a `confirmed_flush_lsn` offset of replication slot.
- `snapshot`: Only the snapshot phase is performed and exits after the snapshot phase reading is completed.

## Available Source metrics

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

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        BOOLEAN <br>
        BIT(1) <br>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        BIT( > 1)
      <td>BYTES</td>
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
        BIGSERIAL<br>
        OID<br>
      </td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>
        REAL<br>
        FLOAT4
      </td>
      <td>FLOAT</td>
    </tr>
   <tr>
      <td>NUMERIC</td>
      <td>DECIMAL(38, 0)</td>
    </tr>
    <tr>
      <td>DOUBLE PRECISION<br>
          FLOAT8
      </td>
      <td>DOUBLE</td>
    </tr>
     <tr>
       <td> CHAR[(M)]<br>
            VARCHAR[(M)]<br>
            CHARACTER[(M)]<br>
            BPCHAR[(M)]<br>
            CHARACTER VARYING[(M)]
       </td>
       <td>STRING</td>
     </tr>
    <tr>
      <td>TIMESTAMPTZ<br>
          TIMESTAMP WITH TIME ZONE</td>
      <td>ZonedTimestampType</td>
    </tr>
    <tr>
      <td>INTERVAL [P]</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>INTERVAL [P]</td>
      <td>STRING(when <code>debezium.interval.handling.mode</code> is set to string)</td>
    </tr>
    <tr>
      <td>BYTEA</td>
      <td>BYTES or STRING (when <code>debezium.binary.handling.mode</code> is set to base64 or base64-url-safe or hex)</td>
    </tr>
    <tr>
      <td>
        JSON<br>
        JSONB<br>
        XML<br>
        UUID<br>
        POINT<br>
        LTREE<br>
        CITEXT<br>
        INET<br>
        INT4RANGE<br>
        INT8RANGE<br>
        NUMRANGE<br>
        TSRANGE<br>
        DATERANGE<br>
        ENUM
      </td>
      <td>STRING</td>
    </tr>
    </tbody>
</table>
</div>

Note: Since the Debezium version does not support multidimensional arrays, only PostgresSQL single-dimensional arrays are currently supported, such as :'ARRAY[1,2,3]', 'int[]'.

### Temporal types Mapping
Other than PostgreSQL’s TIMESTAMPTZ data types, which contain time zone information, how temporal types are mapped depends on the value of <code>debezium.the time.precision.mode</code> connector configuration property. The following sections describe these mappings:
- debezium.time.precision.mode=adaptive
- debezium.time.precision.mode=adaptive_time_microseconds
- debezium.time.precision.mode=connect 

Note: Due to the current CDC limitation, the precision for the TIME type is fixed at 3. Regardless of whether <code>debezium.time.precision.mode<code> is set to adaptive, adaptive_time_microseconds, or connect, the TIME type will be converted to TIME(3).

<u>debezium.time.precision.mode=adaptive</u>

When the <code>debezium.time.precision.mode</code> property is set to the default value `adaptive_time_microseconds`, the precision of `TIME` is 3, and the precision of `TIMESTAMP` is 6.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP([P])</td>
      </tr>
    </tbody>
</table>
</div>

<u>debezium.time.precision.mode=adaptive_time_microseconds</u>

When the `debezium.time.precision.mode` property is set to the value `adaptive_time_microseconds`, the precision of `TIME` is 3, and the precision of `TIMESTAMP` is 6.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP([P])</td>
      </tr>
    </tbody>
</table>
</div>

<u>debezium.time.precision.mode=connect</u>

When the <code>debezium.time.precision.mode</code> property is set to the default value connect, both TIME and TIMESTAMP have a precision of 3.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          DATE
        <td>DATE</td>
      </tr>
      <tr>
        <td>
          TIME([P])
        </td>
        <td>TIME(3)</td>
      </tr>
      <tr>
        <td>
          TIMESTAMP([P])
        </td>
        <td>TIMESTAMP(3)</td>
      </tr>
    </tbody>
</table>
</div>

### Decimal types Mapping
The setting of the PostgreSQL connector configuration property <code>debezium.decimal.handling.mode</code> determines how the connector maps decimal types.

When the <code>debezium.decimal.handling.mode</code> property is set to precise, the connector uses the Kafka Connect org.apache.kafka.connect.data.Decimal logical type for all DECIMAL, NUMERIC and MONEY columns. This is the default mode.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>DECIMAL[(M[,D])]</td>
      </tr>
      <tr>
        <td>
          NUMERIC
        <td>DECIMAL(38,0)</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>DECIMAL[(M[,D])]</td>
      </tr>
      <tr>
        <td>
          DECIMAL
        <td>DECIMAL(38,0)</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>DECIMAL(38,digits)(The scale schema parameter contains an integer representing how many digits the decimal point was shifted. The scale schema parameter is determined by the money.fraction.digits connector configuration property.)</td>
      </tr>
    </tbody>
</table>
</div>

When the <code>debezium.decimal.handling.mode</code> property is set to double, the connector represents all DECIMAL, NUMERIC and MONEY values as Java double values and encodes them as shown in the following table.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>DOUBLE</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>DOUBLE</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>DOUBLE</td>
      </tr>
    </tbody>
</table>
</div>

The last possible setting for the <code>debezium.decimal.handling.mode</code> configuration property is string. In this case, the connector represents DECIMAL, NUMERIC and MONEY values as their formatted string representation, and encodes them as shown in the following table.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
          NUMERIC[(M[,D])]
        <td>STRING</td>
      </tr>
      <tr>
        <td>
          DECIMAL[(M[,D])]
        <td>STRING</td>
      </tr>
      <tr>
        <td>
          MONEY[(M[,D])]
        <td>STRING</td>
      </tr>
    </tbody>
</table>
</div>

PostgreSQL supports NaN (not a number) as a special value to be stored in DECIMAL/NUMERIC values when the setting of <code>debezium.decimal.handling.mode</code> is string or double. In this case, the connector encodes NaN as either Double.NaN or the string constant NAN.

### HSTORE type Mapping
The setting of the PostgreSQL connector configuration property <code>debezium.hstore.handling.mode</code> determines how the connector maps HSTORE values.

When the <code>debezium.hstore.handling.mode</code> property is set to json (the default), the connector represents HSTORE values as string representations of JSON values and encodes them as shown in the following table. When the <code>debezium.hstore.handling.mode</code> property is set to map, the connector uses the MAP schema type for HSTORE values.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
         HSTORE
        <td>STRING(<code>`debezium.hstore.handling.mode`=`string`</code>)</td>
      </tr>
      <tr>
        <td>
         HSTORE
        <td>MAP(<code>`debezium.hstore.handling.mode`=`map`</code>)</td>
      </tr>
    </tbody>
</table>
</div>

### Network address types Mapping
PostgreSQL has data types that can store IPv4, IPv6, and MAC addresses. It is better to use these types instead of plain text types to store network addresses. Network address types offer input error checking and specialized operators and functions.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">CDC type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
       <tr>
        <td>
         INET
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         CIDR
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         MACADDR
        <td>STRING</td>
      </tr>
      <tr>
        <td>
         MACADDR8
        <td>STRING</td>
      </tr> 
    </tbody>
</table>
</div>

### PostGIS Types Mapping
PostgreSQL supports spatial data types through the PostGIS extension:
```
    GEOMETRY(POINT, xx): Represents a point in a Cartesian coordinate system, with EPSG:xx defining the coordinate system. It is suitable for local planar calculations. 
    GEOGRAPHY(MULTILINESTRING): Stores multiple line strings in latitude and longitude, based on a spherical model. It is suitable for global-scale spatial analysis.
```
The former is used for small-area planar data, while the latter is used for large-area data requiring consideration of Earth's curvature.
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data in Postgres</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>GEOMETRY(POINT, xx)</td>
        <td>{"coordinates":"[[174.9479, -36.7208]]","type":"Point","srid":3187}"</td>
      </tr>
      <tr>
        <td>GEOGRAPHY(MULTILINESTRING)</td>
        <td>{"coordinates":"[[169.1321, -44.7032],[167.8974, -44.6414]]","type":"MultiLineString","srid":4326}</td>
      </tr>
    </tbody>
</table>
</div>

{{< top >}}