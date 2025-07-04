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
Note: Since the Postgres WAL log cannot parse table structure change records, Postgres CDC Pipeline does not support table structure changes.

## Example

An example of the pipeline for reading data from Postgres and sink to Doris can be defined as follows:

```yaml
source:
   type: posgtres
   name: Postgres Source
   hostname: 127.0.0.1
   port: 5432
   username: admin
   password: pass
   tables: adb.\.*.\.*, bdb.user_schema_[0-9].user_table_[0-9]+, [app|web].schema_\.*.order_\.*
   decoding.plugin.name:  pgoutput
   slot.name: pgtest

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
   name: Postgres to Doris Pipeline
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
          It is important to note that the dot (.) is treated as a delimiter for database and table names. 
          If there is a need to use a dot (.) in a regular expression to match any character, it is necessary to escape the dot with a backslash.<br>
          例如，adb.\.*.\.*, bdb.user_schema_[0-9].user_table_[0-9]+, [app|web].schema_\.*.order_\.*</td>
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

### Postgres Spatial Data Types Mapping
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
        <td>{"hexewkb":"0101000020730c00001c7c613255de6540787aa52c435c42c0","srid":3187}</td>
      </tr>
      <tr>
        <td>GEOGRAPHY(MULTILINESTRING)</td>
        <td>{"hexewkb":"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0","srid":4326}</td>
      </tr>
    </tbody>
</table>
</div>

{{< top >}}