---
title: "MySQL"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/mysql
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

# MySQL Connector

MySQL connector allows reading snapshot data and incremental data from MySQL database and provides end-to-end full-database data synchronization capabilities.
This document describes how to setup the MySQL connector.

## Dependencies

Since MySQL Connector's GPLv2 license is incompatible with Flink CDC project, we can't provide MySQL connector in prebuilt connector jar packages.
You may need to configure the following dependencies manually, and pass it with `--jar` argument of Flink CDC CLI when submitting YAML pipeline jobs.

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
        <td>Used for connecting to MySQL database.</td>
      </tr>
    </tbody>
</table>
</div>

## Example

An example of the pipeline for reading data from MySQL and sink to Doris can be defined as follows:

```yaml
source:
   type: mysql
   name: MySQL Source
   hostname: 127.0.0.1
   port: 3306
   username: admin
   password: pass
   tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*
   server-id: 5401-5404

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
   name: MySQL to Doris Pipeline
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
      <td>IP address or hostname of the MySQL database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td>Integer port number of the MySQL database server.</td>
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
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the MySQL database to monitor. The table-name also supports regular expressions to monitor multiple tables that satisfy the regular expressions. <br>
          It is important to note that the dot (.) is treated as a delimiter for database and table names. 
          If there is a need to use a dot (.) in a regular expression to match any character, it is necessary to escape the dot with a backslash.<br>
          eg. db0.\.*, db1.user_table_[0-9]+, db[1-2].[app|web]order_\.*</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the MySQL database to exclude, parameter will have an exclusion effect after the tables parameter. The table-name also supports regular expressions to exclude multiple tables that satisfy the regular expressions. <br>
          The usage is the same as the tables parameter</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to send schema change events, so that downstream sinks can respond to schema changes and achieve table structure synchronization.</td>
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
          though we recommend setting an explicit value. </td>
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
      <td>Optional startup mode for MySQL CDC consumer, valid enumerations are "initial", "earliest-offset", "latest-offset", "specific-offset", "timestamp" and "snapshot".</td>
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
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>The maximum time that the connector should wait after trying to connect to the MySQL database server before timing out. 
              This value cannot be less than 250ms.</td>
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
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to enable scan the newly added tables feature or not, by default is false. This option is only useful when we start the job from a savepoint/checkpoint.</td>
    </tr>
    <tr>
      <td>scan.binlog.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>In binlog reading stage, whether to scan the ddl and dml statements of newly added tables or not, by default is false. <br>
          The difference between scan.newly-added-table.enabled and scan.binlog.newly-added-table.enabled options is: <br>
          scan.newly-added-table.enabled: do re-snapshot & binlog-reading for newly added table when restored; <br>
          scan.binlog.newly-added-table.enabled: only do binlog-reading for newly added table during binlog reading phase.
      </td>
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
      <td>include-comments.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether enable include table and column comments, by default is false, if set to true, the table and column comments will be sent.<br>
          Note: Enable this option will bring the implications on memory usage.</td>
    </tr>
    <tr>
      <td>treat-tinyint1-as-boolean.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether treat TINYINT(1) as boolean, by default is true.</td>
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

## Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for MySQL CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
- `earliest-offset`: Skip snapshot phase and start reading binlog events from the earliest accessible binlog offset.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the binlog which means only have the changes since the connector was started.
- `specific-offset`: Skip snapshot phase and start reading binlog events from a specific offset. The offset could be
  specified with binlog filename and position, or a GTID set if GTID is enabled on server.
- `timestamp`: Skip snapshot phase and start reading binlog events from a specific timestamp.
- `snapshot`: Only the snapshot phase is performed and exits after the snapshot phase reading is completed.

For example in YAML definition:

```yaml
source:
  type: mysql
  scan.startup.mode: earliest-offset                    # Start from earliest offset
  scan.startup.mode: latest-offset                      # Start from latest offset
  scan.startup.mode: specific-offset                    # Start from specific offset
  scan.startup.mode: timestamp                          # Start from timestamp
  scan.startup.mode: snapshot                          # Read snapshot only
  scan.startup.specific-offset.file: 'mysql-bin.000003' # Binlog filename under specific offset startup mode
  scan.startup.specific-offset.pos: 4                   # Binlog position under specific offset mode
  scan.startup.specific-offset.gtid-set: 24DA167-...    # GTID set under specific offset startup mode
  scan.startup.timestamp-millis: 1667232000000          # Timestamp under timestamp startup mode
  # ...
```

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
2. For MySQL, the `namespace` will be set to the default value "", and the group name will be like `test_database.test_table`.

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;"><a href="https://dev.mysql.com/doc/refman/8.0/en/data-types.html">MySQL type</a></th>
        <th class="text-left" style="width:10%;">Flink CDC type</th>
        <th class="text-left" style="width:60%;">Note</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT(n)</td>
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
        YEAR<br>
        MEDIUMINT<br>
        MEDIUMINT UNSIGNED<br>
        MEDIUMINT UNSIGNED ZEROFILL<br>
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
        INT UNSIGNED ZEROFILL
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
        DOUBLE PRECISION UNSIGNED ZEROFILL<br>
        FLOAT(p, s)<br>
        REAL(p, s)<br>
        DOUBLE(p, s)
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
      <td>TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP_LTZ [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME [(p)]
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
        LONG<br>
        LONG VARCHAR<br>
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
      <td>-</td>
      <td>Not supported yet.</td>
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

{{< top >}}
