---
title: "DB2"
slug: "/docs/connectors/pipeline-connectors/db2"
sidebar_position: 3
hide_title: true
displayed_sidebar: docs
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

# DB2 Connector

DB2 connector allows reading snapshot data and incremental data from DB2 databases and provides end-to-end table synchronization capabilities.
This document describes how to set up the DB2 connector.

## Prerequisites

- DB2 CDC is based on DB2's SQL Replication feature. The SQL Replication capture feature must be enabled on the DB2 server, and the capture tables must be configured for the monitored tables.
- The configured DB2 user can connect to the server and read the captured tables.
- Every captured table must be registered with `CHG_UPD_TO_DEL_INS='Y'`. Direct `IBMSNAP_OPERATION='U'` records are not supported; see the update prerequisites below.
- In `tables`, every table pattern uses the format `schema.table`, and the `database` option must be a single fixed database name.

## Dependencies

The connector JAR does not bundle the IBM DB2 JDBC driver because its IPLA license is incompatible with Apache Flink CDC distribution requirements. Obtain [db2jcc4](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4) separately under the applicable IBM license.

Place the DB2 Pipeline connector JAR and the required sink connector JAR in `<FLINK_CDC_HOME>/lib/`. Put the DB2 JDBC driver in `<FLINK_HOME>/lib/` on the submission client and every JobManager and TaskManager, and restart the Flink cluster after installing the driver. For container deployments, include the driver in the Flink image used by all these processes.

## Example

An example of the pipeline for reading data from DB2 and sink to Doris can be defined as follows:

```yaml
source:
   type: db2
   name: DB2 Source
   hostname: 127.0.0.1
   port: 50000
   username: db2inst1
   password: 123456
   database: testdb
   # Every table pattern uses the format "schema.table".
   tables: DB2INST1.\.*

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: 123456

pipeline:
   name: DB2 to Doris Pipeline
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
      <td>IP address or hostname of the DB2 database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>Integer port number of the DB2 database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the DB2 user to use when connecting to the DB2 database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the DB2 database server.</td>
    </tr>
    <tr>
      <td>database</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the DB2 database server to monitor.</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the DB2 tables to monitor. Every table pattern uses the format "schema.table" and supports regular expressions to match multiple objects.<br>
          Examples: DB2INST1.\.*, DB2INST1.user_table_[0-9]+, DB2INST1.(APP|WEB)_ORDER_\.*</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the DB2 tables to exclude after applying `tables`. Every exclude pattern uses the format "schema.table" and supports regular expressions to match multiple objects.<br>
          Examples: DB2INST1.audit_\.*, DB2INST1.tmp_[0-9]+</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to forward schema change events reported by the underlying source. This option does not enable runtime DDL discovery for DB2. Initial table schemas are still emitted as CreateTableEvent events; see the Limitations section.</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in database server. If not set, ZoneId.systemDefault() is used to determine the server time zone.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.key-column</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The chunk key of table snapshot. By default, the chunk key is the first column of the primary key. This column must be a primary key column.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>The chunk size, in rows, of table snapshots.</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>The maximum fetch size per poll when reading table snapshots.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for DB2 CDC consumer. Valid values are "initial" and "latest-offset".</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to skip backfill in snapshot reading phase. Skipping backfill may lead to replayed change log events with at-least-once semantics.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to assign the unbounded chunks first during snapshot reading phase. This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk.</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>The maximum time that the connector should wait after trying to connect to the DB2 database server before timing out.</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum retry times for building DB2 database server connections.</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>The connection pool size.</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>List of readable metadata from SourceRecord to be passed downstream, split by commas. Available metadata keys are: database_name, schema_name, table_name, op_ts.</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. This feature depends on FLIP-147.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium properties to Debezium Embedded Engine.</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass IBM DB2 JDBC properties through Debezium's database configuration. For example: <code>jdbc.properties.currentSchema=DB2INST1</code>. An explicit <code>debezium.database.*</code> property takes precedence over the corresponding <code>jdbc.properties.*</code> alias.</td>
    </tr>
    </tbody>
</table>
</div>

## Available Metadata

The following metadata can be passed downstream when configured in `metadata.list`.

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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contains the row.</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the schema that contains the row.</td>
    </tr>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the table that contains the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>Time when the change was made in the database. For snapshot records, the value is always 0.</td>
    </tr>
  </tbody>
</table>

## Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for DB2 CDC consumer. The valid values are:

- `initial`: Performs an initial snapshot on the monitored tables and continues to read the latest changes.
- `latest-offset`: Starts from the latest change log offset.

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">DB2 type<a href="https://www.ibm.com/docs/en/db2-for-zos/12?topic=language-built-in-data-types"></a></th>
        <th class="text-left" style="width:20%;">CDC type</th>
        <th class="text-left" style="width:50%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>REAL</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)<br>NUMERIC(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td>When the precision is not available, it falls back to <code>DECIMAL(38, s)</code>.</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td>Mapped to <code>STRING</code> when the length is not available.</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td>Mapped to <code>STRING</code> when the length is not available.</td>
    </tr>
    <tr>
      <td>CHARACTER VARYING<br>GRAPHIC<br>VARGRAPHIC</td>
      <td>STRING</td>
      <td>Mapped to <code>STRING</code> when the length is not available.</td>
    </tr>
    <tr>
      <td>CHAR(n) FOR BIT DATA<br>VARCHAR(n) FOR BIT DATA</td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME(p)</td>
      <td>TIME(p)</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP(p)</td>
      <td>TIMESTAMP(p)</td>
      <td>Precision defaults to 6 when not specified.</td>
    </tr>
    </tbody>
</table>
</div>

## Limitations

### Single Database

The `database` option must be a single fixed database name. Cross-database data capture is not supported. In `tables`, every table pattern uses the format `schema.table`, and schema and table support regular expressions to match multiple objects.

### Unsupported Column Types

This Pipeline connector does not support `DECFLOAT`, `XML`, `CLOB`, `DBCLOB` or `BLOB` columns. Their schema and value conversion is not supported end to end by the current capture and deserialization path. In particular, Debezium omits `DECFLOAT` columns reported as JDBC `OTHER`, which would make the row fields inconsistent with the advertised schema. These types are rejected instead of publishing incomplete or incorrectly converted records.

The source therefore fails when reading the schema of a table containing such a column, with an error identifying the unsupported type:

```text
Doesn't support Db2 type 'DECFLOAT', JDBC type '1111' yet.
```

### Schema Changes Are Not Discovered At Runtime

The DB2 source does not discover newly added or recaptured change tables at runtime because its discovery query is disabled with `AND 1=0`. Runtime DDL propagation, including ALTER followed by recapture, is not supported. Setting `schema-change.enabled=true` does not enable this discovery; initial `CreateTableEvent` events are still emitted.

Do not use a restart from an existing checkpoint or savepoint as a way to refresh table schemas after DDL. Recovery can retain the old schema, even when capture registrations are read again. Schema changes require a separately planned reinitialization with a new snapshot and coordinated downstream consistency; this connector does not provide automatic migration across the change.

### Updates Require CHG_UPD_TO_DEL_INS=Y

The SQL Replication registration of every captured table must set `CHG_UPD_TO_DEL_INS='Y'`, so that an update is written as a `D` record immediately followed by an `I` record in the same unit of work. The connector reconstructs an update from that pair; it does not classify a record written directly as `IBMSNAP_OPERATION='U'`, which is what `CHG_UPD_TO_DEL_INS='N'` produces, and such records are not delivered as update events.

Check the registration before deploying the pipeline:

```sql
SELECT CHG_UPD_TO_DEL_INS FROM ASNCDC.IBMSNAP_REGISTER
WHERE SOURCE_OWNER = 'SCHEMA_NAME' AND SOURCE_TABLE = 'TABLE_NAME';
```
