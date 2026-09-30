---
title: "SQL Server"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/sqlserver
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

# SQL Server Connector

SQL Server connector allows reading snapshot data and incremental data from SQL Server databases and provides end-to-end table synchronization capabilities.
This document describes how to set up the SQL Server connector.

## Prerequisites

- SQL Server Agent is running.
- Change Data Capture is enabled for the database and captured tables.
- The configured SQL Server user can connect to the server and read the captured tables.
- In `tables`, the database must be a single fixed database name; schema and table support regular expressions to match multiple objects.

Enable CDC on a database and table:

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_enable_db;
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'MyTable',
@role_name     = NULL,
@supports_net_changes = 0;
GO
```

Verify that the table is enabled for CDC:

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture;
GO
```

## Example

An example of the pipeline for reading data from SQL Server and sink to Doris can be defined as follows:

```yaml
source:
   type: sqlserver
   name: SQL Server Source
   hostname: 127.0.0.1
   port: 1433
   username: root
   password: 123456
   # The database must be a single fixed name; schema and table support regex to match multiple tables.
   tables: inventory.dbo.\.*
   schema-change.enabled: true

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: 123456

pipeline:
   name: SQL Server to Doris Pipeline
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
      <td>IP address or hostname of the SQL Server database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>Integer port number of the SQL Server database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the SQL Server user to use when connecting to the SQL Server database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the SQL Server database server.</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the SQL Server tables to monitor. The database must be a single fixed database name, while schema and table support regular expressions to match multiple objects. Database-level regular expressions and cross-database patterns are not supported. The dot (.) is treated as a delimiter for database, schema, and table names. If a dot (.) is needed in a regular expression to match any character, escape it with a backslash.<br>
          Examples: inventory.dbo.\.*, inventory.dbo.user_table_[0-9]+, inventory.dbo.(app|web)_order_\.*</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the SQL Server tables to exclude after applying `tables`. Schema and table support regular expressions to match multiple objects. All exclude patterns must use the same fixed database name as the `tables` option.<br>
          Examples: inventory.dbo.audit_\.*, inventory.dbo.tmp_[0-9]+</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to send schema change events so downstream sinks can synchronize table structure changes.</td>
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
      <td>Optional startup mode for SQL Server CDC consumer. Valid values are "initial", "latest-offset", "snapshot", and "timestamp".</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Optional timestamp used when `scan.startup.mode` is "timestamp".</td>
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
      <td>The maximum time that the connector should wait after trying to connect to the SQL Server database server before timing out.</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum retry times for building SQL Server database server connections.</td>
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
      <td>List of readable metadata from SourceRecord to be passed to downstream, split by commas. Available metadata keys are: database_name, schema_name, table_name, op_ts.</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. This feature depends on FLIP-147.</td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to scan newly added tables. This option is only useful when starting the job from a savepoint or checkpoint.</td>
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
      <td>Pass custom JDBC URL properties. For example: <code>jdbc.properties.encrypt=false</code>.</td>
    </tr>
    </tbody>
</table>
</div>

> Compatibility note: To keep the behavior of older Pipeline versions, explicitly configure `scan.incremental.snapshot.backfill.skip: true` and `scan.incremental.snapshot.unbounded-chunk-first.enabled: false`.

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

The config option `scan.startup.mode` specifies the startup mode for SQL Server CDC consumer. The valid values are:

- `initial`: Performs an initial snapshot on the monitored tables and continues to read the latest changes.
- `latest-offset`: Starts from the latest change log offset.
- `snapshot`: Reads the snapshot only.
- `timestamp`: Starts from the specified timestamp in `scan.startup.timestamp-millis`.

## Data Type Mapping

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">SQL Server type<a href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql"></a></th>
        <th class="text-left" style="width:20%;">CDC type</th>
        <th class="text-left" style="width:50%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>BIT</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>SMALLINT</td>
      <td>SQL Server <code>TINYINT</code> is unsigned (0-255), so it is widened to <code>SMALLINT</code>.</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
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
      <td>FLOAT</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DECIMAL(p, s)<br>
        NUMERIC(p, s)
      </td>
      <td>DECIMAL(p, s)</td>
      <td>When precision is greater than 38, it falls back to <code>DECIMAL(38, 0)</code>.</td>
    </tr>
    <tr>
      <td>MONEY</td>
      <td>DECIMAL(19, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLMONEY</td>
      <td>DECIMAL(10, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        NCHAR(n)
      </td>
      <td>CHAR(n)</td>
      <td>Mapped to <code>STRING</code> when the length is not available.</td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)<br>
        NVARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td>Mapped to <code>STRING</code> when the length is not available.</td>
    </tr>
    <tr>
      <td>
        TEXT<br>
        NTEXT
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY<br>
        VARBINARY<br>
        IMAGE
      </td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TIMESTAMP<br>
        ROWVERSION
      </td>
      <td>BYTES</td>
      <td>SQL Server <code>TIMESTAMP</code>/<code>ROWVERSION</code> is a row-version binary value, not a datetime.</td>
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
      <td>SMALLDATETIME</td>
      <td>TIMESTAMP(0)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME</td>
      <td>TIMESTAMP(3)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME2(p)</td>
      <td>TIMESTAMP(p)</td>
      <td>Precision defaults to 7 when not specified.</td>
    </tr>
    <tr>
      <td>DATETIMEOFFSET(p)</td>
      <td>TIMESTAMP_LTZ(p)</td>
      <td>Precision defaults to 7 when not specified.</td>
    </tr>
    <tr>
      <td>
        UNIQUEIDENTIFIER<br>
        XML<br>
        SQL_VARIANT<br>
        HIERARCHYID<br>
        GEOMETRY<br>
        GEOGRAPHY
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

## Limitations

### Single Database

All entries in `tables` must belong to the same database. The database must be a single fixed name, while schema and table support regular expressions to match multiple objects.

{{< top >}}
