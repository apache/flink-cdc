---
title: "DB2"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/db2
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

DB2 CDC Pipeline connector allows reading snapshot data and incremental change data from DB2 databases.
The connector is based on the DB2 CDC source connector and emits Flink CDC pipeline events for end-to-end synchronization.

## Dependencies

The IBM DB2 JDBC driver uses the IBM IPLA license, which is incompatible with the Flink CDC project.
Flink CDC does not package this driver in the DB2 pipeline connector jar.
You need to configure the following dependency manually, and pass it with the `--jar` argument of Flink CDC CLI when submitting YAML pipeline jobs.

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
        <td><a href="https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4">com.ibm.db2.jcc:db2jcc:db2jcc4</a></td>
        <td>Used for connecting to DB2 databases.</td>
      </tr>
    </tbody>
</table>
</div>

## Prerequisites

Before using the DB2 pipeline connector, make sure that:

1. DB2 is configured for CDC capture, and the target tables are enabled for DB2 change capture.
2. The DB2 server is reachable from the Flink cluster over TCP.
3. The configured user has permission to read the captured tables and DB2 CDC metadata tables.
4. All tables in one pipeline source belong to the same DB2 database. The `tables` option uses the `database.schema.table` format.

### Enable CDC in DB2

The DB2 pipeline connector does not create DB2 SQL Replication artifacts automatically. A DB2 instance owner or database administrator must complete the DB2-side CDC setup before submitting the pipeline. For the full upstream procedure, see [Setting up Db2 in the Debezium DB2 connector documentation](https://debezium.io/documentation/reference/stable/connectors/db2.html#setting-up-db2).

1. Enable DB2 SQL Replication and archive logging or log retention for the source database. In containerized DB2 environments this is commonly configured before database creation. For example, the connector test image starts DB2 with `ARCHIVE_LOGS=true`. After changing logging mode, take a full database backup and restart or activate the database so the ASN capture service has a valid log start point.
2. Install the Debezium DB2 management UDFs, or create equivalent DB2 SQL Replication control objects. The UDF setup compiles `asncdc`, installs it into the DB2 function directory, binds catalog access with `db2 bind db2schema.bnd blocking all grant public sqlerror continue`, and creates the `ASNCDC` schema, metadata tables, and helper routines such as `ASNCDC.ADDTABLE`, `ASNCDC.REMOVETABLE`, and `ASNCDC.ASNCDCSERVICES`.
3. Start the ASN capture service and verify that it is running:

   ```sql
   VALUES ASNCDC.ASNCDCSERVICES('start','asncdc');
   VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');
   ```

4. Put every source table into capture mode, then reinitialize the ASN service. Replace the schema and table names with your source table identifiers:

   ```sql
   CALL ASNCDC.ADDTABLE('DB2INST1', 'ORDERS');
   VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
   ```

5. Verify that the table is registered and has an associated change table:

   ```sql
   SELECT SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE, STATE
   FROM ASNCDC.IBMSNAP_REGISTER
   WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = 'ORDERS';
   ```

6. Grant the Flink CDC runtime user read access to the source table, the ASNCDC metadata table, and each generated change table:

   ```sql
   GRANT SELECT ON TABLE DB2INST1.ORDERS TO USER FLINK_USER;
   GRANT SELECT ON TABLE ASNCDC.IBMSNAP_REGISTER TO USER FLINK_USER;
   GRANT SELECT ON TABLE ASNCDC.CDC_DB2INST1_ORDERS TO USER FLINK_USER;
   ```

When DB2 identifiers are created without quotes, DB2 stores schema and table names in uppercase. Use the stored uppercase names in both `ASNCDC.ADDTABLE` and the pipeline `tables` option, for example `TESTDB.DB2INST1.ORDERS`.

DB2 SQL Replication does not support capturing tables that contain `BOOLEAN` columns. Such tables can be read during snapshot, but incremental CDC records are not available until the schema uses a supported replacement type. Review IBM and Debezium licensing requirements for SQL Replication before enabling this setup in production.

## Example

An example pipeline for reading data from DB2 and writing to Fluss can be defined as follows:

```yaml
source:
  type: db2
  name: DB2 Source
  hostname: 127.0.0.1
  port: 50000
  username: db2inst1
  password: pass
  tables: TESTDB.DB2INST1.ORDERS
  schema-change.enabled: true
  metadata.list: op_ts,table_name,database_name,schema_name

sink:
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123

pipeline:
  name: DB2 to Fluss Pipeline
  parallelism: 4
```

## Connector Options

| Option | Required | Default | Type | Description |
|--------|----------|---------|------|-------------|
| hostname | required | (none) | String | IP address or hostname of the DB2 database server. |
| port | optional | 50000 | Integer | Integer port number of the DB2 database server. |
| username | required | (none) | String | Name of the DB2 user to use when connecting to the DB2 database server. |
| password | required | (none) | String | Password to use when connecting to the DB2 database server. |
| tables | required | (none) | String | DB2 tables to monitor. Regular expressions are supported. The expected format is `database.schema.table`. Dot characters are treated as delimiters; escape dots used as regular expression wildcards with a backslash. |
| tables.exclude | optional | (none) | String | DB2 tables to exclude after applying the `tables` selector. The format is the same as `tables`. |
| schema-change.enabled | optional | true | Boolean | Whether to send schema change events so that downstream sinks can respond to table structure changes. |
| server-time-zone | optional | UTC | String | The session time zone of the database server. |
| scan.startup.mode | optional | initial | String | Startup mode for DB2 CDC consumer. Valid values are `initial` and `latest-offset`. |
| scan.incremental.snapshot.chunk.size | optional | 8096 | Integer | The chunk size, in rows, used when splitting captured tables during snapshot reading. |
| scan.snapshot.fetch.size | optional | 1024 | Integer | The maximum fetch size per poll when reading a table snapshot. |
| scan.incremental.snapshot.chunk.key-column | optional | (none) | String | The chunk key column used for table snapshot splitting. By default, the first primary-key column is used. |
| scan.incremental.snapshot.backfill.skip | optional | true | Boolean | Whether to skip backfill in the snapshot reading phase. Skipping backfill can replay changes that happened during the snapshot phase, so downstream sinks must handle at-least-once events correctly. |
| scan.incremental.close-idle-reader.enabled | optional | false | Boolean | Whether to close idle readers at the end of the snapshot phase. This requires checkpoint support after tasks finish. |
| connect.timeout | optional | 30s | Duration | The maximum time to wait while establishing a DB2 connection. |
| connect.max-retries | optional | 3 | Integer | The maximum number of retries for building a DB2 database connection. |
| connection.pool.size | optional | 20 | Integer | The connection pool size. |
| metadata.list | optional | (none) | String | List of readable metadata fields from SourceRecord to pass downstream and use in transform expressions, separated by commas. Available metadata fields are: `op_ts`, `table_name`, `database_name`, and `schema_name`. |
| jdbc.properties.* | optional | (none) | String | Custom JDBC URL properties passed to the DB2 JDBC connection. |
| debezium.* | optional | (none) | String | Pass-through Debezium properties for the embedded DB2 connector. |

## Startup Reading Position

The `scan.startup.mode` option specifies where DB2 CDC starts reading:

- `initial` (default): Performs an initial snapshot on the monitored tables, then continues to read the DB2 change stream.
- `latest-offset`: Skips the snapshot and starts reading from the end of the DB2 change stream.

## Supported Metadata Columns

DB2 CDC pipeline connector supports reading metadata columns from source records. These metadata columns can be used in transform operations or passed to downstream sinks.

Some metadata information is also available through Transform expressions, such as `__namespace_name__`, `__schema_name__`, and `__table_name__`. The `op_ts` metadata field is only available through `metadata.list` and represents the operation timestamp from the database.

```yaml
source:
  type: db2
  # ... other configurations
  metadata.list: op_ts,table_name,database_name,schema_name
```

| Metadata Column | Data Type | Description |
|-----------------|-----------|-------------|
| op_ts | BIGINT NOT NULL | The timestamp, in milliseconds since epoch, when the change event occurred in the database. For snapshot records, this value is 0. |
| table_name | STRING NOT NULL | The name of the table that contains the changed row. |
| database_name | STRING NOT NULL | The name of the database that contains the changed row. |
| schema_name | STRING NOT NULL | The name of the schema that contains the changed row. |

## Available Source Metrics

Metrics can help understand the progress of assignments. The following Flink metrics are supported:

| Group | Name | Type | Description |
|-------|------|------|-------------|
| namespace.schema.table | isSnapshotting | Gauge | Whether the table is snapshotting |
| namespace.schema.table | isStreamReading | Gauge | Whether the table is stream reading |
| namespace.schema.table | numTablesSnapshotted | Gauge | The number of tables that have been snapshotted |
| namespace.schema.table | numTablesRemaining | Gauge | The number of tables that have not been snapshotted |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | The number of splits that are being processed |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | The number of splits that have not been processed |
| namespace.schema.table | numSnapshotSplitsFinished | Gauge | The number of splits that have been processed |
| namespace.schema.table | snapshotStartTime | Gauge | The time when the snapshot started |
| namespace.schema.table | snapshotEndTime | Gauge | The time when the snapshot ended |

The group name is `namespace.schema.table`, where `namespace` is the DB2 database name, `schema` is the DB2 schema name, and `table` is the DB2 table name.

{{< top >}}
