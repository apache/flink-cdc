---
title: "Migrate Paimon MySQL CDC Actions to Flink CDC"
slug: "/docs/examples-and-best-practices/migrate-paimon-cdc-action"
sidebar_position: 8
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

# Migrate Paimon MySQL CDC Actions to Flink CDC

This guide translates Apache Paimon MySQL CDC Action configurations into Apache Flink CDC YAML pipelines.
It covers two migration paths:

| Existing Action | Flink CDC pipeline |
|-----------------|--------------------|
| `mysql_sync_table` | Read one or more MySQL tables and route them to a fixed Paimon table. |
| `mysql_sync_database` | Select MySQL tables and route each table to a Paimon database, optionally adding a prefix or suffix. |

The examples retain source primary keys and use a filesystem catalog.
Check the options accepted by your existing Action JAR before translating its configuration.
For background on the Actions, see the [Paimon MySQL CDC documentation](https://paimon.apache.org/docs/master/cdc-ingestion/mysql-cdc/).

:::warning

This is a configuration migration, not a state migration. Do not restore a Flink CDC pipeline
from a Paimon Action checkpoint or savepoint.
The YAML examples start with an initial snapshot and write to the separate `lake_migration` database.
Do not change this to the old target database without planning the reading position and validating existing tables.

:::

## Prepare the environment

1. Choose compatible Flink and Flink CDC distributions. Install the MySQL and Paimon Pipeline connector JARs
   in `$FLINK_CDC_HOME/lib`, and provide the MySQL JDBC driver and required Hadoop/filesystem dependencies.
   See [Real-time MySQL Ingestion into Paimon](mysql-to-paimon.md) for environment setup and verification queries.
   The Action JAR is not a replacement for these Pipeline connectors.
2. Enable MySQL binlog with `binlog_format=ROW` and `binlog_row_image=FULL`, and grant the synchronization
   account the permissions described in the [MySQL connector documentation](../connectors/pipeline-connectors/mysql.md).
   Inventory the tables actually synchronized by the old job; the database Action only synchronizes tables with primary keys.
   Keep that table set when migrating, rather than assuming a broad pattern automatically excludes tables without primary keys.
3. Record the original catalog type, warehouse, table names, primary keys, partitions, and table properties.
   The warehouse must be accessible to the submitting client and Flink processes.
   On a distributed cluster, use shared storage rather than a machine-local directory.
4. Enable periodic checkpointing in the Flink cluster configuration so Paimon can commit streaming writes.
   For example, merge the following into the existing `execution` section of `config.yaml`:

   ```yaml
   execution:
     checkpointing:
       interval: 30s
   ```

In the YAML below, replace the host, account, password placeholder, timezone, warehouse, and table patterns.
Replace `'<mysql-password>'` with the actual value through your deployment's secure configuration process;
do not commit credentials. The placeholder is not a secret lookup or environment-variable expression.
Reserve a non-overlapping MySQL `server-id` range for the new job, with at least one ID per source subtask.

## Migrate `mysql_sync_table`

Suppose the original job copies `shop.orders` to `lake.orders`. The source table has a primary key,
and the original job does not add computed columns or partitions.

### Existing Action configuration

The following command is a configuration reference only. Do not run a second Action during migration.
`<action-jar>` denotes the JAR used by the original job; the shell variables represent its connection settings.

```shell
flink run <action-jar> mysql_sync_table \
  --warehouse hdfs:///warehouse/paimon \
  --database lake \
  --table orders \
  --mysql_conf hostname="${MYSQL_HOST}" \
  --mysql_conf username="${MYSQL_USER}" \
  --mysql_conf password="${MYSQL_PASSWORD}" \
  --mysql_conf database-name=shop \
  --mysql_conf table-name=orders \
  --table_conf bucket=4
```

### Flink CDC pipeline

Save this as `mysql-orders-to-paimon.yaml`. It intentionally creates `lake_migration.orders`, not `lake.orders`.

```yaml
source:
  type: mysql
  hostname: mysql-host
  port: 3306
  username: cdc_user
  password: '<mysql-password>'
  tables: 'shop.orders'
  server-id: 6401-6404
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: hdfs:///warehouse/paimon
  commit.user-prefix: orders_migration
  table.properties.bucket: 4

route:
  - source-table: 'shop.orders'
    sink-table: lake_migration.orders

pipeline:
  name: mysql-orders-to-paimon
  parallelism: 4
```

Submit it with the Flink CDC CLI, adding any filesystem dependencies required by your deployment:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-orders-to-paimon.yaml
```

The configuration changes are:

- Combine Action `database-name` and `table-name` into `source.tables`.
- Set the target database and table in `route.sink-table`.
- Configure the catalog directly with `catalog.properties.*` and the table with `table.properties.*`.
- Use `commit.user-prefix` for the commit-user prefix. It does not transfer the original job's progress.

For a new target, Flink CDC creates the database and table using the source schema and primary key.
`initial` reads a snapshot and then continues with binlog changes.
After a successful checkpoint and Paimon commit, check an insert, update, and delete against the target table.

To merge multiple source tables, expand both `tables` and `source-table` and keep a fixed `sink-table`.
Validate schema compatibility and ensure the output primary key is unique across all source tables;
otherwise, rows from different tables can overwrite one another.

## Migrate `mysql_sync_database`

Suppose the original job selects `shop.orders` and `shop.users`, adding the target prefix `ods_`.

### Existing Action configuration

```shell
flink run <action-jar> mysql_sync_database \
  --warehouse hdfs:///warehouse/paimon \
  --database lake \
  --mysql_conf hostname="${MYSQL_HOST}" \
  --mysql_conf username="${MYSQL_USER}" \
  --mysql_conf password="${MYSQL_PASSWORD}" \
  --mysql_conf database-name=shop \
  --including_tables 'orders|users' \
  --table_prefix ods_ \
  --table_conf bucket=4
```

### Flink CDC pipeline

Save this as `mysql-shop-to-paimon.yaml`. Both selected source tables must have primary keys.

```yaml
source:
  type: mysql
  hostname: mysql-host
  port: 3306
  username: cdc_user
  password: '<mysql-password>'
  tables: 'shop.(orders|users)'
  server-id: 6501-6504
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: hdfs:///warehouse/paimon
  commit.user-prefix: shop_migration
  table.properties.bucket: 4

route:
  - source-table: 'shop.(orders|users)'
    sink-table: lake_migration.ods_<>
    replace-symbol: <>

pipeline:
  name: mysql-shop-to-paimon
  parallelism: 4
```

Submit it with:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-shop-to-paimon.yaml
```

The resulting tables are `lake_migration.ods_orders` and `lake_migration.ods_users`.
The `<>` symbol is replaced with the source table name only, not its database name.
For a suffix as well, use `sink-table: lake_migration.ods_<>_cdc`.

| Action setting | Pipeline configuration |
|----------------|------------------------|
| `--mysql_conf database-name=shop` and `--including_tables` | Combine the database and table patterns in `source.tables`. |
| `--excluding_tables` | Use `source.tables.exclude`, including the source database name. |
| `--database`, `--table_prefix`, `--table_suffix` | Define the final target name in `route.sink-table`. |

### Table selection and routing

Use `source.tables` for inclusion and `source.tables.exclude` for exclusion.
Both match source identifiers including the database name. For example, to select all `shop` tables
except those whose names start with `tmp_`, replace the inclusion pattern and add the exclusion:

```yaml
tables: 'shop.\.*'
tables.exclude: 'shop.tmp_\.*'
```

These are fields inside `source`, not a complete pipeline. If used, also expand the route's
`source-table` to `'shop.\.*'` and check that every selected table has the required primary key.
In these expressions the unescaped dot separates database and table; `\.` is needed for a regex wildcard dot.

Routing is not a filter: an unmatched source table keeps its original identifier.
By default, all matching routes apply, so use non-overlapping rules to avoid accidentally writing a table twice.
See [Route](../core-concept/route.md) for routing modes and naming rules.

For shard merging, route same-named tables from different databases to the same target only if their schemas
are compatible and their primary keys are globally unique.
To preserve the source database in the target name instead, replace the route list and adjust `source.tables`:

```yaml
route:
  - source-table: 'shop_01.orders'
    sink-table: lake_migration.shop_01_orders
  - source-table: 'shop_02.orders'
    sink-table: lake_migration.shop_02_orders
```

This separates the targets as required by an Action using `--merge_shards false`.
For multiple tables per database, use a separate rule per database with a target such as
`lake_migration.shop_01_<>` and `replace-symbol: <>`.

## Translate additional configuration

The basic pipelines omit optional Action features. If the old job uses them, handle them before validating the migration.
Do not copy every `--mysql_conf` property verbatim: the Pipeline connector and the SQL connector used by the Action
have different configuration surfaces.

### Catalog and table properties

| Action setting | Flink CDC configuration |
|----------------|-------------------------|
| `--warehouse` | `sink.catalog.properties.warehouse` |
| `--catalog_conf key=value` | `sink.catalog.properties.key: value`, provided the catalog supports the property |
| Table properties in `--table_conf key=value` | `sink.table.properties.key: value`, provided the bundled Paimon version supports the property |
| `--primary_keys` | `transform.primary-keys`, using fields present in the projected schema |
| `--partition_keys` | `transform.partition-keys`, configured separately for tables with different partition fields |
| `--metadata_column` | Rewrite metadata using the source metadata configuration and Transform projections described below |
| `--table_conf sink.parallelism=4` | Review pipeline-level parallelism and Flink resources; `pipeline.parallelism: 4` changes the pipeline, not just the sink |
| `-Dpipeline.name` | `pipeline.name` |
| `--ignore_incompatible true` | No equivalent automatic table-skipping option; fix incompatibilities or exclude affected source tables |

Do not put Action runtime settings such as `sink.parallelism` under `table.properties.*`.
For a Hive catalog, use `catalog.properties.metastore: hive` and the appropriate `catalog.properties.uri`,
and supply the required Hive/Hadoop configuration and dependencies.
Changing the warehouse or metastore URI does not move existing table metadata or data files.
See the [Paimon connector](../connectors/pipeline-connectors/paimon.md) for catalog and sink options.

### Computed columns, primary keys, and partitions

An Action's `--computed_column` expressions must be rewritten as
[Transform](../core-concept/transform.md) projections, not copied as SQL function calls without checking their semantics.
For example, if `create_time` is a timestamp and the partition date never changes on update,
add this top-level section to the single-table pipeline:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: "*, DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt"
    primary-keys: id,dt
    partition-keys: dt
```

This example assumes the source primary key is `id`, and corresponds to a target with primary key `id,dt`
and partition key `dt`. All key fields must exist in the projected schema.
Check timestamp precision, timezone, function argument indexing, and null handling against the original expression.
For partitioned primary-key tables, keep the partition fields in the target primary key and validate the actual business key.
Do not apply this example to a job that depends on moving a key between partitions.

Without a primary-key override, Flink CDC retains the source primary key.
This guide does not cover converting an Action target that intentionally discards source primary keys:
an empty `primary-keys` value does not remove inherited primary keys.
Do not silently replace the old job's write semantics with primary-key upserts.

For database synchronization, apply partition rules only to source tables that contain the required fields.
The Action's behavior of leaving other tables unpartitioned must not be replaced with one invalid shared projection.
Alternatively, the Paimon sink's `partition.key` can configure partitions by routed target identifier;
choose one partition configuration method per table.
Changing YAML does not automatically migrate an existing table's keys, partitions, or bucket layout.

### Per-table properties

To translate `--table_conf_by_table orders:bucket=8`, add the following top-level section:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*'
    table-options: 'bucket=8'
```

These options apply at table creation and override same-named common `table.properties.*` options.
`source-table` refers to the source identifier before routing.
If a Transform already exists for this table, merge the property into that rule instead of adding a duplicate rule.
Existing tables are not reconfigured by changing these creation options.

### Newly added tables

An Action's `divided` and `combined` modes describe how it organizes its operators; there is no corresponding YAML mode.
Choose the MySQL source behavior required by the workload:

| Requirement | Pipeline configuration |
|-------------|------------------------|
| Capture matching tables created while the pipeline is reading binlog | Select a matching source/route pattern and set `source.scan.binlog.newly-added-table.enabled: true`. This reads binlog changes, not a historical snapshot of a newly included table. |
| Include an existing table with historical data in an already running Flink CDC pipeline | Expand its table selection, set `source.scan.newly-added-table.enabled: true`, and restore from that pipeline's own checkpoint or savepoint. |

Do not enable both options together. The snapshot-based option is incompatible with
`scan.incremental.snapshot.metadata.release.enabled: true`.
See [Table Selection and Adding Tables](table-selection-and-new-tables.md) for the restore workflow.
That workflow does not make an Action savepoint compatible with a Flink CDC pipeline.

### Source metadata

To retain source database and table names, add metadata fields to a Transform projection:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*, __schema_name__ AS database_name, __table_name__ AS table_name'
```

These are source names, not routed target names. Merge them into existing projections and avoid column-name conflicts.
For `op_ts`, enable `metadata.list: op_ts` inside `source` and explicitly project `op_ts` in Transform.
The Pipeline connector exposes milliseconds as `BIGINT`, whereas SQL metadata used by an Action exposes
`TIMESTAMP_LTZ(3)`. Validate the conversion and timezone before writing to an existing timestamp column.

### Type mapping and schema evolution

There is no general YAML `type_mapping` switch. Compare the original target schema with the
[MySQL type mappings](../connectors/pipeline-connectors/mysql.md) and the
[Paimon type mappings](../connectors/pipeline-connectors/paimon.md).

| Original mapping | Migration consideration |
|------------------|-------------------------|
| `tinyint1-not-bool` | Set `source.treat-tinyint1-as-boolean.enabled: false`. |
| `to-string`, `char-to-string` | Explicitly cast the affected columns to `STRING` in a Transform projection; check the supported casts. |
| `bigint-unsigned-to-bigint` | MySQL `BIGINT UNSIGNED` maps to `DECIMAL(20,0)` by default. Before casting to `BIGINT`, ensure every value fits its signed range. |
| `longtext-to-bytes`, `to-nullable` | No direct equivalent source option; assess output types and nullability rather than silently dropping the old mapping. |

The default `pipeline.schema.change.behavior` is `lenient`.
It retains dropped columns and handles renames by keeping the old column and adding the new one.
This does not guarantee that every DDL or type change accepted by an Action is supported.
Review [Schema Evolution](../core-concept/schema-evolution.md), including failure handling, and validate the DDL
used by your application. Changing a schema-evolution policy is not a substitute for an incompatible-table filter.

## Validate and cut over

### Prefer a separate target

1. Start the new pipeline against the independent migration database. Keep the old target unchanged.
2. Wait for the snapshot to finish, the binlog stream to catch up, and the relevant data to be committed to Paimon.
   A running job or one completed checkpoint alone does not demonstrate catch-up.
3. Verify target names, column types, nullability, primary keys, partitions, and table properties.
   Compare record contents and aggregates, not just row counts.
4. Check inserts, updates, deletes, and application-specific DDL. For merged tables, check overlapping source key values.
5. Coordinate a cutover boundary with downstream readers and verify the data at that boundary before switching them.
   Retain the old tables, job state, and required binlog until the rollback window has closed.

### Continuing to write existing tables

Treat in-place continuation as a separate migration plan, not as replacing `lake_migration` with `lake` in an example.
Before stopping the Action, arrange how to determine the source position through which writes were successfully
committed to Paimon. Stop the old writer before starting a new writer against the same tables.
Validate all of the following:

- Target schema, field order and types, primary keys, partitions, table properties, and write semantics are compatible.
- The new source starts from a verified position covering the handoff, with the required binlog still retained.
  MySQL supports `scan.startup.mode: specific-offset` and its associated offset fields;
  see the [MySQL connector](../connectors/pipeline-connectors/mysql.md) for requirements and schema-history limitations.
- The source schema used for initialization is compatible with events at that position, including DDL around the handoff.

Do not use `latest-offset` as a shortcut: it can skip changes between stopping the Action and starting the pipeline.
A fresh `initial` snapshot into an existing table is not a rebuild either:
rows deleted from MySQL before the snapshot can remain in the old Paimon table.
If the committed position and schema cannot be established reliably, use a separate target and reconciliation
rather than claiming a lossless in-place migration.
