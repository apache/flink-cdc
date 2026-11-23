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

This guide converts Apache Paimon MySQL CDC Action configurations to Apache Flink CDC YAML pipelines:

| Existing Action | Flink CDC pipeline |
|-----------------|--------------------|
| `mysql_sync_table` | Read one or more MySQL tables and route them to a fixed Paimon table. |
| `mysql_sync_database` | Select MySQL tables and route each table to a Paimon database, optionally adding a prefix or suffix. |

The examples retain source primary keys and use a filesystem catalog.
Check your Action JAR's supported options against the [Paimon MySQL CDC documentation](https://paimon.apache.org/docs/master/cdc-ingestion/mysql-cdc/).

:::warning

Paimon Action checkpoints and savepoints cannot restore a Flink CDC pipeline.
The examples take an initial snapshot into a separate `lake_migration` database.
To reuse existing tables, follow [Continuing to write existing tables](#continuing-to-write-existing-tables).

:::

## Prepare the environment

Choose matching distributions and dependencies below.
For SNAPSHOT versions, build the distribution and Pipeline connectors from the same revision;
see [Set up Flink CDC](../deployment/standalone.md#set-up-flink-cdc).

| Flink distribution | Required dependencies | Optional SQL verification connector |
|--------------------|-----------------------|-------------------------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [Paimon SQL 2.1.0 for Flink 1.20](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/2.1.0/paimon-flink-1.20-2.1.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [Paimon SQL 2.1.0 for Flink 2.1](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.1.0/paimon-flink-2.1-2.1.0.jar) |

The optional SQL connectors match the bundled Paimon version.
For Flink 2.2, use Paimon's Flink 2.1 artifact.

Download and extract the selected distributions, then set `FLINK_HOME` and `FLINK_CDC_HOME`:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the MySQL and Paimon Pipeline connector JARs in `$FLINK_CDC_HOME/lib` and the MySQL JDBC driver
in `$FLINK_HOME/lib` before starting Flink. Save the Hadoop JAR as
`$FLINK_HOME/opt/flink-shaded-hadoop.jar`; the submission commands below add it with `--jar`.
For SQL verification, save the optional Paimon SQL connector as `$FLINK_HOME/opt/paimon-sql-connector.jar`.
See [Real-time MySQL Ingestion into Paimon](mysql-to-paimon.md#verify-the-initial-snapshot) for verification queries.

Enable MySQL binlog with `binlog_format=ROW` and `binlog_row_image=FULL`, and grant the synchronization
account the permissions described in the [MySQL connector documentation](../connectors/pipeline-connectors/mysql.md).
Preserve the old job's table set: the database Action only synchronizes primary-key tables,
but Pipeline table patterns do not exclude tables without primary keys.

Record the original catalog type, warehouse, table names, primary keys, partitions, and table properties.
The warehouse must be accessible to the client and Flink processes; use shared storage on distributed clusters.

Enable periodic checkpointing in the Flink cluster configuration so Paimon can commit streaming writes.
For example, merge the following into the existing `execution` section of `config.yaml`:

```yaml
execution:
  checkpointing:
    interval: 30s
```

Replace the host, account, timezone, warehouse, and table patterns in the YAML below.
`'<mysql-password>'` is a literal placeholder, not a secret reference; replace it securely and do not commit credentials.
Reserve a non-overlapping MySQL `server-id` range for the new job, with at least one ID per source subtask.

## Migrate `mysql_sync_table`

Suppose the original job copies `shop.orders` to `lake.orders`. The source table has a primary key,
and the original job does not add computed columns or partitions.

### Existing Action configuration

The command below is for reference only: `<action-jar>` is the original job's JAR,
and the shell variables are its connection settings.

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

Save the following as `mysql-orders-to-paimon.yaml`:

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

Submit with the Flink CDC CLI and the required filesystem dependencies:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-orders-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

The configuration changes are:

- Combine Action `database-name` and `table-name` into `source.tables`.
- Set the target database and table in `route.sink-table`.
- Configure the catalog directly with `catalog.properties.*` and the table with `table.properties.*`.
- Set the commit-user prefix with `commit.user-prefix`.

Flink CDC creates new target databases and tables using the source schema and primary key.
`initial` reads a snapshot, then binlog changes.

To merge multiple source tables, expand both `tables` and `source-table` and keep a fixed `sink-table`.
Schemas must be compatible and output primary keys globally unique to avoid overwriting rows from other tables.

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
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-shop-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
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

`source.tables` and `source.tables.exclude` match source identifiers including the database name.
For example, to select all `shop` tables except those starting with `tmp_`, use:

```yaml
tables: 'shop.\.*'
tables.exclude: 'shop.tmp_\.*'
```

Place these fields inside `source` and set the route's `source-table` to `'shop.\.*'`.
Every selected table must have the required primary key.
An unescaped dot separates database and table; escape regex wildcard dots as `\.`.

Routing is not a filter: an unmatched source table keeps its original identifier.
All matching routes apply by default; avoid overlapping rules that write a table twice.
See [Route](../core-concept/route.md) for routing modes and naming rules.

Shard merging requires compatible schemas and globally unique primary keys.
To keep same-named tables from different databases separate, adjust `source.tables` and use:

```yaml
route:
  - source-table: 'shop_01.orders'
    sink-table: lake_migration.shop_01_orders
  - source-table: 'shop_02.orders'
    sink-table: lake_migration.shop_02_orders
```

This corresponds to `--merge_shards false`.
For multiple tables, use a rule per database with a target such as
`lake_migration.shop_01_<>` and `replace-symbol: <>`.

## Translate additional configuration

The Pipeline connector and the Action's SQL connector accept different options.
Translate `--mysql_conf` properties individually using the mappings below.

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

Rewrite `--computed_column` expressions as [Transform](../core-concept/transform.md) projections.
For a timestamp `create_time` whose partition date never changes on update, add this top-level section:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: "*, DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt"
    primary-keys: id,dt
    partition-keys: dt
```

With source primary key `id`, this produces target primary key `id,dt` and partition key `dt`.
All key fields must be projected; partition fields must be part of the target primary key.
Check the business key, timestamp precision, timezone, function indexing, and null handling against the Action.
This example does not support moving a key between partitions.

Flink CDC retains source primary keys unless overridden; an empty `primary-keys` value does not remove them.
Migrating Action targets that discard source primary keys is outside this guide's scope.

For database synchronization, apply partition rules only to tables containing the required fields;
leave other tables unpartitioned. Alternatively, use the Paimon sink's `partition.key` by routed target identifier.
Choose one partition configuration method per table.
Changing YAML does not automatically migrate an existing table's keys, partitions, or bucket layout.

### Per-table properties

To translate `--table_conf_by_table orders:bucket=8`, add the following top-level section:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*'
    table-options: 'bucket=8'
```

These creation-time options override same-named `table.properties.*` options but do not reconfigure existing tables.
`source-table` is the source identifier before routing. Merge properties into an existing Transform rule rather than duplicating it.

### Newly added tables

Action modes `divided` and `combined` have no YAML equivalent. Choose the MySQL source behavior by requirement:

| Requirement | Pipeline configuration |
|-------------|------------------------|
| Capture matching tables created while the pipeline is reading binlog | Select a matching source/route pattern and set `source.scan.binlog.newly-added-table.enabled: true`. This reads binlog changes, not a historical snapshot of a newly included table. |
| Include an existing table with historical data in an already running Flink CDC pipeline | Expand its table selection, set `source.scan.newly-added-table.enabled: true`, and restore from that pipeline's own checkpoint or savepoint. |

Do not enable both options together. The snapshot-based option is incompatible with
`scan.incremental.snapshot.metadata.release.enabled: true`.
See [Table Selection and Adding Tables](table-selection-and-new-tables.md) for the restore workflow.

### Source metadata

To retain source database and table names, add metadata fields to a Transform projection:

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*, __schema_name__ AS database_name, __table_name__ AS table_name'
```

These are source names, not routed target names. Merge into existing projections without column-name conflicts.
For `op_ts`, enable `metadata.list: op_ts` inside `source` and explicitly project `op_ts` in Transform.
Unlike the Action's SQL `TIMESTAMP_LTZ(3)` metadata, the Pipeline connector exposes milliseconds as `BIGINT`.
Check conversion and timezone before writing to an existing timestamp column.

### Type mapping and schema evolution

There is no general YAML `type_mapping` switch. Compare the original target schema with the
[MySQL type mappings](../connectors/pipeline-connectors/mysql.md) and the
[Paimon type mappings](../connectors/pipeline-connectors/paimon.md).

| Original mapping | Migration consideration |
|------------------|-------------------------|
| `tinyint1-not-bool` | Set `source.treat-tinyint1-as-boolean.enabled: false`. |
| `to-string`, `char-to-string` | Explicitly cast the affected columns to `STRING` in a Transform projection; check the supported casts. |
| `bigint-unsigned-to-bigint` | MySQL `BIGINT UNSIGNED` maps to `DECIMAL(20,0)` by default. Before casting to `BIGINT`, ensure every value fits its signed range. |
| `longtext-to-bytes`, `to-nullable` | No direct source option; review output types and nullability. |

The default `pipeline.schema.change.behavior` is `lenient`.
It retains dropped columns and handles renames by keeping the old column and adding the new one.
Validate your application's DDL against [Schema Evolution](../core-concept/schema-evolution.md) and its failure handling.
Schema-evolution policies do not replace incompatible-table filtering.

## Validate and cut over

### Prefer a separate target

1. Start the new pipeline against the independent migration database. Keep the old target unchanged.
2. Wait for the snapshot to finish, the binlog stream to catch up, and the relevant data to be committed to Paimon.
   A completed checkpoint alone does not prove catch-up.
3. Verify target names, column types, nullability, primary keys, partitions, and table properties.
   Compare record contents and aggregates, not just row counts.
4. Check inserts, updates, deletes, and application-specific DDL. For merged tables, check overlapping source key values.
5. Coordinate a cutover boundary with downstream readers and verify the data at that boundary before switching them.
   Retain the old tables, job state, and required binlog until the rollback window has closed.

### Continuing to write existing tables

For an existing target, stop the Action before starting the pipeline and verify:

- Target schema, field order and types, primary keys, partitions, table properties, and write semantics are compatible.
- The new source starts from the position committed to Paimon by the Action, with the required binlog still retained.
  MySQL supports `scan.startup.mode: specific-offset` and its associated offset fields;
  see the [MySQL connector](../connectors/pipeline-connectors/mysql.md) for requirements and schema-history limitations.
- The source schema used for initialization is compatible with events at that position, including DDL around the handoff.

An `initial` snapshot does not remove rows deleted from MySQL before migration; use a separate target for a full rebuild.
