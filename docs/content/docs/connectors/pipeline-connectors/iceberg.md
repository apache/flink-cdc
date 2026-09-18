---
title: "Iceberg"
weight: 9
type: docs
aliases:
- /connectors/pipeline-connectors/iceberg
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

# Iceberg Pipeline Connector

The Iceberg Pipeline Connector functions as a *Data Sink* for data pipelines, enabling data writes to [Apache Iceberg](https://iceberg.apache.org) tables. This document explains how to configure the connector.

## Key Capabilities
* **Automatic Table Creation:**
creates Iceberg tables dynamically when they do not exist
* **Schema Synchronization:**
propagates schema changes (e.g., column additions) from source systems to Iceberg
* **Data Replication:**
supports both batch and streaming data synchronization

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Iceberg can be defined as follows:

### Hadoop Catalog Example

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
  type: iceberg
  name: Iceberg Sink
  catalog.properties.type: hadoop
  catalog.properties.warehouse: /path/warehouse

pipeline:
  name: MySQL to Iceberg Pipeline
  parallelism: 2
```

### AWS Glue Catalog Example

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
  type: iceberg
  name: Iceberg Sink
  catalog.properties.type: glue
  catalog.properties.warehouse: s3://my-bucket/warehouse
  catalog.properties.io-impl: org.apache.iceberg.aws.s3.S3FileIO
  catalog.properties.client.region: us-east-1
  catalog.properties.glue.skip-archive: true

pipeline:
  name: MySQL to Iceberg via Glue Pipeline
  parallelism: 2
```

***Note:***
Depending on the catalog type, you may need to add extra JARs manually and pass them with the `--jar` argument of Flink CDC CLI when submitting YAML pipeline jobs.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Catalog Type</th>
        <th class="text-left">Dependency Item</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>all</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-flink-runtime-1.20">org.apache.iceberg:iceberg-flink-runtime-1.20</a></td>
        <td>Iceberg Flink runtime. Required for all catalog types when not pre-installed in the runtime environment (e.g., standalone Flink clusters).</td>
      </tr>
      <tr>
        <td>hadoop</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber/2.8.3-10.0">org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0</a></td>
        <td>Provides Hadoop filesystem dependencies.</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws">org.apache.iceberg:iceberg-aws</a></td>
        <td>Provides AWS Glue Catalog and S3 FileIO implementation.</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/software.amazon.awssdk/bundle">software.amazon.awssdk:bundle</a></td>
        <td>AWS SDK bundle required by iceberg-aws.</td>
      </tr>
    </tbody>
</table>
</div>

Pipeline Connector Options
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
      <td>type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>iceberg</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>catalog.properties.type</td>
      <td>conditionally required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Metastore type of Iceberg catalog, supports <code>hadoop</code>, <code>hive</code>, and <code>glue</code>. Either this option or <code>catalog.properties.catalog-impl</code> must be set.</td>
    </tr>
    <tr>
      <td>catalog.properties.catalog-impl</td>
      <td>conditionally required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom catalog implementation class, e.g. <code>org.apache.iceberg.aws.glue.GlueCatalog</code>. Either this option or <code>catalog.properties.type</code> must be set.</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The warehouse root path of the Iceberg catalog, used by all catalog types. For <code>hadoop</code> and <code>hive</code> catalogs, this is typically a local or distributed filesystem path. For <code>glue</code> catalog, this is typically an object storage path like <code>s3://my-bucket/warehouse</code>.</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>URI of the metastore server (e.g. Hive Metastore thrift URI).</td>
    </tr>
    <tr>
      <td>catalog.properties.io-impl</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom FileIO implementation class. For AWS S3, use <code>org.apache.iceberg.aws.s3.S3FileIO</code>.</td>
    </tr>
    <tr>
      <td>catalog.properties.client.region</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region for the Glue catalog client (e.g. <code>us-east-1</code>).</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Glue catalog ID (AWS account ID). By default, the caller's AWS account ID is used.</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-archive</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to skip archiving older table versions in Glue.</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-name-validation</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to skip name validation for Glue catalog.</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Partition keys for each partitioned table. Allow setting multiple primary keys for multiTables. Tables are separated by ';', and partition keys are separated by ','. For example, we can set <code>partition.key</code> of two tables using 'testdb.table1:id1,id2;testdb.table2:name'. For partition transforms, we can set <code>partition.key</code> using 'testdb.table1:truncate[10](id);testdb.table2:hour(create_time);testdb.table3:day(create_time);testdb.table4:month(create_time);testdb.table5:year(create_time);testdb.table6:bucket[10](create_time)'.</td>
    </tr>
    <tr>
      <td>catalog.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Iceberg catalog options to the pipeline，See <a href="https://iceberg.apache.org/docs/nightly/flink-configuration/#catalog-configuration">Iceberg catalog options</a>. </td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Iceberg table options to the pipeline，See <a href="https://iceberg.apache.org/docs/nightly/configuration/#write-properties">Iceberg table options</a>. </td>
    </tr>
    <tr>
      <td>hadoop.conf.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass Hadoop <code>Configuration</code> options used by Iceberg catalog/table operations. The prefix <code>hadoop.conf.</code> will be stripped. For example, <code>hadoop.conf.fs.s3a.endpoint</code>.</td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The source table must have a primary key. Tables with no primary key are not supported.

* Exactly-once semantics are not supported. The connector uses at-least-once + the table's primary key for idempotent writing.

Table Maintenance
-----------------

The Iceberg sink directly integrates Iceberg 1.10.1's `TableMaintenance` API into the streaming CDC job. It supports `RewriteDataFiles` (binpack data file compaction), `ExpireSnapshots`, and `DeleteOrphanFiles`. Maintenance is disabled by default; each task must also be enabled explicitly.

### Target tables

When `sink.maintenance.tables` is omitted, the submission client discovers the tables captured by the source (including its inclusion and exclusion rules), applies the pipeline's routing rules and route mode, and deduplicates the final target identifiers. Discovery supports the MySQL, PostgreSQL, Oracle, and SQL Server pipeline sources and requires source metadata access from the submission client. PostgreSQL discovery also respects `table-id.include-database`. Other sources must configure `sink.maintenance.tables` explicitly.

To maintain only selected tables, set `sink.maintenance.tables` to semicolon-separated final target identifiers after routing; this explicit list takes precedence over discovery. Identifiers must be unquoted `database.table` or `namespace.database.table`, containing ASCII letters, digits, underscores, or hyphens. Patterns are not supported in this optional list. Automatically discovered names retain their literal components, including Unicode and characters such as `$` and `;`; they are not parsed as this configuration list.

The target set is fixed at submission; adding tables requires resubmitting the job. Targets can be **created by CDC after the job starts**. Until a target exists, its maintenance tasks remain inactive without blocking operator startup or checkpoints. Empty discovery fails submission, and different identifiers resolving to the same Iceberg table UUID are rejected.

### Enabling maintenance

Use streaming execution mode and enable checkpointing in the Flink configuration (for example, `execution.checkpointing.interval: 60 s`). Use matching CDC runtime/composer, source connector, and Iceberg connector builds; replacing only the Iceberg connector does not add source discovery to an older CDC installation. The cluster must provide `flink-table-runtime` matching its Flink version for orphan metadata reads.

Maintenance uses the sink's catalog, which must return Iceberg `BaseTable` instances as the built-in catalogs do. Each target adds monitoring and maintenance operators, so provision slots for the maintenance slot sharing group as well as the CDC operators. Do not run multiple maintenance jobs against the same table concurrently or enable the legacy `sink.compaction.enabled` together with this feature.

The following sink configuration rewrites files after 10 observed commits or one hour, expires snapshots daily, and checks for orphan files weekly:

```yaml
sink:
  type: iceberg
  catalog.properties.type: hadoop
  catalog.properties.warehouse: /path/warehouse
  sink.maintenance.enabled: true
  # Optional: limit maintenance to these routed targets.
  # sink.maintenance.tables: sales.orders;sales.customers
  sink.maintenance.uid-prefix: sales-maintenance
  sink.maintenance.parallelism: 2
  sink.maintenance.lock.jdbc.uri: jdbc:postgresql://lock-db:5432/iceberg
  sink.maintenance.lock.jdbc.properties.user: maintenance
  sink.maintenance.lock.jdbc.properties.password: <password>
  sink.maintenance.lock.jdbc.init-lock-tables: true
  sink.maintenance.rewrite-data-files.enabled: true
  sink.maintenance.rewrite-data-files.commit-count: 10
  sink.maintenance.rewrite-data-files.interval: 1 h
  sink.maintenance.expire-snapshots.enabled: true
  sink.maintenance.expire-snapshots.interval: 1 d
  sink.maintenance.expire-snapshots.max-age: 7 d
  sink.maintenance.expire-snapshots.retain-last: 100
  sink.maintenance.delete-orphan-files.enabled: true
  sink.maintenance.delete-orphan-files.interval: 7 d
  sink.maintenance.delete-orphan-files.min-age: 7 d
```

The JDBC database stores maintenance locks independently of the table catalog. Put the JDBC driver in the CDC installation's `lib` directory for the submission client, and also supply it with the CLI `--jar` argument for the job. Set `sink.maintenance.lock.jdbc.init-lock-tables` to `true` to create Iceberg's lock table once on the submission client, or provision it beforehand. The database must be reachable from the Flink workers and, when initializing the lock table, from the submission client. Use `false` after provisioning.

### Maintenance options

All keys below have the prefix `sink.maintenance.`. Settings apply to every selected target table. Task-specific settings take effect when that task is enabled.

| Key | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Enable the maintenance topology. Requires at least one enabled task. |
| `tables` | (none) | Optional semicolon-separated target identifiers. If omitted, derive targets from the captured source tables and routing rules at submission. |
| `uid-prefix` | `iceberg-maintenance` | Stable operator UID and lock identity prefix. Use a different prefix for each independent pipeline. |
| `parallelism` | `1` | Default task parallelism per table. Monitoring and scheduling remain single-parallelism operators. |
| `slot-sharing-group` | `iceberg-maintenance` | Slot sharing group for maintenance operators. |
| `rate-limit` | `1 min` | Table polling interval and minimum scheduling interval; a positive whole number of seconds. |
| `lock-check-delay` | `30 s` | Delay before retrying a held maintenance lock. |
| `max-read-back` | `100` | Maximum snapshots examined per poll, including at startup. |
| `lock.jdbc.uri` | (none) | Required JDBC URL for persistent maintenance locks. |
| `lock.jdbc.init-lock-tables` | `false` | Create the Iceberg lock table on submission if it does not exist. |
| `lock.jdbc.properties.*` | (none) | JDBC connection properties. For example, suffix `user` is passed as Iceberg's `jdbc.user`. |
| `rewrite-data-files.enabled` | `false` | Enable data file rewriting. |
| `rewrite-data-files.interval` | `1 h` | Time trigger for rewriting. |
| `rewrite-data-files.commit-count` | (none) | Additional trigger based on observed non-replace snapshot commits. |
| `rewrite-data-files.data-file-count` | (none) | Additional trigger based on added data files. |
| `rewrite-data-files.target-file-size-bytes` | `536870912` | Target output file size (512 MiB). |
| `rewrite-data-files.min-input-files` | `5` | Input file count threshold used by the rewrite planner. |
| `rewrite-data-files.delete-file-threshold` | `2147483647` | Associated delete file count that makes a data file eligible for rewriting. |
| `rewrite-data-files.max-rewrite-bytes` | `10737418240` | Maximum input bytes to rewrite per run (10 GiB). File groups are capped at the smaller of this budget and 100 GiB, so large partitions can be processed across runs. A single input file larger than the budget requires increasing it. |
| `expire-snapshots.enabled` | `false` | Enable snapshot expiration and cleanup of files no longer referenced. |
| `expire-snapshots.interval` | `1 d` | Time trigger for snapshot expiration. |
| `expire-snapshots.commit-count` | (none) | Additional commit-count trigger for expiration. |
| `expire-snapshots.max-age` | `7 d` | Age threshold for expiring snapshots. |
| `expire-snapshots.retain-last` | `100` | Minimum number of snapshots to retain. |
| `delete-orphan-files.enabled` | `false` | Enable orphan file deletion under the table location. |
| `delete-orphan-files.interval` | `7 d` | Time trigger for orphan cleanup. |
| `delete-orphan-files.min-age` | `7 d` | Minimum age of orphan candidates; must be at least 3 days. |
| `delete-batch-size` | `1000` | Deletion batch size for expiration and orphan cleanup. |

### Triggers and task outcomes

Triggers are evaluated independently for each task and table. A task's configured conditions are combined with **OR**. The monitor counts Iceberg snapshot commits; empty checkpoints do not increase the count. Commits from other writers are observable, while `replace` snapshots generated by maintenance are skipped. On startup the monitor can count existing history; history beyond `max-read-back`, or history already expired, is not counted. Choose the polling interval and read-back bound for the table's commit rate.

Existing tables remain eligible for interval triggers even when no new snapshots are committed. Polling, rate limits, lock availability, and earlier tasks can delay execution. Only one maintenance task per table runs at a time. The thresholds trigger a planning attempt; the rewrite planner may find no eligible files.

Monitor Iceberg's maintenance logs and metrics: a task can report failure while the CDC job continues running. Later triggers can attempt the task again.

### Retention and orphan cleanup

Expiration must preserve snapshots needed by readers, incremental consumers, and CDC recovery. Configure both age and minimum snapshot count for the required retention window; branches and tags also affect Iceberg retention. Orphan cleanup permanently removes unreferenced old files, including potentially uncommitted files from long-running writers. Its minimum age must exceed the longest write, outage, and recovery window, and each table must have its own storage location. The 3-day validation floor alone does not establish a safe retention window for every deployment.

Orphan cleanup preserves files in tables with no committed snapshot or with `gc.enabled=false`; such attempts are recorded as failed tasks.

The FileIO must implement `SupportsPrefixOperations` for orphan listing and `SupportsBulkOperations` for file deletion; Iceberg's `HadoopFileIO` and `S3FileIO` support both. Orphan listing uses the table's configured FileIO, including `hadoop.conf.*`, but Iceberg's JSON metadata scan tasks do not retain those custom Hadoop settings. With `HadoopFileIO`, make the required settings available through the default Hadoop configuration on every TaskManager, for example through `core-site.xml` on its classpath. `S3FileIO` carries its storage settings in its own properties. Verify that orphan metadata scans can access storage even if CDC writes already succeed.

### Recovery

When restoring from a checkpoint or savepoint, keep the target table set, UID prefix, enabled task set, and lock database and credentials stable. Reordering the table list is allowed. If targets are discovered automatically, discovery runs again against the source's current metadata; use an explicit list of the original targets if source tables or routes have changed.

Changing the enabled task set changes stateful operator UIDs, so normal restore rejects the unmatched old maintenance state. Such changes require explicitly starting fresh maintenance state. See [Iceberg Flink maintenance](https://iceberg.apache.org/docs/1.10.1/flink-maintenance/) for the underlying APIs and task behavior.

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">Iceberg type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
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
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>TIMESTAMP_LTZ</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
