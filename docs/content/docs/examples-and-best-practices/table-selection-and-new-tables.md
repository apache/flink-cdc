---
title: "Table Selection and Adding Tables"
slug: "/docs/examples-and-best-practices/table-selection-and-new-tables"
sidebar_position: 5
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

# Table Selection and Adding Tables

A MySQL ingestion pipeline can select tables with regular expressions and exclude temporary tables.
Adding a newly created empty table and adding an existing table with historical data require different source options:

| Scenario | MySQL source option | How it takes effect | Data captured |
|----------|---------------------|---------------------|---------------|
| A new empty table is created while the job is reading binlog | `scan.binlog.newly-added-table.enabled: true` | The running job captures the table's creation and subsequent changes. | Changes recorded in binlog; no historical snapshot is taken for the new table. |
| An existing table with historical rows is added to the captured set | `scan.newly-added-table.enabled: true` | Update the table selection and restore the job from a savepoint. | A snapshot of the newly included table, followed by its changes. |

These options cannot both be enabled.
This example first captures a new table without restarting, then restores the same pipeline to include a historical table.
Temporary tables remain excluded throughout.

## Prepare the Flink environment

Prepare a Linux or macOS machine with Docker Compose, Java 11, and a Flink standalone cluster.
Choose a Flink distribution and the corresponding dependencies from the following table:

| Flink distribution | Required dependencies |
|--------------------|-----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[Paimon SQL 2.1.0 for Flink 1.20](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/2.1.0/paimon-flink-1.20-2.1.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[Paimon SQL 2.1.0 for Flink 2.1](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.1.0/paimon-flink-2.1-2.1.0.jar) |

The Paimon SQL connector in each row matches the Paimon version bundled with the corresponding Pipeline connector.
Download and extract the selected distributions, then set the following paths to your installation directories:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the MySQL and Paimon Pipeline connector JARs from the table in `$FLINK_CDC_HOME/lib`.

Place the MySQL JDBC driver from the table in `$FLINK_HOME/lib` before starting Flink.
Save the Hadoop JAR as `$FLINK_HOME/opt/flink-shaded-hadoop.jar` and the Paimon SQL connector
as `$FLINK_HOME/opt/paimon-sql-connector.jar`.
The commands below add Hadoop with `--jar` when submitting the pipeline and verification queries.
If this Hadoop JAR is already in `$FLINK_HOME/lib`, move it to the `opt` path above before starting the cluster.
Loading it as a job dependency avoids conflicts with the libraries used by Flink's own CLI.

In `$FLINK_HOME/conf/config.yaml`, add or update the following settings.
Merge them into existing `execution`, `jobmanager`, and `taskmanager` sections if those sections already exist:

```yaml
execution:
  checkpointing:
    interval: 3s
jobmanager:
  memory:
    process:
      size: 2048m
    jvm-metaspace:
      size: 512m
taskmanager:
  memory:
    process:
      size: 2048m
    jvm-metaspace:
      size: 512m
  numberOfTaskSlots: 4
```

Checkpointing makes Paimon commits visible to readers.
Four task slots allow the ingestion pipeline and verification queries to run together.
These memory settings accommodate the verification queries and savepoint restore in this walkthrough.
Start the cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

The [Flink Web UI](http://localhost:8081/) should show an available TaskManager.

## Prepare the source tables

Save the following content as `compose.yaml` in a new working directory:

```yaml
services:
  mysql:
    image: mysql:8.0
    platform: linux/amd64
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: tutorial
      MYSQL_ROOT_HOST: "%"
    command:
      - --server-id=1
      - --log-bin=mysql-bin
      - --binlog-format=ROW
      - --binlog-row-image=FULL
      - --default-time-zone=+00:00
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-ptutorial"]
      interval: 2s
      timeout: 5s
      retries: 30
```

Start MySQL and open its client:

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

Create one business table, one temporary table, and one historical table:

```sql
CREATE DATABASE inventory;
USE inventory;

CREATE TABLE customers (
  id BIGINT NOT NULL,
  name VARCHAR(100),
  PRIMARY KEY (id)
);
INSERT INTO customers VALUES (101, 'Alice');

CREATE TABLE customers_tmp LIKE customers;
INSERT INTO customers_tmp VALUES (901, 'Temporary customer');

CREATE TABLE orders_history (
  id BIGINT NOT NULL,
  total_price DECIMAL(10, 2),
  PRIMARY KEY (id)
);
INSERT INTO orders_history VALUES (201, 100.00), (202, 200.00);
```

Keep this client open for subsequent changes. Run shell commands in another terminal.

## Select tables and exclude temporary data

Save the following configuration as `tables.yaml`:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: inventory.\.*
  tables.exclude: inventory.customers_tmp,inventory.products_tmp,inventory.orders_history
  server-id: 5400-5404
  server-time-zone: UTC
  scan.binlog.newly-added-table.enabled: true

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: file:///tmp/flink-cdc-table-selection/warehouse

pipeline:
  name: Table selection and new tables
  parallelism: 2
  schema.change.behavior: evolve
```

The `tables` expression selects tables in `inventory`.
The `tables.exclude` list excludes the two temporary-table names and the existing `orders_history` table.
An exclusion takes precedence when a table matches both expressions.

The warehouse and savepoint paths in this example use the local filesystem.
For a distributed cluster, use shared storage accessible from all Flink processes.

Submit the pipeline and record the Job ID:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" tables.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Wait for the initial snapshot and a completed checkpoint. Open the Flink SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Create the catalog and verify the captured tables:

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG lake WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = 'file:///tmp/flink-cdc-table-selection/warehouse',
  'cache-enabled' = 'false'
);
USE CATALOG lake;

SHOW TABLES FROM inventory;
SELECT * FROM inventory.customers ORDER BY id;
```

Only `customers` should appear in the table list, containing `(101, Alice)`.
Neither `customers_tmp` nor `orders_history` should exist in the target catalog.

## Capture a newly created table without restarting

First, update the existing customer in MySQL:

```sql
UPDATE inventory.customers SET name = 'Alicia' WHERE id = 101;
```

Run the following query in the SQL Client until the name becomes `Alicia`:

```sql
SELECT * FROM inventory.customers ORDER BY id;
```

This confirms that the job is consuming incremental changes before you create the new table.

In MySQL, create and populate a business table and another temporary table:

```sql
CREATE TABLE inventory.products (
  id BIGINT NOT NULL,
  name VARCHAR(100),
  price DECIMAL(10, 2),
  PRIMARY KEY (id)
);
INSERT INTO inventory.products VALUES (301, 'Book', 30.00);

CREATE TABLE inventory.products_tmp LIKE inventory.products;
INSERT INTO inventory.products_tmp VALUES (999, 'Temporary product', 1.00);
```

After the changes and a subsequent checkpoint complete, query Paimon:

```sql
SHOW TABLES FROM inventory;
SELECT * FROM inventory.products ORDER BY id;
```

The table list should contain `customers` and `products`.
The product query should return `(301, Book, 30.00)`.
The job keeps the same Job ID, and both temporary tables remain excluded.

The new table must match `tables` and must not match `tables.exclude`.
This mode reads the new table's changes from binlog; use the restore procedure below when historical rows need a snapshot.

## Include a table with historical data

### Stop the job with a savepoint

Replace `<job-id>` with the running ingestion Job ID:

```shell
"$FLINK_HOME/bin/flink" stop \
  --savepointPath file:///tmp/flink-cdc-table-selection/savepoints \
  <job-id>
```

Wait for the command to finish and record the complete savepoint path printed by Flink.
Keep the savepoint, the Paimon warehouse, and the MySQL binlog needed for recovery.

While the ingestion job is stopped, run the following changes in MySQL:

```sql
UPDATE inventory.customers SET name = 'Alice Smith' WHERE id = 101;
INSERT INTO inventory.orders_history VALUES (203, 300.00);
```

The customer update will verify recovery of a previously captured table.
The new order will be included with the other historical orders when that table is first captured.

### Update the source configuration and restore

Replace the contents of `tables.yaml` with:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: inventory.\.*
  tables.exclude: inventory.customers_tmp,inventory.products_tmp
  server-id: 5400-5404
  server-time-zone: UTC
  scan.binlog.newly-added-table.enabled: false
  scan.newly-added-table.enabled: true

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: file:///tmp/flink-cdc-table-selection/warehouse

pipeline:
  name: Table selection and new tables
  parallelism: 2
  schema.change.behavior: evolve
```

The historical table is no longer excluded.
The binlog-only option is disabled, and snapshot scanning for newly included tables is enabled.

Keep the sink, warehouse, pipeline topology, and parallelism unchanged.
For this workflow, leave `scan.incremental.snapshot.metadata.release.enabled` at its default value of `false`;
a savepoint from a job that has already released this metadata cannot be used to enable newly added table scanning.

Replace the placeholder below with the complete savepoint path returned by the stop command:

```shell
export SAVEPOINT_PATH=/path/to/savepoint

"$FLINK_CDC_HOME/bin/flink-cdc.sh" tables.yaml \
  --from-savepoint "$SAVEPOINT_PATH" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Record the new Job ID. Wait for the historical-table snapshot and a successful checkpoint, then query Paimon:

```sql
SHOW TABLES FROM inventory;
SELECT * FROM inventory.customers ORDER BY id;
SELECT * FROM inventory.orders_history ORDER BY id;
```

The target catalog should now contain `customers`, `products`, and `orders_history`.
The customer should be `(101, Alice Smith)`, proving that changes made while the job was stopped were recovered.

The historical table should contain all three rows:

| id | total_price |
|----|-------------|
| 201 | 100.00 |
| 202 | 200.00 |
| 203 | 300.00 |

The temporary tables should still be absent.

## Verify subsequent changes to the added table

Run the following statements in MySQL:

```sql
UPDATE inventory.orders_history SET total_price = 120.00 WHERE id = 201;
DELETE FROM inventory.orders_history WHERE id = 202;
INSERT INTO inventory.orders_history VALUES (204, 400.00);
```

After a subsequent checkpoint completes, query the historical table again:

```sql
SELECT * FROM inventory.orders_history ORDER BY id;
```

The result should be:

| id | total_price |
|----|-------------|
| 201 | 120.00 |
| 203 | 300.00 |
| 204 | 400.00 |

The added table now participates in normal incremental ingestion, together with the previously captured tables.
For source option details, see the [MySQL Pipeline connector](../connectors/pipeline-connectors/mysql.md).

## Clean up

Use the restored Job ID to cancel the job, then stop the tutorial services:

```shell
"$FLINK_HOME/bin/flink" cancel <restored-job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

The warehouse and savepoints remain under `/tmp/flink-cdc-table-selection`.
Use empty tutorial storage when starting this example again from scratch.
