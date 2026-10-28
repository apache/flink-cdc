---
title: "Metadata Columns, Computed Columns, and Soft Deletes"
slug: "/docs/examples-and-best-practices/metadata-and-soft-deletes"
sidebar_position: 6
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

# Metadata Columns, Computed Columns, and Soft Deletes

A Flink CDC Transform rule can enrich records as they are ingested.
This example reads MySQL orders, adds source identity and operation metadata, computes a date partition,
and writes the result to a Paimon primary key table.

When a source row is deleted, the pipeline keeps its last values in Paimon with a deletion marker.
A later insert with the same target primary key makes the row active again.

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
These memory settings accommodate the verification queries in this walkthrough.
Start the cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

The [Flink Web UI](http://localhost:8081/) should show an available TaskManager.

## Prepare the source data

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

Create the source table and two orders:

```sql
CREATE DATABASE inventory;
USE inventory;

CREATE TABLE orders (
  id BIGINT NOT NULL,
  product VARCHAR(100) NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  PRIMARY KEY (id)
);
INSERT INTO orders VALUES
  (101, 'book', 29.90, '2026-01-01 12:00:00'),
  (102, 'pen', 5.00, '2026-01-02 09:30:00');
```

Keep this client open for the changes below. Run shell commands in another terminal.

## Define the Transform rule

Save the following configuration as `transform.yaml`:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: inventory.orders
  server-id: 5400-5404
  server-time-zone: UTC
  metadata.list: op_ts

transform:
  - source-table: inventory.orders
    projection: >-
      *,
      CONCAT_WS('.', __namespace_name__, __schema_name__, __table_name__) AS source_table,
      op_ts AS source_ts,
      __data_event_type__ AS op_type,
      UPPER(product) AS product_upper,
      DATE_FORMAT(created_at, 'yyyyMMdd') AS order_day
    primary-keys: id, order_day
    partition-keys: order_day
    converter-after-transform: SOFT_DELETE

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: file:///tmp/flink-cdc-transform/warehouse

pipeline:
  name: Enrich MySQL orders with soft deletes
  parallelism: 2
  local-time-zone: UTC
  schema.change.behavior: evolve
```

The folded YAML scalar after `projection: >-` allows the expression to span multiple lines.
The `*` projects all source columns, followed by the additional columns:

| Column | Expression | Meaning |
|--------|------------|---------|
| `source_table` | `CONCAT_WS('.', __namespace_name__, __schema_name__, __table_name__)` | Builds the source identifier while skipping null hierarchy levels. This example produces `inventory.orders`. |
| `source_ts` | `op_ts` | Source operation time in epoch milliseconds. Requires `metadata.list: op_ts`; snapshot records have a value of `0`. |
| `op_type` | `__data_event_type__` | The original change type, retained when a delete is converted to an insert. |
| `product_upper` | `UPPER(product)` | An uppercase product name. |
| `order_day` | `DATE_FORMAT(created_at, 'yyyyMMdd')` | The order date used to partition the target table. |

Paimon partition columns must be included in the primary key, so the target key is `id, order_day`.
This example keeps `created_at` unchanged, making the derived partition key stable.
The warehouse uses a local path because all Flink processes run on one machine;
use shared storage when deploying to multiple machines.

`SOFT_DELETE` runs after the projection and converts a delete event into an insert containing the projected old row.
The stored `op_type` still identifies the source delete.
The main operation markers are:

| Marker | Meaning |
|--------|---------|
| `+I` | Snapshot row or insert. |
| `-U` | Before image of an update. |
| `+U` | After image of an update. |
| `-D` | Delete; the row is retained by this pipeline's soft-delete converter. |

The Paimon primary key table stores the latest state for each target key.
Readers can select active rows by excluding the `-D` marker.
For a pipeline that publishes individual change events, see [Real-time MySQL Change Distribution to Kafka](mysql-to-kafka.md).

Submit the pipeline and record its Job ID:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" transform.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Wait for the initial snapshot and a completed checkpoint.

## Verify metadata and computed columns

Open the SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Create a catalog pointing to the same warehouse and inspect the target:

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG lake WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = 'file:///tmp/flink-cdc-transform/warehouse',
  'cache-enabled' = 'false'
);
USE CATALOG lake;

SHOW CREATE TABLE inventory.orders;

SELECT id, product_upper, price, order_day, op_type
FROM inventory.orders
ORDER BY id;

SELECT id, source_table, source_ts
FROM inventory.orders
ORDER BY id;
```

The table definition should contain the added columns, the `id, order_day` primary key, and the `order_day` partition.

The first query should return:

| id | product_upper | price | order_day | op_type |
|----|---------------|-------|-----------|---------|
| 101 | BOOK | 29.90 | 20260101 | +I |
| 102 | PEN | 5.00 | 20260102 | +I |

The metadata query should return:

| id | source_table | source_ts |
|----|--------------|-----------|
| 101 | inventory.orders | 0 |
| 102 | inventory.orders | 0 |

`source_ts` records database operation time; `created_at` is the business timestamp used for partitioning.
The zero value for snapshot rows does not represent their business creation time.

## Verify updates and soft deletes

Run these statements in MySQL:

```sql
UPDATE inventory.orders
SET product = 'notebook', price = 39.90
WHERE id = 101;
DELETE FROM inventory.orders WHERE id = 102;
INSERT INTO inventory.orders VALUES
  (103, 'pencil', 3.50, '2026-01-03 12:00:00');
```

After the changes and a subsequent checkpoint complete, query all rows and then the active rows:

```sql
SELECT id, product_upper, price, order_day, op_type,
       source_ts > 0 AS has_binlog_time
FROM inventory.orders
ORDER BY id;

SELECT id, product_upper, price
FROM inventory.orders
WHERE op_type <> '-D'
ORDER BY id;
```

The full result should retain the deleted order:

| id | product_upper | price | order_day | op_type | has_binlog_time |
|----|---------------|-------|-----------|---------|-----------------|
| 101 | NOTEBOOK | 39.90 | 20260101 | +U | TRUE |
| 102 | PEN | 5.00 | 20260102 | -D | TRUE |
| 103 | PENCIL | 3.50 | 20260103 | +I | TRUE |

The active-row query should contain only:

| id | product_upper | price |
|----|---------------|-------|
| 101 | NOTEBOOK | 39.90 |
| 103 | PENCIL | 3.50 |

Order 102 has been physically deleted from MySQL.
In Paimon, its last values remain under the same target key with `op_type = '-D'`.
The nonzero operation timestamps confirm that these rows reflect binlog changes.

## Reinsert a deleted key

Insert order 102 again, keeping its original partition date:

```sql
INSERT INTO inventory.orders VALUES
  (102, 'blue pen', 6.00, '2026-01-02 09:30:00');
```

After a subsequent checkpoint completes, query the target:

```sql
SELECT id, product_upper, price, order_day, op_type,
       source_ts > 0 AS has_binlog_time
FROM inventory.orders
ORDER BY id;

SELECT id, product_upper, price
FROM inventory.orders
WHERE op_type <> '-D'
ORDER BY id;
```

The new insert should replace the deletion marker for the same `id, order_day` key:

| id | product_upper | price | order_day | op_type | has_binlog_time |
|----|---------------|-------|-----------|---------|-----------------|
| 101 | NOTEBOOK | 39.90 | 20260101 | +U | TRUE |
| 102 | BLUE PEN | 6.00 | 20260102 | +I | TRUE |
| 103 | PENCIL | 3.50 | 20260103 | +I | TRUE |

The active-row query now includes all three orders.
For more expressions and metadata fields, see [Transform](../core-concept/transform.md).

## Clean up

Replace `<job-id>` with the ingestion Job ID, cancel the job, and stop the tutorial services:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
