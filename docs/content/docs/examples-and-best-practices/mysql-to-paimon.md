---
title: "Real-time MySQL Ingestion into Paimon"
slug: "/docs/examples-and-best-practices/mysql-to-paimon"
sidebar_position: 2
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

# Real-time MySQL Ingestion into Paimon

This example uses a Flink CDC YAML pipeline to merge two MySQL order tables into one Apache Paimon table.
It covers the initial snapshot, schema evolution, and subsequent inserts, updates, and deletes.
SQL queries are used to verify the ingested data.

The two source tables have the same schema, and their order keys are unique across both tables.
When merging tables with overlapping primary key values, define a composite key that also identifies the source table;
otherwise, records with the same key can overwrite each other in Paimon.

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
Merge them into existing `execution` and `taskmanager` sections if those sections already exist:

```yaml
execution:
  checkpointing:
    interval: 3s
taskmanager:
  numberOfTaskSlots: 4
```

Checkpointing makes Paimon commits visible to readers.
Four task slots allow the ingestion pipeline and verification queries to run together.
Start the cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

The [Flink Web UI](http://localhost:8081/) should show an available TaskManager.

## Prepare the MySQL tables

Create a working directory and save the following content as `compose.yaml`:

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

Start MySQL and wait for its health check to succeed:

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

In the MySQL client, create the two source tables and insert sample data:

```sql
CREATE DATABASE orders;
USE orders;

CREATE TABLE orders_1 (
  orderkey BIGINT NOT NULL,
  custkey BIGINT,
  total_price DECIMAL(10, 2),
  order_date DATE,
  PRIMARY KEY (orderkey)
);
CREATE TABLE orders_2 LIKE orders_1;

INSERT INTO orders_1 VALUES
  (1, 1, 100.00, '2026-01-01'),
  (2, 2, 200.00, '2026-01-02');
INSERT INTO orders_2 VALUES
  (11, 1, 300.00, '2026-01-03'),
  (12, 3, 400.00, '2026-01-04');
```

Keep this client open for the schema and data changes below. Run the remaining shell commands in another terminal.

## Define and submit the ingestion pipeline

Save the following configuration as `mysql-to-paimon.yaml`:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: orders.orders_[0-9]+
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: file:///tmp/flink-cdc-paimon-tutorial/warehouse

route:
  - source-table: orders.orders_[0-9]+
    sink-table: ods.orders

pipeline:
  name: MySQL orders to Paimon
  parallelism: 2
  schema.change.behavior: evolve
```

The `tables` expression selects both order tables, and the route merges them into `ods.orders`.
Flink CDC creates the target database and table automatically.

The `file:///` warehouse is suitable for this example because all Flink processes run on one machine.
For a distributed cluster, use shared storage accessible from the client, JobManager, and every TaskManager.

Submit the pipeline:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Record the returned Job ID. In the Flink Web UI, confirm that the job is running and a checkpoint completes.

## Verify the initial snapshot

Open the Flink SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

Create a Paimon catalog pointing to the same warehouse, then query the merged table:

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG lake WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = 'file:///tmp/flink-cdc-paimon-tutorial/warehouse',
  'cache-enabled' = 'false'
);
USE CATALOG lake;

SELECT orderkey, custkey, total_price, order_date
FROM ods.orders
ORDER BY orderkey;
```

The result should contain these four rows:

| orderkey | custkey | total_price | order_date |
|----------|---------|-------------|------------|
| 1 | 1 | 100.00 | 2026-01-01 |
| 2 | 2 | 200.00 | 2026-01-02 |
| 11 | 1 | 300.00 | 2026-01-03 |
| 12 | 3 | 400.00 | 2026-01-04 |

If the table is not available yet, wait for the snapshot and a successful checkpoint, then run the query again.
Batch queries read a committed snapshot; run them again to see later changes.

## Synchronize schema changes

In the MySQL client, add the same column to both source tables and update two records:

```sql
ALTER TABLE orders.orders_1 ADD COLUMN quantity BIGINT;
ALTER TABLE orders.orders_2 ADD COLUMN quantity BIGINT;
UPDATE orders.orders_1 SET quantity = 2 WHERE orderkey = 1;
UPDATE orders.orders_2 SET quantity = 3 WHERE orderkey = 11;
```

After a subsequent checkpoint completes, run the following statements in the Flink SQL Client:

```sql
DESCRIBE ods.orders;

SELECT orderkey, total_price, quantity
FROM ods.orders
ORDER BY orderkey;
```

The target schema should contain `quantity BIGINT`, with the following values:

| orderkey | total_price | quantity |
|----------|-------------|----------|
| 1 | 100.00 | 2 |
| 2 | 200.00 | NULL |
| 11 | 300.00 | 3 |
| 12 | 400.00 | NULL |

## Synchronize inserts, updates, and deletes

In the MySQL client, insert an order, update an existing order, and delete another:

```sql
INSERT INTO orders.orders_2 VALUES (13, 3, 500.00, '2026-01-05', 1);
UPDATE orders.orders_1
SET total_price = 120.00, quantity = 4
WHERE orderkey = 1;
DELETE FROM orders.orders_1 WHERE orderkey = 2;
```

After the changes are committed by a checkpoint, query Paimon again:

```sql
SELECT orderkey, custkey, total_price, quantity
FROM ods.orders
ORDER BY orderkey;
```

The result should contain the inserted order, the updated price and quantity, and no row for order 2:

| orderkey | custkey | total_price | quantity |
|----------|---------|-------------|----------|
| 1 | 1 | 120.00 | 4 |
| 11 | 1 | 300.00 | 3 |
| 12 | 3 | 400.00 | NULL |
| 13 | 3 | 500.00 | 1 |

## Clean up

Exit the SQL and MySQL clients. Replace `<job-id>` with the ingestion Job ID, cancel the job, and stop the tutorial services:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

The warehouse files remain under `/tmp/flink-cdc-paimon-tutorial/warehouse`.
Use an empty warehouse when repeating this example from scratch.
