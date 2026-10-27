---
title: "Real-time PostgreSQL Ingestion into Iceberg"
slug: "/docs/examples-and-best-practices/postgres-to-iceberg"
sidebar_position: 4
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

# Real-time PostgreSQL Ingestion into Iceberg

This example uses a Flink CDC YAML pipeline to read PostgreSQL orders and write them to an Apache Iceberg table.
It covers the initial snapshot, inserts, updates, deletes, and adding a column.

PostgreSQL provides changes through logical replication.
The source table has a primary key and uses `REPLICA IDENTITY FULL` so that update and delete events contain complete old rows.

## Prepare the Flink environment

Prepare a Linux or macOS machine with Docker Compose and Java 11.
Choose a Flink distribution and the corresponding dependencies:

| Flink distribution | Required dependencies |
|--------------------|-----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[Postgres Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/@@VERSION@@-1.20/flink-cdc-pipeline-connector-postgres-@@VERSION@@-1.20.jar)<br />[Iceberg Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-iceberg/@@VERSION@@-1.20/flink-cdc-pipeline-connector-iceberg-@@VERSION@@-1.20.jar)<br />[Iceberg Flink runtime 1.10.1 (1.20)](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar)<br />[Hadoop client API 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar)<br />[Hadoop client runtime 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar)<br />[Commons Logging 1.2](https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[Postgres Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/@@VERSION@@-2.2/flink-cdc-pipeline-connector-postgres-@@VERSION@@-2.2.jar)<br />[Iceberg Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-iceberg/@@VERSION@@-2.2/flink-cdc-pipeline-connector-iceberg-@@VERSION@@-2.2.jar)<br />[Iceberg Flink runtime 1.10.1 (2.0)](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar)<br />[Hadoop client API 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar)<br />[Hadoop client runtime 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar)<br />[Commons Logging 1.2](https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar) |

Use the Iceberg Flink runtime listed for your Flink row; it matches the Iceberg version bundled with that Pipeline connector.
Download and extract the selected distributions, then set their installation paths:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the Postgres and Iceberg Pipeline connector JARs in `$FLINK_CDC_HOME/lib`.
The Postgres connector already includes its JDBC driver.

Place the matching Iceberg Flink runtime, Hadoop client API, Hadoop client runtime, and Commons Logging JARs
in `$FLINK_HOME/lib` before starting Flink.
Use these Hadoop client dependencies for this example and remove any Hadoop 2 uber JAR left in `$FLINK_HOME/lib` from another tutorial.
The Iceberg runtime must also be available to the SQL Client when it creates the catalog.

In `$FLINK_HOME/conf/config.yaml`, add or update the following settings.
Merge them into existing `execution`, `jobmanager`, and `taskmanager` sections as necessary:

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

Checkpointing commits the Iceberg writes so that queries can see them.
The memory and task slots allow ingestion and verification queries to run together.
Start the standalone cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

Confirm that a TaskManager is available in the [Flink Web UI](http://localhost:8081/).

## Prepare PostgreSQL logical replication

Save the following content as `compose.yaml` in a new working directory:

```yaml
services:
  postgres:
    image: postgres:16.4-bookworm
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: tutorial
      POSTGRES_DB: inventory
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres", "-d", "inventory"]
      interval: 2s
      timeout: 5s
      retries: 30
```

The server starts with logical WAL enabled and capacity for replication slots and WAL senders.
Start PostgreSQL and open its client:

```shell
docker compose up -d --wait
docker compose exec postgres psql -U postgres -d inventory
```

Create the table, initial rows, and a publication containing only that table:

```sql
CREATE TABLE public.orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount NUMERIC(10, 2) NOT NULL,
  order_status VARCHAR(20) NOT NULL
);
ALTER TABLE public.orders REPLICA IDENTITY FULL;

INSERT INTO public.orders VALUES
  (101, 'Alice', 29.90, 'NEW'),
  (102, 'Bob', 5.00, 'NEW');

CREATE PUBLICATION cdc_orders FOR TABLE public.orders;
```

This local example uses the container's default database administrator.
For another deployment, the ingestion account needs table read access and logical replication permissions.

Keep the PostgreSQL client open for subsequent changes. Run shell commands in another terminal.

## Define and submit the pipeline

Save the following configuration as `postgres-to-iceberg.yaml`:

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: postgres
  password: tutorial
  tables: inventory.public.orders
  decoding.plugin.name: pgoutput
  slot.name: cdc_orders
  table-id.include-database: false
  schema-change.enabled: true
  debezium.publication.name: cdc_orders
  debezium.publication.autocreate.mode: disabled

sink:
  type: iceberg
  catalog.properties.type: hadoop
  catalog.properties.warehouse: file:///tmp/flink-cdc-postgres-iceberg/warehouse
  table.properties.format-version: 2

route:
  - source-table: public.orders
    sink-table: ods.orders

pipeline:
  name: PostgreSQL orders to Iceberg
  parallelism: 2
  local-time-zone: UTC
  schema.change.behavior: evolve
```

The source reuses the publication created above and creates its logical replication slot when the job starts.
Use a unique slot name for each ingestion job.

The source `tables` option uses `database.schema.table`.
With `table-id.include-database: false`, emitted table identifiers use `schema.table`,
so the route matches `public.orders` and writes to `ods.orders`.

The Iceberg connector automatically creates the target namespace and table.
This example uses an unpartitioned format-v2 table to support the update and delete flow.
The warehouse is a local path because all Flink processes run on one machine;
use shared storage for a distributed cluster.

Submit the ingestion pipeline and record its Job ID:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" postgres-to-iceberg.yaml
```

Wait for the job to run and complete a checkpoint.

## Verify the initial snapshot

Open the SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh"
```

Create a catalog pointing to the same warehouse and query the target table:

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 'file:///tmp/flink-cdc-postgres-iceberg/warehouse',
  'cache-enabled' = 'false'
);
USE CATALOG lake;

SELECT id, customer_name, amount, order_status
FROM ods.orders
ORDER BY id;
```

The result should contain:

| id | customer_name | amount | order_status |
|----|---------------|--------|--------------|
| 101 | Alice | 29.90 | NEW |
| 102 | Bob | 5.00 | NEW |

If the table or rows are not available yet, wait for the snapshot and a successful checkpoint, then query again.
Batch queries read committed Iceberg snapshots; rerun them to see later changes.

## Verify inserts, updates, and deletes

In the PostgreSQL client, update an order, delete another, and insert a new one:

```sql
UPDATE public.orders SET amount = 39.90 WHERE id = 101;
DELETE FROM public.orders WHERE id = 102;
INSERT INTO public.orders VALUES (103, 'Carol', 12.50, 'NEW');
```

After the changes are consumed and a subsequent checkpoint completes, query Iceberg again:

```sql
SELECT id, customer_name, amount, order_status
FROM ods.orders
ORDER BY id;
```

The result should contain the updated price and new order, with no row for order 102:

| id | customer_name | amount | order_status |
|----|---------------|--------|--------------|
| 101 | Alice | 39.90 | NEW |
| 103 | Carol | 12.50 | NEW |

The source primary key supplies the identifier for applying row changes to Iceberg.
The connector uses at-least-once delivery with primary-key-based idempotent writes.

## Verify an added column

In PostgreSQL, add a column and populate it:

```sql
ALTER TABLE public.orders ADD COLUMN quantity INTEGER;
UPDATE public.orders SET quantity = 2 WHERE id = 101;
UPDATE public.orders SET quantity = 3 WHERE id = 103;
```

With `schema-change.enabled: true` and `pgoutput`, the source infers schema changes from relation metadata.
The updates after the DDL cause the new column to appear in the replicated records.

After a subsequent checkpoint completes, inspect the target schema and rows:

```sql
DESCRIBE ods.orders;

SELECT id, amount, quantity
FROM ods.orders
ORDER BY id;
```

The target should include `quantity INT` and return:

| id | amount | quantity |
|----|--------|----------|
| 101 | 39.90 | 2 |
| 103 | 12.50 | 3 |

## Inspect and clean up the replication slot

In PostgreSQL, inspect the slot:

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cdc_orders';
```

During ingestion, `active` should be true and the WAL positions should be populated.
The slot retains WAL required for recovery; stopping a job does not by itself remove a slot that you intend to reuse.

When this example is complete and you no longer need to restore the job, replace `<job-id>` and cancel it:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
```

Rerun the slot query above until `active` is false. The replication connection may take a moment to close after job cancellation.
Then drop the inactive slot and stop the tutorial services:

```shell
docker compose exec postgres psql -U postgres -d inventory \
  -c "SELECT pg_drop_replication_slot('cdc_orders');"

"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

The local warehouse remains under `/tmp/flink-cdc-postgres-iceberg/warehouse`.
Use an empty warehouse when starting this example again from scratch.

For connector details, see [Postgres](../connectors/pipeline-connectors/postgres.md) and
[Iceberg](../connectors/pipeline-connectors/iceberg.md).
