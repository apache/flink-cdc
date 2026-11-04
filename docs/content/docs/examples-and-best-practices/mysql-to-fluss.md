---
title: "Real-time MySQL Ingestion into Fluss"
slug: "/docs/examples-and-best-practices/mysql-to-fluss"
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

# Real-time MySQL Ingestion into Fluss

This example uses a Flink CDC YAML pipeline to synchronize a MySQL orders table into an Apache Fluss primary key table.
It captures the initial snapshot and subsequent binlog changes, preserving the database name, table name, and primary key.
Downstream Flink SQL queries read the synchronized data from Fluss instead of querying the business database.

The example verifies inserts, updates, deletes, and adding a column, then computes paid order totals per customer.

## Prepare the Flink environment

Prepare a Linux or macOS machine with Docker Compose.
Use Java 11 for Flink 1.20 or Java 17 for Flink 2.2.
Choose a Flink distribution and its corresponding dependencies:

| Flink distribution | Required dependencies |
|--------------------|-----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Fluss Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/@@VERSION@@-1.20/flink-cdc-pipeline-connector-fluss-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Fluss Flink 1.20 connector 1.0.0](https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-flink-1.20/1.0.0/fluss-flink-1.20-1.0.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Fluss Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/@@VERSION@@-2.2/flink-cdc-pipeline-connector-fluss-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Fluss Flink 2.2 connector 1.0.0](https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-flink-2.2/1.0.0/fluss-flink-2.2-1.0.0.jar) |

Extract the selected distributions and set the installation directories:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the MySQL and Fluss **Pipeline** connector JARs in `$FLINK_CDC_HOME/lib`.
Place the MySQL JDBC driver and the matching **Fluss Flink** connector JAR in `$FLINK_HOME/lib` before starting Flink.
The Fluss Flink connector provides the catalog and SQL access used to verify the results.

In `$FLINK_HOME/conf/config.yaml`, add or update the following settings, merging them into existing sections if necessary:

```yaml
execution:
  checkpointing:
    interval: 3s
jobmanager:
  memory:
    process:
      size: 2048m
taskmanager:
  memory:
    process:
      size: 2048m
  numberOfTaskSlots: 4
```

Four slots allow the ingestion pipeline and a verification query to run together at parallelism two.
Start the standalone cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

Confirm that a TaskManager is available in the [Flink Web UI](http://localhost:8081/).

## Prepare MySQL and Fluss

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
  zookeeper:
    image: zookeeper:3.9.2
  coordinator-server:
    image: apache/fluss:1.0.0
    command: coordinatorServer
    depends_on:
      - zookeeper
    ports:
      - "9123:9123"
    volumes:
      - fluss-remote:/tmp/fluss/remote-data
    environment:
      FLUSS_PROPERTIES: |
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass";
        super.users: User:admin
  tablet-server:
    image: apache/fluss:1.0.0
    command: tabletServer
    depends_on:
      - coordinator-server
    ports:
      - "9124:9123"
    volumes:
      - fluss-remote:/tmp/fluss/remote-data
    environment:
      FLUSS_PROPERTIES: |
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass";
        super.users: User:admin
volumes:
  fluss-remote:
```

The Flink processes and SQL Client run on the host. They connect to the coordinator at `localhost:9123`
and the tablet server at `localhost:9124`, so both advertised addresses must be reachable.
For clients on another machine, replace `localhost` in the advertised listeners with a reachable hostname.
The passwords and single tablet server configuration are for this local tutorial.

Start the services, wait for the MySQL health check, and check that the Fluss coordinator and tablet server have started successfully:

```shell
docker compose up -d --wait
docker compose logs coordinator-server tablet-server
docker compose exec mysql mysql -uroot -ptutorial
```

In the MySQL client, create a fresh test database and insert six orders:

```sql
CREATE DATABASE fluss_demo;

CREATE TABLE fluss_demo.orders (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (order_id)
);

INSERT INTO fluss_demo.orders VALUES
  (1001, 101, 100.50, 'CREATED'),
  (1002, 102, 80.25, 'PAID'),
  (1003, 101, 60.25, 'PAID'),
  (1004, 101, 39.25, 'PAID'),
  (1005, 102, 19.75, 'CREATED'),
  (1006, 103, 50.50, 'PAID');
```

## Configure and submit the pipeline

Save the following configuration as `mysql-to-fluss.yaml`:

```yaml
source:
  type: mysql
  name: MySQL Orders Source
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: fluss_demo.orders
  server-id: 5401-5404
  server-time-zone: UTC

sink:
  type: fluss
  name: Fluss Orders Sink
  bootstrap.servers: localhost:9123
  bucket.num: fluss_demo.orders:4
  properties.client.security.protocol: SASL
  properties.client.security.sasl.mechanism: PLAIN
  properties.client.security.sasl.username: developer
  properties.client.security.sasl.password: developer-pass

pipeline:
  name: MySQL orders to Fluss
  parallelism: 2
  schema.change.behavior: lenient
```

The connector automatically creates `fluss_demo.orders` in Fluss with `order_id` as its primary key.
There is no need to create the target database or table manually.
`bucket.num` sets four buckets when the table is created; it does not change the bucket count of an existing table.
Without an explicit `bucket.key`, a primary key table uses its primary key, excluding partition keys, for distribution.
Use a `server-id` range that is unused by other replication clients, with at least as many IDs as the source parallelism.

Submit the ingestion job:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-fluss.yaml
```

Record the Job ID and confirm that the job is running and completing checkpoints.

## Verify the snapshot

In another terminal, start the Flink SQL Client:

```shell
"$FLINK_HOME/bin/sql-client.sh"
```

Create a Fluss catalog. SQL catalog options use `client.*`, whereas the Pipeline sink passes the same options through `properties.client.*`.

```sql
SET 'parallelism.default' = '2';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123',
  'client.security.protocol' = 'SASL',
  'client.security.sasl.mechanism' = 'PLAIN',
  'client.security.sasl.username' = 'developer',
  'client.security.sasl.password' = 'developer-pass'
);

SELECT * FROM fluss_catalog.fluss_demo.orders ORDER BY order_id;
```

After the snapshot has arrived, the query should return all six source orders.
Batch queries read a bounded snapshot; rerun the query to see later changes.
If the target table is not yet available or the snapshot is incomplete, wait for ingestion to catch up and query again.

## Verify inserts, updates, and deletes

Run the following statements in the MySQL client:

```sql
UPDATE fluss_demo.orders
SET amount = 110.50, status = 'PAID'
WHERE order_id = 1001;

DELETE FROM fluss_demo.orders WHERE order_id = 1002;

INSERT INTO fluss_demo.orders VALUES (1007, 102, 120.00, 'PAID');
```

Rerun the batch query in the SQL Client. After synchronization catches up, the expected rows are:

| order_id | customer_id | amount | status |
|----------|-------------|--------|--------|
| 1001 | 101 | 110.50 | PAID |
| 1003 | 101 | 60.25 | PAID |
| 1004 | 101 | 39.25 | PAID |
| 1005 | 102 | 19.75 | CREATED |
| 1006 | 103 | 50.50 | PAID |
| 1007 | 102 | 120.00 | PAID |

The primary key table retains the latest row for each order. Updating order 1001 replaces its previous values,
and deleting order 1002 removes it from the query result.

### Compute paid order totals

Query the current paid orders in Fluss:

```sql
SELECT customer_id, COUNT(*) AS paid_orders, SUM(amount) AS paid_amount
FROM fluss_catalog.fluss_demo.orders
WHERE status = 'PAID'
GROUP BY customer_id
ORDER BY customer_id;
```

The expected result after the changes above is:

| customer_id | paid_orders | paid_amount |
|-------------|-------------|-------------|
| 101 | 3 | 210.00 |
| 102 | 1 | 120.00 |
| 103 | 1 | 50.50 |

To observe subsequent changes continuously, switch to streaming mode and run the aggregation without `ORDER BY`:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';

SELECT customer_id, COUNT(*) AS paid_orders, SUM(amount) AS paid_amount
FROM fluss_catalog.fluss_demo.orders
WHERE status = 'PAID'
GROUP BY customer_id;
```

Further changes in MySQL update the streaming result after they reach Fluss.
The query keeps running; quit the result view with `q` before running the next SQL statements.

## Verify adding a column

In MySQL, append a nullable column and update an order:

```sql
ALTER TABLE fluss_demo.orders ADD COLUMN payment_method VARCHAR(16);
UPDATE fluss_demo.orders SET payment_method = 'CARD' WHERE order_id = 1007;
```

In the SQL Client, return to batch mode and query the new column:

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

DESCRIBE fluss_catalog.fluss_demo.orders;
SELECT order_id, payment_method
FROM fluss_catalog.fluss_demo.orders
ORDER BY order_id;
```

After the schema and data changes arrive, `payment_method` should be `CARD` for order 1007 and `NULL` for the other orders.

The pipeline uses `schema.change.behavior: lenient` to accommodate schema changes supported by the Fluss sink.
Added columns are appended as nullable columns. Dropping a column preserves it in Fluss, with subsequent writes setting it to `NULL`.
Renaming a column preserves the old column and adds a nullable column with the new name; historical values are not backfilled into the new column.
Changing a column's data type is not supported by this connector.
For supported options and schema evolution details, see the [Fluss Pipeline connector](../connectors/pipeline-connectors/fluss.md).

## Clean up

Exit the SQL Client after stopping any running query.
Replace `<job-id>` with the ingestion Job ID, then cancel the job and stop the tutorial services.
The last command also removes the tutorial volume:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down -v
```
