---
title: "Real-time MySQL Change Distribution to Kafka"
slug: "/docs/examples-and-best-practices/mysql-to-kafka"
sidebar_position: 3
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

# Real-time MySQL Change Distribution to Kafka {#distributing-mysql-changes-through-kafka}

When several downstream applications need the same MySQL data, a single Flink CDC ingestion pipeline can publish
the initial snapshot and subsequent changes to Kafka. Each application then consumes Kafka with its own consumer group,
reducing repeated snapshot scans and binlog connections against the business database.

This example publishes two MySQL tables to separate Kafka topics, with three partitions per topic.
Kafka messages contain structured change events that downstream consumers can use to maintain their own tables.
The available replay history depends on the topic retention settings.

## Prepare the Flink environment

Prepare a Linux or macOS machine with Docker Compose and Java 11.
Choose a Flink distribution and its corresponding dependencies:

| Flink distribution | Required dependencies |
|--------------------|-----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Kafka Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/@@VERSION@@-1.20/flink-cdc-pipeline-connector-kafka-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Kafka Pipeline connector](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/@@VERSION@@-2.2/flink-cdc-pipeline-connector-kafka-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar) |

Extract the selected distributions and set the installation directories:

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

Place the MySQL and Kafka Pipeline connector JARs in `$FLINK_CDC_HOME/lib`,
and the MySQL JDBC driver in `$FLINK_HOME/lib`.

In `$FLINK_HOME/conf/config.yaml`, add or update the following settings.
Merge them into existing sections if necessary:

```yaml
execution:
  checkpointing:
    interval: 3s
taskmanager:
  numberOfTaskSlots: 2
```

Start the standalone cluster:

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

Confirm that a TaskManager is available in the [Flink Web UI](http://localhost:8081/).

## Prepare MySQL and Kafka

Create a working directory and save the following content as `compose.yaml`.
The Kafka service uses KRaft, so no ZooKeeper service is needed.

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
  kafka:
    image: apache/kafka:3.9.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092", "--list"]
      interval: 5s
      timeout: 10s
      retries: 30
```

The Flink processes on the host connect to `localhost:9092`.
Commands running inside Docker use `kafka:29092`.
Both listener addresses are advertised so that clients can resolve the broker in their own environment.

Start the services and wait for their health checks:

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

Create the source tables and insert sample data in the MySQL client:

```sql
CREATE DATABASE inventory;
USE inventory;

CREATE TABLE customers (
  id BIGINT NOT NULL,
  name VARCHAR(100),
  email VARCHAR(100),
  PRIMARY KEY (id)
);
CREATE TABLE products (
  id BIGINT NOT NULL,
  name VARCHAR(100),
  price DECIMAL(10, 2),
  PRIMARY KEY (id)
);

INSERT INTO customers VALUES
  (101, 'Alice', 'alice@example.org'),
  (102, 'Bob', 'bob@example.org');
INSERT INTO products VALUES
  (201, 'Book', 30.00),
  (202, 'Pen', 5.00);
```

Keep the MySQL client open. In another terminal, create the target topics:

```shell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic customers \
  --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic products \
  --partitions 3 --replication-factor 1
```

Creating the topics explicitly ensures that each has the intended partition count.

## Configure and submit the pipeline

Save the following configuration as `mysql-to-kafka.yaml`:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: tutorial
  tables: inventory.\.*
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: kafka
  properties.bootstrap.servers: localhost:9092
  value.format: debezium-json
  partition.strategy: hash-by-key
  sink.delivery-guarantee: at-least-once
  sink.tableId-to-topic.mapping: inventory.customers:customers;inventory.products:products

pipeline:
  name: MySQL changes to Kafka
  parallelism: 2
```

The key options are:

| Option | Purpose |
|--------|---------|
| `tables` | Selects the tables in the source database. |
| `sink.tableId-to-topic.mapping` | Sends each source table to its configured topic while preserving source table identity in the message. |
| `partition.strategy: hash-by-key` | Distributes records by primary key. |
| `value.format: debezium-json` | Encodes changes with `before`, `after`, `op`, and `source` fields. |

Topic mapping changes the destination topic without merging table schemas.
Without an explicit mapping, the default topic name for a MySQL table is `database.table`.
A [route rule](../core-concept/route.md) changes the table identity itself, which serves a different purpose.

Submit the ingestion job:

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-kafka.yaml
```

Record the Job ID and confirm that the job is running and completing checkpoints.

## Verify the snapshot

Consume the first two customer records:

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic customers --from-beginning \
  --max-messages 2 \
  --property print.partition=true \
  --property print.key=true
```

The output includes the partition, message key, and JSON value.
The `after` fields should contain customers 101 and 102, and `op` should be `c`.

Verify the product records in the same way:

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic products --from-beginning \
  --max-messages 2 \
  --property print.partition=true \
  --property print.key=true
```

The two records should contain products 201 and 202.

## Verify inserts, updates, and deletes

Run the following changes in the MySQL client:

```sql
UPDATE inventory.customers
SET email = 'alice.new@example.org'
WHERE id = 101;
INSERT INTO inventory.customers VALUES (103, 'Cathy', 'cathy@example.org');
DELETE FROM inventory.customers WHERE id = 102;
UPDATE inventory.products SET price = 35.00 WHERE id = 201;
```

Read the customer topic from the beginning:

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic customers --from-beginning \
  --max-messages 5 \
  --property print.partition=true \
  --property print.key=true
```

In a normal run without retries, these five messages consist of the two snapshot records and three subsequent changes:

| Change | Key | Expected message fields |
|--------|-----|-------------------------|
| Update customer 101 | 101 | `op: u`; `before.email: alice@example.org`; `after.email: alice.new@example.org` |
| Insert customer 103 | 103 | `op: c`; `after.name: Cathy` |
| Delete customer 102 | 102 | `op: d`; the old row in `before`, and `after: null` |

For example, the update event contains the following fields, with source metadata omitted here:

```json
{
  "before": {
    "id": 101,
    "name": "Alice",
    "email": "alice@example.org"
  },
  "after": {
    "id": 101,
    "name": "Alice",
    "email": "alice.new@example.org"
  },
  "op": "u"
}
```

Read three messages from the product topic to verify the price update:

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic products --from-beginning \
  --max-messages 3 \
  --property print.partition=true \
  --property print.key=true
```

The update for product 201 should have `op: u` and `after.price: 35`.

## Partitioning and downstream consumption

The default `all-to-zero` strategy sends all records to partition 0.
This example uses `hash-by-key` so that different keys can use different partitions.
With an unchanged partition count, changes for the same primary key are sent to the same partition.
Records from different partitions have no global ordering guarantee; downstream applications should process each partition in order.

The example explicitly uses `at-least-once` delivery. Recovery can produce duplicate events, so downstream consumers
should apply changes idempotently using the primary key and event type.
A delete is represented by a change event; consumers must interpret `op: d` to remove the corresponding business row.

Use a separate consumer group for each downstream application so that they can consume and recover independently.
Configure Kafka retention to cover the required replay period.
For other output formats and delivery guarantees, see the [Kafka Pipeline connector](../connectors/pipeline-connectors/kafka.md).

## Clean up

Replace `<job-id>` with the ingestion Job ID, cancel the job, and stop the tutorial services:

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
