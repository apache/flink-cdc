---
title: "MySQL 变更实时分发投递到 Kafka"
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

# MySQL 变更实时分发投递到 Kafka {#通过-kafka-实时分发-mysql-变更数据}

当多个下游应用需要读取相同的 MySQL 数据时，可以使用一个 Flink CDC 数据摄入作业，
将全量快照及后续变更统一写入 Kafka，再由各应用使用独立的 Consumer Group 消费，
减少对业务数据库的重复全量扫描和 Binlog 连接。

本示例将两张 MySQL 表分别写入两个 Kafka Topic，每个 Topic 包含三个分区。
Kafka 消息保存结构化变更事件，下游可以据此维护自己的数据表；可重放的历史范围取决于 Topic 的保留策略。

## 准备 Flink 环境

准备一台安装了 Docker Compose 和 Java 11 的 Linux 或 macOS 计算机。
根据下表选择 Flink 发行包及其对应的依赖：

| Flink 发行包 | 所需依赖 |
|--------------|----------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Kafka Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/@@VERSION@@-1.20/flink-cdc-pipeline-connector-kafka-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Kafka Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/@@VERSION@@-2.2/flink-cdc-pipeline-connector-kafka-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar) |

解压选定的发行包，将以下路径替换为实际安装目录：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将 MySQL 和 Kafka Pipeline 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`，
将 MySQL JDBC 驱动放入 `$FLINK_HOME/lib`。

在 `$FLINK_HOME/conf/config.yaml` 中添加或更新以下配置。
如果已有对应节点，请将配置合并到现有节点中：

```yaml
execution:
  checkpointing:
    interval: 3s
taskmanager:
  numberOfTaskSlots: 2
```

启动 Standalone 集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备 MySQL 和 Kafka

新建一个工作目录，将以下内容保存为 `compose.yaml`。
Kafka 使用 KRaft 模式，无需额外启动 ZooKeeper。

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

本机的 Flink 进程通过 `localhost:9092` 连接 Kafka，
容器内的命令通过 `kafka:29092` 连接。
配置这两个对外通告的监听地址，可以让两种环境中的客户端分别解析并访问 Broker。

启动服务，等待健康检查通过，然后进入 MySQL 客户端：

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

在 MySQL 客户端中创建源表并写入测试数据：

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

保留 MySQL 客户端。在另一个终端中创建目标 Topic：

```shell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic customers \
  --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic products \
  --partitions 3 --replication-factor 1
```

显式创建 Topic，可以确保其分区数符合本示例的配置。

## 配置并提交作业

将以下配置保存为 `mysql-to-kafka.yaml`：

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

关键配置如下：

| 配置项 | 作用 |
|--------|------|
| `tables` | 选择源数据库中需要同步的表。 |
| `sink.tableId-to-topic.mapping` | 将源表写入指定 Topic，同时在消息中保留源表标识。 |
| `partition.strategy: hash-by-key` | 根据主键分散记录的写入分区。 |
| `value.format: debezium-json` | 将变更编码为包含 `before`、`after`、`op` 和 `source` 的消息。 |

Topic 映射只改变写入的 Topic，不会合并源表结构。
未配置映射时，MySQL 表对应的默认 Topic 名称为 `数据库名.表名`。
[路由规则](../core-concept/route.md)则会改变表标识本身，两者用途不同。

提交数据摄入作业：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-kafka.yaml
```

记下 Job ID，并确认作业正常运行、Checkpoint 正常完成。

## 验证全量快照

消费前两条客户记录：

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic customers --from-beginning \
  --max-messages 2 \
  --property print.partition=true \
  --property print.key=true
```

输出包含分区、消息 Key 和 JSON 消息体。
`after` 字段应包含客户 101 和 102 的数据，`op` 应为 `c`。

用同样的方式检查商品记录：

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic products --from-beginning \
  --max-messages 2 \
  --property print.partition=true \
  --property print.key=true
```

这两条记录应包含商品 201 和 202。

## 验证插入、更新与删除

在 MySQL 客户端中执行以下变更：

```sql
UPDATE inventory.customers
SET email = 'alice.new@example.org'
WHERE id = 101;
INSERT INTO inventory.customers VALUES (103, 'Cathy', 'cathy@example.org');
DELETE FROM inventory.customers WHERE id = 102;
UPDATE inventory.products SET price = 35.00 WHERE id = 201;
```

从头读取客户 Topic：

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic customers --from-beginning \
  --max-messages 5 \
  --property print.partition=true \
  --property print.key=true
```

在没有重试的正常执行中，这五条消息包括两条快照记录，以及以下三条增量变更：

| 变更 | Key | 预期消息字段 |
|------|-----|--------------|
| 更新客户 101 | 101 | `op: u`；`before.email: alice@example.org`；`after.email: alice.new@example.org` |
| 插入客户 103 | 103 | `op: c`；`after.name: Cathy` |
| 删除客户 102 | 102 | `op: d`；`before` 包含旧记录，`after: null` |

例如，更新事件包含以下字段，此处省略来源元数据：

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

从商品 Topic 读取三条消息，检查价格更新：

```shell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic products --from-beginning \
  --max-messages 3 \
  --property print.partition=true \
  --property print.key=true
```

商品 201 的更新事件应包含 `op: u` 和 `after.price: 35`。

## 分区与下游消费

默认的 `all-to-zero` 策略将所有记录写入 0 号分区。
本示例使用 `hash-by-key`，让不同主键的记录可以分散到不同分区。
在分区数不变的情况下，同一主键的变更会写入同一分区。
不同分区之间没有全局顺序保证，下游应按分区顺序处理消息。

本示例显式使用 `at-least-once` 投递语义。故障恢复时可能出现重复事件，
下游应根据主键和事件类型幂等应用变更。
删除也以变更事件表示，下游需要解释 `op: d`，删除对应的业务记录。

不同下游应用应使用独立的 Consumer Group，以便分别消费和恢复。
Kafka 的消息保留时间应覆盖所需的重放周期。
其他输出格式和投递语义请参见 [Kafka Pipeline 连接器](../connectors/pipeline-connectors/kafka.md)。

## 清理环境

将 `<job-id>` 替换为摄入作业的 Job ID，取消作业并停止本示例的服务：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
