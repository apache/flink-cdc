---
title: "MySQL 数据实时同步到 Fluss"
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

# MySQL 数据实时同步到 Fluss

本示例使用 Flink CDC YAML 数据摄入作业，将 MySQL 订单表同步到 Apache Fluss 主键表。
作业读取全量快照及后续 Binlog 变更，保留源库名、表名和主键。
下游 Flink SQL 查询从 Fluss 读取同步后的数据，减少对业务数据库的直接查询。

本示例验证插入、更新、删除和新增字段，并按客户统计已支付订单金额。

## 准备 Flink 环境

准备一台安装了 Docker Compose 的 Linux 或 macOS 计算机。
Flink 1.20 使用 Java 11，Flink 2.2 使用 Java 17。
根据下表选择 Flink 发行包及其对应的依赖：

| Flink 发行包 | 所需依赖 |
|-------------|----------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Fluss Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/@@VERSION@@-1.20/flink-cdc-pipeline-connector-fluss-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Fluss Flink 1.20 连接器 1.0.0](https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-flink-1.20/1.0.0/fluss-flink-1.20-1.0.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Fluss Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/@@VERSION@@-2.2/flink-cdc-pipeline-connector-fluss-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Fluss Flink 2.2 连接器 1.0.0](https://repo.maven.apache.org/maven2/org/apache/fluss/fluss-flink-2.2/1.0.0/fluss-flink-2.2-1.0.0.jar) |

解压选定的发行包，将以下路径替换为实际安装目录：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将 MySQL 和 Fluss **Pipeline** 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`。
在启动 Flink 前，将 MySQL JDBC 驱动及对应版本的 **Fluss Flink** 连接器 JAR 放入 `$FLINK_HOME/lib`。
Fluss Flink 连接器提供后续验证结果所需的 Catalog 和 SQL 访问能力。

在 `$FLINK_HOME/conf/config.yaml` 中添加或更新以下配置。如果已有对应节点，请合并到现有节点中：

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
      size: 4096m
    task:
      off-heap:
        size: 1024m
  numberOfTaskSlots: 4
```

四个 Slot 可以让并行度为二的数据摄入作业和验证查询同时运行。
任务堆外内存用于容纳这些作业中 Fluss 客户端使用的网络和 Arrow 缓冲区。
启动 Standalone 集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备 MySQL 和 Fluss

新建一个工作目录，将以下内容保存为 `compose.yaml`：

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

Flink 进程和 SQL Client 均在本机运行，通过 `localhost:9123` 访问 Coordinator，
通过 `localhost:9124` 访问 TabletServer，因此两个对外通告的地址都必须可达。
如果客户端运行在其他计算机上，请将 `advertised.listeners` 中的 `localhost` 替换为可访问的主机名。
示例密码和单 TabletServer 配置仅用于本地演示。
请确保 Docker 数据磁盘有足够的可用空间：Fluss 1.0.0 默认会在数据磁盘使用率达到 85% 时拒绝写入。

启动服务，等待 MySQL 健康检查通过，并查看日志确认 Fluss Coordinator 和 TabletServer 已成功启动：

```shell
docker compose up -d --wait
docker compose logs coordinator-server tablet-server
docker compose exec mysql mysql -uroot -ptutorial
```

在 MySQL 客户端中创建未占用的测试数据库，并写入六条订单：

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

## 配置并提交作业

将以下配置保存为 `mysql-to-fluss.yaml`：

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

连接器会在 Fluss 中自动创建 `fluss_demo.orders`，并以 `order_id` 为主键，无需手动建库建表。
`bucket.num` 在创建表时设置四个桶，不会修改已有表的桶数量。
主键表未显式配置 `bucket.key` 时，默认使用主键中除分区键之外的字段进行分桶。
`server-id` 范围需未被其他复制客户端占用，ID 数量不少于源端并行度。

提交数据摄入作业：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-fluss.yaml
```

记下 Job ID，并确认作业正常运行、Checkpoint 正常完成。

## 验证全量快照

在另一个终端中启动 Flink SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh"
```

创建 Fluss Catalog。SQL Catalog 使用 `client.*` 参数，Pipeline Sink 则通过 `properties.client.*` 传递相同的客户端参数。

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

全量数据同步完成后，查询应返回源表的六条订单。
批查询读取有界快照，后续数据变更需要重新执行查询才能看到。
如果目标表尚未创建或全量数据尚未同步完整，请等待数据摄入追平后重试。

## 验证插入、更新与删除

在 MySQL 客户端中执行以下语句：

```sql
UPDATE fluss_demo.orders
SET amount = 110.50, status = 'PAID'
WHERE order_id = 1001;

DELETE FROM fluss_demo.orders WHERE order_id = 1002;

INSERT INTO fluss_demo.orders VALUES (1007, 102, 120.00, 'PAID');
```

在 SQL Client 中重新执行批查询。同步追平后，预期记录如下：

| order_id | customer_id | amount | status |
|----------|-------------|--------|--------|
| 1001 | 101 | 110.50 | PAID |
| 1003 | 101 | 60.25 | PAID |
| 1004 | 101 | 39.25 | PAID |
| 1005 | 102 | 19.75 | CREATED |
| 1006 | 103 | 50.50 | PAID |
| 1007 | 102 | 120.00 | PAID |

主键表保留每条订单的最新状态。订单 1001 的更新会覆盖原有字段值，订单 1002 被删除后不再出现在查询结果中。

### 统计已支付订单金额

在 Fluss 中查询当前已支付订单：

```sql
SELECT customer_id, COUNT(*) AS paid_orders, SUM(amount) AS paid_amount
FROM fluss_catalog.fluss_demo.orders
WHERE status = 'PAID'
GROUP BY customer_id
ORDER BY customer_id;
```

完成上述变更后，预期结果如下：

| customer_id | paid_orders | paid_amount |
|-------------|-------------|-------------|
| 101 | 3 | 210.00 |
| 102 | 1 | 120.00 |
| 103 | 1 | 50.50 |

如果需要持续观察后续变更，可以切换到流模式，执行不包含 `ORDER BY` 的聚合查询：

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';

SELECT customer_id, COUNT(*) AS paid_orders, SUM(amount) AS paid_amount
FROM fluss_catalog.fluss_demo.orders
WHERE status = 'PAID'
GROUP BY customer_id;
```

继续修改 MySQL 数据，变更写入 Fluss 后，流式查询结果也会更新。
查询会持续运行，执行后续 SQL 前请按 `q` 退出结果视图。

## 验证新增字段

在 MySQL 中追加一个可空字段：

```sql
ALTER TABLE fluss_demo.orders ADD COLUMN payment_method VARCHAR(16);
```

在 SQL Client 中检查新表结构，并从最新位点启动流式查询：
先执行 `DESCRIBE`，如果尚未出现 `payment_method`，请等待并重试，确认字段已同步后再执行 `SELECT`。

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';

DESCRIBE fluss_catalog.fluss_demo.orders;
SELECT order_id, payment_method
FROM fluss_catalog.fluss_demo.orders
/*+ OPTIONS('scan.startup.mode' = 'latest') */;
```

等待查询正常运行后，在 MySQL 中更新订单：

```sql
UPDATE fluss_demo.orders SET payment_method = 'CARD' WHERE order_id = 1007;
```

查询应显示 `order_id = 1007`、`payment_method = CARD` 的更新记录。
该查询仅消费启动后的变更。在 Fluss 1.0.0 中，回放旧日志批次时投影新增列可能报
`INVALID_COLUMN_PROJECTION` 错误；从最新位点开始读取可以避开该读取路径。
验证更新后，按 `q` 退出结果视图。

作业使用 `schema.change.behavior: lenient` 处理 Fluss Sink 支持的表结构变更。
新增列会以可空列的形式追加到 Fluss 表尾部。删除列时，Fluss 保留原列，后续写入会将该列置为 `NULL`。
重命名列时保留旧列，并新增一个使用新名称的可空列；历史值不会回填到新列。
该连接器不支持修改列的数据类型。
完整配置和表结构变更语义请参见 [Fluss Pipeline 连接器](../connectors/pipeline-connectors/fluss.md)。

## 清理环境

停止仍在运行的查询，然后退出 SQL Client。
将 `<job-id>` 替换为数据摄入作业的 Job ID，取消作业并停止示例服务。
最后一条命令也会删除示例使用的数据卷：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down -v
```
