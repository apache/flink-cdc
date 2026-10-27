---
title: "MySQL 数据实时入湖 Paimon"
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

# MySQL 数据实时入湖 Paimon

本示例通过 Flink CDC YAML 数据摄入作业，将 MySQL 中的两张订单分表合并写入一张 Apache Paimon 表，
演示全量快照、表结构变更，以及后续插入、更新和删除的同步。文中的 SQL 查询用于验证摄入结果。

两张源表具有相同的表结构，且订单主键在两表之间不重复。
如果需要合并的分表存在重复主键，应将来源表标识纳入联合主键；
否则，相同主键的记录可能在 Paimon 中相互覆盖。

## 准备 Flink 环境

准备一台安装了 Docker Compose 和 Java 11 的 Linux 或 macOS 计算机，以及一个 Flink Standalone 集群。
根据下表选择 Flink 发行包及其对应的依赖：

| Flink 发行包 | 所需依赖 |
|--------------|----------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[面向 Flink 1.20 的 Paimon SQL 2.1.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/2.1.0/paimon-flink-1.20-2.1.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)<br />[面向 Flink 2.1 的 Paimon SQL 2.1.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.1.0/paimon-flink-2.1-2.1.0.jar) |

表中 Paimon SQL 连接器的版本与对应 Pipeline 连接器内置的 Paimon 版本一致。
下载并解压选定的发行包，将以下路径替换为实际安装目录：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将表中的 MySQL 和 Paimon Pipeline 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`。

启动 Flink 前，将表中的 MySQL JDBC 驱动放入 `$FLINK_HOME/lib`。
将 Hadoop JAR 保存为 `$FLINK_HOME/opt/flink-shaded-hadoop.jar`，
将 Paimon SQL 连接器保存为 `$FLINK_HOME/opt/paimon-sql-connector.jar`。
下文在提交摄入作业和验证查询时，通过 `--jar` 附加 Hadoop 依赖。
如果 `$FLINK_HOME/lib` 中已有该 Hadoop JAR，请在启动集群前将其移至上述 `opt` 路径。
按作业加载该依赖，可以避免它与 Flink 自身命令行所用的依赖发生冲突。

在 `$FLINK_HOME/conf/config.yaml` 中添加或更新以下配置。
如果文件中已有 `execution` 和 `taskmanager` 节点，请将配置合并到对应节点中：

```yaml
execution:
  checkpointing:
    interval: 3s
taskmanager:
  numberOfTaskSlots: 4
```

Checkpoint 用于提交 Paimon 数据，使其对查询可见。
配置四个 Task Slot，以便摄入作业和验证查询同时运行。
启动集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备 MySQL 数据表

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
```

启动 MySQL，等待健康检查通过，然后进入 MySQL 客户端：

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

在 MySQL 客户端中创建两张源表并写入测试数据：

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

保留此客户端，供后续执行表结构和数据变更。其余 Shell 命令请在另一个终端中执行。

## 配置并提交数据摄入作业

将以下配置保存为 `mysql-to-paimon.yaml`：

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

`tables` 表达式选中两张订单表，路由规则将其合并到 `ods.orders`。
Flink CDC 会自动创建目标数据库和表。

本示例的 Flink 进程运行在同一台计算机上，因此使用 `file:///` 形式的本地仓库路径。
部署到分布式集群时，应使用客户端、JobManager 和所有 TaskManager 都能访问的共享存储。

提交作业：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

记下返回的 Job ID。在 Flink Web UI 中确认作业正常运行，且至少完成一次 Checkpoint。

## 验证全量快照

启动 Flink SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

创建指向同一仓库路径的 Paimon Catalog，并查询合并后的目标表：

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

结果应包含以下四条记录：

| orderkey | custkey | total_price | order_date |
|----------|---------|-------------|------------|
| 1 | 1 | 100.00 | 2026-01-01 |
| 2 | 2 | 200.00 | 2026-01-02 |
| 11 | 1 | 300.00 | 2026-01-03 |
| 12 | 3 | 400.00 | 2026-01-04 |

如果目标表尚不可用，请等待全量快照和一次成功的 Checkpoint，再执行查询。
批查询读取已提交的快照；后续变更需要重新执行查询才能看到。

## 同步表结构变更

在 MySQL 客户端中，为两张源表增加同一字段，并更新其中两条记录：

```sql
ALTER TABLE orders.orders_1 ADD COLUMN quantity BIGINT;
ALTER TABLE orders.orders_2 ADD COLUMN quantity BIGINT;
UPDATE orders.orders_1 SET quantity = 2 WHERE orderkey = 1;
UPDATE orders.orders_2 SET quantity = 3 WHERE orderkey = 11;
```

等待后续 Checkpoint 完成，然后在 Flink SQL Client 中执行：

```sql
DESCRIBE ods.orders;

SELECT orderkey, total_price, quantity
FROM ods.orders
ORDER BY orderkey;
```

目标表结构应包含 `quantity BIGINT`，对应的数据如下：

| orderkey | total_price | quantity |
|----------|-------------|----------|
| 1 | 100.00 | 2 |
| 2 | 200.00 | NULL |
| 11 | 300.00 | 3 |
| 12 | 400.00 | NULL |

## 同步插入、更新与删除

在 MySQL 客户端中插入新订单、更新已有订单，并删除另一条订单：

```sql
INSERT INTO orders.orders_2 VALUES (13, 3, 500.00, '2026-01-05', 1);
UPDATE orders.orders_1
SET total_price = 120.00, quantity = 4
WHERE orderkey = 1;
DELETE FROM orders.orders_1 WHERE orderkey = 2;
```

等待这些变更通过 Checkpoint 提交后，再次查询 Paimon：

```sql
SELECT orderkey, custkey, total_price, quantity
FROM ods.orders
ORDER BY orderkey;
```

结果应包含新增订单、更新后的价格和数量，且不再包含订单 2：

| orderkey | custkey | total_price | quantity |
|----------|---------|-------------|----------|
| 1 | 1 | 120.00 | 4 |
| 11 | 1 | 300.00 | 3 |
| 12 | 3 | 400.00 | NULL |
| 13 | 3 | 500.00 | 1 |

## 清理环境

退出 SQL Client 和 MySQL 客户端，将 `<job-id>` 替换为摄入作业的 Job ID，
取消作业并停止本示例的服务：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

仓库文件保留在 `/tmp/flink-cdc-paimon-tutorial/warehouse`。
如果需要从头重做本示例，请使用空仓库。
