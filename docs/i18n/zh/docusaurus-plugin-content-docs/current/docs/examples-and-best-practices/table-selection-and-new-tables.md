---
title: "表筛选与新增表同步"
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

# 表筛选与新增表同步

MySQL 数据摄入作业可以通过正则表达式选择源表，并排除临时表。
运行期间新建空表，与将已有历史数据的表纳入同步，需要使用不同的 Source 选项：

| 场景 | MySQL Source 选项 | 生效方式 | 同步的数据 |
|------|------------------|----------|------------|
| 作业已进入 Binlog 阶段，此时新建空表 | `scan.binlog.newly-added-table.enabled: true` | 运行中的作业捕获建表及后续变更，无需重启。 | Binlog 中的变更，不为新表补读历史快照。 |
| 将已有历史数据的表加入同步范围 | `scan.newly-added-table.enabled: true` | 修改表筛选配置后，从 Savepoint 恢复作业。 | 新纳入表的全量快照及后续变更。 |

这两个选项不能同时开启。
本示例先不停机捕获新建表，再从同一作业的状态恢复，纳入一张历史表；临时表始终排除在同步范围之外。

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
如果文件中已有 `execution`、`jobmanager` 和 `taskmanager` 节点，请将配置合并到对应节点中：

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

Checkpoint 用于提交 Paimon 数据，使其对查询可见。
配置四个 Task Slot，以便摄入作业和验证查询同时运行。
上述内存配置为本流程中的多次验证查询和 Savepoint 恢复预留空间。
启动集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备源表

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

启动 MySQL 并进入客户端：

```shell
docker compose up -d --wait
docker compose exec mysql mysql -uroot -ptutorial
```

创建业务表、临时表和历史表：

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

保留此客户端，供后续执行变更。Shell 命令请在另一个终端中执行。

## 选择源表并排除临时数据

将以下配置保存为 `tables.yaml`：

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

`tables` 表达式选择 `inventory` 库中的表。
`tables.exclude` 按名称排除两张临时表，以及已有的 `orders_history` 表。
当一张表同时匹配包含和排除表达式时，以排除规则为准。

本示例的仓库和 Savepoint 使用本地文件系统路径。
部署到分布式集群时，请使用所有 Flink 进程都能访问的共享存储。

提交作业并记下 Job ID：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" tables.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

等待全量快照和一次 Checkpoint 完成，然后启动 Flink SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

创建 Catalog，并检查实际同步的表：

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

表列表中应仅有 `customers`，其中包含 `(101, Alice)`。
目标 Catalog 中不应出现 `customers_tmp` 和 `orders_history`。

## 不停机捕获新建表

先在 MySQL 中更新已有的客户记录：

```sql
UPDATE inventory.customers SET name = 'Alicia' WHERE id = 101;
```

在 SQL Client 中重新执行以下查询，直到客户名变为 `Alicia`：

```sql
SELECT * FROM inventory.customers ORDER BY id;
```

这一步用于确认作业已开始消费增量变更，再继续执行新建表操作。

在 MySQL 中新建业务表和另一张临时表，并写入数据：

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

等待变更同步及后续 Checkpoint 完成，再查询 Paimon：

```sql
SHOW TABLES FROM inventory;
SELECT * FROM inventory.products ORDER BY id;
```

表列表中应包含 `customers` 和 `products`，商品查询应返回 `(301, Book, 30.00)`。
作业的 Job ID 保持不变，两张临时表仍被排除。

新表必须匹配 `tables`，且不能匹配 `tables.exclude`。
此模式从 Binlog 读取新表的变更；如果需要补读已有历史数据，应使用下文的恢复流程。

## 纳入已有历史数据的表

### 保存状态并停止作业

将 `<job-id>` 替换为当前摄入作业的 Job ID：

```shell
"$FLINK_HOME/bin/flink" stop \
  --savepointPath file:///tmp/flink-cdc-table-selection/savepoints \
  <job-id>
```

等待命令完成，记录 Flink 输出的完整 Savepoint 路径。
恢复前请保留该 Savepoint、Paimon 仓库，以及恢复所需的 MySQL Binlog。

在摄入作业停止期间，执行以下 MySQL 变更：

```sql
UPDATE inventory.customers SET name = 'Alice Smith' WHERE id = 101;
INSERT INTO inventory.orders_history VALUES (203, 300.00);
```

客户更新用于验证原有表的状态恢复。
新增订单会与其他历史订单一起，在该表首次纳入同步时被读取。

### 修改 Source 配置并恢复

将 `tables.yaml` 的内容替换为：

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

此时历史表不再被排除；仅消费新表 Binlog 的选项被关闭，补读新纳入表快照的选项被开启。

保持 Sink、仓库路径、作业拓扑和并行度不变。
本流程中，`scan.incremental.snapshot.metadata.release.enabled` 应保持默认值 `false`；
如果原作业已释放这部分元数据，其 Savepoint 便不能用于开启新增表扫描。

将下面的占位路径替换为停止作业命令实际返回的完整 Savepoint 路径：

```shell
export SAVEPOINT_PATH=/path/to/savepoint

"$FLINK_CDC_HOME/bin/flink-cdc.sh" tables.yaml \
  --from-savepoint "$SAVEPOINT_PATH" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

记下恢复后作业的新 Job ID。等待历史表快照和一次成功的 Checkpoint 完成，再查询 Paimon：

```sql
SHOW TABLES FROM inventory;
SELECT * FROM inventory.customers ORDER BY id;
SELECT * FROM inventory.orders_history ORDER BY id;
```

目标 Catalog 中应包含 `customers`、`products` 和 `orders_history`。
客户记录应变为 `(101, Alice Smith)`，说明停机期间原有表的变更已恢复读取。

历史表应包含以下三条记录：

| id | total_price |
|----|-------------|
| 201 | 100.00 |
| 202 | 200.00 |
| 203 | 300.00 |

临时表仍不应出现在目标 Catalog 中。

## 验证新增表的后续变更

在 MySQL 中执行：

```sql
UPDATE inventory.orders_history SET total_price = 120.00 WHERE id = 201;
DELETE FROM inventory.orders_history WHERE id = 202;
INSERT INTO inventory.orders_history VALUES (204, 400.00);
```

等待后续 Checkpoint 完成，再次查询历史表：

```sql
SELECT * FROM inventory.orders_history ORDER BY id;
```

结果应为：

| id | total_price |
|----|-------------|
| 201 | 120.00 |
| 203 | 300.00 |
| 204 | 400.00 |

新纳入的历史表此后与原有表一起进行正常的增量同步。
Source 选项的详细说明请参见 [MySQL Pipeline 连接器](../connectors/pipeline-connectors/mysql.md)。

## 清理环境

使用恢复后的 Job ID 取消作业，再停止本示例的服务：

```shell
"$FLINK_HOME/bin/flink" cancel <restored-job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
