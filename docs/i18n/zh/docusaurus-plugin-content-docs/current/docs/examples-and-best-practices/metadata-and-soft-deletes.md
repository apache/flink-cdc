---
title: "元数据列、计算列与软删除"
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

# 元数据列、计算列与软删除

Flink CDC 的 Transform 规则可以在数据摄入过程中补充和转换字段。
本示例读取 MySQL 订单，加入来源标识和操作元数据，计算日期分区字段，再写入 Paimon 主键表。

源端删除记录时，作业会在 Paimon 中保留该行的最后值及删除标记。
随后插入具有相同目标主键的记录，可以让该行重新成为有效数据。

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
上述内存配置为本流程中的多次验证查询预留空间。
启动集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备源数据

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

创建源表和两条订单：

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

保留此客户端，供后续执行变更。Shell 命令请在另一个终端中执行。

## 配置 Transform 规则

将以下配置保存为 `transform.yaml`：

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

`projection: >-` 后的 YAML 折叠块允许将表达式分行编写。
`*` 保留源表的全部字段，其后添加以下字段：

| 字段 | 表达式 | 含义 |
|------|--------|------|
| `source_table` | `CONCAT_WS('.', __namespace_name__, __schema_name__, __table_name__)` | 跳过为 NULL 的层级，拼接来源标识。本例得到 `inventory.orders`。 |
| `source_ts` | `op_ts` | 源端操作时间的毫秒时间戳。需要开启 `metadata.list: op_ts`；快照记录的值为 `0`。 |
| `op_type` | `__data_event_type__` | 原始变更类型；删除事件转换为插入后，仍保留原来的删除标记。 |
| `product_upper` | `UPPER(product)` | 大写形式的商品名。 |
| `order_day` | `DATE_FORMAT(created_at, 'yyyyMMdd')` | 用于目标表分区的订单日期。 |

Paimon 分区字段需要包含在主键中，因此将目标主键设置为 `id, order_day`。
本例保持 `created_at` 不变，使计算得到的分区键保持稳定。
所有 Flink 进程运行在同一台计算机上，因此使用本地仓库路径；多机部署时应改用共享存储。

`SOFT_DELETE` 在投影完成后，将删除事件转换为包含投影后旧记录的插入事件。
写入的 `op_type` 仍能标识源端删除操作。主要操作标记如下：

| 标记 | 含义 |
|------|------|
| `+I` | 快照记录或插入。 |
| `-U` | 更新前的记录。 |
| `+U` | 更新后的记录。 |
| `-D` | 删除；本例通过软删除转换器保留该行。 |

Paimon 主键表保存每个目标主键的最新状态。
读取时排除 `-D` 标记，即可得到当前有效记录。
需要逐条变更消息时，可参考 [MySQL 变更实时分发投递到 Kafka](mysql-to-kafka.md)。

提交作业并记下 Job ID：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" transform.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

等待全量快照和一次 Checkpoint 完成。

## 验证元数据列与计算列

启动 SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh" \
  --jar "$FLINK_HOME/opt/paimon-sql-connector.jar" \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

创建指向同一仓库的 Catalog，并检查目标表：

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

表定义中应包含新增字段、`id, order_day` 主键，以及 `order_day` 分区。

第一条查询应返回：

| id | product_upper | price | order_day | op_type |
|----|---------------|-------|-----------|---------|
| 101 | BOOK | 29.90 | 20260101 | +I |
| 102 | PEN | 5.00 | 20260102 | +I |

元数据查询应返回：

| id | source_table | source_ts |
|----|--------------|-----------|
| 101 | inventory.orders | 0 |
| 102 | inventory.orders | 0 |

`source_ts` 记录数据库操作时间；`created_at` 是本例用于分区的业务时间。
快照记录中的零值并不表示其业务创建时间。

## 验证更新与软删除

在 MySQL 中执行：

```sql
UPDATE inventory.orders
SET product = 'notebook', price = 39.90
WHERE id = 101;
DELETE FROM inventory.orders WHERE id = 102;
INSERT INTO inventory.orders VALUES
  (103, 'pencil', 3.50, '2026-01-03 12:00:00');
```

等待变更同步和后续 Checkpoint 完成，再分别查询全部记录和有效记录：

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

全部记录中应保留已删除的订单：

| id | product_upper | price | order_day | op_type | has_binlog_time |
|----|---------------|-------|-----------|---------|-----------------|
| 101 | NOTEBOOK | 39.90 | 20260101 | +U | TRUE |
| 102 | PEN | 5.00 | 20260102 | -D | TRUE |
| 103 | PENCIL | 3.50 | 20260103 | +I | TRUE |

有效记录查询应仅包含：

| id | product_upper | price |
|----|---------------|-------|
| 101 | NOTEBOOK | 39.90 |
| 103 | PENCIL | 3.50 |

订单 102 已从 MySQL 中物理删除，但在 Paimon 中仍以相同的目标主键保留最后值，
并标记为 `op_type = '-D'`。
非零的操作时间戳说明这些行反映了 Binlog 中的增量变更。

## 重新插入已删除的主键

再次插入订单 102，并保持其原有分区日期：

```sql
INSERT INTO inventory.orders VALUES
  (102, 'blue pen', 6.00, '2026-01-02 09:30:00');
```

等待后续 Checkpoint 完成，再查询目标表：

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

新插入应覆盖同一 `id, order_day` 主键对应的删除标记：

| id | product_upper | price | order_day | op_type | has_binlog_time |
|----|---------------|-------|-----------|---------|-----------------|
| 101 | NOTEBOOK | 39.90 | 20260101 | +U | TRUE |
| 102 | BLUE PEN | 6.00 | 20260102 | +I | TRUE |
| 103 | PENCIL | 3.50 | 20260103 | +I | TRUE |

此时，有效记录查询应包含全部三条订单。
更多表达式和元数据字段请参见 [Transform](../core-concept/transform.md)。

## 清理环境

将 `<job-id>` 替换为摄入作业的 Job ID，取消作业并停止本示例的服务：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```
