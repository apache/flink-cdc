---
title: "PostgreSQL 数据实时入湖 Iceberg"
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

# PostgreSQL 数据实时入湖 Iceberg

本示例使用 Flink CDC YAML 数据摄入作业，将 PostgreSQL 订单写入 Apache Iceberg 表，
验证全量快照、插入、更新、删除，以及新增字段的同步。

PostgreSQL 通过逻辑复制提供变更数据。
源表包含主键，并设置 `REPLICA IDENTITY FULL`，使更新和删除事件携带完整的旧记录。

## 准备 Flink 环境

准备一台安装了 Docker Compose 和 Java 11 的 Linux 或 macOS 计算机。
根据下表选择 Flink 发行包及其对应的依赖：

| Flink 发行包 | 所需依赖 |
|--------------|----------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[Postgres Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/@@VERSION@@-1.20/flink-cdc-pipeline-connector-postgres-@@VERSION@@-1.20.jar)<br />[Iceberg Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-iceberg/@@VERSION@@-1.20/flink-cdc-pipeline-connector-iceberg-@@VERSION@@-1.20.jar)<br />[Iceberg Flink runtime 1.10.1 (1.20)](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar)<br />[Hadoop client API 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar)<br />[Hadoop client runtime 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar)<br />[Commons Logging 1.2](https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[Postgres Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/@@VERSION@@-2.2/flink-cdc-pipeline-connector-postgres-@@VERSION@@-2.2.jar)<br />[Iceberg Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-iceberg/@@VERSION@@-2.2/flink-cdc-pipeline-connector-iceberg-@@VERSION@@-2.2.jar)<br />[Iceberg Flink runtime 1.10.1 (2.0)](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar)<br />[Hadoop client API 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar)<br />[Hadoop client runtime 3.3.6](https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar)<br />[Commons Logging 1.2](https://repo.maven.apache.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar) |

请按对应的 Flink 行选择 Iceberg Flink runtime，其版本与该 Pipeline 连接器内置的 Iceberg 一致。
下载并解压选定的发行包，将以下路径替换为实际安装目录：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将 Postgres 和 Iceberg Pipeline 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`。
Postgres 连接器已包含其 JDBC 驱动。

启动 Flink 前，将匹配的 Iceberg Flink runtime、Hadoop client API、Hadoop client runtime
和 Commons Logging JAR 放入 `$FLINK_HOME/lib`。
本例使用这组 Hadoop client 依赖；如果其他教程在 `$FLINK_HOME/lib` 中留下了 Hadoop 2 uber JAR，请先移出该目录。
SQL Client 创建 Catalog 时也需要能加载 Iceberg runtime。

在 `$FLINK_HOME/conf/config.yaml` 中添加或更新以下配置。
如果已有 `execution`、`jobmanager` 和 `taskmanager` 节点，请将配置合并到对应节点中：

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

Checkpoint 用于提交 Iceberg 写入，使查询能够读取已提交的数据。
上述内存和 Task Slot 配置供摄入作业与验证查询同时运行。
启动 Standalone 集群：

```shell
"$FLINK_HOME/bin/start-cluster.sh"
```

在 [Flink Web UI](http://localhost:8081/) 中确认已有可用的 TaskManager。

## 准备 PostgreSQL 逻辑复制

新建一个工作目录，将以下内容保存为 `compose.yaml`：

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

服务启动时开启逻辑 WAL，并预留复制槽和 WAL Sender 的容量。
启动 PostgreSQL 并进入客户端：

```shell
docker compose up -d --wait
docker compose exec postgres psql -U postgres -d inventory
```

创建源表、初始数据，以及仅包含该表的 Publication：

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

本地示例使用容器默认的数据库管理员。
其他部署环境中的摄入账号需要具备表读取和逻辑复制权限。

保留 PostgreSQL 客户端，供后续执行变更。Shell 命令请在另一个终端中执行。

## 配置并提交作业

将以下配置保存为 `postgres-to-iceberg.yaml`：

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

Source 复用前面创建的 Publication，并在作业启动时创建逻辑复制槽。
每个摄入作业应使用独立的复制槽名称。

Source 的 `tables` 配置采用 `数据库.schema.表名` 形式。
设置 `table-id.include-database: false` 后，输出的表标识采用 `schema.表名` 形式，
因此路由匹配 `public.orders`，并写入 `ods.orders`。

Iceberg 连接器会自动创建目标命名空间和表。
本例使用不分区的 format-v2 表，验证更新和删除。
所有 Flink 进程运行在同一台计算机上，因此使用本地仓库路径；分布式集群应改用共享存储。

提交数据摄入作业并记下 Job ID：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" postgres-to-iceberg.yaml
```

等待作业正常运行，并完成一次 Checkpoint。

## 验证全量快照

启动 SQL Client：

```shell
"$FLINK_HOME/bin/sql-client.sh"
```

创建指向同一仓库的 Catalog，并查询目标表：

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

结果应包含：

| id | customer_name | amount | order_status |
|----|---------------|--------|--------------|
| 101 | Alice | 29.90 | NEW |
| 102 | Bob | 5.00 | NEW |

如果表或记录尚不可用，请等待全量快照及一次成功的 Checkpoint，再执行查询。
批查询读取已提交的 Iceberg 快照；后续变更需要重新查询才能看到。

## 验证插入、更新与删除

在 PostgreSQL 客户端中更新一条订单、删除另一条，并插入新订单：

```sql
UPDATE public.orders SET amount = 39.90 WHERE id = 101;
DELETE FROM public.orders WHERE id = 102;
INSERT INTO public.orders VALUES (103, 'Carol', 12.50, 'NEW');
```

等待变更被消费且后续 Checkpoint 完成，再次查询 Iceberg：

```sql
SELECT id, customer_name, amount, order_status
FROM ods.orders
ORDER BY id;
```

结果应包含更新后的金额和新订单，且不再包含订单 102：

| id | customer_name | amount | order_status |
|----|---------------|--------|--------------|
| 101 | Alice | 39.90 | NEW |
| 103 | Carol | 12.50 | NEW |

源表主键为 Iceberg 应用行级变更提供标识。
连接器采用 at-least-once 投递语义，并基于主键进行幂等写入。

## 验证新增字段

在 PostgreSQL 中新增字段并填充值：

```sql
ALTER TABLE public.orders ADD COLUMN quantity INTEGER;
UPDATE public.orders SET quantity = 2 WHERE id = 101;
UPDATE public.orders SET quantity = 3 WHERE id = 103;
```

开启 `schema-change.enabled: true` 并使用 `pgoutput` 后，
Source 会根据关系元数据推断表结构变化。
DDL 后的更新使复制记录携带新增字段的信息。

等待后续 Checkpoint 完成，再检查目标表结构和数据：

```sql
DESCRIBE ods.orders;

SELECT id, amount, quantity
FROM ods.orders
ORDER BY id;
```

目标表应包含 `quantity INT`，并返回：

| id | amount | quantity |
|----|--------|----------|
| 101 | 39.90 | 2 |
| 103 | 12.50 | 3 |

## 检查并清理复制槽

在 PostgreSQL 中检查复制槽：

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cdc_orders';
```

作业运行期间，`active` 应为 true，且 WAL 位点已生成。
复制槽会保留恢复所需的 WAL；停止作业本身不会删除后续恢复仍需使用的复制槽。

示例完成且不再需要恢复作业时，将 `<job-id>` 替换后取消作业：

```shell
"$FLINK_HOME/bin/flink" cancel <job-id>
```

重新执行上面的复制槽查询，等待 `active` 变为 false。取消作业后，复制连接可能还需要片刻才能关闭。
随后删除已不活跃的复制槽，并停止本示例的服务：

```shell
docker compose exec postgres psql -U postgres -d inventory \
  -c "SELECT pg_drop_replication_slot('cdc_orders');"

"$FLINK_HOME/bin/stop-cluster.sh"
docker compose down
```

连接器详情请参见 [Postgres](../connectors/pipeline-connectors/postgres.md)和
[Iceberg](../connectors/pipeline-connectors/iceberg.md)。
