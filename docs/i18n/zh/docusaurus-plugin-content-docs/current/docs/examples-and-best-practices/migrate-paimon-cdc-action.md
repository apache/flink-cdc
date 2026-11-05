---
title: "从 Paimon MySQL CDC Action 迁移到 Flink CDC"
slug: "/docs/examples-and-best-practices/migrate-paimon-cdc-action"
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

# 从 Paimon MySQL CDC Action 迁移到 Flink CDC

本文介绍如何把 Apache Paimon MySQL CDC Action 的配置改写成 Apache Flink CDC YAML Pipeline，涵盖以下两种场景：

| 原 Action | Flink CDC Pipeline |
|-----------|--------------------|
| `mysql_sync_table` | 读取一张或多张 MySQL 表，路由到指定的 Paimon 表。 |
| `mysql_sync_database` | 筛选 MySQL 表，分别路由到 Paimon 数据库中的表，并按需添加表名前后缀。 |

示例保留源端主键，使用 filesystem Catalog。
迁移前请结合原作业 Action JAR 支持的参数，查阅
[Paimon MySQL CDC 文档](https://paimon.apache.org/docs/master/cdc-ingestion/mysql-cdc/)。

:::warning

Paimon Action 的 Checkpoint 或 Savepoint 不能用于恢复 Flink CDC Pipeline。
示例从全量快照开始，写入独立的 `lake_migration` 数据库。
复用已有目标表时，请参见[继续写入已有目标表](#继续写入已有目标表)。

:::

## 准备环境

根据下表选择配套的发行包和依赖。
使用 SNAPSHOT 版本时，从同一源码版本构建发行包和 Pipeline 连接器，
请参见[安装 Flink CDC](../deployment/standalone.md#set-up-flink-cdc)。

| Flink 发行包 | 所需依赖 | 可选的 SQL 验证连接器 |
|--------------|----------|----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-1.20/flink-cdc-pipeline-connector-mysql-@@VERSION@@-1.20.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-1.20/flink-cdc-pipeline-connector-paimon-@@VERSION@@-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [面向 Flink 1.20 的 Paimon SQL 2.0.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/2.0.0/paimon-flink-1.20-2.0.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC @@VERSION@@](https://archive.apache.org/dist/flink/flink-cdc-@@VERSION@@/flink-cdc-@@VERSION@@-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/@@VERSION@@-2.2/flink-cdc-pipeline-connector-mysql-@@VERSION@@-2.2.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/@@VERSION@@-2.2/flink-cdc-pipeline-connector-paimon-@@VERSION@@-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [面向 Flink 2.1 的 Paimon SQL 2.0.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.0.0/paimon-flink-2.1-2.0.0.jar) |

可选 SQL 连接器与内置的 Paimon 版本一致。
Flink 2.2 使用 Paimon 的 Flink 2.1 依赖。

下载并解压选定的发行包，设置 `FLINK_HOME` 和 `FLINK_CDC_HOME`：

```shell
export FLINK_HOME=/path/to/flink
export FLINK_CDC_HOME=/path/to/flink-cdc
```

将 MySQL、Paimon Pipeline 连接器 JAR 放入 `$FLINK_CDC_HOME/lib`，
启动 Flink 前将 MySQL JDBC 驱动放入 `$FLINK_HOME/lib`。
将 Hadoop JAR 保存为 `$FLINK_HOME/opt/flink-shaded-hadoop.jar`，下文提交命令通过 `--jar` 附加该依赖。
需要 SQL 验证时，将可选的 Paimon SQL 连接器保存为 `$FLINK_HOME/opt/paimon-sql-connector.jar`。
查询方式请参见 [MySQL 数据实时入湖 Paimon](mysql-to-paimon.md#验证全量快照)。

MySQL 应开启 Binlog，设置 `binlog_format=ROW` 和 `binlog_row_image=FULL`，并按
[MySQL 连接器文档](../connectors/pipeline-connectors/mysql.md)为同步账号授权。
保留原作业的表集合：数据库 Action 只同步主键表，但 Pipeline 的表匹配规则不会排除无主键表。

记录原 Catalog 类型、Warehouse、目标表名、主键、分区和表属性。
Warehouse 必须可由客户端和 Flink 进程访问；分布式集群应使用共享存储。

在 Flink 集群中开启周期性 Checkpoint，使 Paimon 能提交流式写入。
例如，将以下配置合并到 `config.yaml` 已有的 `execution` 节点中：

```yaml
execution:
  checkpointing:
    interval: 30s
```

替换以下 YAML 中的地址、账号、时区、Warehouse 和表匹配规则。
`'<mysql-password>'` 仅为占位符，不会自动读取密钥；请安全地替换为实际密码，不要提交凭证。
为新作业分配不与其他 MySQL 同步作业冲突的 `server-id` 范围，ID 数量至少满足 Source 并行度。

## 迁移 `mysql_sync_table`

假设原作业把 `shop.orders` 写入 `lake.orders`，源表有主键，原作业未添加计算列或分区。

### 原 Action 配置

以下命令仅供参考：`<action-jar>` 为原作业 JAR，Shell 环境变量为原连接信息。

```shell
flink run <action-jar> mysql_sync_table \
  --warehouse hdfs:///warehouse/paimon \
  --database lake \
  --table orders \
  --mysql_conf hostname="${MYSQL_HOST}" \
  --mysql_conf username="${MYSQL_USER}" \
  --mysql_conf password="${MYSQL_PASSWORD}" \
  --mysql_conf database-name=shop \
  --mysql_conf table-name=orders \
  --table_conf bucket=4
```

### Flink CDC Pipeline

将以下配置保存为 `mysql-orders-to-paimon.yaml`：

```yaml
source:
  type: mysql
  hostname: mysql-host
  port: 3306
  username: cdc_user
  password: '<mysql-password>'
  tables: 'shop.orders'
  server-id: 6401-6404
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: hdfs:///warehouse/paimon
  commit.user-prefix: orders_migration
  table.properties.bucket: 4

route:
  - source-table: 'shop.orders'
    sink-table: lake_migration.orders

pipeline:
  name: mysql-orders-to-paimon
  parallelism: 4
```

使用 Flink CDC CLI 和所需文件系统依赖提交：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-orders-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

主要配置变化如下：

- 将 Action 的 `database-name` 和 `table-name` 合并为 `source.tables`。
- 通过 `route.sink-table` 指定目标库表。
- 使用 `catalog.properties.*` 直接配置 Catalog，通过 `table.properties.*` 配置表属性。
- 使用 `commit.user-prefix` 配置提交用户名的前缀。

Flink CDC 根据源表 Schema 和主键创建新目标库表。
`initial` 先读取全量快照，再读取 Binlog 变更。

需要合表时，同时扩大 `tables` 和 `source-table` 的匹配范围，并保持 `sink-table` 为固定表名。
合表要求表结构兼容、输出主键全局唯一，避免覆盖其他表的数据。

## 迁移 `mysql_sync_database`

假设原作业选择 `shop.orders` 和 `shop.users`，并给目标表添加 `ods_` 前缀。

### 原 Action 配置

```shell
flink run <action-jar> mysql_sync_database \
  --warehouse hdfs:///warehouse/paimon \
  --database lake \
  --mysql_conf hostname="${MYSQL_HOST}" \
  --mysql_conf username="${MYSQL_USER}" \
  --mysql_conf password="${MYSQL_PASSWORD}" \
  --mysql_conf database-name=shop \
  --including_tables 'orders|users' \
  --table_prefix ods_ \
  --table_conf bucket=4
```

### Flink CDC Pipeline

保存为 `mysql-shop-to-paimon.yaml`。示例要求两张源表都有主键。

```yaml
source:
  type: mysql
  hostname: mysql-host
  port: 3306
  username: cdc_user
  password: '<mysql-password>'
  tables: 'shop.(orders|users)'
  server-id: 6501-6504
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: hdfs:///warehouse/paimon
  commit.user-prefix: shop_migration
  table.properties.bucket: 4

route:
  - source-table: 'shop.(orders|users)'
    sink-table: lake_migration.ods_<>
    replace-symbol: <>

pipeline:
  name: mysql-shop-to-paimon
  parallelism: 4
```

提交作业：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-shop-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

目标表分别为 `lake_migration.ods_orders` 和 `lake_migration.ods_users`。
`<>` 只替换为源表名，不包含源库名。如果还需要后缀，可以使用
`sink-table: lake_migration.ods_<>_cdc`。

| Action 配置 | Pipeline 配置 |
|-------------|---------------|
| `--mysql_conf database-name=shop` 与 `--including_tables` | 将源库和表匹配规则合并到 `source.tables`。 |
| `--excluding_tables` | 改为 `source.tables.exclude`，包含源库名。 |
| `--database`、`--table_prefix`、`--table_suffix` | 在 `route.sink-table` 中指定最终目标名称。 |

### 表筛选与路由

`source.tables` 和 `source.tables.exclude` 匹配包含库名的源表标识。
例如，选择 `shop` 下除 `tmp_` 开头以外的表：

```yaml
tables: 'shop.\.*'
tables.exclude: 'shop.tmp_\.*'
```

将这些字段放入 `source`，并将路由的 `source-table` 设为 `'shop.\.*'`。
所选表都必须具有所需主键。未转义的点分隔库名和表名；正则通配的点需写成 `\.`。

路由不负责过滤：未匹配路由的源表会保留原始标识。
默认所有匹配路由都会生效，应避免规则重叠导致重复写入。
路由模式和命名规则请参见[路由](../core-concept/route.md)。

分库合并要求表结构兼容、主键全局唯一。
若要将不同库的同名表分别写入独立目标，调整 `source.tables` 并使用以下路由：

```yaml
route:
  - source-table: 'shop_01.orders'
    sink-table: lake_migration.shop_01_orders
  - source-table: 'shop_02.orders'
    sink-table: lake_migration.shop_02_orders
```

这对应 `--merge_shards false`。
每个库有多张表时，按库设置路由，使用 `lake_migration.shop_01_<>` 这样的目标名，
并设置 `replace-symbol: <>`。

## 迁移其他配置

Pipeline 连接器与 Action 的 SQL 连接器支持的配置不同，请按下文逐项转换 `--mysql_conf` 属性。

### Catalog 和表属性

| Action 配置 | Flink CDC 配置 |
|-------------|----------------|
| `--warehouse` | `sink.catalog.properties.warehouse` |
| `--catalog_conf key=value` | `sink.catalog.properties.key: value`，前提是所用 Catalog 支持该属性 |
| `--table_conf key=value` 中的表属性 | `sink.table.properties.key: value`，前提是连接器使用的 Paimon 版本支持该属性 |
| `--primary_keys` | `transform.primary-keys`，字段必须存在于投影后的 Schema 中 |
| `--partition_keys` | `transform.partition-keys`，分区字段不同时按表分别设置 |
| `--metadata_column` | 按下文说明改写为 Source 元数据配置和 Transform 投影 |
| `--table_conf sink.parallelism=4` | 重新规划 Pipeline 并行度和 Flink 资源；`pipeline.parallelism: 4` 调整整个 Pipeline，不只是 Sink |
| `-Dpipeline.name` | `pipeline.name` |
| `--ignore_incompatible true` | 没有等价的自动跳过不兼容表开关；处理结构差异，或排除相应源表 |

不要把 `sink.parallelism` 等 Action 运行参数写入 `table.properties.*`。
使用 Hive Catalog 时，配置 `catalog.properties.metastore: hive` 和对应的 `catalog.properties.uri`，
并提供所需的 Hive、Hadoop 配置和依赖。
修改 Warehouse 或 Metastore URI 不会迁移已有表的元数据和文件。
Catalog 与 Sink 的配置项请参见 [Paimon 连接器](../connectors/pipeline-connectors/paimon.md)。

### 计算列、主键和分区

将 `--computed_column` 表达式改写为 [Transform](../core-concept/transform.md) 投影。
若 `create_time` 是时间戳，且更新时分区日期不变，可增加以下顶层配置：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: "*, DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt"
    primary-keys: id,dt
    partition-keys: dt
```

源主键为 `id` 时，此例生成目标主键 `id,dt` 和分区键 `dt`。
所有主键字段必须在投影中，分区字段必须纳入目标主键。
应对照 Action 核对业务主键、时间精度、时区、函数下标及 NULL 处理。此例不支持主键跨分区移动。

Flink CDC 默认保留源主键，空的 `primary-keys` 值不会移除它。
原 Action 不继承源主键的目标不在本文迁移范围内。

整库同步时，只对包含所需字段的表设置分区规则，其余表保持不分区。
也可用 Paimon Sink 的 `partition.key` 按路由后的目标表标识设置分区；每张表选择一种方式。
修改 YAML 不会自动迁移已有表的主键、分区或 Bucket 布局。

### 分表属性

原作业使用 `--table_conf_by_table orders:bucket=8` 时，可增加以下顶层配置：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*'
    table-options: 'bucket=8'
```

这些建表配置覆盖同名的 `table.properties.*` 属性，但不修改已有目标表。
`source-table` 使用路由前的源表标识；已有 Transform 时，将属性合并到原规则。

### 新增表

Action 的 `divided`、`combined` 没有对应的 YAML 模式，应按需求选择 MySQL Source 行为：

| 需求 | Pipeline 配置 |
|------|---------------|
| 作业进入 Binlog 阶段后，捕获新建的匹配表 | 设置能匹配新表的 Source、Route 规则，并开启 `source.scan.binlog.newly-added-table.enabled: true`。此方式读取 Binlog，不为新纳入的历史表补全量快照。 |
| 给已经运行的 Flink CDC Pipeline 增加带历史数据的已有表 | 扩大表范围，开启 `source.scan.newly-added-table.enabled: true`，从该 Pipeline 自己的 Checkpoint 或 Savepoint 恢复。 |

不要同时开启这两个选项。补全量快照的选项不能与 `scan.incremental.snapshot.metadata.release.enabled: true` 同时使用。
恢复流程请参见[表筛选与新增表同步](table-selection-and-new-tables.md)。

### 源端元数据

需要保留源库名和源表名时，在 Transform 投影中添加元数据字段：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*, __schema_name__ AS database_name, __table_name__ AS table_name'
```

这些是源端名称，不是路由后的目标名称。合并到已有投影时，避免字段重名。
使用 `op_ts` 时，在 `source` 中启用 `metadata.list: op_ts`，并在 Transform 中显式投影 `op_ts`。
Action 的 SQL 元数据类型为 `TIMESTAMP_LTZ(3)`，Pipeline 连接器输出毫秒值 `BIGINT`。
写入已有时间列前，应核对类型转换和时区。

### 类型映射与表结构变更

YAML 没有通用的 `type_mapping` 开关，应对照原目标表结构、
[MySQL 类型映射](../connectors/pipeline-connectors/mysql.md)和
[Paimon 类型映射](../connectors/pipeline-connectors/paimon.md)逐项处理。

| 原类型映射 | 迁移时的处理 |
|------------|--------------|
| `tinyint1-not-bool` | 设置 `source.treat-tinyint1-as-boolean.enabled: false`。 |
| `to-string`、`char-to-string` | 在 Transform 投影中对相关字段显式 CAST 为 `STRING`，并核对支持的转换路径。 |
| `bigint-unsigned-to-bigint` | MySQL `BIGINT UNSIGNED` 默认映射为 `DECIMAL(20,0)`；确需转为 `BIGINT` 时，应先确认所有值都在有符号范围内。 |
| `longtext-to-bytes`、`to-nullable` | 无直接对应的 Source 配置，需评估输出类型和可空性。 |

`pipeline.schema.change.behavior` 默认为 `lenient`，会保留被删除列，将重命名处理为保留旧列并新增一列。
请对照[表结构变更](../core-concept/schema-evolution.md)及其失败处理方式，验证业务 DDL。
表结构变更策略不能代替不兼容表筛选。

## 验证与切换

### 优先使用独立目标

1. 在独立迁移库启动新 Pipeline，保留旧目标不变。
2. 等待全量快照完成、Binlog 追平，并确认相应数据已经提交到 Paimon。
   一次成功的 Checkpoint 不代表已经追平。
3. 核对目标库表名、字段类型、可空性、主键、分区和表属性。
   比较记录内容、关键字段和汇总值，不只比较行数。
4. 验证插入、更新、删除和业务相关 DDL；合表时还要验证不同源表主键值重叠的情况。
5. 与下游约定切换边界，核对该边界的数据后再切换读取。
   在回退窗口结束前保留旧表、旧作业状态和所需 Binlog。

### 继续写入已有目标表

继续写入已有目标表时，先停止 Action，再启动新 Pipeline，并确认：

- 目标 Schema、字段顺序与类型、主键、分区、表属性和写入语义兼容。
- 新 Source 从 Action 已提交到 Paimon 的源端位置开始读取，且所需 Binlog 仍然保留。
  MySQL 支持 `scan.startup.mode: specific-offset` 和相应的位点字段；
  使用要求及 Schema 历史限制请参见 [MySQL 连接器](../connectors/pipeline-connectors/mysql.md)。
- 初始化时使用的源表 Schema 与起点处的事件兼容，尤其需要核对切换前后的 DDL。

`initial` 快照不会清除迁移前已从 MySQL 删除的目标记录；需要全量重建时，应使用独立目标。
