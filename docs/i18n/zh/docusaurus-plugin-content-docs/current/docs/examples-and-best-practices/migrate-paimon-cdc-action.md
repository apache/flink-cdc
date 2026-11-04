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
迁移前应核对原作业 Action JAR 实际支持的参数。Action 的背景说明请参见
[Paimon MySQL CDC 文档](https://paimon.apache.org/docs/master/cdc-ingestion/mysql-cdc/)。

:::warning

本文迁移的是配置，不是作业状态。不能使用 Paimon Action 的 Checkpoint 或 Savepoint 恢复 Flink CDC Pipeline。
示例 YAML 从全量快照开始读取，并写入独立的 `lake_migration` 数据库。
在确认读取起点、核对已有表之前，不要把它直接改为原目标库。

:::

## 准备环境

根据下表选择 Flink 发行包及其对应的依赖。
下载链接使用已发布的 Flink CDC 3.7.0；使用 SNAPSHOT 版本时，应从同一源码版本构建发行包和 Pipeline 连接器，
请参见[安装 Flink CDC](../deployment/standalone.md#set-up-flink-cdc)。

| Flink 发行包 | 所需依赖 | 可选的 SQL 验证连接器 |
|--------------|----------|----------------------|
| [Flink 1.20.5](https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz) | [Flink CDC 3.7.0](https://archive.apache.org/dist/flink/flink-cdc-3.7.0/flink-cdc-3.7.0-1.20-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/3.7.0-1.20/flink-cdc-pipeline-connector-mysql-3.7.0-1.20.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/3.7.0-1.20/flink-cdc-pipeline-connector-paimon-3.7.0-1.20.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [面向 Flink 1.20 的 Paimon SQL 2.0.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/2.0.0/paimon-flink-1.20-2.0.0.jar) |
| [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/flink-2.2.1-bin-scala_2.12.tgz) | [Flink CDC 3.7.0](https://archive.apache.org/dist/flink/flink-cdc-3.7.0/flink-cdc-3.7.0-2.2-bin.tar.gz)<br />[MySQL Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/3.7.0-2.2/flink-cdc-pipeline-connector-mysql-3.7.0-2.2.jar)<br />[Paimon Pipeline 连接器](https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/3.7.0-2.2/flink-cdc-pipeline-connector-paimon-3.7.0-2.2.jar)<br />[MySQL JDBC 8.0.27](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)<br />[Hadoop 2.8.3-10.0](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar) | [面向 Flink 2.1 的 Paimon SQL 2.0.0](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.1/2.0.0/paimon-flink-2.1-2.0.0.jar) |

Flink CDC 3.7.0 内置 Paimon 2.0.0，其 Flink 2.2 构建使用 Paimon 的 Flink 2.1 依赖，
所以 SQL 验证连接器也采用该依赖。使用其他 CDC 版本时，SQL 连接器应与其内置的 Paimon 版本一致。

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
保留原作业的 Action JAR 用于核对配置，新 Pipeline 不使用它。

MySQL 应开启 Binlog，设置 `binlog_format=ROW` 和 `binlog_row_image=FULL`，并按
[MySQL 连接器文档](../connectors/pipeline-connectors/mysql.md)为同步账号授权。
迁移时保留原作业实际同步的表集合：数据库 Action 只同步有主键的表，
而扩大 Pipeline 的表匹配范围不会自动排除无主键表。

记录原 Catalog 类型、Warehouse、目标表名、主键、分区和表属性。
提交客户端和 Flink 进程都必须能够访问 Warehouse。
分布式集群应使用共享存储，而不是某台机器的本地目录。

在 Flink 集群中开启周期性 Checkpoint，使 Paimon 能提交流式写入。
例如，将以下配置合并到 `config.yaml` 已有的 `execution` 节点中：

```yaml
execution:
  checkpointing:
    interval: 30s
```

以下 YAML 中的地址、账号、密码占位符、时区、Warehouse 和表匹配规则都需要替换。
通过部署环境的安全配置流程将 `'<mysql-password>'` 替换为实际密码，不要把凭证提交到代码仓库。
该占位符不是密钥引用，也不是环境变量表达式。
为新作业分配不与其他 MySQL 同步作业冲突的 `server-id` 范围，ID 数量至少满足 Source 并行度。

## 迁移 `mysql_sync_table`

假设原作业把 `shop.orders` 写入 `lake.orders`，源表有主键，原作业未添加计算列或分区。

### 原 Action 配置

下面的命令仅用于核对配置，迁移时不要再次启动一个 Action。
`<action-jar>` 表示原作业使用的 JAR，Shell 环境变量表示原连接信息。

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

保存为 `mysql-orders-to-paimon.yaml`。此示例有意写入 `lake_migration.orders`，而不是 `lake.orders`。

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

使用 Flink CDC CLI 提交，并按部署环境需要添加文件系统依赖：

```shell
"$FLINK_CDC_HOME/bin/flink-cdc.sh" mysql-orders-to-paimon.yaml \
  --jar "$FLINK_HOME/opt/flink-shaded-hadoop.jar"
```

主要配置变化如下：

- 将 Action 的 `database-name` 和 `table-name` 合并为 `source.tables`。
- 通过 `route.sink-table` 指定目标库表。
- 使用 `catalog.properties.*` 直接配置 Catalog，通过 `table.properties.*` 配置表属性。
- 使用 `commit.user-prefix` 配置提交用户名的前缀；它不会继承原作业的处理进度。

对于新目标，Flink CDC 根据源表 Schema 和主键创建目标库表。
`initial` 先读取全量快照，再持续读取 Binlog 变更。
等待 Checkpoint 成功、Paimon 提交完成后，分别验证插入、更新和删除的目标结果。

需要合表时，同时扩大 `tables` 和 `source-table` 的匹配范围，并保持 `sink-table` 为固定表名。
合表前必须确认表结构兼容、输出主键在所有源表之间全局唯一，否则不同表的数据可能相互覆盖。

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

使用 `source.tables` 选择源表，通过 `source.tables.exclude` 排除源表。
两者都匹配包含库名的源表标识。例如，选择 `shop` 下除 `tmp_` 开头以外的表时，替换包含规则并增加排除规则：

```yaml
tables: 'shop.\.*'
tables.exclude: 'shop.tmp_\.*'
```

这是 `source` 内的字段片段，不是完整 Pipeline。采用此规则时，还要把路由中的
`source-table` 扩大为 `'shop.\.*'`，并核对所有选中的表都具有所需主键。
表达式中，未转义的点分隔库名和表名；正则通配的点需要写成 `\.`。

路由不负责过滤：未匹配路由的源表会保留原始标识。
默认所有匹配的路由都会生效，应避免规则重叠导致同一张表意外写入多个目标。
路由模式和命名规则请参见[路由](../core-concept/route.md)。

分库合并时，只有在表结构兼容、主键全局唯一的前提下，才能将不同库的同名表路由到同一个目标。
需要在目标名中保留源库信息时，替换路由列表，并相应调整 `source.tables`：

```yaml
route:
  - source-table: 'shop_01.orders'
    sink-table: lake_migration.shop_01_orders
  - source-table: 'shop_02.orders'
    sink-table: lake_migration.shop_02_orders
```

这样可以保留原 Action 使用 `--merge_shards false` 时区分源库的效果。
每个库有多张表时，可以按源库分别设置路由，使用 `lake_migration.shop_01_<>` 这样的目标名，
并设置 `replace-symbol: <>`。

## 迁移其他配置

基础示例未包含 Action 的可选功能。原作业使用这些功能时，必须处理后再验证迁移结果。
不能把所有 `--mysql_conf` 属性原样复制到 Source：Pipeline 连接器与 Action 使用的 SQL 连接器并不共享全部配置。

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

Action 的 `--computed_column` 表达式应改写为 [Transform](../core-concept/transform.md) 投影，
不能只替换函数名称而不核对语义。
例如，`create_time` 是时间戳字段，且更新记录时分区日期保持不变，可以在单表 Pipeline 中增加以下顶层配置：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: "*, DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt"
    primary-keys: id,dt
    partition-keys: dt
```

此例假设源主键为 `id`，对应目标主键 `id,dt`、分区键 `dt`。
主键和分区字段必须存在于投影后的 Schema 中。
计算表达式还要核对时间精度、时区、函数参数的下标规则和 NULL 处理。
分区主键表应将分区字段纳入目标主键，并核对真实的业务主键；依赖跨分区更新的作业不能直接套用此例。

未覆盖主键时，Flink CDC 保留源端主键。
原 Action 有意不继承源主键的目标不属于本文的迁移范围：
空的 `primary-keys` 值不会移除继承的主键，不能静默地把原写入语义改为主键更新。

整库同步时，只对包含相应字段的源表设置分区规则。
不要把 Action 对缺少分区字段的表不设置分区的行为，改成一条对这些表无效的统一投影。
也可以使用 Paimon Sink 的 `partition.key`，按路由后的目标表标识设置分区；每张表选择一种分区配置方式。
修改 YAML 不会自动迁移已有表的主键、分区或 Bucket 布局。

### 分表属性

原作业使用 `--table_conf_by_table orders:bucket=8` 时，可增加以下顶层配置：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*'
    table-options: 'bucket=8'
```

该配置用于建表，会覆盖同名的通用 `table.properties.*` 属性。
`source-table` 使用路由前的源表标识。
已有 Transform 时，应把属性合并到同一条规则，不要新增重复匹配规则。
修改这些建表属性不会重新配置已有目标表。

### 新增表

Action 的 `divided`、`combined` 描述其算子组织方式，并没有对应的 YAML 模式开关。
应按实际需求选择 MySQL Source 行为：

| 需求 | Pipeline 配置 |
|------|---------------|
| 作业进入 Binlog 阶段后，捕获新建的匹配表 | 设置能匹配新表的 Source、Route 规则，并开启 `source.scan.binlog.newly-added-table.enabled: true`。此方式读取 Binlog，不为新纳入的历史表补全量快照。 |
| 给已经运行的 Flink CDC Pipeline 增加带历史数据的已有表 | 扩大表范围，开启 `source.scan.newly-added-table.enabled: true`，从该 Pipeline 自己的 Checkpoint 或 Savepoint 恢复。 |

不要同时开启这两个选项。补全量快照的选项不能与 `scan.incremental.snapshot.metadata.release.enabled: true` 同时使用。
恢复流程请参见[表筛选与新增表同步](table-selection-and-new-tables.md)。
该流程不意味着 Action 的 Savepoint 可以用于恢复 Flink CDC Pipeline。

### 源端元数据

需要保留源库名和源表名时，在 Transform 投影中添加元数据字段：

```yaml
transform:
  - source-table: 'shop.orders'
    projection: '*, __schema_name__ AS database_name, __table_name__ AS table_name'
```

这些值来自源端，而不是路由后的目标名称。已有投影时合并到原规则，并避免与业务字段重名。
使用 `op_ts` 时，在 `source` 中启用 `metadata.list: op_ts`，并在 Transform 中显式投影 `op_ts`。
Pipeline 连接器输出的是毫秒值 `BIGINT`，而 Action 使用的 SQL 元数据为 `TIMESTAMP_LTZ(3)`；
写入已有时间列之前，必须核对类型转换和时区。

### 类型映射与表结构变更

YAML 没有通用的 `type_mapping` 开关，应对照原目标表结构、
[MySQL 类型映射](../connectors/pipeline-connectors/mysql.md)和
[Paimon 类型映射](../connectors/pipeline-connectors/paimon.md)逐项处理。

| 原类型映射 | 迁移时的处理 |
|------------|--------------|
| `tinyint1-not-bool` | 设置 `source.treat-tinyint1-as-boolean.enabled: false`。 |
| `to-string`、`char-to-string` | 在 Transform 投影中对相关字段显式 CAST 为 `STRING`，并核对支持的转换路径。 |
| `bigint-unsigned-to-bigint` | MySQL `BIGINT UNSIGNED` 默认映射为 `DECIMAL(20,0)`；确需转为 `BIGINT` 时，应先确认所有值都在有符号范围内。 |
| `longtext-to-bytes`、`to-nullable` | 没有直接对应的 Source 开关；需要评估输出类型和可空性，不能静默删除原映射。 |

`pipeline.schema.change.behavior` 默认为 `lenient`，会保留被删除列，将重命名处理为保留旧列并新增一列。
这不代表 Action 接受的所有 DDL 和类型变更都能迁移。
请查阅[表结构变更](../core-concept/schema-evolution.md)及其失败处理方式，验证业务实际使用的 DDL。
修改表结构变更策略不能代替对不兼容表的筛选。

## 验证与切换

### 优先使用独立目标

1. 在独立迁移库启动新 Pipeline，保留旧目标不变。
2. 等待全量快照完成、Binlog 追平，并确认相应数据已经提交到 Paimon。
   作业处于运行状态或只有一次成功的 Checkpoint，都不能证明已经追平。
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
