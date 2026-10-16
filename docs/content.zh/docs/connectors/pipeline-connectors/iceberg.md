---
title: "Iceberg"
weight: 9
type: docs
aliases:
- /connectors/pipeline-connectors/iceberg
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

# Iceberg Pipeline 连接器

Iceberg Pipeline 连接器作为数据管道的 *Data Sink*，支持将数据写入 [Apache Iceberg](https://iceberg.apache.org) 表。本文档介绍如何配置该连接器。

## 核心能力
* **自动建表：**
当 Iceberg 表不存在时，自动动态创建
* **Schema 同步：**
将上游数据源的 Schema 变更（例如新增列）自动同步到 Iceberg 表
* **数据同步：**
支持批处理和流式数据同步

Pipeline 创建方式
----------------

以下示例展示了如何定义一个从 MySQL 读取数据并写入 Iceberg 的数据管道：

### Hadoop Catalog 示例

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: admin
  password: pass
  tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*
  server-id: 5401-5404

sink:
  type: iceberg
  name: Iceberg Sink
  catalog.properties.type: hadoop
  catalog.properties.warehouse: /path/warehouse

pipeline:
  name: MySQL to Iceberg Pipeline
  parallelism: 2
```

### AWS Glue Catalog 示例

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: admin
  password: pass
  tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*
  server-id: 5401-5404

sink:
  type: iceberg
  name: Iceberg Sink
  catalog.properties.type: glue
  catalog.properties.warehouse: s3://my-bucket/warehouse
  catalog.properties.io-impl: org.apache.iceberg.aws.s3.S3FileIO
  catalog.properties.client.region: us-east-1
  catalog.properties.glue.skip-archive: true

pipeline:
  name: MySQL to Iceberg via Glue Pipeline
  parallelism: 2
```

***注意：***
根据所使用的 Catalog 类型，可能需要手动添加额外的 JAR 依赖，并在使用 Flink CDC CLI 提交 YAML 管道作业时通过 `--jar` 参数传入。

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Catalog 类型</th>
        <th class="text-left">依赖项</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>所有类型</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-flink-runtime-1.20">org.apache.iceberg:iceberg-flink-runtime-1.20</a></td>
        <td>Iceberg Flink 运行时依赖。当运行环境中未预装 Iceberg 时（例如独立部署的 Flink 集群），所有 Catalog 类型均需要此依赖。</td>
      </tr>
      <tr>
        <td>hadoop</td>
        <td>
          <a href="https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client-api/3.3.4">org.apache.hadoop:hadoop-client-api:3.3.4</a><br/>
          <a href="https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client-runtime/3.3.4">org.apache.hadoop:hadoop-client-runtime:3.3.4</a><br/>
          <a href="https://mvnrepository.com/artifact/commons-logging/commons-logging/1.1.3">commons-logging:commons-logging:1.1.3</a>
        </td>
        <td>提供 Hadoop 文件系统相关依赖。运行环境未提供这些依赖时，需要同时添加两个 Hadoop 客户端 JAR 和 Commons Logging。</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws">org.apache.iceberg:iceberg-aws</a></td>
        <td>提供 AWS Glue Catalog 和 S3 FileIO 实现。</td>
      </tr>
      <tr>
        <td>glue</td>
        <td><a href="https://mvnrepository.com/artifact/software.amazon.awssdk/bundle">software.amazon.awssdk:bundle</a></td>
        <td>iceberg-aws 所依赖的 AWS SDK。</td>
      </tr>
    </tbody>
</table>
</div>

Hadoop 3.3.4 是 Iceberg 连接器端到端测试已验证的版本。提交客户端、JobManager 和所有 TaskManager 应使用相同版本的 Hadoop 客户端。不要将 Hadoop 3 客户端 JAR 与 `flink-shaded-hadoop-2-uber` 混用：通过 `HadoopFileIO` 合并 Parquet 数据文件时，需要 Hadoop 2.x 未提供的文件系统 API。

Pipeline 连接器选项
----------------
<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">选项</th>
        <th class="text-left" style="width: 8%">是否必填</th>
        <th class="text-left" style="width: 7%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>type</td>
      <td>必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>指定使用的连接器类型，此处应为 <code>iceberg</code>。</td>
    </tr>
    <tr>
      <td>name</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Sink 的名称。</td>
    </tr>
    <tr>
      <td>catalog.properties.type</td>
      <td>条件必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Iceberg Catalog 的元数据存储类型，支持 <code>hadoop</code>、<code>hive</code> 和 <code>glue</code>。此选项与 <code>catalog.properties.catalog-impl</code> 必须设置其一。</td>
    </tr>
    <tr>
      <td>catalog.properties.catalog-impl</td>
      <td>条件必填</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>自定义 Catalog 实现类，例如 <code>org.apache.iceberg.aws.glue.GlueCatalog</code>。此选项与 <code>catalog.properties.type</code> 必须设置其一。</td>
    </tr>
    <tr>
      <td>catalog.properties.warehouse</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Iceberg Catalog 的仓库根路径，适用于所有 Catalog 类型。对于 <code>hadoop</code> 和 <code>hive</code> Catalog，通常为本地或分布式文件系统路径；对于 <code>glue</code> Catalog，通常为对象存储路径，例如 <code>s3://my-bucket/warehouse</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.uri</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>元数据服务 URI（例如 Hive Metastore 的 thrift URI）。</td>
    </tr>
    <tr>
      <td>catalog.properties.io-impl</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>自定义 FileIO 实现类。使用 AWS S3 时，请设置为 <code>org.apache.iceberg.aws.s3.S3FileIO</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.client.region</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Glue Catalog 客户端的 AWS 区域（例如 <code>us-east-1</code>）。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.id</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Glue Catalog ID（即 AWS 账户 ID）。默认使用调用者的 AWS 账户 ID。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-archive</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否跳过在 Glue 中归档旧版本的表元数据。</td>
    </tr>
    <tr>
      <td>catalog.properties.glue.skip-name-validation</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否跳过 Glue Catalog 的名称校验。</td>
    </tr>
    <tr>
      <td>partition.key</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>各分区表的分区键。支持为多张表设置不同的分区键，表之间以 <code>;</code> 分隔，分区键之间以 <code>,</code> 分隔。例如，可以通过 <code>testdb.table1:id1,id2;testdb.table2:name</code> 为两张表设置分区键。对于分区转换，可以使用以下语法：<code>testdb.table1:truncate[10](id);testdb.table2:hour(create_time);testdb.table3:day(create_time);testdb.table4:month(create_time);testdb.table5:year(create_time);testdb.table6:bucket[10](create_time)</code>。</td>
    </tr>
    <tr>
      <td>catalog.properties.*</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>透传 Iceberg Catalog 选项到管道，详见 <a href="https://iceberg.apache.org/docs/nightly/flink-configuration/#catalog-configuration">Iceberg Catalog 配置</a>。</td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>透传 Iceberg 表选项到管道，详见 <a href="https://iceberg.apache.org/docs/nightly/configuration/#write-properties">Iceberg 表配置</a>。</td>
    </tr>
    <tr>
      <td>hadoop.conf.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>传递 Hadoop <code>Configuration</code> 参数（用于 Iceberg 的 catalog/table 相关操作）。前缀 <code>hadoop.conf.</code> 会被剥离。例如 <code>hadoop.conf.fs.s3a.endpoint</code>。</td>
    </tr>
    </tbody>
</table>    
</div>

使用须知
--------

* 源表必须包含主键，不支持无主键的表。

* 不支持精确一次（Exactly-Once）语义。连接器采用至少一次（At-Least-Once）+ 主键幂等写入的方式保证数据一致性。

表维护
-----------------

Iceberg sink 可以在流式 CDC 作业中直接接入 Iceberg 1.10.1 的 `TableMaintenance` API，支持 `RewriteDataFiles`（binpack 数据文件合并）、`ExpireSnapshots`（快照过期）和 `DeleteOrphanFiles`（孤儿文件清理）。维护功能默认关闭，各维护任务也需要单独开启。

### 目标表

省略 `sink.maintenance.tables` 时，提交客户端会发现 source 实际同步的表（应用包含和排除规则），按 pipeline 的路由规则及匹配模式推导最终目标表，并去重。自动推导支持 MySQL、PostgreSQL、Oracle 和 SQL Server pipeline source，提交客户端需要具备源端元数据访问权限。PostgreSQL 还会遵循 `table-id.include-database` 配置。其他 source 需要显式配置 `sink.maintenance.tables`。

如果只需维护部分表，可选填 `sink.maintenance.tables`，使用路由后的最终目标标识符，以分号分隔；显式列表优先，不再执行自动推导。标识符为不带引号的 `database.table` 或 `namespace.database.table`，各部分仅包含 ASCII 字母、数字、下划线或连字符。此可选列表不支持匹配表达式。自动推导的表名直接保留原始名称，支持中文以及 `$`、`;` 等字符，不会按此配置列表重新解析。

目标表集合在提交时确定，新增维护目标需要重新提交作业。支持 **CDC 在作业启动后自动建表**；目标表出现前，其维护任务不会触发，也不会阻塞算子启动和 checkpoint。推导结果为空会拒绝提交；不同标识符解析到同一个 Iceberg 表 UUID 也会报错。

### 启用维护

请使用流式执行模式，并在 Flink 配置中开启 checkpoint（例如 `execution.checkpointing.interval: 60 s`）。CDC runtime/composer、source 连接器和 Iceberg 连接器需要使用同一构建版本；只替换 Iceberg 连接器，无法给旧版 CDC 安装增加源表推导能力。集群需要提供匹配 Flink 版本的 `flink-table-runtime`，用于孤儿清理读取元数据。

维护复用 sink 的 catalog，要求其返回 Iceberg `BaseTable` 实例，内置 catalog 满足此条件。每张目标表都会增加监控和维护算子，除了 CDC 算子所需的 slot，还需为维护任务的 slot sharing group 分配资源。不要通过多个维护作业同时维护同一张表，也不能同时开启原有的 `sink.compaction.enabled`。

以下 sink 配置在观测到 10 次提交或经过一小时后尝试合并文件，每天执行快照过期，每周检查孤儿文件：

```yaml
sink:
  type: iceberg
  catalog.properties.type: hadoop
  catalog.properties.warehouse: /path/warehouse
  sink.maintenance.enabled: true
  # 可选：只维护指定的目标表；省略时根据 source 和 route 自动推导。
  # sink.maintenance.tables: sales.orders;sales.customers
  sink.maintenance.uid-prefix: sales-maintenance
  sink.maintenance.parallelism: 2
  sink.maintenance.lock.jdbc.uri: jdbc:postgresql://lock-db:5432/iceberg
  sink.maintenance.lock.jdbc.properties.user: maintenance
  sink.maintenance.lock.jdbc.properties.password: <password>
  sink.maintenance.lock.jdbc.init-lock-tables: true
  sink.maintenance.rewrite-data-files.enabled: true
  sink.maintenance.rewrite-data-files.commit-count: 10
  sink.maintenance.rewrite-data-files.interval: 1 h
  sink.maintenance.expire-snapshots.enabled: true
  sink.maintenance.expire-snapshots.interval: 1 d
  sink.maintenance.expire-snapshots.max-age: 7 d
  sink.maintenance.expire-snapshots.retain-last: 100
  sink.maintenance.delete-orphan-files.enabled: true
  sink.maintenance.delete-orphan-files.interval: 7 d
  sink.maintenance.delete-orphan-files.min-age: 7 d
```

JDBC 数据库用于持久化维护锁，与表的 catalog 类型无关。将 JDBC 驱动放入 CDC 安装目录的 `lib` 供提交客户端加载，同时通过 CLI 的 `--jar` 参数将驱动提交给作业。首次使用时可将 `sink.maintenance.lock.jdbc.init-lock-tables` 设为 `true`，由提交客户端统一建立 Iceberg 所需的锁表，也可以提前建表。Flink worker 必须能访问数据库；由客户端初始化锁表时，客户端也需要访问数据库。建表后请使用 `false`。

### 维护配置

下列配置项均以 `sink.maintenance.` 为前缀，统一应用于所有选中的目标表。任务专属配置仅在相应任务开启时生效。

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | `false` | 开启维护拓扑，必须至少开启一项维护任务。 |
| `tables` | 无 | 可选，以分号分隔的目标表标识符；省略时在提交阶段根据 source 实际同步表及 route 推导。 |
| `uid-prefix` | `iceberg-maintenance` | 稳定的算子 UID 和锁身份前缀，不同独立 pipeline 应使用不同前缀。 |
| `parallelism` | `1` | 每张表维护任务的默认并行度；监控和调度算子保持单并行度。 |
| `slot-sharing-group` | `iceberg-maintenance` | 维护算子的 slot sharing group。 |
| `rate-limit` | `1 min` | 表轮询间隔和最小调度间隔，必须为正整数秒。 |
| `lock-check-delay` | `30 s` | 获取维护锁失败后的重试等待时间。 |
| `max-read-back` | `100` | 每次轮询最多检查的快照数，也适用于启动时。 |
| `lock.jdbc.uri` | 无 | 必填，保存维护锁的 JDBC 数据库 URL。 |
| `lock.jdbc.init-lock-tables` | `false` | 提交时创建尚不存在的 Iceberg 锁表。 |
| `lock.jdbc.properties.*` | 无 | JDBC 连接属性，例如后缀 `user` 对应 Iceberg 的 `jdbc.user`。 |
| `rewrite-data-files.enabled` | `false` | 开启数据文件重写。 |
| `rewrite-data-files.interval` | `1 h` | 数据文件重写的时间触发间隔。 |
| `rewrite-data-files.commit-count` | 无 | 按观测到的非 replace 快照提交数增加触发条件。 |
| `rewrite-data-files.data-file-count` | 无 | 按新增数据文件数增加触发条件。 |
| `rewrite-data-files.target-file-size-bytes` | `536870912` | 重写目标文件大小，单位为字节，默认 512 MiB。 |
| `rewrite-data-files.min-input-files` | `5` | 重写规划使用的输入文件数阈值。 |
| `rewrite-data-files.delete-file-threshold` | `2147483647` | 单个数据文件关联的 delete file 数量达到此阈值时可被选中重写。 |
| `rewrite-data-files.max-rewrite-bytes` | `10737418240` | 每轮重写的最大输入字节数，默认 10 GiB。文件组上限取此预算与 100 GiB 的较小值，大分区可分多轮处理；单个输入文件超过预算时，需要调大此值。 |
| `expire-snapshots.enabled` | `false` | 开启快照过期及不再被引用的文件清理。 |
| `expire-snapshots.interval` | `1 d` | 快照过期的时间触发间隔。 |
| `expire-snapshots.commit-count` | 无 | 快照过期的额外提交次数触发条件。 |
| `expire-snapshots.max-age` | `7 d` | 快照过期的时间阈值。 |
| `expire-snapshots.retain-last` | `100` | 最少保留的快照数量。 |
| `delete-orphan-files.enabled` | `false` | 开启表存储位置下的孤儿文件清理。 |
| `delete-orphan-files.interval` | `7 d` | 孤儿文件清理的时间触发间隔。 |
| `delete-orphan-files.min-age` | `7 d` | 孤儿候选文件的最小年龄，必须至少为 3 天。 |
| `delete-batch-size` | `1000` | 快照过期和孤儿清理的文件删除批大小。 |

### 触发与任务结果

每张表的每个任务独立计算触发条件，同一任务的多个条件按 **OR** 组合。监控按 Iceberg 快照提交计数，空 checkpoint 不增加计数。其他 writer 的提交也可被观测到，维护产生的 `replace` 快照会跳过。首次启动可能计入已有快照历史，超出 `max-read-back` 或已经过期的历史不会计入。请按表的提交频率配置轮询间隔和回溯上限。

已存在的表即使没有新快照提交，也可以按时间触发维护。执行时间可能受到轮询、限速、锁和前序任务耗时的影响。同一张表的维护任务串行执行。达到触发阈值只表示开始一次规划，重写规划仍可能找不到符合条件的文件。

请通过 Iceberg 的维护日志和指标监控任务结果：维护任务失败时，CDC 作业可能仍继续运行。后续触发可以再次尝试执行该任务。

### 保留策略与孤儿清理

快照过期必须保留读取任务、增量消费者和 CDC 恢复所需的历史。应同时配置年龄和最少保留数量，以覆盖所需保留窗口；Iceberg 的分支和标签也会影响快照保留。孤儿清理会永久删除旧的未引用文件，其中可能包含长时间运行的 writer 尚未提交的文件。最小年龄必须大于最长写入、停机和恢复时间，并且不同表必须使用独立的存储位置。3 天的校验下限并不能保证所有部署的保留窗口都足够安全。

表没有已提交快照或设置了 `gc.enabled=false` 时，孤儿清理会保留文件，并将该次尝试记录为失败任务。

孤儿文件枚举要求 FileIO 实现 `SupportsPrefixOperations`，文件删除要求实现 `SupportsBulkOperations`；Iceberg 的 `HadoopFileIO` 和 `S3FileIO` 均支持这两项能力。枚举使用表中已配置的 FileIO，包括 `hadoop.conf.*` 参数，但 Iceberg 的 JSON 元数据扫描任务不会保留这些自定义 Hadoop 配置。使用 `HadoopFileIO` 时，需要让每个 TaskManager 都能通过默认 Hadoop 配置获取所需参数，例如将 `core-site.xml` 放入其 classpath。`S3FileIO` 通过自身 properties 携带存储配置。即使 CDC 写入成功，也应验证孤儿清理的元数据扫描能访问存储。

### 恢复

从 checkpoint 或 savepoint 恢复时，请保持目标表集合、UID 前缀、已启用任务集合，以及锁数据库和凭据稳定；调整表列表顺序不受影响。自动推导模式会重新读取源端当前元数据，若源表或路由已发生变化，可显式配置原目标表列表。

修改已启用任务集合会改变有状态算子的 UID，常规恢复将拒绝无法匹配的旧维护状态；这类修改需要显式使用新的维护状态。底层 API 和任务行为参见 [Iceberg Flink maintenance 文档](https://iceberg.apache.org/docs/1.10.1/flink-maintenance/)。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC 类型</th>
        <th class="text-left">Iceberg 类型</th>
        <th class="text-left" style="width:60%;">说明</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>TIMESTAMP_LTZ</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
