---
title: "Db2"
weight: 7
type: docs
aliases:
- /connectors/flink-sources/db2-cdc
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

# Db2 CDC 连接器

Db2 CDC 连接器允许从 Db2 数据库读取快照数据和增量数据。本文描述了如何设置 Db2 CDC 连接器来对 Db2 数据库运行 SQL 查询。


## 支持的数据库

| Connector | Database                                           | Driver               |
|-----------|----------------------------------------------------|----------------------|
| Db2-cdc   | <li> [Db2](https://www.ibm.com/products/db2): 11.5 | Db2 Driver: 11.5.0.0 |

依赖
------------

为了设置 Db2 CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-db2-cdc >}}

### SQL Client JAR

下载 [flink-sql-connector-db2-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-db2-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-db2-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-db2-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

由于 Db2 Connector 采用的 IPLA 协议与 Flink CDC 项目不兼容，我们无法在 jar 包中提供 Db2 连接器。
您可能需要手动配置以下依赖：

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">依赖名称</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4">com.ibm.db2.jcc:db2jcc:db2jcc4</a></td>
        <td>用于连接到 Db2 数据库。</td>
      </tr>
    </tbody>
</table>
</div>

设置 Db2 服务器
----------------

按照 [Debezium Db2 Connector](https://debezium.io/documentation/reference/1.9/connectors/db2.html#setting-up-db2) 中的步骤进行操作。


注意事项
----------------

###  Db2 的 SQL Replication 不支持 BOOLEAN 类型

包含 BOOLEAN 类型列的表只能执行快照读取。目前 Db2 上的 SQL Replication 不支持 BOOLEAN 类型，因此 Debezium 无法对这些表执行 CDC。
请考虑使用其他类型来替代 BOOLEAN 类型。


如何创建 Db2 CDC 表
----------------

Db2 CDC 表可以定义如下：

```sql
-- checkpoint every 3 seconds                     
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a Db2 table 'products' in Flink SQL
Flink SQL> CREATE TABLE products (
     ID INT NOT NULL,
     NAME STRING,
     DESCRIPTION STRING,
     WEIGHT DECIMAL(10,3),
     PRIMARY KEY(ID) NOT ENFORCED
     ) WITH (
     'connector' = 'db2-cdc',
     'hostname' = 'localhost',
     'port' = '50000',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'table-name' = 'myschema.products');
  
-- read snapshot and redo logs from products table
Flink SQL> SELECT * FROM products;
```

连接器配置项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器, 这里应该是 <code>'db2-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> Db2 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 Db2 数据库服务器时要使用的 Db2 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 Db2 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 Db2 服务器的数据库名称。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 Db2 数据库的表名，例如："db1.table1"</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td> Db2 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Db2 CDC 消费者可选的启动模式，合法的模式为 "initial"
           和 "latest-offset"。请查阅 <a href="#启动模式">启动模式</a> 章节了解更多详细信息。</td>
    </tr> 
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区， 例如： "Asia/Shanghai".
          它控制 Db2 中的 TIMESTAMP 类型如何转换为 STRING。
          更多请参考 <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-temporal-types"> 这里</a>.
          如果没有设置，则使用ZoneId.systemDefault()来确定服务器时区。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否启用并行快照。</td>
    </tr>
    <tr>
      <td>chunk-meta.group.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>分片元数据的分组大小，如果元数据大小超过分组大小，则元数据将被划分为多个分组。</td>
    </tr>
    <tr>
      <td>chunk-key.even-distribution.factor.lower-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0.05d</td>
      <td>Double</td>
      <td>分片键分布因子的下界。分布因子用于判断表的数据分布是否均匀。
          当数据分布均匀时，表分片会使用均匀计算优化；当数据分布不均匀时，则会通过查询进行拆分。
          分布因子可通过 (MAX(id) - MIN(id) + 1) / rowCount 计算。</td>
    </tr> 
    <tr>
      <td>chunk-key.even-distribution.factor.upper-bound</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000.0d</td>
      <td>Double</td>
      <td>分片键分布因子的上界。分布因子用于判断表的数据分布是否均匀。
          当数据分布均匀时，表分片会使用均匀计算优化；当数据分布不均匀时，则会通过查询进行拆分。
          分布因子可通过 (MAX(id) - MIN(id) + 1) / rowCount 计算。</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>表快照的分片键，在读取表的快照时，被捕获的表会按分片键拆分为多个分片。
            默认情况下，分片键是主键的第一列。可以使用非主键列作为分片键，但这可能会导致查询性能下降。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 Db2 服务器捕获数据更改。
          例如：<code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-connector-properties"> Debezium 的 Db2 连接器属性</a></td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束后关闭空闲的 Reader。 <br>
          当 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 设置为 true 时，要求 flink 版本大于等于 1.14。<br>
          如果 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 的默认值已变更为 true，
          因此不需要显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.metadata.release.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在 source 进入增量阶段后，释放 source coordinator 持有的快照分片元数据（已分配的分片、已完成分片的位点以及表结构），以降低快照分片数量非常大的作业的 JobManager 内存占用。默认关闭。仅在成功完成一次 checkpoint 后才会释放；若未开启 checkpoint 或没有 checkpoint 完成，则会保留该元数据，因此该配置项在未开启 checkpoint 时不生效。开启该配置项后生成的 checkpoint 或 savepoint，无法在降级到 Flink CDC 3.6.0 及更早版本后用于恢复作业。</td>
    </tr>
     <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
        快照读取阶段是否先分配 UnboundedChunk。<br>
        这可能有助于降低 TaskManager 在对最大的 UnboundedChunk 执行快照时出现内存溢出 (OOM) 错误的风险。<br> 
      </td>
    </tr>
    </tbody>
</table>
</div>

可用的元数据
----------------

下表中的元数据可以在 DDL 中作为只读（虚拟）meta 列声明。

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的表名称。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的 schema 名称。</td>
    </tr>    
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是更改流读取记录，该值将始终为0。</td>
    </tr>
  </tbody>
</table>

特性
--------
### 启动模式

配置选项`scan.startup.mode`指定 DB2 CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 redo logs。
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 redo logs 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。

_注意：`scan.startup.mode` 选项的机制依赖于 Debezium 的 `snapshot.mode` 配置，因此请不要同时使用它们。如果在表 DDL 中同时指定 `scan.startup.mode` 和 `debezium.snapshot.mode` 选项，可能会导致 `scan.startup.mode` 失效。_

### DataStream Source

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class Db2SourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> db2Source =
            Db2Source.<String>builder()
                    .hostname("yourHostname")
                    .port(50000)
                    .database("yourDatabaseName") // set captured database
                    .tableList("yourSchemaName.yourTableName") // set captured table
                    .username("yourUsername")
                    .password("yourPassword")
                    .deserializer(
                            new JsonDebeziumDeserializationSchema()) // converts SourceRecord to
                    // JSON String
                    .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    env.addSource(db2Source)
            .print()
            .setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print Db2 Snapshot + Change Stream");
  }
}
```

DB2 CDC 增量连接器（自 3.1.0 版本起）可以按如下方式使用：
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder.Db2IncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class Db2ParallelSourceExample {

  public static void main(String[] args) throws Exception {

    Db2IncrementalSource<String> sqlServerSource =
            new Db2SourceBuilder()
                    .hostname("localhost")
                    .port(50000)
                    .databaseList("TESTDB")
                    .tableList("DB2INST1.CUSTOMERS")
                    .username("flink")
                    .password("flinkpw")
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .startupOptions(StartupOptions.initial())
                    .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // enable checkpoint
    env.enableCheckpointing(3000);
    // set the source parallelism to 2
    env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "Db2IncrementalSource")
            .setParallelism(2)
            .print()
            .setParallelism(1);

    env.execute("Print DB2 Snapshot + Change Stream");
  }
}
```

### 可用的指标

指标系统能够帮助了解分片分发的进展， 下面列举出了支持的 Flink 指标 [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description    |
|------------------------|----------------------------|-------|----------------|
| namespace.schema.table | isSnapshotting             | Gauge | 表是否在快照读取阶段     |     
| namespace.schema.table | isStreamReading            | Gauge | 表是否在增量读取阶段     |
| namespace.schema.table | numTablesSnapshotted       | Gauge | 已经被快照读取完成的表的数量 |
| namespace.schema.table | numTablesRemaining         | Gauge | 还没有被快照读取的表的数据  |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | 正在处理的分片的数量     |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | 还没有被处理的分片的数量   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | 已经处理完成的分片的数据   |
| namespace.schema.table | snapshotStartTime          | Gauge | 快照读取阶段开始的时间    |
| namespace.schema.table | snapshotEndTime            | Gauge | 快照读取阶段结束的时间    |

注意:
1. Group 名称是 `namespace.schema.table`，这里的 `namespace` 是实际的数据库名称， `schema` 是实际的 schema 名称， `table` 是实际的表名称。
2. 对于 DB2，Group 的名称会类似于 `test_database.test_schema.test_table`。

### 关于无主键表

从3.4.0 版本开始支持无主键表，使用无主键表必须设置 `scan.incremental.snapshot.chunk.key-column`，且只能选择非空类型的一个字段。

在使用无主键表时，需要注意以下两种情况。

1. 配置 `scan.incremental.snapshot.chunk.key-column` 时，如果表中存在索引，请尽量使用索引中的列来加快 select 速度。
2. 无主键表的处理语义由 `scan.incremental.snapshot.chunk.key-column` 指定的列的行为决定：
* 如果指定的列不存在更新操作，此时可以保证 Exactly once 语义。
* 如果指定的列存在更新操作，此时只能保证 At least once 语义。但可以结合下游，通过指定下游主键，结合幂等性操作来保证数据的正确性。

#### 警告

在 DB2 表中，若使用 **非主键列** 作为有主键表的 `scan.incremental.snapshot.chunk.key-column`，可能导致**数据不一致**。以下为可能出现的问题及其缓解方案。

#### 问题场景

- **表结构：**
  - **主键：** `id`
  - **分片键列 ：** `pid`（非主键）

- **快照分片 ：**
  - **分片 0:** `1 < pid <= 3`
  - **分片 1:** `3 < pid <= 5`

- **操作 ：**
  - 两个子任务并行读取 **分片 0** 和 **分片 1**。
  - 在读取过程中，发生了一次 **更新** 操作，使 `id=0` 的 `pid` 从 `2` 变为 `4`，在两个分片的**高低水位**间都包含此次变更，导致该更新操作在增量阶段不会被处理。

- **结果 ：**
  - **分片 0:** 记录 `[id=0, pid=2]`
  - **分片 1:** 记录 `[id=0, pid=4]`

由于**处理顺序**无法保证，最终 `id=0` 的 `pid` 可能为 `2` 或 `4`，从而导致数据不一致。



数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;"><a href="https://www.ibm.com/docs/en/db2/11.5?topic=elements-data-types">Db2 type</a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        SMALLINT<br>
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INTEGER
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TIME</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHARACTER(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>VARBINARY(N)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BLOB<br>
        CLOB<br>
        DBCLOB<br>
      </td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARGRAPHIC<br>
        XML
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
