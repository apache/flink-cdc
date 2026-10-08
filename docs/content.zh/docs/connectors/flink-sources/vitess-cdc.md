---
title: "Vitess"
weight: 10
type: docs
aliases:
- /connectors/flink-sources/vitess-cdc
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

# Vitess CDC 连接器

Vitess CDC 连接器允许从 Vitess 集群读取增量数据。该连接器目前不支持快照功能。本文描述了如何设置 Vitess CDC 连接器来对 Vitess 数据库运行 SQL 查询。
[Vitess debezium 文档](https://debezium.io/documentation/reference/connectors/vitess.html)

依赖
------------

为了设置 Vitess CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-vitess-cdc >}}

### SQL Client JAR

下载 [flink-sql-connector-vitess-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-vitess-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-vitess-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-vitess-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

设置 Vitess 服务器
----------------

你可以按照 [Docker 指南](https://vitess.io/docs/get-started/vttestserver-docker-image/) 中的本地安装方式，或者按照 [Kubernetes 指南](https://vitess.io/docs/get-started/operator/) 使用 Vitess Operator 来安装 Vitess。支持 Vitess 连接器不需要任何特殊的设置。

### 检查清单
* 确保在安装 Vitess 连接器的机器上可以访问 VTGate 主机及其 gRPC 端口（默认为 15991）

### gRPC 认证
由于 Vitess 连接器从 VTGate VStream gRPC 服务器读取变更事件，因此它不需要直接连接到 MySQL 实例。
所以，不需要特殊的数据库用户和权限。目前，Vitess 连接器仅支持以未经认证的方式访问 VTGate gRPC 服务器。

如何创建 Vitess CDC 表
----------------

Vitess CDC 表可以定义如下：

```sql
-- checkpoint every 3000 milliseconds
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a Vitess table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'vitess-cdc',
     'hostname' = 'localhost',
     'port' = '3306',
     'keyspace' = 'mydb',
     'table-name' = 'orders');

-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
```

连接器配置项
----------------


<div class="highlight">
    <table class="colwidths-auto">
        <thead>
            <tr>
                <th class="text-left">Option</th>
                <th class="text-left">Required</th>
                <th class="text-left">Default</th>
                <th class="text-left">Type</th>
                <th class="text-left">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>connector</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>指定要使用的连接器，这里应该是 <code>&lsquo;vitess-cdc&rsquo;</code>。</td>
            </tr>
            <tr>
                <td>hostname</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>Vitess 数据库服务器（VTGate）的 IP 地址或主机名。</td>
            </tr>
            <tr>
                <td>keyspace</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>要从中流式读取变更的 keyspace 的名称。</td>
            </tr>
            <tr>
                <td>username</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>Vitess 数据库服务器（VTGate）的可选用户名。如果未配置，则使用未经认证的 VTGate gRPC。</td>
            </tr>
            <tr>
                <td>password</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>Vitess 数据库服务器（VTGate）的可选密码。如果未配置，则使用未经认证的 VTGate gRPC。</td>
            </tr>
            <tr>
                <td>shard</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>要从中流式读取变更的 shard 的可选名称。如果未配置，对于未分片的 keyspace，连接器会从唯一的 shard 流式读取变更；对于已分片的 keyspace，连接器会从该 keyspace 中的所有 shard 流式读取变更。</td>
            </tr>
            <tr>
                <td>gtid</td>
                <td>optional</td>
                <td>current</td>
                <td>String</td>
                <td>可选的 GTID 位点，shard 将从该位点开始流式读取。</td>
            </tr>
            <tr>
                <td>stopOnReshard</td>
                <td>optional</td>
                <td>false</td>
                <td>Boolean</td>
                <td>控制 Vitess 标志 stop_on_reshard。</td>
            </tr>
            <tr>
                <td>tombstonesOnDelete</td>
                <td>optional</td>
                <td>true</td>
                <td>Boolean</td>
                <td>控制删除事件之后是否跟随 tombstone 事件。</td>
            </tr>
            <tr>
                <td>tombstonesOnDelete</td>
                <td>optional</td>
                <td>true</td>
                <td>Boolean</td>
                <td>控制删除事件之后是否跟随 tombstone 事件。</td>
            </tr>
            <tr>
                <td>schemaNameAdjustmentMode</td>
                <td>optional</td>
                <td>avro</td>
                <td>String</td>
                <td>指定应如何调整 schema 名称，以便与连接器所使用的消息转换器兼容。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>需要监视的 MySQL 数据库的表名。</td>
            </tr>
            <tr>
                <td>tablet.type</td>
                <td>optional</td>
                <td>RDONLY</td>
                <td>String</td>
                <td>要从中流式读取变更的 Tablet（即 MySQL）的类型：MASTER 表示从主 MySQL 实例流式读取，REPLICA 表示从 replica 从属 MySQL 实例流式读取，RDONLY 表示从只读从属 MySQL 实例流式读取。</td>
            </tr>
        </tbody>
    </table>
</div>

特性
--------

### 增量读取

Vitess 连接器将其全部时间用于从其订阅的 VTGate 的 VStream gRPC 服务中流式读取变更。客户端从 VStream 接收变更，这些变更是在底层 MySQL 服务器的 binlog 中的特定位置提交的，这些位置被称为 VGTID。

Vitess 中的 VGTID 等价于 MySQL 中的 GTID，它描述了变更事件在 VStream 中发生的位置。通常，一个 VGTID 包含多个 shard GTID，每个 shard GTID 是一个 (Keyspace, Shard, GTID) 三元组，用于描述给定 shard 的 GTID 位点。

订阅 VStream 服务时，连接器需要提供一个 VGTID 和一个 Tablet 类型（例如 MASTER、REPLICA）。VGTID 描述了 VStream 应从哪个位置开始发送变更事件；Tablet 类型描述了从每个 shard 中的哪个底层 MySQL 实例（master 或 replica）读取变更事件。

连接器第一次连接到 Vitess 集群时，会获取当前的 VGTID 并将其提供给 VStream。

Debezium Vitess 连接器充当 VStream 的 gRPC 客户端。当连接器接收到变更时，会将这些事件转换为 Debezium 的 create、update 或 delete 事件，其中包含该事件的 VGTID。Vitess 连接器以记录的形式将这些变更事件转发到运行在同一进程中的 Kafka Connect 框架。Kafka Connect 进程会按照变更事件记录生成时的相同顺序，异步地将它们写入相应的 Kafka topic。

#### 全量阶段支持 checkpoint

增量快照读取提供了在 chunk 级别执行 checkpoint 的能力。它解决了以前版本中使用旧快照读取机制时的 checkpoint 超时问题。

### Exactly-Once 处理

Vitess CDC 连接器是一个 Flink Source 连接器，它将首先读取表快照分片，然后继续读取 binlog，
无论是在快照阶段还是读取 binlog 阶段，Vitess CDC 连接器都会在处理时**准确读取数据**，即使任务出现了故障。

### DataStream Source

Vitess CDC Source 的增量读取特性目前仅在 SQL 中提供，如果你使用的是 DataStream，请使用 Vitess Source：

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.vitess.VitessSource;

public class VitessSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = VitessSource.<String>builder()
      .hostname("localhost")
      .port(15991)
      .keyspace("inventory")
      .username("flinkuser")
      .password("flinkpw")
      .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute();
  }
}
```

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">MySQL type<a href="https://dev.mysql.com/doc/refman/8.0/en/data-types.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        DOUBLE<br>
        DOUBLE PRECISION</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
         DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
         TINYINT(1)</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        VARCHAR(n)<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
