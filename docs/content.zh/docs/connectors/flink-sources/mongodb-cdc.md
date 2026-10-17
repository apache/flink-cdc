---
title: "MongoDB"
weight: 6
type: docs
aliases:
- /connectors/flink-sources/mongodb-cdc
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

# MongoDB CDC 连接器

MongoDB CDC 连接器允许从 MongoDB 读取快照数据和增量数据。 本文档描述了如何设置 MongoDB CDC 连接器以针对 MongoDB 运行 SQL 查询。

依赖
------------

为了设置 MongoDB CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-mongodb-cdc >}}

### SQL Client JAR

```下载链接仅适用于稳定版本。```

下载 [flink-sql-connector-mongodb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-mongodb-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-mongodb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-mongodb-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

设置 MongoDB
----------------

### 可用性
- MongoDB 版本

  MongoDB 版本 >= 3.6 <br>
  我们使用 [更改流](https://docs.mongodb.com/manual/changeStreams/) 功能（3.6 版中新增），以捕获更改数据。

- 集群部署

  [副本集](https://docs.mongodb.com/manual/replication/) 或者 [分片集群](https://docs.mongodb.com/manual/sharding/) 是必需的。

- 存储引擎

  [WiredTiger](https://docs.mongodb.com/manual/core/wiredtiger/#std-label-storage-wiredtiger) 存储引擎是必需的。

- [副本集协议版本](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion)

  副本集协议版本 1 [(pv1)](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion) 是必需的。 <br>
  从 4.0 版本开始，MongoDB 只支持pv1。 pv1 是使用 MongoDB 3.2 或更高版本创建的所有新副本集的默认值。

- 权限

  `changeStream` 和 `read` 权限是 MongoDB Kafka Connector 所必需的。

  你可以使用以下示例进行简单的授权。<br>
  有关更详细的授权, 请参照 [MongoDB 数据库用户角色](https://docs.mongodb.com/manual/reference/built-in-roles/#database-user-roles).

  ```javascript
  use admin;
  db.createRole(
      {
          role: "flinkrole",
          privileges: [{
              // Grant privileges on all non-system collections in all databases
              resource: { db: "", collection: "" },
              actions: [
                  "splitVector",
                  "listDatabases",
                  "listCollections",
                  "collStats",
                  "find",
                  "changeStream" ]
          }],
          roles: [
              // Read config.collections and config.chunks
              // for sharded cluster snapshot splitting.
              { role: 'read', db: 'config' }
          ]
      }
  );

  db.createUser(
    {
        user: 'flinkuser',
        pwd: 'flinkpw',
        roles: [
           { role: 'flinkrole', db: 'admin' }
        ]
    }
  );
  ```


如何创建 MongoDB CDC 表
----------------

MongoDB CDC 表可以定义如下：

```sql
-- 在 Flink SQL 中注册 MongoDB 表 `products`
CREATE TABLE products (
  _id STRING, // 必须声明
  name STRING,
  weight DECIMAL(10,3),
  tags ARRAY<STRING>, -- array
  price ROW<amount DECIMAL(10,2), currency STRING>, -- 嵌入式文档
  suppliers ARRAY<ROW<name STRING, address STRING>>, -- 嵌入式文档
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = 'localhost:27017,localhost:27018,localhost:27019',
  'username' = 'flinkuser',
  'password' = 'flinkpw',
  'database' = 'inventory',
  'collection' = 'products'
);

-- 从 `products` 集合中读取快照和更改事件
SELECT * FROM products;
```

**请注意**

MongoDB 的更改事件记录没有 UPDATE_BEFORE 消息。因此，我们只能将其转换为 Flink 的 UPSERT 更改日志流。
因为 upsert 流需要唯一键，所以我们必须声明 `_id` 作为主键。
我们不能将其他列声明为主键, 因为删除操作不包含除 `_id` 和 `sharding key` 之外的键和值。

连接器选项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，此处应为 <code>mongodb-cdc</code>.</td>
    </tr>
    <tr>
      <td>scheme</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">mongodb</td>
      <td>String</td>
      <td>指定 MongoDB 连接协议。 eg. <code>mongodb or mongodb+srv.</code></td>
    </tr>
    <tr>
      <td>hosts</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MongoDB 服务器的主机名和端口对的逗号分隔列表。<br>
          eg. <code>localhost:27017,localhost:27018</code>
      </td>
    </tr>
    <tr>
      <td>username</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 MongoDB 时要使用的数据库用户的名称。<br>
          只有当 MongoDB 配置为使用身份验证时，才需要这样做。
      </td>
    </tr>
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 MongoDB 时要使用的密码。<br>
          只有当 MongoDB 配置为使用身份验证时，才需要这样做。
      </td>
    </tr>
    <tr>
      <td>database</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视更改的数据库的名称。 如果未设置，则将捕获所有数据库。 <br>
          该数据库还支持正则表达式来监视与正则表达式匹配的多个数据库。</td>
    </tr>
    <tr>
      <td>collection</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库中要监视更改的集合的名称。 如果未设置，则将捕获所有集合。<br>
          该集合还支持正则表达式来监视与完全限定的集合标识符匹配的多个集合。</td>
    </tr>
    <tr>
      <td>connection.options</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td><a href="https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options">MongoDB连接选项</a>。 例如: <br>
          <code>replicaSet=test&connectTimeoutMS=300000</code>
      </td>
    </tr>
    <tr>
        <td>scan.startup.mode</td>
        <td>optional</td>
        <td style="word-wrap: break-word;">initial</td>
        <td>String</td>
        <td> MongoDB CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"latest-offset" 和 "timestamp"。
           请查阅 <a href="#启动模式">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
        <td>scan.startup.timestamp-millis</td>
        <td>optional</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Long</td>
        <td>起始毫秒数, 仅适用于 <code>'timestamp'</code> 启动模式.</td>
    </tr>
    <tr>
      <td>initial.snapshotting.queue.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16000</td>
      <td>Integer</td>
      <td>进行初始快照时的队列大小。仅在 scan.startup.mode 选项设置为 initial 时生效。<br>
          注意：已弃用的选项名是 copy.existing.queue.size，为了兼容旧版本的作业，该选项名仍可用，但是推荐升级到新选项名
      </td>
    </tr>
    <tr>
      <td>initial.snapshotting.max.threads</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">Processors Count</td>
      <td>Integer</td>
      <td>执行数据复制时使用的线程数。仅在 scan.startup.mode 选项设置为 initial 时生效。<br>
          注意：已弃用的选项名是 copy.existing.max.threads，为了兼容旧版本的作业，该选项名仍可用，但是推荐升级到新选项名
      </td>
    </tr>
    <tr>
      <td>initial.snapshotting.pipeline</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>MongoDB 管道操作的 JSON 对象数组，在快照读取阶段，会把该操作下推到 MongoDB，只筛选所需的数据，从而提高读取效率，
          比如管道操作 [{"$match": {"closed": "false"}}] 表示只复制 closed 字段为 "false" 的文档。<br>
          该选项仅在 scan.startup.mode 选项设置为 initial 时生效，且仅限于在 Debezium 模式下使用，不能用于增量快照模式，因为会出现语义不一致的问题。<br>
          注意：已弃用的选项名是 copy.existing.pipeline，为了兼容旧版本的作业，该选项名仍可用，但是推荐升级到新选项名
      </td>
    </tr>
    <tr>
      <td>batch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>Cursor 批次大小。</td>
    </tr>
    <tr>
      <td>poll.max.batch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>轮询新数据时，单个批处理中要包含的更改流文档的最大数量。</td>
    </tr>
    <tr>
      <td>poll.await.time.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>在更改流上检查新结果之前等待的时间。</td>
    </tr>
    <tr>
      <td>heartbeat.interval.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>心跳间隔（毫秒）。使用 0 禁用。</td>
    </tr>
    <tr>
      <td>scan.full-changelog</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否尝试基于 MongoDB 中的前镜像和后镜像生成 full-mode changelog。详情请参阅 <a href="#a-name-id-003-a">Full Changelog</a>。仅支持 MongoDB 6.0 及以上版本。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否启用增量快照。增量快照功能仅支持 MongoDB 4.0 之后的版本。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size.mb</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">64</td>
      <td>Integer</td>
      <td>增量快照的分片大小 mb。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.samples</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>增量快照期间使用 sample 分区策略时每个分片的采样数量。</td>
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
      <td>是否在 source 进入增量阶段后，释放 source coordinator 持有的快照分片元数据（已分配的分片、已完成分片的位点以及表结构），以降低快照分片数量非常大的作业的 JobManager 内存占用。默认关闭。与 scan.newly-added-table.enabled 不兼容：同时开启两者会导致作业启动失败，且已释放元数据的作业无法再开启动态加表功能。仅在成功完成一次 checkpoint 后才会释放；若未开启 checkpoint 或没有 checkpoint 完成，则会保留该元数据，因此该配置项在未开启 checkpoint 时不生效。开启该配置项后生成的 checkpoint 或 savepoint，无法在降级到 Flink CDC 3.6.0 及更早版本后用于恢复作业。</td>
    </tr>
    <tr>
      <td>scan.cursor.no-timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>MongoDB 服务器通常会在空闲一段时间（10 分钟）后使游标超时，以防止占用过多内存。将该选项设置为 true 可以避免游标超时。仅在启用并行快照时可用。</td>
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
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br> 
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 changelog 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 changelog 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 changelog 事件应进行特殊处理。
      </td>
    </tr>
    <tr>
      <td>mongodb.ssl.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>连接器是否使用 SSL 连接 MongoDB 实例。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.invalid.hostname.allowed</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>启用 SSL 时，该配置控制是否在连接阶段禁用严格的主机名校验。若设为 <code>true</code>，连接将无法防范中间人攻击。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.keystore</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>密钥库（keystore）文件的位置。该项为可选，可用于客户端与 MongoDB 服务端之间的双向认证。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.keystore.password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>密钥库文件的密码。该项为可选，仅在配置了 <code>mongodb.ssl.keystore</code> 时才需要。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.keystore.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">PKCS12</td>
      <td>String</td>
      <td>密钥库文件的类型。该项为可选，仅在配置了 <code>mongodb.ssl.keystore</code> 时才需要。默认为 PKCS12。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.truststore</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于校验服务端证书的信任库（truststore）文件的位置。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.truststore.password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>信任库文件的密码。用于校验信任库的完整性并解锁信任库。</td>
    </tr>
    <tr>
      <td>mongodb.ssl.truststore.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">PKCS12</td>
      <td>String</td>
      <td>信任库文件的类型。该项为可选，仅在配置了 <code>mongodb.ssl.truststore</code> 时才需要。默认为 PKCS12。</td>
    </tr>
    <tr>
      <td>records.per.second</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Double</td>
      <td>
        每秒发出的最大记录数，默认值为 -1，表示不进行速率限制。（仅适用于 flink 2.x）<br>
        警告：增量/二进制日志阶段：速率限制可能导致连接器落后于上游变更流，在连接器赶上之前，二进制日志/WAL 可能会被清除（MySQL 数据丢失，PostgreSQL 复制槽问题）。
      </td>
    </tr>
    </tbody>
</table>
</div>

注意: `heartbeat.interval.ms` 强烈建议设置一个大于 0 的适当值 **如果集合更改缓慢**.
当我们从 checkpoint 或 savepoint 恢复 Flink 作业时，心跳事件可以向前推送 `resumeToken`，以避免 `resumeToken` 过期。

注意: `mongodb.ssl.*` 相关配置仅在开启增量快照（`scan.incremental.snapshot.enabled` = `true`）时才会生效，非增量快照的数据源会忽略这些配置。

可用元数据
----------------

以下格式元数据可以在表定义中公开为只读（VIRTUAL）列。

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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的数据库的名称。</td>
    </tr>
    <tr>
      <td>collection_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的集合的名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是更改流读取记录，该值将始终为0。</td>
    </tr>
    <tr>
      <td>row_kind</td>
      <td>STRING NOT NULL</td>
      <td>当前记录对应的 changelog 类型。注意：当 Source 算子选择为每条记录输出 row_kind 字段后，下游 SQL 算子在处理消息撤回时会因为这个字段不同而比对失败，
建议只在简单的同步作业中引用该元数据列。<br>'+I' 表示 INSERT 数据，'-D' 表示 DELETE 数据，'-U' 表示 UPDATE_BEFORE 数据，'+U' 表示 UPDATE_AFTER 数据。</td>
    </tr>
  </tbody>
</table>

扩展的 CREATE TABLE 示例演示了用于公开这些元数据字段的语法：
```sql
CREATE TABLE products (
    db_name         STRING METADATA FROM 'database_name' VIRTUAL,
    collection_name STRING METADATA  FROM 'collection_name' VIRTUAL,
    operation_ts    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation       STRING METADATA FROM 'row_kind' VIRTUAL,
    _id             STRING, // 必须声明
    name            STRING,
    weight          DECIMAL(10,3),
    tags            ARRAY<STRING>, -- array
    price           ROW<amount DECIMAL(10,2), currency STRING>, -- 嵌入式文档
    suppliers       ARRAY<ROW<name STRING, address STRING>>, -- 嵌入式文档
    PRIMARY KEY(_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb-cdc',
    'hosts' = 'localhost:27017,localhost:27018,localhost:27019',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database' = 'inventory',
    'collection' = 'products'
);
```

特性
--------

### Exactly-Once 处理

MongoDB CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后继续读取更改流事件，即使在处理时出现故障，也能**准确读取数据**。

### 启动模式

配置选项`scan.startup.mode`指定 MongoDB CDC 消费者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 oplog。
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 oplog 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
- `timestamp`：跳过快照阶段，从指定的时间戳开始读取 oplog 事件。

例如使用 DataStream API:
```java
MongoDBSource.builder()
    .startupOptions(StartupOptions.latest()) // Start from latest offset
    .startupOptions(StartupOptions.timestamp(1667232000000L) // Start from timestamp
    .build()
```

使用 SQL:

```SQL
CREATE TABLE mongodb_source (...) WITH (
    'connector' = 'mongodb-cdc',
    'scan.startup.mode' = 'latest-offset', -- Start from latest offset
    ...
    'scan.startup.mode' = 'timestamp', -- Start from timestamp
    'scan.startup.timestamp-millis' = '1667232000000' -- Timestamp under timestamp startup mode
    ...
)
```

### 快照数据筛选器

配置选项 `initial.snapshotting.pipeline` 描述复制现有数据时的筛选器。<br>
在快照读取阶段，会把该筛选器下推到 MongoDB，只筛选所需的数据，从而提高读取效率。

在下面的示例中，`$match` 聚合运算符确保只复制 closed 字段设置为 "false" 的文档。

```
'initial.snapshotting.pipeline' = '[ { "$match": { "closed": "false" } } ]'
```

### 更改流

我们集成了 [MongoDB's official Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/)，从 MongoDB 中读取快照或更改事件，并通过 Debezium 的 `EmbeddedEngine` 进行驱动。

Debezium 的 `EmbeddedEngine` 提供了一种在应用程序进程中运行单个 Kafka Connect `SourceConnector` 的机制，并且它可以正确地驱动任何标准的 Kafka Connect `SourceConnector`，即使它不是由 Debezium 提供的。

我们选择 **MongoDB 的官方 Kafka连接器**，而不是 **Debezium 的MongoDB 连接器**，因为它们使用了不同的更改数据捕获机制。

- 对于 Debezium 的 MongoDB 连接器，它读取每个副本集主节点的 `oplog.rs` 集合。
- 对于 MongoDB 的 Kafka 连接器，它订阅了 MongoDB 的 `更改流`。

MongoDB 的 `oplog.rs` 集合不会保留更改记录的更新前状态，因此很难通过单个 `oplog.rs` 记录提取完整的文档状态，并将其转换为 Flink 接受的更改日志流（Insert Only，Upsert，All）。
此外，MongoDB 5（2021 7月发布）改变了 oplog 格式，因此当前的 Debezium 连接器不能与其一起使用。

**Change Stream**是 MongoDB 3.6 为副本集和分片集群提供的一项新功能，它允许应用程序访问实时数据更改，而不会带来跟踪 oplog 的复杂性和风险。<br>
应用程序可以使用更改流来订阅单个集合、数据库或整个部署上的所有数据更改，并立即对其做出反应。

**查找更新操作的完整文档**是**更改流**提供的一项功能，它可以配置更改流以返回更新文档的最新多数提交版本。由于该功能，我们可以轻松收集最新的完整文档，并将更改日志转换为 Flink 的**Upsert Changelog Stream**。

顺便说一句，[DBZ-435](https://issues.redhat.com/browse/DBZ-435)提到的Debezium的MongoDB更改流探索,正在制定路线图。<br>
如果完成了，我们可以考虑集成两种源连接器供用户选择。

### 动态加表

**注意:** 该功能从 Flink CDC 3.1.0 版本开始支持。

动态加表功能使你可以为正在运行的作业添加新集合进行监控。新添加的集合将首先读取其快照数据,然后自动读取其更改流。

想象一下这个场景:一开始,Flink 作业监控集合 `[product, user, address]`,但几天后,我们希望这个作业还可以监控集合 `[order, custom]`,这些集合包含历史数据,我们需要作业仍然可以复用作业的已有状态。动态加表功能可以优雅地解决此问题。

以下操作显示了如何启用此功能来解决上述场景。使用现有的 MongoDB CDC Source 作业,如下:

```java
    MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
        .hosts("yourHostname:27017")
        .databaseList("db") // set captured database
        .collectionList("db.product", "db.user", "db.address") // set captured collections
        .username("yourUsername")
        .password("yourPassword")
        .scanNewlyAddedTableEnabled(true) // enable scan the newly added collections feature
        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .build();
   // your business code
```

如果我们想添加新集合 `[order, custom]` 到现有的 Flink 作业,只需更新作业的 `collectionList()` 将新增集合 `[order, custom]` 加入并从已有的 savepoint 恢复作业。

_Step 1_: 使用 savepoint 停止现有的 Flink 作业。
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_Step 2_: 更新现有 Flink 作业的集合列表选项。
1. 更新 `collectionList()` 参数。
2. 编译更新后的作业,示例如下:
```java
    MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
        .hosts("yourHostname:27017")
        .databaseList("db")
        .collectionList("db.product", "db.user", "db.address", "db.order", "db.custom") // set captured collections [product, user, address, order, custom]
        .username("yourUsername")
        .password("yourPassword")
        .scanNewlyAddedTableEnabled(true) // enable scan newly added tables feature
        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .build();
   // your business code
```
_Step 3_: 从 savepoint 还原更新后的 Flink 作业。
```shell
$ ./bin/flink run \
      --detached \
      --from-savepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**注意:** 请参考文档 [Restore the job from previous savepoint](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/#command-line-interface) 了解更多详细信息。

### DataStream Source

MongoDB CDC 连接器也可以是一个数据流源。 你可以创建 SourceFunction，如下所示：

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;

public class MongoDBSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .username("flink")
                .password("flinkpw")
                .databaseList("inventory") // set captured database, support regex
                .collectionList("inventory.products", "inventory.orders") //set captured collections, support regex
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
```

MongoDB CDC 增量连接器（2.3.0 之后）可以使用，如下所示：
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class MongoDBIncrementalSourceExample {
    public static void main(String[] args) throws Exception {
        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts("localhost:27017")
                        .databaseList("inventory") // set captured database, support regex
                        .collectionList("inventory.products", "inventory.orders") //set captured collections, support regex
                        .username("flink")
                        .password("flinkpw")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print MongoDB Snapshot + Change Stream");
    }
}
```

**注意:**
- 如果使用数据库正则表达式，则需要 `readAnyDatabase` 角色。
- 增量快照功能仅支持 MongoDB 4.0 之后的版本。

### 完整的 Changelog<a name="Full Changelog" id="003" ></a>

MongoDB 6.0 以及更高的版本支持发送更改流事件，其中包含文档的更新前和更新后的内容（或者说数据的前后镜像）。

- 前镜像是指被替换、更新或删除之前的文档。对于插入操作没有前镜像。

- 后镜像是指被插入、替换或更新之后的文档。对于删除操作没有后镜像。

MongoDB CDC 能够使用前镜像和后镜像来生成完整的更改日志流，包括插入、更新前、更新后和删除的数据行，从而避免了额外的 `ChangelogNormalize` 下游节点。

为了启用此功能，你需要满足以下条件：

- MongoDB 的版本必须为 6.0 或更高版本。
- 在数据库级别启用 `preAndPostImages` 功能：
```javascript
db.runCommand({
  setClusterParameter: {
    changeStreamOptions: {
      preAndPostImages: {
        expireAfterSeconds: 'off' // replace with custom image expiration time
      }
    }
  }
})
```
- 为希望监控的 collection 启用 `changeStreamPreAndPostImages` 功能：
```javascript
db.runCommand({
  collMod: "<< collection name >>", 
  changeStreamPreAndPostImages: {
    enabled: true 
  } 
})
```
- 启用 MongoDB CDC 的 `scan.full-changelog` 功能：

```java
MongoDBSource.builder()
    .scanFullChangelog(true)
    ...
    .build()
```

或者使用 Flink SQL:

```SQL
CREATE TABLE mongodb_source (...) WITH (
    'connector' = 'mongodb-cdc',
    'scan.full-changelog' = 'true',
    ...
)
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
2. 对于 MongoDB，这里的 `namespace` 会被设置成默认值 ""，也就是一个空字符串，Group 名称的格式会类似于 `test_database.test_table`。

数据类型映射
----------------
[BSON](https://docs.mongodb.com/manual/reference/bson-types/) **二进制 JSON**的缩写是一种类似 JSON 格式的二进制编码序列，用于在 MongoDB 中存储文档和进行远程过程调用。

[Flink SQL Data Type](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/types/) 类似于 SQL 标准的数据类型术语，该术语描述了表生态系统中值的逻辑类型。它可以用于声明操作的输入和/或输出类型。

为了使 Flink SQL 能够处理来自异构数据源的数据，异构数据源的数据类型需要统一转换为 Flink SQL 数据类型。

以下是 BSON 类型和 Flink SQL 类型的映射。


<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">BSON type<a href="https://docs.mongodb.com/manual/reference/bson-types/"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td></td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        Int<br>
      <td>INT</td>
    </tr>
    <tr>
      <td>Long</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td></td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>Double</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>Decimal128</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>Boolean</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>Date</br>Timestamp</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>Date</br>Timestamp</td>
      <td>TIME</td>
    </tr>
    <tr>
      <td>Date</td>
      <td>TIMESTAMP(3)</br>TIMESTAMP_LTZ(3)</td>
    </tr>
    <tr>
      <td>Timestamp</td>
      <td>TIMESTAMP(0)</br>TIMESTAMP_LTZ(0)
      </td>
    </tr>
    <tr>
      <td>
        String<br>
        ObjectId<br>
        UUID<br>
        Symbol<br>
        MD5<br>
        JavaScript</br>
        Regex</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BinData</td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>Object</td>
      <td>ROW</td>
    </tr>
    <tr>
      <td>Array</td>
      <td>ARRAY</td>
    </tr>
    <tr>
      <td>DBPointer</td>
      <td>ROW&lt;$ref STRING, $id STRING&gt;</td>
    </tr>
    <tr>
      <td>
        <a href="https://docs.mongodb.com/manual/reference/geojson/">GeoJSON</a>
      </td>
      <td>
        Point : ROW&lt;type STRING, coordinates ARRAY&lt;DOUBLE&gt;&gt;</br>
        Line  : ROW&lt;type STRING, coordinates ARRAY&lt;ARRAY&lt; DOUBLE&gt;&gt;&gt;</br>
        ...
      </td>
    </tr>
    </tbody>
</table>
</div>

参考
--------
- [MongoDB Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/)
- [Change Streams](https://docs.mongodb.com/manual/changeStreams/)
- [Replication](https://docs.mongodb.com/manual/replication/)
- [Sharding](https://docs.mongodb.com/manual/sharding/)
- [Database User Roles](https://docs.mongodb.com/manual/reference/built-in-roles/#database-user-roles)
- [WiredTiger](https://docs.mongodb.com/manual/core/wiredtiger/#std-label-storage-wiredtiger)
- [Replica set protocol](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion)
- [Connection String Options](https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options)
- [Document Pre- and Post-Images](https://www.mongodb.com/docs/v6.0/changeStreams/#change-streams-with-document-pre--and-post-images)
- [BSON Types](https://docs.mongodb.com/manual/reference/bson-types/)
- [Flink DataTypes](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/types/)

{{< top >}}
