# MongoDB CDC 连接器

MongoDB CDC 连接器允许从 MongoDB 读取快照数据和增量数据。 本文档描述了如何设置 MongoDB CDC 连接器以针对 MongoDB 运行 SQL 查询。

依赖
------------

为了设置 MongoDB CDC 连接器, 下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQLJar 捆绑包的 SQLClient 的两个项目的依赖关系信息。

### Maven dependency
```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mongodb-cdc</artifactId>
  <!-- 依赖项仅适用于稳定版本，SNAPSHOT依赖项需要自己构建。 -->
  <version>2.4.0</version>
</dependency>
```

### SQL Client JAR

```下载链接仅适用于稳定版本。```

下载 [flink-sql-connector-mongodb-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/2.4.0/flink-sql-connector-mongodb-cdc-2.4.0.jar) 把它放在 `<FLINK_HOME>/lib/`.

**注意:** flink-sql-connector-mongodb-cdc-XXX-SNAPSHOT 版本是与开发分支相对应的代码。 用户需要下载源代码并编译相应的jar。 用户应使用已发布的版本，例如 [flink-sql-connector-mongodb-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-mongodb-cdc), 发布的版本将在 Maven 中央仓库中提供。

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

  `changeStream` and `read` 是 MongoDB Kafka Connector 必需权限。

  你可以使用以下示例进行简单的授权。<br>
  有关更详细的授权, 请参照 [MongoDB 数据库用户角色](https://docs.mongodb.com/manual/reference/built-in-roles/#database-user-roles).

  ```javascript
  use admin;
  db.createRole(
      {
          role: "flinkrole",
          privileges: [{
              // 所有数据库中所有非系统集合的 grant 权限
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
             // 阅读 config.collections 和 config.chunks
             // 用于分片集群快照拆分。
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

MongoDB 的更改事件记录在消息之前没有更新。因此，我们只能将其转换为 Flink 的 UPSERT 更改日志流。
upstart 流需要一个唯一的密钥，所以我们必须声明 `_id` 作为主键。
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
           请查阅 <a href="#a-name-id-002-a">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
        <td>scan.startup.timestamp-millis</td>
        <td>optional</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Long</td>
        <td>起始毫秒数, 仅适用于 <code>'timestamp'</code> 启动模式.</td>
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
      <td>增量快照的区块大小 mb。</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。</td>
    </tr>
    </tbody>
</table>
</div>

注意: `heartbeat.interval.ms` 强烈建议设置一个大于 0 的适当值 **如果集合更改缓慢**.
当我们从检查点或保存点恢复 Flink 作业时，心跳事件可以向前推送 `resumeToken`，以避免 `resumeToken` 过期。

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
      <td>它指示在数据库中进行更改的时间。 <br>如果记录是从表的快照而不是改变流中读取的，该值将始终为0。</td>
    </tr>
  </tbody>
</table>

扩展的 CREATE TABLE 示例演示了用于公开这些元数据字段的语法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    collection_name STRING METADATA  FROM 'collection_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
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
```

特性
--------

### 精确一次处理

MongoDB CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后在处理**甚至失败时继续读取带有**的更改流事件。

### 启动模式<a name="启动模式" id="002" ></a>

配置选项```scan.startup.mode```指定 MySQL CDC 使用者的启动模式。有效枚举包括：

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

and with SQL:

```SQL
CREATE TABLE mongodb_source (...) WITH (
    'connector' = 'mongodb-cdc',
    'scan.startup.mode' = 'latest-offset', -- 从最晚位点启动
    ...
    'scan.incremental.snapshot.enabled' = 'true', -- 指定时间戳启动，需要开启增量快照读
    'scan.startup.mode' = 'timestamp', -- 指定时间戳启动模式
    'scan.startup.timestamp-millis' = '1667232000000' -- 启动毫秒时间
    ...
)
```

**Notes:**
- 'timestamp' 指定时间戳启动模式，需要开启增量快照读。

### 更改流

我们将 [MongoDB's official Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/) 从 MongoDB 中读取快照或更改事件，并通过 Debezium 的 `EmbeddedEngine` 进行驱动。

Debezium 的 `EmbeddedEngine` 提供了一种在应用程序进程中运行单个 Kafka Connect `SourceConnector` 的机制，并且它可以正确地驱动任何标准的 Kafka Connect `SourceConnector`，即使它不是由 Debezium 提供的。

我们选择 **MongoDB 的官方 Kafka连接器**，而不是 **Debezium 的MongoDB 连接器**，因为它们使用了不同的更改数据捕获机制。

- 对于 Debezium 的 MongoDB 连接器，它读取每个复制集主节点的 `oplog.rs` 集合。
- 对于 MongoDB 的 Kafka 连接器，它订阅了 MongoDB 的 `更改流`。

MongoDB 的`oplog.rs` 集合没有在状态之前保持更改记录的更新， 因此，很难通过单个 `oplog.rs` 记录提取完整的文档状态，并将其转换为 Flink 接受的更改日志流（Insert Only，Upsert，All）。
此外，MongoDB 5（2021 7月发布）改变了 oplog 格式，因此当前的 Debezium 连接器不能与其一起使用。

**Change Stream**是 MongoDB 3.6 为副本集和分片集群提供的一项新功能，它允许应用程序访问实时数据更改，而不会带来跟踪操作日志的复杂性和风险。<br>
应用程序可以使用更改流来订阅单个集合上的所有数据更改， 数据库或整个部署，并立即对其做出反应。

**查找更新操作的完整文档**是**变更流**提供的一项功能，它可以配置变更流以返回更新文档的最新多数提交版本。由于该功能，我们可以轻松收集最新的完整文档，并将更改日志转换为 Flink 的**Upsert Changelog Stream**。

顺便说一句，[DBZ-435](https://issues.redhat.com/browse/DBZ-435)提到的Debezium的MongoDB变更流探索,正在制定路线图。<br>
如果完成了，我们可以考虑集成两种源连接器供用户选择。

### DataStream Source

MongoDB CDC 连接器也可以是一个数据流源。 你可以创建 SourceFunction，如下所示：

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;

public class MongoDBSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .username("flink")
                .password("flinkpw")
                .databaseList("inventory") // 设置捕获的数据库，支持正则表达式
                .collectionList("inventory.products", "inventory.orders") //设置捕获的集合，支持正则表达式
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // 对 sink 使用并行度 1 以保持消息顺序

        env.execute();
    }
}
```

MongoDB CDC 增量连接器（2.3.0 之后）可以使用，如下所示：
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class MongoDBIncrementalSourceExample {
    public static void main(String[] args) throws Exception {
        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts("localhost:27017")
                        .databaseList("inventory") // 设置捕获的数据库，支持正则表达式
                        .collectionList("inventory.products", "inventory.orders") //设置捕获的集合，支持正则表达式
                        .username("flink")
                        .password("flinkpw")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用检查点
        env.enableCheckpointing(3000);
        // 将 source 并行度设置为 2
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

数据类型映射
----------------
[BSON](https://docs.mongodb.com/manual/reference/bson-types/) **二进制 JSON**的缩写是一种类似 JSON 格式的二进制编码序列，用于在 MongoDB 中存储文档和进行远程过程调用。

[Flink SQL Data Type](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/types/) 类似于 SQL 标准的数据类型术语，该术语描述了表生态系统中值的逻辑类型。它可以用于声明操作的输入和/或输出类型。

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
- [BSON Types](https://docs.mongodb.com/manual/reference/bson-types/)
- [Flink DataTypes](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/types/)

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))