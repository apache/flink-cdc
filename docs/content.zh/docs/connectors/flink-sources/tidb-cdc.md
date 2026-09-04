---
title: "TiDB"
weight: 8
type: docs
aliases:
- /connectors/flink-sources/tidb-cdc
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

# TiDB CDC Connector

TiDB CDC Connector 首先读取 TiDB 表的一致性快照，随后通过 TiKV CDC 持续读取行级变更。Connector 支持 Flink SQL/Table API 和统一的 DataStream Source API。

依赖项
------

### Maven 依赖

{{< artifact flink-connector-tidb-cdc >}}

### SQL Client JAR

下载 [flink-sql-connector-tidb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-tidb-cdc)，并将 JAR 放入 `<FLINK_HOME>/lib/`。

下载链接仅适用于已发布版本。Connector 构件的 Flink 主版本必须与运行作业的 Flink 主版本一致。

创建 TiDB CDC 表
-----------------

Connector 使用两类连接地址：

- `hostname` 和 `port` 连接 TiDB SQL 服务，用于发现表结构和读取快照。
- `pd-addresses` 连接 PD 和 TiKV，用于读取增量变更。

PD 和 TiKV 对外发布的所有地址都必须能被每个 TaskManager 访问。如果集群发布的是 Flink 无法直接解析的内网主机名或地址，请配置 `host-mapping`。

```sql
-- 定期执行 checkpoint，使 Source 在故障恢复后能够继续读取。
SET 'execution.checkpointing.interval' = '3s';

CREATE TABLE orders (
    order_id INT,
    order_date TIMESTAMP(3),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'hostname' = 'tidb.example.com',
    'port' = '4000',
    'username' = 'root',
    'password' = '',
    'pd-addresses' = 'pd-0.example.com:2379,pd-1.example.com:2379',
    'database-name' = 'mydb',
    'table-name' = 'orders',
    'scan.startup.mode' = 'initial'
);

SELECT * FROM orders;
```

{{< hint info >}}
如果 TiDB 表存在主键，请在 Flink 表中定义相同的主键。下游算子和 Sink 可以借助主键正确处理更新和删除。当前 Connector 为一张捕获表创建一个流读取器，因此每个 Flink SQL Source 表只应对应一张 TiDB 表。
{{< /hint >}}

Connector 参数
--------------

| 参数 | 是否必填 | 默认值 | 类型 | 说明 |
| --- | --- | --- | --- | --- |
| `connector` | 是 | 无 | String | 必须为 `tidb-cdc`。 |
| `hostname` | 是 | 无 | String | TiDB SQL 服务的主机名或 IP 地址。 |
| `port` | 是 | `4000` | Integer | TiDB SQL 服务端口。配置对象虽然提供默认值，但 Table Factory 当前将该参数声明为必填。 |
| `username` | 是 | 无 | String | 建立 TiDB JDBC 连接时使用的用户名。 |
| `password` | 是 | 无 | String | 建立 TiDB JDBC 连接时使用的密码。账号没有密码时也需要显式配置空字符串。 |
| `pd-addresses` | 是 | 无 | String | TiKV Client 使用的 PD 地址，多个地址以逗号分隔。 |
| `database-name` | 是 | 无 | String | 捕获表所在的数据库。 |
| `table-name` | 是 | 无 | String | 要捕获的单张表名称。 |
| `scan.startup.mode` | 否 | `initial` | String | 启动模式：`initial`、`snapshot`、`latest-offset` 或 `timestamp`。 |
| `scan.startup.timestamp-millis` | 条件必填 | 无 | Long | Epoch 毫秒时间戳；`scan.startup.mode` 为 `timestamp` 时必须配置。 |
| `server-time-zone` | 否 | `UTC` | String | 转换时间类型时使用的 TiDB 会话时区。 |
| `connect.timeout` | 否 | `30s` | Duration | 建立 TiDB JDBC 连接的最长等待时间。 |
| `connect.max-retries` | 否 | `3` | Integer | 建立 TiDB JDBC 连接的最大重试次数。 |
| `connection.pool.size` | 否 | `20` | Integer | JDBC 连接池大小。 |
| `jdbc.driver` | 否 | `com.mysql.cj.jdbc.Driver` | String | 连接 TiDB 使用的 JDBC Driver 类名。 |
| `scan.incremental.snapshot.enabled` | 否 | `true` | Boolean | Factory 可以接收该参数。当前 SQL 运行路径始终构建增量 Source，不会根据该值选择其他实现。 |
| `scan.incremental.snapshot.chunk.size` | 否 | `8096` | Integer | 每个快照 Chunk 的近似行数。 |
| `scan.snapshot.fetch.size` | 否 | `1024` | Integer | 单次快照轮询最多拉取的行数。 |
| `chunk-meta.group.size` | 否 | `1000` | Integer | 每组 Chunk 元数据包含的条目数。 |
| `scan.incremental.snapshot.chunk.key-column` | 否 | 主键第一列 | String | 切分快照 Chunk 使用的列，建议选择可比较且分布均匀的列。 |
| `chunk-key.even-distribution.factor.upper-bound` | 否 | `1000.0` | Double | 判断 Chunk Key 是否均匀分布的上界。 |
| `chunk-key.even-distribution.factor.lower-bound` | 否 | `0.05` | Double | 判断 Chunk Key 是否均匀分布的下界。 |
| `host-mapping` | 否 | 无 | String | 将 TiKV 发布的主机映射为 Flink 可访问的地址，格式为 `内网主机:外部主机;内网主机2:外部主机2`，端口保持不变。 |
| `heartbeat.interval.ms` | 否 | `30s` | Duration | 配置的心跳间隔。当前 TiDB SQL 运行路径会保存该值，但尚未把心跳配置传入 Source Builder。 |
| `table-list` | 否 | 无 | String | Factory 可以接收该参数，但当前 SQL 运行路径通过 `database-name` 和 `table-name` 构造捕获列表，不能用它替代这两个参数。 |

`jdbc.properties.*`、`debezium.*` 和 `tikv.*` 前缀的参数都能通过表参数校验。当前 SQL 运行路径不会把任意 `tikv.*` 参数复制到 `TiConfiguration`，因此这些参数目前不会产生运行时效果。

启动模式
--------

- `initial`（默认）：读取表快照，然后从对应变更位置继续读取增量事件。
- `snapshot`：只读取快照，不进入增量读取阶段。
- `latest-offset`：跳过已有数据，只读取 Source 启动之后产生的变更。
- `timestamp`：从 `scan.startup.timestamp-millis` 指定的时间开始读取变更。

Source Offset 保存在 Flink Checkpoint 状态中。生产环境应开启 Checkpoint，以便 Source 在恢复时从已完成的 Checkpoint 继续读取。

Changelog 语义
--------------

TiDB Table Source 声明完整的 Flink Changelog：

| RowKind | 含义 |
| --- | --- |
| `+I` | 插入数据或快照数据 |
| `-U` | 更新前的数据 |
| `+U` | 更新后的数据 |
| `-D` | 被删除的数据 |

TiKV 必须为更新和删除事件提供旧行数据。定义了主键的下游 Sink 可以利用这些事件维护最新表状态。

可用元数据
----------

元数据列是只读列，必须使用 `METADATA ... VIRTUAL` 声明。

| Key | 数据类型 | 说明 |
| --- | --- | --- |
| `table_name` | `STRING NOT NULL` | 源表名称。 |
| `database_name` | `STRING NOT NULL` | 源数据库名称。 |
| `op_ts` | `TIMESTAMP_LTZ(3) NOT NULL` | 数据库变更时间；快照记录的时间戳为 `0`。 |
| `row_kind` | `STRING NOT NULL` | Flink RowKind：`+I`、`-U`、`+U` 或 `-D`。 |

```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    source_table STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    change_kind STRING METADATA FROM 'row_kind' VIRTUAL,
    id BIGINT,
    name STRING,
    weight DECIMAL(10, 3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'hostname' = 'tidb.example.com',
    'port' = '4000',
    'username' = 'root',
    'password' = '',
    'pd-addresses' = 'pd.example.com:2379',
    'database-name' = 'inventory',
    'table-name' = 'products'
);
```

DataStream Source
-----------------

DataStream API 应使用 `TiDBSourceBuilder.TiDBIncrementalSource` 和 `StreamExecutionEnvironment#fromSource`。旧文档中的 `SourceFunction` 版 `TiDBSource` API 已不属于当前 Connector。

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.source.TiDBSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

JdbcIncrementalSource<String> source =
        TiDBSourceBuilder.TiDBIncrementalSource.<String>builder()
                .hostname("tidb.example.com")
                .port(4000)
                .username("root")
                .password("")
                .pdAddresses("pd.example.com:2379")
                .databaseList("inventory")
                .tableList("inventory.products")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000L);
env.fromSource(source, WatermarkStrategy.noWatermarks(), "TiDB CDC")
        .print();
env.execute("TiDB CDC example");
```

数据类型映射
------------

| TiDB 类型 | Flink SQL 类型 | 说明 |
| --- | --- | --- |
| `TINYINT` | `TINYINT` | |
| `TINYINT UNSIGNED`、`SMALLINT` | `SMALLINT` | |
| `SMALLINT UNSIGNED`、`MEDIUMINT`、`MEDIUMINT UNSIGNED`、`INT` | `INT` | |
| `INT UNSIGNED`、`BIGINT` | `BIGINT` | |
| `BIGINT UNSIGNED` | `DECIMAL(20, 0)` | |
| `FLOAT` | `FLOAT` | |
| `REAL`、`DOUBLE` | `DOUBLE` | |
| `NUMERIC(p,s)`、`DECIMAL(p,s)`，且 `p <= 38` | `DECIMAL(p,s)` | |
| `NUMERIC(p,s)`、`DECIMAL(p,s)`，且 `38 < p <= 65` | `STRING` | Flink Decimal 最多支持 38 位精度，使用 `STRING` 可以避免精度丢失。 |
| `BOOLEAN`、`TINYINT(1)`、`BIT(1)` | `BOOLEAN` | |
| `DATE` | `DATE` | |
| `TIME(p)` | `TIME(p)` | |
| `TIMESTAMP(p)` | `TIMESTAMP_LTZ(p)` | 根据 `server-time-zone` 解释。 |
| `DATETIME(p)` | `TIMESTAMP(p)` | |
| `CHAR(n)` | `CHAR(n)` | |
| `VARCHAR(n)` | `VARCHAR(n)` | |
| `BIT(n)` | `BINARY(ceil(n / 8))` | |
| `BINARY(n)` | `BINARY(n)` | |
| `TINYTEXT`、`TEXT`、`MEDIUMTEXT`、`LONGTEXT` | `STRING` | |
| `TINYBLOB`、`BLOB`、`MEDIUMBLOB`、`LONGBLOB` | `BYTES` | 不支持超过 Java 数组最大长度的值。 |
| `YEAR` | `INT` | |
| `ENUM` | `STRING` | |
| `SET` | `ARRAY<STRING>` | 将 SET 值拆分为字符串元素。 |
| `JSON` | `STRING` | 序列化为 JSON 文本。 |

{{< top >}}
