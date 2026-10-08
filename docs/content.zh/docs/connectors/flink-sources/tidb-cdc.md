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

# TiDB CDC 连接器

TiDB CDC 连接器允许从 TiDB 数据库读取快照数据和增量数据。本文描述了如何设置 TiDB CDC 连接器来对 TiDB 数据库运行 SQL 查询。

依赖
------------

为了设置 TiDB CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-tidb-cdc >}}

### SQL Client JAR

```下载链接仅适用于稳定版本。```

下载 [flink-sql-connector-tidb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-tidb-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-tidb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-tidb-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

如何创建 TiDB CDC 表
----------------

TiDB CDC 表可以定义如下：

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a TiDB table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(3),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'tidb-cdc',
     'tikv.grpc.timeout_in_ms' = '20000', 
     'pd-addresses' = 'localhost:2379',
     'database-name' = 'mydb',
     'table-name' = 'orders'
);
  
-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
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
      <td>指定要使用的连接器, 这里应该是 <code>'tidb-cdc'</code>.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 TiDB 服务器的数据库名称。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 TiDB 数据库的表名。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>TiDB CDC 消费者可选的启动模式，合法的模式为 "initial" 和 "latest-offset"。</td>
    </tr>
    <tr>
      <td>pd-addresses</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>TiKV 集群的 PD 地址。</td>
    </tr>
    <tr>
      <td>host-mapping</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>TiKV 集群的 host-mapping，用于配置公网 IP 和内网 IP 的映射。当 TiKV 集群运行在内网时，你可以将一组内网 IP 映射到公网 IP，以供外部的 Flink 集群访问。格式为 {Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}，例如 192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9。</td>
    </tr>
    <tr>
      <td>tikv.grpc.timeout_in_ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>TiKV GRPC 超时时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>tikv.grpc.scan_timeout_in_ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>TiKV GRPC 扫描超时时间，单位为毫秒。</td>
    </tr>
    <tr>
      <td>tikv.batch_get_concurrency</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>TiKV GRPC batch get 的并发度。</td>
    </tr>
    <tr>
      <td>tikv.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>透传 TiDB 客户端的属性。</td>
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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是 binlog 读取记录，该值将始终为0。</td>
    </tr>
  </tbody>
</table>

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'tikv.grpc.timeout_in_ms' = '20000',
    'pd-addresses' = 'localhost:2379',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);
```

特性
--------
### Exactly-Once 处理

TiDB CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后继续读取变更事件，即使在处理时出现故障，也能**准确读取数据**。

### 启动模式

配置选项`scan.startup.mode`指定 TiDB CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：对捕获表的结构和数据执行快照；适用于需要获取捕获表数据完整表示的场景。
- `latest-offset`：仅对捕获表的结构执行快照；适用于只需要获取从现在开始发生的变更的场景。

### 多线程读取

TiDB CDC source 可通过并行读取方式工作，因为有多个任务可以接收变更事件。

### DataStream Source

TiDB CDC 连接器也可以是一个数据流源。 你可以创建 SourceFunction，如下所示：

### DataStream Source

```java
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.cdc.connectors.tidb.TDBSourceOptions;
import org.apache.flink.cdc.connectors.tidb.TiDBSource;
import org.apache.flink.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import org.apache.flink.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;

public class TiDBSourceExample {

    public static void main(String[] args) throws Exception {

        SourceFunction<String> tidbSource =
            TiDBSource.<String>builder()
                .database("mydb") // set captured database
                .tableName("products") // set captured table
                .tiConf(
                    TDBSourceOptions.getTiConfiguration(
                        "localhost:2399", new HashMap<>()))
                .snapshotEventDeserializer(
                    new TiKVSnapshotEventDeserializationSchema<String>() {
                        @Override
                        public void deserialize(
                            Kvrpcpb.KvPair record, Collector<String> out)
                            throws Exception {
                            out.collect(record.toString());
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    })
                .changeEventDeserializer(
                    new TiKVChangeEventDeserializationSchema<String>() {
                        @Override
                        public void deserialize(
                            Cdcpb.Event.Row record, Collector<String> out)
                            throws Exception {
                            out.collect(record.toString());
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    })
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }
}
```

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">TiDB type<a href="https://dev.tidb.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>在 TiDB 中，十进制数据类型的精度高达 65，但在 Flink 中，十进制数据类型的精度仅限于 38。
  所以，如果定义精度大于 38 的十进制列，则应将其映射到字符串以避免精度损失。</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
        TINYINT(1)<br>
        BIT(1)
        </td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]</td>
      <td>TIMESTAMP_LTZ [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME [(p)]</td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)
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
        BIT(n)
      </td>
      <td>BINARY(⌈n/8⌉)</td>
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
        TINYTEXT<br>
        TEXT<br>
        MEDIUMTEXT<br>
        LONGTEXT<br>
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYBLOB<br>
        BLOB<br>
        MEDIUMBLOB<br>
        LONGBLOB<br>
      </td>
      <td>BYTES</td>
      <td>目前，对于 TiDB 中的 BLOB 数据类型，仅支持长度不大于 2,147,483,647(2 ** 31 - 1) 的 blob。</td>
    </tr>
    <tr>
      <td>
        YEAR
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td>JSON 数据类型将在 Flink 中转换为 JSON 格式的字符串。</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>ARRAY&lt;STRING&gt;</td>
      <td>因为 TiDB 中的 SET 数据类型是一个字符串对象，可以有零个或多个值，
          它应该始终映射到字符串数组。
      </td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
