---
title: "Kafka"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/kafka
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

# Kafka Pipeline 连接器

Kafka Pipeline 连接器可以用作 Pipeline 的 *Data Source* 或 *Data Sink*。作为 Source 时，它消费 Debezium JSON 或 Canal JSON Changelog，并将推断出的表结构变化转换为 Pipeline Schema 事件。

## 连接器的功能
* 自动建表
* 表结构变更同步
* 数据实时同步
* 消费 Debezium JSON 或 Canal JSON Changelog

Kafka Source
----------------

下面的 Pipeline 从 Kafka 多分区消费 Debezium JSON，并写入 StarRocks：

```yaml
source:
  type: kafka
  name: Kafka Debezium Source
  topic: inventory.customers
  group-id: flink-cdc-kafka-source
  scan.startup.mode: group-offsets
  properties.bootstrap.servers: localhost:9092

transform:
  - source-table: inventory.\.*
    primary-keys: id

sink:
  type: starrocks
  name: StarRocks Sink
  jdbc-url: jdbc:mysql://localhost:9030
  load-url: localhost:8030
  username: root
  password: ""

pipeline:
  name: Kafka to StarRocks Pipeline
  parallelism: 4
  schema.change.behavior: lenient
```

Kafka Source 配置项：

* `topic`：单个 Topic 或逗号分隔的 Topic 列表。
* `topic-pattern`：用于动态发现 Topic 的正则表达式；`topic` 与 `topic-pattern` 必须且只能配置一个。
* `group-id`（未配置 `properties.group.id` 时必填）：Kafka Consumer Group。
* `scan.startup.mode`：`group-offsets`（默认）、`earliest-offset`、`latest-offset`、`timestamp` 或 `specific-offsets`。
* `scan.startup.timestamp-millis`：当 `scan.startup.mode` 为 `timestamp` 时必填。
* `scan.startup.specific-offsets`：当 `scan.startup.mode` 为 `specific-offsets` 时必填。仅配置一个 `topic` 时可写 `partition:0,offset:42;partition:1,offset:300`；多 Topic 或 `topic-pattern` 时每条必须带 topic，例如 `topic:dbz.customers,partition:0,offset:42`。未列出的分区从 earliest offset 开始。
* `tables`：可选，按 Debezium `source.db`/`source.table` 或 Canal `database`/`table` 做包含过滤，语法与 MySQL source 的 `tables` 相同，例如 `inventory.customers` 或 `inventory.\\.*`。
* `tables.exclude`：可选，排除匹配的表，可单独使用，也可与 `tables` 同时使用。
* `value.format`：`debezium-json`（默认）或 `canal-json`。
* `properties.bootstrap.servers`（必填）及 `properties.*`：Kafka Consumer 参数。

Debezium JSON 不推断主键，请在 Pipeline 的 `transform` 中通过 `primary-keys` 指定。Canal JSON 会使用消息里的 `pkNames` 作为主键，仍可用 transform 覆盖。写入 StarRocks 前表必须具备主键。

Debezium JSON 的 value 必须同时包含 `schema` 与 `payload`。不包含 schema 的 value 无法可靠识别字段类型变化，因此会被拒绝。

Canal JSON 用 `mysqlType` 推断列类型、用 `pkNames` 作为主键。只消费 `INSERT`/`UPDATE`/`DELETE`；`isDdl=true` 以及其他 `type` 会被跳过。Canal 的 `UPDATE` 往往只在 `old` 里放变更列，Source 会用 `old` 覆盖 `data` 拼出完整 before。

Transform 中的主键用于下游分区和 upsert 语义，但无法恢复 Kafka 中已经丢失的顺序；生产端仍需保证相同逻辑主键的变更进入同一个 Kafka 分区。

### Schema Evolution 与多分区

Kafka 只保证分区内有序。Source 会先在每个 Source subtask 内对其负责分区的 schema 做单调扩宽，再由 distributed schema coordinator 跨 subtask 合并。新 schema 出现后到达的旧格式消息会被转换到当前最宽 schema，不会触发类型回退。

Source 支持首次发现表时建表、增加 nullable 字段，以及把 schema 做成单调超集：源端删列或改名时会保留旧列（NOT NULL 会改为 nullable）并加入新列名，不会发出 Drop/Rename。历史行的新列为 null，之后行的旧列为 null。另外支持 `INT → BIGINT`、`INT → STRING`、Decimal 精度扩大等兼容扩宽。Kafka Connect 的 `string` 一律映射为 `STRING`（MySQL 的 `CHAR`/`VARCHAR`/`TEXT` 在 Debezium JSON 中都是 `string`）。从旧 offset 重放并跨过 `INT → STRING` 时，历史整型值会被转成字符串，而不会失败。类型缩窄、不兼容类型变化会明确失败。并行 Kafka Source 应配置 `schema.change.behavior: lenient`。

从旧 offset 重刷时，空目标表会按历史顺序执行 `CREATE → ADD/ALTER`。已有 StarRocks 表必须是历史 schema 的兼容超集；重复的 Create/Add/Alter 会按幂等方式处理，目标表额外字段必须 nullable 或有默认值。StarRocks 主键表可以通过 upsert 覆盖旧记录；duplicate-key 表全量重刷前应清表或改写新表。

如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 Kafka 的 Pipeline 可以定义如下：

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
  type: kafka
  name: Kafka Sink
  properties.bootstrap.servers: PLAINTEXT://localhost:62510

pipeline:
  name: MySQL to Kafka Pipeline
  parallelism: 2
```

Pipeline 连接器配置项
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
      <td>type</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器, 这里需要设置成 <code>'kafka'</code>。 </td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称。 </td>
    </tr>
    <tr>
      <td>partition.strategy</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>定义发送数据到 Kafka 分区的策略， 可以设置的选项有 `all-to-zero`（将所有数据发送到 0 号分区） 以及 `hash-by-key`（所有数据根据主键的哈希值分发），默认值为 `all-to-zero`。 </td>
    </tr>
    <tr>
      <td>key.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于序列化 Kafka 消息的键部分数据的格式。可以设置的选项有 `csv` 以及 `json`， 默认值为 `json`。 </td>
    </tr>
    <tr>
      <td>value.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于序列化 Kafka 消息的值部分数据的格式。可选的填写值包括 <a href="https://debezium.io/documentation/reference/stable/integrations/serdes.html">debezium-json</a> 和 <a href="https://github.com/alibaba/canal/wiki">canal-json</a>, 默认值为 `debezium-json`，并且目前不支持用户自定义输出格式。 </td>
    </tr>
    <tr>
      <td>properties.bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于建立与 Kafka 集群初始连接的主机/端口对列表。</td>
    </tr>
    <tr>
      <td>topic</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>如果配置了这个参数，所有的消息都会发送到这一个主题。</td>
    </tr>
    <tr>
      <td>sink.add-tableId-to-header-enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Boolean</td>
      <td>如果配置了这个参数，所有的消息都会带上键为 `namespace`, 'schemaName', 'tableName'，值为事件 TableId 里对应的 字符串的 header。</td>
    </tr>
    <tr>
      <td>properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Kafka 支持的参数传递给 pipeline，参考 <a href="https://kafka.apache.org/28/documentation.html#consumerconfigs">Kafka consume options</a>。 </td>
    </tr>
    <tr>
      <td>sink.custom-header</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Kafka 记录自定义的 Header。每个 Header 使用 ','分割， 键值使用 ':' 分割。举例来说，可以使用这种方式 'key1:value1,key2:value2'。 </td>
    </tr>
    <tr>
      <td>sink.tableId-to-topic.mapping</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>自定义的上游表名到下游 Kafka Topic 名的映射关系。 每个映射关系由 `;` 分割，上游表的 TableId 和下游 Kafka 的 Topic 名由 `:` 分割。 举个例子，我们可以配置 `sink.tableId-to-topic.mapping` 的值为 `mydb.mytable1:topic1;mydb.mytable2:topic2`。 </td>
    </tr>
    <tr>
      <td>debezium-json.include-schema.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>如果配置了这个参数，每条debezium记录都将包含debezium schema信息。 只有当`value.format`为`debezium-json`时才生效。 </td>
    </tr>
    </tbody>
</table>    
</div>

使用说明
--------

* 写入 Kafka 的 topic 默认会是上游表 `namespace.schemaName.tableName` 对应的字符串，可以通过 pipeline 的 route 功能进行修改。
* 如果配置了 `topic` 参数，所有的消息都会发送到这一个主题。
* 写入 Kafka 的 topic 如果不存在，则会默认创建。

### 输出格式
对于不同的内置 `value.format` 选项，输出的格式也是不同的:
#### debezium-json
参考 [Debezium docs](https://debezium.io/documentation/reference/1.9/connectors/mysql.html)， debezium-json 格式会包含 `before`,`after`,`op`,`source` 几个元素， 但是 `ts_ms` 字段并不会包含在 `source` 元素中。    
一个输出的示例是:
```json
{
  "before": null,
  "after": {
    "col1": "1",
    "col2": "1"
  },
  "op": "c",
  "source": {
    "db": "default_namespace",
    "table": "table1"
  }
}
```
当`debezium-json.include-schema.enabled=true`时，输出示例如下:
```json
{
  "schema":{
    "type":"struct",
    "fields":[
      {
        "type":"struct",
        "fields":[
          {
            "type":"string",
            "optional":true,
            "field":"col1"
          },
          {
            "type":"string",
            "optional":true,
            "field":"col2"
          }
        ],
        "optional":true,
        "field":"before"
      },
      {
        "type":"struct",
        "fields":[
          {
            "type":"string",
            "optional":true,
            "field":"col1"
          },
          {
            "type":"string",
            "optional":true,
            "field":"col2"
          }
        ],
        "optional":true,
        "field":"after"
      }
    ],
    "optional":false
  },
  "payload":{
    "before": null,
    "after": {
      "col1": "1",
      "col2": "1"
    },
    "op": "c",
    "source": {
      "db": "default_namespace",
      "table": "table1"
    }
  }
}
```

#### canal-json
参考 [Canal | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/canal/#available-metadata)， canal-json 格式会包含 `old`,`data`,`type`,`database`,`table`,`pkNames` 几个元素， 但是 `ts` 并不会包含在其中。   
一个输出的示例是:
```json
{
    "old": null,
    "data": [
        {
            "col1": "1",
            "col2": "1"
        }
    ],
    "type": "INSERT",
    "database": "default_schema",
    "table": "table1",
    "pkNames": [
        "col1"
    ]
}
```

数据类型映射
----------------
[Literal type](https://debezium.io/documentation/reference/3.1/connectors/mysql.html#mysql-data-types): 反映数据的实际存储类型 (对应debezium schema中的type字段)<br>
[Semantic type](https://debezium.io/documentation/reference/3.1/connectors/mysql.html#mysql-data-types): 反映数据的逻辑类型 (对应对应debezium schema中的name字段)。
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">JSON type</th>
        <th class="text-left">Literal type</th>
        <th class="text-left">Semantic type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td>INT16</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td>INT16</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td>INT32</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td>INT64</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td>BYTES</td>
      <td>org.apache.kafka.connect.data.Decimal</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td>io.debezium.time.Date</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP(p)</td>
      <td>TIMESTAMP(p)</td>
      <td>INT64</td>
      <td>p <=3 io.debezium.time.Timestamp <br>p >3 io.debezium.time.MicroTimestamp </td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>TIMESTAMP_LTZ</td>
      <td>STRING</td>
      <td>io.debezium.time.ZonedTimestamp</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}