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

# Kafka Pipeline Connector

The Kafka Pipeline connector can be used as a *Data Source* or *Data Sink* of the pipeline. As a source, it consumes Debezium JSON or Canal JSON changelog records and converts inferred schema changes into pipeline schema events.

## What can the connector do?
* Data synchronization
* Consume Debezium JSON or Canal JSON changelog records
* Infer create table, add column, and compatible column type widening events

Kafka Source
----------------

The following pipeline consumes Debezium JSON from multiple Kafka partitions and writes it to StarRocks:

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

Kafka source options:

* `topic`: one topic or a comma-separated topic list.
* `topic-pattern`: a regular expression for discovering topics. Configure exactly one of `topic` and `topic-pattern`.
* `group-id` (required unless `properties.group.id` is set): Kafka consumer group.
* `scan.startup.mode`: `group-offsets` (default), `earliest-offset`, `latest-offset`, `timestamp`, or `specific-offsets`.
* `scan.startup.timestamp-millis`: required when `scan.startup.mode` is `timestamp`.
* `scan.startup.specific-offsets`: required when `scan.startup.mode` is `specific-offsets`. Use `partition:0,offset:42;partition:1,offset:300` when exactly one `topic` is configured, or include a topic in each entry such as `topic:dbz.customers,partition:0,offset:42`. Partitions that are not listed start from the earliest offset.
* `tables`: optional inclusion patterns matched against Debezium `source.db`/`source.table` or Canal `database`/`table` (same selector syntax as the MySQL source, for example `inventory.customers` or `inventory.\\.*`).
* `tables.exclude`: optional exclusion patterns. Can be used alone or together with `tables`.
* `value.format`: `debezium-json` (default) or `canal-json`.
* `properties.bootstrap.servers` (required) and `properties.*`: Kafka consumer properties.

Primary keys are not inferred from Debezium JSON. Assign them with a pipeline `transform` `primary-keys` option. Canal JSON uses `pkNames` as the table primary key when present; transform can still override them. A StarRocks sink still requires a primary key before it can create a table.

Debezium JSON values must include the `schema` and `payload` fields. Values without an embedded schema cannot reliably describe column type changes and are rejected.

Canal JSON values use `mysqlType` for column types and `pkNames` for primary keys. `INSERT`/`UPDATE`/`DELETE` are consumed; `isDdl=true` and other `type` values are skipped. Canal `UPDATE` records often put only changed columns in `old`: the source builds a full before image by overlaying `old` onto `data`.

The transform primary key determines downstream partitioning and upsert semantics, but it cannot restore ordering already lost in Kafka. Producers must still send changes for the same logical primary key to the same Kafka partition.

### Schema evolution and multiple partitions

Kafka only guarantees ordering within a partition. The source therefore merges schemas monotonically across the partitions assigned to each source subtask and the distributed schema coordinator merges them again across subtasks. Old records arriving after a newer schema are converted to the widest known schema instead of reverting it.

The source supports:

* creating a table from the first record seen for a table;
* adding nullable columns;
* keeping a monotonic column superset: dropped or renamed source columns are retained (NOT NULL columns become nullable) and new names are added. Historical rows have nulls in new columns; later rows have nulls in old columns. The source does not emit drop or rename events;
* compatible type widening, such as `INT` to `BIGINT`, `INT` to `STRING`, or increasing decimal precision. Kafka Connect `string` is always mapped to `STRING` (MySQL `CHAR`/`VARCHAR`/`TEXT` all become `string` in Debezium JSON). Replaying from an old offset that spans an `INT` → `STRING` change converts historical integer values to strings instead of failing.

Narrowing types and incompatible type changes fail explicitly. Use `schema.change.behavior: lenient` for a parallel Kafka source.

When replaying from an old offset, an empty target table follows the historical `CREATE → ADD/ALTER` sequence. An existing StarRocks table must be a compatible superset. Replayed create/add/alter operations are idempotent; extra target columns must be nullable or have defaults. Replaying rows is safe for primary-key tables through upserts. Duplicate-key tables should be cleared or replaced before a full replay.

How to create Pipeline
----------------

The pipeline for reading data from MySQL and sink to Kafka can be defined as follows:

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

Pipeline Connector Options
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
      <td>Specify what connector to use, here should be <code>'kafka'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the sink.</td>
    </tr>
    <tr>
      <td>partition.strategy</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Defines the strategy for sending record to kafka topic, available options are `all-to-zero`(sending all records to 0 partition) and `hash-by-key`(distributing all records by hash of primary keys), default option is `all-to-zero`. </td>
    </tr>
    <tr>
      <td>key.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Defines the format identifier for encoding key data, available options are `csv` and `json`, default option is `json`. </td>
    </tr>
    <tr>
      <td>value.format</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to serialize the value part of Kafka messages. Available options are <a href="https://debezium.io/documentation/reference/stable/integrations/serdes.html">debezium-json</a> and <a href="https://github.com/alibaba/canal/wiki">canal-json</a>, default option is `debezium-json`, and do not support user-defined format now. </td>
    </tr>
    <tr>
      <td>properties.bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.</td>
    </tr>
    <tr>
      <td>topic</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>If this parameter is configured, all events will be sent to this topic.</td>
    </tr>
    <tr>
      <td>sink.add-tableId-to-header-enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Boolean</td>
      <td>If this parameter is true, a header with key of 'namespace','schemaName','tableName' will be added for each Kafka record. Default value is false.</td>
    </tr>
    <tr>
      <td>properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass options of Kafka table to pipeline，See <a href="https://kafka.apache.org/28/documentation.html#consumerconfigs">Kafka consume options</a>. </td>
    </tr>
    <tr>
      <td>sink.custom-header</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>custom headers for each kafka record. Each header are separated by ',', separate key and value by ':'. For example, we can set headers like 'key1:value1,key2:value2'. </td>
    </tr>
    <tr>
      <td>sink.tableId-to-topic.mapping</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom table mappings for each table from upstream tableId to downstream Kafka topic. Each mapping is separated by `;`, separate upstream tableId and downstream Kafka topic by `:`, For example, we can set `sink.tableId-to-topic.mapping` like `mydb.mytable1:topic1;mydb.mytable2:topic2`. </td>
    </tr>
    <tr>
      <td>debezium-json.include-schema.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>If this parameter is configured, each debezium record will contain debezium schema information. Is only supported when using debezium-json. </td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* The written topic of Kafka will be `namespace.schemaName.tableName` string of TableId，this can be changed using route function of pipeline.

* If the written topic of Kafka is not existed, we will create one automatically.

### Output Format
For different built-in `value.format` options, the output format is different:
#### debezium-json
Refer to [Debezium docs](https://debezium.io/documentation/reference/1.9/connectors/mysql.html), debezium-json format will contains `before`,`after`,`op`,`source` elements, but `ts_ms` is not included in `source`.    
An output example is:
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
When `debezium-json.include-schema.enabled` is true, the output format will be:
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
Refer to [Canal | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/canal/#available-metadata), canal-json format will contains `old`,`data`,`type`,`database`,`table`,`pkNames` elements, but `ts` is not included.      
An output example is:
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

Data Type Mapping
----------------
[Literal type](https://debezium.io/documentation/reference/3.1/connectors/mysql.html#mysql-data-types): defines the physical storage format of data (type field of the debezium schema)<br>
[Semantic type](https://debezium.io/documentation/reference/3.1/connectors/mysql.html#mysql-data-types): defines the logical meaning of data (name field of the debezium schema).
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
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td>INT16</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td>INT32</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td>INT64</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
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
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td>INT32</td>
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
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td>STRING</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>ARRAY</td>
      <td>ARRAY</td>
      <td></td>
      <td>Serialized as JSON array. Element types are recursively converted according to this table.</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>MAP</td>
      <td>MAP</td>
      <td></td>
      <td>Serialized as JSON object. Key and value types are recursively converted according to this table.</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>ROW</td>
      <td>STRUCT</td>
      <td></td>
      <td>Serialized as JSON object. Field types are recursively converted according to this table.</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}