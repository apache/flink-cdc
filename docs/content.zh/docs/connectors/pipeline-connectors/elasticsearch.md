---
title: "Elasticsearch"
weight: 7
type: docs
aliases:
- /connectors/pipeline-connectors/elasticsearch
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

# Elasticsearch Pipeline Connector

Elasticsearch Pipeline 连接器可以用作 Pipeline 的 Data Sink, 将数据写入 Elasticsearch。 本文档介绍如何设置 Elasticsearch Pipeline 连接器。


How to create Pipeline
----------------

从 MySQL 读取数据同步到 Elasticsearch 的 Pipeline 可以定义如下：

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
  type: elasticsearch
  name: Elasticsearch Sink
  hosts: http://127.0.0.1:9092,http://127.0.0.1:9093
  
route:
  - source-table: adb.\.*
    sink-table: default_index
    description: sync adb.\.* table to default_index

pipeline:
  name: MySQL to Elasticsearch Pipeline
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'elasticsearch'</code>.</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称。</td>
    </tr>
    <tr>
      <td>hosts</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要连接到的一台或多台 Elasticsearch 主机，例如: 'http://host_name:9092,http://host_name:9093'.</td>
    </tr>
    <tr>
      <td>version</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">7</td>
      <td>Integer</td>
      <td>指定要使用的连接器，有效值为：
      <ul>
        <li>6: 连接到 Elasticsearch 6.x 的集群。</li>
        <li>7: 连接到 Elasticsearch 7.x 的集群。</li>
        <li>8: 连接到 Elasticsearch 8.x 的集群。</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td>username</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接 Elasticsearch 实例认证的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接 Elasticsearch 实例认证的密码。</td>
    </tr>
    <tr>
      <td>batch.size.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td>每个批量请求的最大缓冲操作数。 可以设置为'0'来禁用它。</td>
    </tr>
    <tr>
      <td>inflight.requests.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5</td>
      <td>Integer</td>
      <td>连接器将尝试执行的最大并发请求数。</td>
    </tr>
    <tr>
      <td>buffered.requests.max</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>每个批量请求的内存缓冲区中保留的最大请求数。</td>
    </tr>
    <tr>
      <td>batch.size.max.bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>每个批量请求的缓冲操作在内存中的最大值(以byte为单位)。</td>
    </tr>
    <tr>
      <td>buffer.time.max.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>每个批量请求的缓冲 flush 操作的间隔(以ms为单位)。</td>
    </tr>
    <tr>
      <td>record.size.max.bytes</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10485760</td>
      <td>Long</td>
      <td>单个记录的最大大小（以byte为单位）。</td>
    </tr>
    <tr>
      <td>sharding.suffix.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>每个表的分片后缀字段，允许为多个表设置分片后缀字段。默认 sink 表名为 test_table${suffix_key}。默认分片字段为第一个分区列。表之间用';'分隔。表和字段之间用':'分割。例如，我们设置 sharding.suffix.key 为'table1:col1;table2:col2'。</td>
    </tr>
    <tr>
      <td>sharding.suffix.separator</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">"_"</td>
      <td>String</td>
      <td>用于分割表名称和分片后缀的分隔符。默认是 '_'。如果设置为 '-'，那么表名称会是 test_table-${suffix}。</td>
    </tr>
    </tbody>
</table>    
</div>

Usage Notes
--------

* 写入 Elasticsearch 的 index 默认为与上游表同名字符串，可以通过 pipeline 的 route 功能进行修改。

* 如果写入 Elasticsearch 的 index 不存在，不会被默认创建。

Data Type Mapping
----------------
Elasticsearch 将文档存储在 JSON 字符串中，数据类型之间的映射关系如下表所示：
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">JSON type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>NUMBER</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>STRING</td>
      <td>with format: date (yyyy-MM-dd), example: 2024-10-21</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>STRING</td>
      <td>with format: date-time (yyyy-MM-dd HH:mm:ss.SSSSSS, with UTC time zone), example: 2024-10-21 14:10:56.000000</td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>STRING</td>
      <td>with format: date-time (yyyy-MM-dd HH:mm:ss.SSSSSS, with UTC time zone), example: 2024-10-21 14:10:56.000000</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>ARRAY</td>
      <td></td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}