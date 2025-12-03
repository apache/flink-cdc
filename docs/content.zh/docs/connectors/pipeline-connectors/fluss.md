---
title: "Fluss"
weight: 4
type: docs
aliases:
- /connectors/pipeline-connectors/fluss
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

# Fluss Pipeline 连接器
Fluss Pipeline 连接器可用作 Pipeline 的 *Data Sink*，将数据写入 [Fluss](https://fluss.apache.org)。本文档介绍如何配置 Fluss Pipeline 连接器。

## What can the connector do?
* 自动创建不存在的表 
* 数据同步

How to create Pipeline
----------------

从 MySQL 读取数据并写入 Fluss 的 Pipeline 可定义如下：

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
  type: fluss
  name: Fluss Sink
  bootstrap.servers: localhost:9123
  # Security-related properties for the Fluss client
  properties.client.security.protocol: sasl
  properties.client.security.sasl.mechanism: PLAIN
  properties.client.security.sasl.username: developer
  properties.client.security.sasl.password: developer-pass

pipeline:
  name: MySQL to Fluss Pipeline
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'fluss'</code>。 </td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称。 </td>
    </tr>
    <tr>
      <td>bootstrap.servers</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于建立与 Fluss 集群初始连接的主机/端口对列表。 </td>
    </tr>
    <tr>
      <td>bucket.key</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定每个 Fluss 表的数据分布策略。表之间用 ';' 分隔，分桶键之间用 ',' 分隔。格式：database1.table1:key1,key2;database1.table2:key3。
          数据将根据分桶键的哈希值分配到各个桶中（分桶键必须是主键的子集，且不包含主键表的分区键）。
          若表有主键但未指定分桶键，则分桶键默认为主键（不含分区键）；若表无主键且未指定分桶键，则数据将随机分配到各个桶中。 </td>
    </tr>
    <tr>
      <td>bucket.num</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>每个 Fluss 表的桶数量。表之间用 ';' 分隔。格式：database1.table1:4;database1.table2:8。 </td>
    </tr>
    <tr>
      <td>properties.table.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Fluss table 支持的参数传递给 pipeline，参考，See <a href="https://fluss.apache.org/docs/engine-flink/options/#storage-options">Fluss table options</a>. </td>
    </tr>
    <tr>
      <td>properties.client.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Fluss client 支持的参数传递给 pipeline，See <a href="https://fluss.apache.org/docs/engine-flink/options/#write-options">Fluss client options</a>. </td>
    </tr>
    </tbody>
</table>    
</div>

## 使用说明

* 支持 Fluss 主键表和日志表。

* 关于自动建表
  * 没有分区键
  * 桶数量由 `bucket.num` 选项控制
  * 数据分布由 `bucket.key` 选项控制。对于主键表，若未指定分桶键，则分桶键默认为主键（不含分区键）；对于无主键的日志表，若未指定分桶键，则数据将随机分配到各个桶中。 

* 不支持 schema 变更同步。如果需要忽略 schema 变更，可使用 `schema.change.behavior: IGNORE`。

* 关于数据同步， Pipeline 连接器使用 [Fluss Java Client](https://fluss.apache.org/docs/apis/java-client/) 向 Fluss 写入数据.

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Flink CDC type</th>
        <th class="text-left">Fluss type</th>
        <th class="text-left" style="width:60%;">Note</th>
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
      <td>TIME</td>
      <td>TIME</td>
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
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY(n)</td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARBINARY(N)</td>
      <td>BYTES</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
