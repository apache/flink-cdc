---
title: "Hudi"
weight: 10
type: docs
aliases:
- /connectors/pipeline-connectors/hudi
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

# Hudi Pipeline 连接器

Hudi Pipeline 连接器可以用作 Pipeline 的 *Data Sink*，将数据写入[Apache Hudi](https://hudi.apache.org)。 本文档介绍如何设置 Hudi Pipeline 连接器。

## 连接器的功能
* 自动创建 Hudi 表
* 自动的表结构变更同步
* 数据实时同步

如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 Hudi 的 Pipeline 可以定义如下：

```yaml
source:
  type: mysql
  name: MySQL Source
  hostname: 127.0.0.1
  port: 3306
  username: admin
  password: pass
  tables: adb.\.*, bdb.user_table_[0-9]+
  server-id: 5401-5404

sink:
  type: hudi 
  name: Hudi Sink
  path: /path/warehouse
  hoodie.table.type: MERGE_ON_READ

transform:
  - source-table: adb.\.*
    table-options: ordering.fields=ts1
  - source-table: bdb.user_table_[0-9]+
    table-options: ordering.fields=ts2

pipeline:
  name: MySQL to Hudi Pipeline
  parallelism: 4
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
      <td>指定要使用的连接器, 这里需要设置成 <code>'hudi'</code></td>
    </tr>
    <tr>
      <td>path</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>为目标 <code>hudi</code> 表指定基本路径</td>
    </tr>
    <tr>
      <td>name</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Sink 的名称</td>
    </tr>
    <tr>
      <td>hoodie.table.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>目标 <code>hudi</code> 表的类型, 目前仅支持 <code>MERGE_ON_READ</code></td>
    </tr>
    <tr>
      <td>index.type</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>目标 <code>hudi</code> 表的索引类型, 目前仅支持 <code>BUCKET</code></td>
    </tr>
    <tr>
      <td>table.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 <code>hudi</code> 表支持的参数传递给 pipeline，参考 <a href="https://hudi.apache.org/docs/configurations/#FLINK_SQL">Hudi table options</a></td>
    </tr>
    </tbody>
</table>    
</div>

使用说明
--------

* 源表必须定义主键。用户可通过 <code>transform</code> 规则来自覆盖主键配置，或设置 Ordering Fields 配置。
* 目前只支持 MERGE_ON_READ 表类型，以及简单 BUCKET 索引。
* 暂不支持 exactly-once 语义，连接器通过 at-least-once 语义和主键表实现幂等写。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">CDC type</th>
        <th class="text-left">Hudi type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
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
      <td>BINARY</td>
      <td>BINARY</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
