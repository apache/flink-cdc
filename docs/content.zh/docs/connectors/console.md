---
title: "Console"
weight: 3
type: docs
aliases:
- /connectors/console
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

# Console Connector

Value Pipeline 连接器可以用作 Data Sink，将数据写入Console。 本文档介绍如何设置 Console Pipeline 连接器。

## What can the connector do?
* 自动建表
* 表结构变更同步
* 数据实时同步
* 输出数据变更日志

## 如何创建 Pipeline

从 MySQL 读取数据输出到 Console 的 Pipeline 可以定义如下：
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
  type: values
  name: Console Sink


pipeline:
   parallelism: 2

```

## Pipeline 连接器配置项

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
       <td>type</td>
       <td>required</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>指定要使用的连接器, 这里需要设置成 <code>'values'</code>.</td>
     </tr>
     <tr>
       <td>name</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">(none)</td>
       <td>String</td>
       <td>Sink 的名称.</td>
     </tr>
     <tr>
       <td>print.enabled</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">true</td>
       <td>Boolean</td>
       <td> 是否输出 Event 到 Stdout </td>
     </tr>
     <tr>
       <td>sink.api</td>
       <td>optional</td>
       <td style="word-wrap: break-word;">SINK_V2</td>
       <td>Enum</td>
       <td> Sink 所基于的sink api: SinkFunction 或者 SinkV2. </td>
     </tr>
     </tbody>
</table>
</div>

{{< top >}}
