---
title: "Route"
weight: 6
type: docs
aliases:
  - /core-concept/route/
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

# 定义
**Route** 代表一个路由规则，用来匹配一个或多个source 表，并映射到 sink 表。最常见的场景是合并子数据库和子表，将多个上游源表路由到同一个目标表。

# 参数
为了定义一个路由规则，需要提供以下参数：

| 参数             | 含义                                       | optional/required |
|----------------|------------------------------------------|-------------------|
| source-table   | Source 的 table id， 支持正则表达式               | required          |
| sink-table     | Sink 的 table id，支持符号替换                   | required          |
| replace-symbol | 用于在 sink-table 中进行模式替换的特殊字符串， 会被源表中的表名替换 | optional          |
| description    | Route 规则的描述(提供了一个默认描述)                   | optional          |

一个 Route 模块可以包含一个或多个 source-table/sink-table 规则。

# 示例
## 路由一个 Data Source 表到一个 Data Sink 表
如果同步一个 `mydb` 数据库中的 `web_order` 表到一个相同库的 `ods_web_order` 表，我们可以使用下面的 yaml 文件来定义这个路由：

```yaml
route:
  - source-table: mydb.web_order
    sink-table: mydb.ods_web_order
    description: sync table to one destination table with given prefix ods_
```

## 路由多个 Data Source 表到一个 Data Sink 表
更进一步的，如果同步一个 `mydb` 数据库中的多个分表到一个相同库的 `ods_web_order` 表，我们可以使用下面的 yaml 文件来定义这个路由：
```yaml
route:
  - source-table: mydb\.*
    sink-table: mydb.ods_web_order
    description: sync sharding tables to one destination table
```
## 使用多个路由规则
更进一步的，如果需要定义多个路由规则，我们可以使用下面的 yaml 文件来定义这个路由：
```yaml
route:
  - source-table: mydb.orders
    sink-table: ods_db.ods_orders
    description: sync orders table to orders
  - source-table: mydb.shipments
    sink-table: ods_db.ods_shipments
    description: sync shipments table to ods_shipments
  - source-table: mydb.products
    sink-table: ods_db.ods_products
    description: sync products table to ods_products
```

## 包含符号替换的路由规则

如果你想将源表路由到 sink 表，并使用特定的模式替换源表名，那么 `replace-symbol` 就可以做到这一点：

```yaml
route:
  - source-table: source_db.\.*
    sink-table: sink_db.<>
    replace-symbol: <>
    description: route all tables in source_db to sink_db
```

然后，`source_db` 库下所有的表都会被同步到 `sink_db` 库下。