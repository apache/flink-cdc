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

# 路由模式
默认情况下，所有匹配的路由规则都会被应用到表上。你可以在 pipeline 配置中通过 `route-mode` 选项来改变这一行为：

| 值            | 描述                                              |
|--------------|-------------------------------------------------|
| `ALL_MATCH`  | 应用所有匹配的路由规则到表上。这是默认模式。                          |
| `FIRST_MATCH`| 只应用第一个匹配的路由规则，并停止后续规则的计算。                       |

例如，使用 `FIRST_MATCH` 模式：

```yaml
pipeline:
  name: Sync MySQL Database to Doris
  parallelism: 2
  route-mode: FIRST_MATCH
```

{{< hint info >}}

当使用 `FIRST_MATCH` 模式时，路由规则会按照定义的顺序进行计算。第一个匹配源表的规则会被应用，后续的规则将被跳过。

{{< /hint >}}

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

## 高级：基于正则捕获组的替换规则

您可以在 `source-table` 字段中定义正则表达式的捕获组：

```yaml
route:
  - source-table: db_(\.*).(\.*)_tbl
    sink-table: sink_db_$1.sink_table_$2
```

这里我们创建了两个捕获组，分别用来匹配数据库名 `db_` 之后的后缀和表名 `_tbl` 之前的前缀。

以上游表 `db_foo.bar_tbl` 为例，我们将会从中提取出 `(foo, bar)` 作为捕获组，并且将其依次绑定到 `$1` 和 `$2` 变量中。
因此，这张表将被路由到 `sink_db_foo.sink_table_bar` 下游表中。

{{< hint info >}}

注意：基于正则捕获组的替换规则无法与 `replace-symbol` 选项搭配使用。

{{< /hint >}}

# 兼容性说明 — 前缀匹配导致的 Sink-Table 后缀泄漏

对 `source-table` 规则，`TableIdRouter` 先判断正则能否**完整匹配**整条 source 表 id，只有
匹配成功才会执行替换。因此，路由结果对"完整匹配"和"前缀匹配"的一个特定交互非常敏感：

- 正则能完整匹配整条 source 表 id（说明该规则整体上会生效），**且**
- 正则也能匹配同一 source 表 id 的**更短前缀**（说明替换只会作用在前缀上）。

当上述两个条件同时成立时，Flink CDC 3.6.0 会针对更短的前缀做替换，把未被匹配的尾部拼到
sink-table 名字之后。出现这种情况的典型场景是正则含有按"短到长"排列的多分支（alternation），
例如 `table_1|table_10`、`([1-9]|1[0-6])`，或者不带括号的 `[1-9]|1[0-6]`；是否带捕获组、
sink 模板里是否使用 `$N` 反向引用都与此无关。

从 Flink CDC 3.7.0 起，替换会作用在整个 source 表 id 上，不再出现后缀泄漏。下表展示了同样
的配置在 3.6.0 与 3.7.0 中分别会被路由到哪里，便于直接对比。

| Source 表 id         | source-table                              | sink-table                          | 3.6.0 中的路由结果                            | 3.7.0 中的路由结果                       |
|--------------------|-------------------------------------------|-------------------------------------|--------------------------------------------|-----------------------------------|
| `new_db_6.table_1` | `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix`                  | `new_db_6.table_1_suffix`         |
| `new_db_6.table_13`| `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_$1_suffix`          | `new_db_6.table_1_suffix3`                 | `new_db_6.table_13_suffix`        |
| `new_db_6.table_13`| `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged3`                   | `new_db_6.table_merged`           |
| `new_db_6.table_14`| `new_db_6.table_([1-9]|1[0-6])`           | `new_db_6.table_merged`             | `new_db_6.table_merged4`                   | `new_db_6.table_merged`           |

如何阅读这张表：

- 第一行是一位数字 source（`table_1`）的示例：两个版本结果恰好一致，因为"短前缀匹配"恰好
  已经覆盖了整个 id，没有剩余字符可以泄漏到 sink-table。
- 第二行展示 `$N` 反向引用情形：3.6.0 中捕获值被截断为首位数字（`1`），剩余字符被拼到
  `_suffix` 字面量之后；3.7.0 中捕获到完整的多位数字。
- 第三、四行展示不带 `$N` 反向引用的情形：3.6.0 中每个被匹配的 source 都会拼上自己的尾部
  （`merged3`、`merged4` 等），相当于一个 source 表对应一个 sink 表；3.7.0 中所有匹配的 source
  会汇聚到规则中声明的同一个 sink-table。

## 对现有任务的兼容性影响

本次行为变化影响所有同时满足"完整匹配"与"更短前缀匹配"的 `source-table` 规则：即同一
条 source 表 id 既能被该正则完整匹配，也能被它匹配为一个更短的前缀。实际中最常见的就是按
"短到长"排列的多分支 alternation，与是否带捕获组、是否使用 `$N` 反向引用都无关。仅使用
字面量表名或 `replace-symbol` 选项的规则不受影响。

对于**确实**会受影响的任务，从 3.6.0 升级到 3.7.0 后可能观察到以下行为变化：

- **原本被（错误）路由的 source 表，会落到不同的 sink-table 上。**
  - *使用 `$N` 反向引用时：* 捕获值由原来的截断子串变为完整的子串（例如从
    `..._table_1_suffix3` 变为 `..._table_13_suffix`）。
  - *不使用 `$N` 反向引用时：* 原本每个被匹配的 source 都会被拼上自己的尾部；修正后所有
    匹配的 source 会汇聚到规则中声明的同一个 sink-table（例如从 `table_merged3` /
    `table_merged4` / ... 全部变为 `table_merged`）。

  如果已经在下游创建过旧（错误）名字的表，它们在升级后将不再是 pipeline 的目标。请决定是
  重命名受影响的下游表，还是把已有数据迁移到新的（修正后的）sink 表中。

总结一下：升级之前，请审查所有 route 规则，定位那些对同一 source 表 id 既能"完整匹配"也能
匹配"更短前缀"的 source-table 正则，确认其匹配集合正是你期望的；如果有任务曾依赖 3.6.0 的
后缀泄漏行为，请决定是重命名受影响的下游表，还是把已有数据迁移到新的（修正后的）sink 表中。
