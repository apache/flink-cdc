---
title: "Table ID"
weight: 4
type: docs
aliases:
  - /core-concept/table-id/
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

**Table Id** 主要用来建立连接器与外部系统存储对象（例如表）的映射关系，所有外部系统的数据集合都可以映射到一个专用的 Table Id。

# 示例

为了兼容绝大部分的外部数据系统，Table Id 由命名空间、模式名、表名 3 个部分组成，即元组 (namespace, schemaName, tableName)。
不同的外部系统 Table Id 的构造参数可能不同，比如 Oracle 数据库的 Table Id 包含命名空间（database）、模式名（schema）和表名（table），Doris 的 Table Id 包含模式名（database）和表名（table），Kafka 的 Table Id 仅包含表名（topic）。

下面是一些常见的外部数据系统及其对应的 Table Id 格式：

| 数据系统                  | Table Id 构造参数           | 字符串示例               |
|-----------------------|-------------------------|---------------------|
| Oracle/PostgreSQL     | database, schema, table | mydb.default.orders |
| MySQL/Doris/StarRocks | database, table         | mydb.orders         |
| Kafka                 | topic                   | orders              |
