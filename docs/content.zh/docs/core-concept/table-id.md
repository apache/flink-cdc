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
在连接外部系统时，有必要建立一个与外部系统存储对象（例如表）的映射关系。这就是 **Table Id** 所代表的含义。

# 示例
为了兼容大部分外部系统，Table Id 被表示为 3 元组：(namespace, schemaName, tableName)。
连接器应该在连接外部系统时建立与外部系统存储对象的映射关系。

下面是不同数据系统对应的 tableId 的格式：

| 数据系统                  | tableId 的组成             | 字符串示例               |
|-----------------------|-------------------------|---------------------|
| Oracle/PostgreSQL     | database, schema, table | mydb.default.orders |
| MySQL/Doris/StarRocks | database, table         | mydb.orders         |
| Kafka                 | topic                   | orders              |
