---
title: "Data Source"
weight: 2
type: docs
aliases:
  - /core-concept/data-source/
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
**Data Source** 是 用来访问元数据以及从外部系统读取变更数据的组件。
一个 Data Source 可以同时访问多个表。

# 参数
为了定义一个 Data Source，需要提供以下参数：

| 参数                            | 含义                                | optional/required |
|-------------------------------|-----------------------------------|-------------------|
| type                          | source 的类型，例如 mysql。              | required          |
| name                          | source 的名称，允许用户配置 (提供了一个默认值)。     | optional          |
| configurations of Data Source | 用于构建 source 组件的配置，例如连接参数或者表属性的配置。 | optional          |

# 示例
我们可以使用yaml文件来定义一个mysql source：
```yaml
source:
    type: mysql
    name: mysql-source   #optional，description information
    host: localhost
    port: 3306
    username: admin
    password: pass
    tables: adb.*, bdb.user_table_[0-9]+, [app|web]_order_\.*
```