---
title: "Data Sink"
weight: 3
type: docs
aliases:
  - /core-concept/data-sink/
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
**Data Sink** 是 用来应用 schema 变更并写入 change data 到外部系统的组件。
一个 Data Sink 可以同时写入多个表。

# 参数
为了定义一个 Data Sink，需要提供以下参数：

| 参数                          | 含义                              | optional/required |
|-----------------------------|---------------------------------|-------------------|
| type                        | sink 的类型，例如 doris 或者 starrocks。 | required          |
| name                        | sink 的名称，允许用户配置 (提供了一个默认值)。     | optional          |
| configurations of Data Sink | 用于构建 sink 组件的配置，例如连接参数或者表属性的配置。 | optional          |

# 示例
我们可以使用以下的 yaml 文件来定义一个 doris sink：
```yaml
sink:
    type: doris
    name: doris-sink           	# Optional parameter for description purpose
    fenodes: 127.0.0.1:8030
    username: root
    password: ""
    table.create.properties.replication_num: 1      	# Optional parameter for advanced functionalities
```