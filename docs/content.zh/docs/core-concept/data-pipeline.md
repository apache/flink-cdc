---
title: "Data Pipeline"
weight: 1
type: docs
aliases:
  - /core-concept/data-pipeline/
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
由于在 Flink CDC 中，事件从上游流转到下游遵循 Pipeline 的模式，因此整个 ETL 作业也被称为 **Data Pipeline**。

# 参数
一个 pipeline 包含着 Flink 的一组算子链。
为了描述 Data Pipeline，我们需要定义以下部分：
- [source]({{< ref "docs/core-concept/data-source" >}})
- [sink]({{< ref "docs/core-concept/data-sink" >}})
- [pipeline](#pipeline-configurations)

下面 是 Data Pipeline 的一些可选配置：
- [route]({{< ref "docs/core-concept/route" >}})
- [transform]({{< ref "docs/core-concept/transform" >}})

# 示例
## 只包含必须部分
我们可以使用以下 yaml 文件来定义一个简单的 Data Pipeline 来同步 MySQL app_db 数据库下的所有表到 Doris：

```yaml
   source:
     type: mysql
     hostname: localhost
     port: 3306
     username: root
     password: 123456
     tables: app_db.\.*

   sink:
     type: doris
     fenodes: 127.0.0.1:8030
     username: root
     password: ""

   pipeline:
     name: Sync MySQL Database to Doris
     parallelism: 2
```

## 包含可选部分
我们可以使用以下 yaml 文件来定义一个复杂的 Data Pipeline 来同步 MySQL app_db 数据库下的所有表到 Doris，并给目标数据库名 ods_db 和目标表名前缀 ods_：

```yaml
   source:
     type: mysql
     hostname: localhost
     port: 3306
     username: root
     password: 123456
     tables: app_db.\.*

   sink:
     type: doris
     fenodes: 127.0.0.1:8030
     username: root
     password: ""

   transform:
     - source-table: adb.web_order01
       projection: \*, format('%S', product_name) as product_name
       filter: addone(id) > 10 AND order_id > 100
       description: project fields and filter
     - source-table: adb.web_order02
       projection: \*, format('%S', product_name) as product_name
       filter: addone(id) > 20 AND order_id > 200
       description: project fields and filter

   route:
     - source-table: app_db.orders
       sink-table: ods_db.ods_orders
     - source-table: app_db.shipments
       sink-table: ods_db.ods_shipments
     - source-table: app_db.products
       sink-table: ods_db.ods_products

   pipeline:
     name: Sync MySQL Database to Doris
     parallelism: 2
     user-defined-function:
       - name: addone
         classpath: com.example.functions.AddOneFunctionClass
       - name: format
         classpath: com.example.functions.FormatFunctionClass
```

# Pipeline 配置
下面 是 Data Pipeline 的一些可选配置：

| 参数                     | 含义                                                                                                        | optional/required |
|------------------------|-----------------------------------------------------------------------------------------------------------|-------------------|
| name                   | 这个 pipeline 的名称，会用在 Flink 集群中作为作业的名称。                                                                     | optional          |
| parallelism            | pipeline的全局并发度，默认值是1。                                                                                     | optional          |
| local-time-zone        | 作业级别的本地时区。                                                                                                | optional          |
| execution.runtime-mode | pipeline 的运行模式，包含 STREAMING 和 BATCH，默认值是 STREAMING。                                                       | optional          |
| operator.uid.prefix    | Pipeline 中算子 UID 的前缀。如果不设置，Flink 会为每个算子生成唯一的 UID。 建议设置这个参数以提供稳定和可识别的算子 ID，这有助于有状态升级、问题排查和在 Flink UI 上的诊断。 | optional          |
