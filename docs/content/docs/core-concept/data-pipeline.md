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

# Definition
Since events in Flink CDC flow from the upstream to the downstream in a pipeline manner, the whole ETL task is referred as a **Data Pipeline**.

# Parameters
A pipeline corresponds to a chain of operators in Flink.   
To describe a Data Pipeline, the following parts are required:
- [source]({{< ref "docs/core-concept/data-source" >}})
- [sink]({{< ref "docs/core-concept/data-sink" >}})
- [pipeline](#pipeline-configurations)

the following parts are optional:
- [route]({{< ref "docs/core-concept/route" >}})
- [transform]({{< ref "docs/core-concept/transform" >}})

# Example
## Only required
We could use following yaml file to define a concise Data Pipeline describing synchronize all tables under MySQL app_db database to Doris :

```yaml
   pipeline:
     name: Sync MySQL Database to Doris
     parallelism: 2

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
```

## With optional
We could use following yaml file to define a complicated Data Pipeline describing synchronize all tables under MySQL app_db database to Doris and give specific target database name ods_db and specific target table name prefix ods_ :

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

# Pipeline Configurations

The following config options of Data Pipeline level are supported. 
Note that whilst the parameters are each individually optional, at least one of them must be specified. That is to say, The `pipeline` section is mandatory and cannot be empty.


| parameter                     | meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | optional/required |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `name`                        | The name of the pipeline, which will be submitted to the Flink cluster as the job name.                                                                                                                                                                                                                                                                                                                                                                                                                   | optional          |
| `parallelism`                 | The global parallelism of the pipeline. Defaults to 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                    | optional          |
| `local-time-zone`             | The local time zone defines current session time zone id.                                                                                                                                                                                                                                                                                                                                                                                                                                                 | optional          |
| `execution.runtime-mode`      | The runtime mode of the pipeline includes STREAMING and BATCH, with the default value being STREAMING.                                                                                                                                                                                                                                                                                                                                                                                                    | optional          |
| `schema.change.behavior`      | How to handle [changes in schema]({{< ref "docs/core-concept/schema-evolution" >}}). One of: [`exception`]({{< ref "docs/core-concept/schema-evolution" >}}#exception-mode), [`evolve`]({{< ref "docs/core-concept/schema-evolution" >}}#evolve-mode), [`try_evolve`]({{< ref "docs/core-concept/schema-evolution" >}}#tryevolve-mode), [`lenient`]({{< ref "docs/core-concept/schema-evolution" >}}#lenient-mode) (default) or [`ignore`]({{< ref "docs/core-concept/schema-evolution" >}}#ignore-mode). | optional          |
| `schema.operator.uid`         | The unique ID for schema operator. This ID will be used for inter-operator communications and must be unique across operators. **Deprecated**: use `operator.uid.prefix` instead.                                                                                                                                                                                                                                                                                                                         | optional          |
| `schema-operator.rpc-timeout` | The timeout time for SchemaOperator to wait downstream SchemaChangeEvent applying finished, the default value is 3 minutes.                                                                                                                                                                                                                                                                                                                                                                               | optional          |
| `operator.uid.prefix`         | The prefix to use for all pipeline operator UIDs. If not set, all pipeline operator UIDs will be generated by Flink. It is recommended to set this parameter to ensure stable and recognizable operator UIDs, which can help with stateful upgrades, troubleshooting, and Flink UI diagnostics.                                                                                                                                                                                                           | optional          |

NOTE: Whilst the above parameters are each individually optional, at least one of them must be specified. The `pipeline` section is mandatory and cannot be empty.

