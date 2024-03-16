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
Since events in Flink CDC flow from the upstream to the downstream in a pipeline manner, the data synchronization task is also referred as a Data Pipeline.

# Parameters
A pipeline corresponds to a chain of operators in Flink.   
To describe a Data Pipeline, the following parts are required:
- [source](data-source.md)
- [sink](data-sink.md)
- [pipeline](data-pipeline.md#global-parameters) (global parameters)

the following parts are optional:
- [route](route.md)
- [transform](transform.md)

# Example
## Only required
We could use this concise yaml file to define a pipeline:

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

## With optional
We could use this complicated yaml file to define a pipeline:

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
```

# Global Parameters
The following parameters are global parameters of the pipeline:

| parameter   | meaning         |
|-------------|--------------------------|
| name        | The name of the pipeline, which will be submitted to the Flink cluster as the job name.     |
| parallelism  | The global parallelism of the pipeline.           |
| local-time-zone | The local time zone defines current session time zone id.                    |