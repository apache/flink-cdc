---
title: "Introduction"
weight: 1
type: docs
aliases:
  - /get-started/introduction/
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

# Welcome to Flink CDC 🎉

Flink CDC is a streaming data integration tool that aims to provide users with
a more robust API. It allows users to describe their ETL pipeline logic via YAML
elegantly and help users automatically generating customized Flink operators and
submitting job. Flink CDC prioritizes optimizing the task submission process and
offers enhanced functionalities such as schema evolution, data transformation,
full database synchronization and exactly-once semantic.

Deeply integrated with and powered by Apache Flink, Flink CDC provides:

* ✅ End-to-end data integration framework
* ✅ API for data integration users to build jobs easily
* ✅ Multi-table support in Source / Sink
* ✅ Synchronization of entire databases
* ✅ Schema evolution capability

## How to Use Flink CDC

Flink CDC provides an YAML-formatted user API that more suitable for data
integration scenarios. Here's an example YAML file defining a data pipeline that
ingests real-time changes from MySQL, and synchronize them to Apache Doris:

```yaml
source:
  type: mysql
  hostname: localhost
  port: 3306
  username: root
  password: 123456
  tables: app_db.\.*
  server-id: 5400-5404
  server-time-zone: UTC

sink:
  type: doris
  fenodes: 127.0.0.1:8030
  username: root
  password: ""
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1

pipeline:
  name: Sync MySQL Database to Doris
  parallelism: 2
```

By submitting the YAML file with `flink-cdc.sh`, a Flink job will be compiled
and deployed to a designated Flink cluster. Please refer to [Core Concept]({{<
ref "docs/core-concept/data-pipeline" >}}) to get full documentation of all
supported functionalities of a pipeline.

## Write Your First Flink CDC Pipeline

Explore Flink CDC document to get hands on your first real-time data integration
pipeline:

### Quickstart

Check out the quickstart guide to learn how to establish a Flink CDC pipeline:

- [MySQL to Apache Doris]({{< ref "docs/get-started/quickstart/mysql-to-doris" >}})
- [MySQL to StarRocks]({{< ref "docs/get-started/quickstart/mysql-to-starrocks" >}})

### Understand Core Concepts

Get familiar with core concepts we introduced in Flink CDC and try to build 
more complex pipelines:

- [Data Pipeline]({{< ref "docs/core-concept/data-pipeline" >}})
- [Data Source]({{< ref "docs/core-concept/data-source" >}})
- [Data Sink]({{< ref "docs/core-concept/data-sink" >}})
- [Table ID]({{< ref "docs/core-concept/table-id" >}})
- [Transform]({{< ref "docs/core-concept/transform" >}})
- [Route]({{< ref "docs/core-concept/route" >}})

### Submit Pipeline to Flink Cluster

Learn how to submit the pipeline to Flink cluster running on different 
deployment mode:

- [standalone]({{< ref "docs/deployment/standalone" >}})
- [Kubernetes]({{< ref "docs/deployment/kubernetes" >}})
- [YARN]({{< ref "docs/deployment/yarn" >}})

## Development and Contribution

If you want to connect Flink CDC to your customized external system, or 
contributing to the framework itself, these sections could be helpful:

- Understand [Flink CDC APIs]({{< ref "docs/developer-guide/understand-flink-cdc-api" >}}) 
  to develop your own Flink CDC connector
- Learn about how to [contributing to Flink CDC]({{< ref "docs/developer-guide/contribute-to-flink-cdc" >}})
- Check out [licenses]({{< ref "docs/developer-guide/licenses" >}}) used by Flink CDC
