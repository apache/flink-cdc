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

## Requirements

Flink CDC has the following requirements:

* **JDK**: JDK 11 or later (Flink CDC is built on JDK 11 since 3.6.0)
* **Apache Flink**: Flink 1.20.x or Flink 2.2.x

{{< hint info >}}
Make sure you have the correct JDK version installed before running Flink CDC. You can verify your Java version with `java -version`.
{{< /hint >}}

## Supported Connectors

Flink CDC provides a rich ecosystem of connectors to interact with various external systems:

| Connector     | Type                                                                                                                                                                   |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL         | [Source Connector]({{< ref "docs/connectors/flink-sources/mysql-cdc" >}}) / [Pipeline Source Connector]({{< ref "docs/connectors/pipeline-connectors/mysql" >}})       |
| Oracle        | [Source Connector]({{< ref "docs/connectors/flink-sources/oracle-cdc" >}}) / [Pipeline Source Connector]({{< ref "docs/connectors/pipeline-connectors/oracle" >}})     |
| PostgreSQL    | [Source Connector]({{< ref "docs/connectors/flink-sources/postgres-cdc" >}}) / [Pipeline Source Connector]({{< ref "docs/connectors/pipeline-connectors/postgres" >}}) |
| Db2           | [Source Connector]({{< ref "docs/connectors/flink-sources/db2-cdc" >}})                                                                                                |
| MongoDB       | [Source Connector]({{< ref "docs/connectors/flink-sources/mongodb-cdc" >}})                                                                                            |
| SQL Server    | [Source Connector]({{< ref "docs/connectors/flink-sources/sqlserver-cdc" >}})                                                                                          |
| TiDB          | [Source Connector]({{< ref "docs/connectors/flink-sources/tidb-cdc" >}})                                                                                               |
| Vitess        | [Source Connector]({{< ref "docs/connectors/flink-sources/vitess-cdc" >}})                                                                                             |
| Apache Doris  | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/doris" >}})                                                                                     |
| Elasticsearch | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/elasticsearch" >}})                                                                             |
| Fluss         | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/fluss" >}})                                                                                     |
| Hudi          | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/hudi" >}})                                                                                      |
| Iceberg       | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/iceberg" >}})                                                                                   |
| Kafka         | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/kafka" >}})                                                                                     |
| MaxCompute    | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/maxcompute" >}})                                                                                |
| OceanBase     | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/oceanbase" >}})                                                                                 |
| Paimon        | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/paimon" >}})                                                                                    |
| StarRocks     | [Pipeline Sink Connector]({{< ref "docs/connectors/pipeline-connectors/starrocks" >}})                                                                                 |

For connector download links, please visit the [Flink Source Connectors]({{< ref "docs/connectors/flink-sources/overview#supported-connectors" >}}) and [Pipeline Connectors]({{< ref "docs/connectors/pipeline-connectors/overview#supported-connectors" >}}) pages.

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

| Example | Version |
|---------|---------|
| MySQL to Apache Doris | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-doris" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-doris" >}}) |
| MySQL to StarRocks | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-starrocks" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-starrocks" >}}) |
| MySQL to Kafka | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-kafka" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-kafka" >}}) |
| PostgreSQL to Fluss | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/postgres-to-fluss" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/postgres-to-fluss" >}}) |

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
