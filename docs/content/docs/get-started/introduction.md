---
title: "Introduction"
slug: "/docs/get-started/introduction"
sidebar_position: 1
hide_title: true
displayed_sidebar: docs
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

:::info

Make sure you have the correct JDK version installed before running Flink CDC. You can verify your Java version with `java -version`.

:::

## Supported Connectors

Flink CDC provides a rich ecosystem of connectors to interact with various external systems:

| Connector     | Type                                                                                                                                                                   |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL         | [Source Connector](../connectors/flink-sources/mysql-cdc.md) / [Pipeline Source Connector](../connectors/pipeline-connectors/mysql.md)       |
| Oracle        | [Source Connector](../connectors/flink-sources/oracle-cdc.md) / [Pipeline Source Connector](../connectors/pipeline-connectors/oracle.md)     |
| PostgreSQL    | [Source Connector](../connectors/flink-sources/postgres-cdc.md) / [Pipeline Source Connector](../connectors/pipeline-connectors/postgres.md) |
| Db2           | [Source Connector](../connectors/flink-sources/db2-cdc.md)                                                                                                |
| MongoDB       | [Source Connector](../connectors/flink-sources/mongodb-cdc.md)                                                                                            |
| SQL Server    | [Source Connector](../connectors/flink-sources/sqlserver-cdc.md)                                                                                          |
| TiDB          | [Source Connector](../connectors/flink-sources/tidb-cdc.md)                                                                                               |
| Vitess        | [Source Connector](../connectors/flink-sources/vitess-cdc.md)                                                                                             |
| Apache Doris  | [Pipeline Sink Connector](../connectors/pipeline-connectors/doris.md)                                                                                     |
| Elasticsearch | [Pipeline Sink Connector](../connectors/pipeline-connectors/elasticsearch.md)                                                                             |
| Fluss         | [Pipeline Sink Connector](../connectors/pipeline-connectors/fluss.md)                                                                                     |
| Hudi          | [Pipeline Sink Connector](../connectors/pipeline-connectors/hudi.md)                                                                                      |
| Iceberg       | [Pipeline Sink Connector](../connectors/pipeline-connectors/iceberg.md)                                                                                   |
| Kafka         | [Pipeline Sink Connector](../connectors/pipeline-connectors/kafka.md)                                                                                     |
| MaxCompute    | [Pipeline Sink Connector](../connectors/pipeline-connectors/maxcompute.md)                                                                                |
| OceanBase     | [Pipeline Sink Connector](../connectors/pipeline-connectors/oceanbase.md)                                                                                 |
| Paimon        | [Pipeline Sink Connector](../connectors/pipeline-connectors/paimon.md)                                                                                    |
| StarRocks     | [Pipeline Sink Connector](../connectors/pipeline-connectors/starrocks.md)                                                                                 |

For connector download links, please visit the [Flink Source Connectors](../connectors/flink-sources/overview.md#supported-connectors) and [Pipeline Connectors](../connectors/pipeline-connectors/overview.md#supported-connectors) pages.

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
and deployed to a designated Flink cluster. Please refer to [Core Concept](../core-concept/data-pipeline.md) to get full documentation of all
supported functionalities of a pipeline.

## Write Your First Flink CDC Pipeline

Explore Flink CDC document to get hands on your first real-time data integration
pipeline:

### Quickstart

Check out the quickstart guide to learn how to establish a Flink CDC pipeline:

| Example | Version |
|---------|---------|
| MySQL to Apache Doris | [1.20.x](quickstart-for-1.20/mysql-to-doris.md) / [2.2.x](quickstart-for-2.2/mysql-to-doris.md) |
| MySQL to StarRocks | [1.20.x](quickstart-for-1.20/mysql-to-starrocks.md) / [2.2.x](quickstart-for-2.2/mysql-to-starrocks.md) |
| MySQL to Kafka | [1.20.x](quickstart-for-1.20/mysql-to-kafka.md) / [2.2.x](quickstart-for-2.2/mysql-to-kafka.md) |
| PostgreSQL to Fluss | [1.20.x](quickstart-for-1.20/postgres-to-fluss.md) / [2.2.x](quickstart-for-2.2/postgres-to-fluss.md) |

### Understand Core Concepts

Get familiar with core concepts we introduced in Flink CDC and try to build 
more complex pipelines:

- [Data Pipeline](../core-concept/data-pipeline.md)
- [Data Source](../core-concept/data-source.md)
- [Data Sink](../core-concept/data-sink.md)
- [Table ID](../core-concept/table-id.md)
- [Transform](../core-concept/transform.md)
- [Route](../core-concept/route.md)

### Submit Pipeline to Flink Cluster

Learn how to submit the pipeline to Flink cluster running on different 
deployment mode:

- [standalone](../deployment/standalone.md)
- [Kubernetes](../deployment/kubernetes.md)
- [YARN](../deployment/yarn.md)

## Development and Contribution

If you want to connect Flink CDC to your customized external system, or 
contributing to the framework itself, these sections could be helpful:

- Understand [Flink CDC APIs](../developer-guide/understand-flink-cdc-api.md)
  to develop your own Flink CDC connector
- Learn about how to [contributing to Flink CDC](../developer-guide/contribute-to-flink-cdc.md)
- Check out [licenses](../developer-guide/licenses.md) used by Flink CDC
