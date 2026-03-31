---
title: "项目介绍"
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

# 欢迎使用 Flink CDC 🎉

Flink CDC 是一个基于流的数据集成工具，旨在为用户提供一套功能更加全面的编程接口（API）。
该工具使得用户能够以 YAML 配置文件的形式，优雅地定义其 ETL（Extract, Transform, Load）流程，并协助用户自动化生成定制化的 Flink 算子并且提交 Flink 作业。
Flink CDC 在任务提交过程中进行了优化，并且增加了一些高级特性，如表结构变更自动同步（Schema Evolution）、数据转换（Data Transformation）、整库同步（Full Database Synchronization）以及 精确一次（Exactly-once）语义。

Flink CDC 深度集成并由 Apache Flink 驱动，提供以下核心功能：
* ✅ 端到端的数据集成框架
* ✅ 为数据集成的用户提供了易于构建作业的 API
* ✅ 支持在 Source 和 Sink 中处理多个表
* ✅ 整库同步
* ✅具备表结构变更自动同步的能力（Schema Evolution），

## 环境要求

Flink CDC 有以下环境要求：

* **JDK**：JDK 11 或更高版本（Flink CDC 从 3.6.0 版本开始基于 JDK 11 构建）
* **Apache Flink**：Flink 1.20.x 或 Flink 2.2.x

{{< hint info >}}
在运行 Flink CDC 之前，请确保已安装正确的 JDK 版本。您可以使用 `java -version` 命令验证 Java 版本。
{{< /hint >}}

## 支持的连接器

Flink CDC 提供了丰富的连接器生态系统，用于与各种外部系统进行交互：

| 连接器           | 类型                                                                                                                                                                     |
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

关于连接器下载链接，请访问 [Flink Source Connectors]({{< ref "docs/connectors/flink-sources/overview#supported-connectors" >}}) 和 [Pipeline Connectors]({{< ref "docs/connectors/pipeline-connectors/overview#supported-connectors" >}}) 页面。

## 如何使用 Flink CDC

Flink CDC 提供了基于 `YAML` 格式的用户 API，更适合于数据集成场景。以下是一个 `YAML` 文件的示例，它定义了一个数据管道(Pipeline)，该Pipeline从 MySQL 捕获实时变更，并将它们同步到 Apache Doris：


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

通过使用 `flink-cdc.sh` 提交 YAML 文件，一个 Flink 作业将会被编译并部署到指定的 Flink 集群。 
请参考 [核心概念]({{<ref "docs/core-concept/data-pipeline" >}}) 以获取 Pipeline 支持的所有功能的完整文档说明。

## 编写你的第一个 Flink CDC Pipeline

浏览 Flink CDC 文档，开始创建您的第一个实时数据集成管道(Pipeline)。

### 快速开始

查看快速入门指南，了解如何建立一个 Flink CDC Pipeline：

| 示例 | 版本 |
|---------|---------|
| MySQL to Apache Doris | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-doris" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-doris" >}}) |
| MySQL to StarRocks | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-starrocks" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-starrocks" >}}) |
| MySQL to Kafka | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/mysql-to-kafka" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/mysql-to-kafka" >}}) |
| PostgreSQL to Fluss | [1.20.x]({{< ref "docs/get-started/quickstart-for-1.20/postgres-to-fluss" >}}) / [2.2.x]({{< ref "docs/get-started/quickstart-for-2.2/postgres-to-fluss" >}}) |

### 理解核心概念

熟悉我们在 Flink CDC 中引入的核心概念，并尝试构建更复杂的数据Pipeline：

- [Data Pipeline]({{< ref "docs/core-concept/data-pipeline" >}})
- [Data Source]({{< ref "docs/core-concept/data-source" >}})
- [Data Sink]({{< ref "docs/core-concept/data-sink" >}})
- [Table ID]({{< ref "docs/core-concept/table-id" >}})
- [Transform]({{< ref "docs/core-concept/transform" >}})
- [Route]({{< ref "docs/core-concept/route" >}})

### 提交 Pipeline 到 Flink 集群

了解如何将 Pipeline 提交到运行在不同部署模式下的 Flink 集群：

- [standalone]({{< ref "docs/deployment/standalone" >}})
- [Kubernetes]({{< ref "docs/deployment/kubernetes" >}})
- [YARN]({{< ref "docs/deployment/yarn" >}})

## 开发与贡献

如果您想要将 Flink CDC 连接到您定制化的外部系统，或者想要为框架本身做出贡献，以下这些部分可能会有所帮助：

- [理解 Flink CDC API]({{< ref "docs/developer-guide/understand-flink-cdc-api" >}})，开发您自己的Flink CDC 连接器。
- 了解如何[向 Flink CDC 提交贡献]({{< ref "docs/developer-guide/contribute-to-flink-cdc" >}})
- 查看 Flink CDC 使用的[许可证]({{< ref "docs/developer-guide/licenses" >}})

