---
title: "Overview"
weight: 1
type: docs
aliases:
- /connectors/pipeline-connectors/overview
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

# Connectors

Flink CDC provides several source and sink connectors to interact with external
systems. You can use these connectors out-of-box, by adding released JARs to
your Flink CDC environment, and specifying the connector in your YAML pipeline
definition.

## Supported Connectors

| Connector                                                                        | Supported Type | External System                                                                                                                                                                                                                                                                                                                                                                                        | Download Page                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|----------------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Apache Doris]({{< ref "docs/connectors/pipeline-connectors/doris" >}})          | Sink           | <li> [Apache Doris](https://doris.apache.org/): 1.2.x, 2.x.x, 3.x.x                                                                                                                                                                                                                                                                                                                                    | [Apache Doris](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-doris) |
| [Elasticsearch]({{< ref "docs/connectors/pipeline-connectors/elasticsearch" >}}) | Sink           | <li> [Elasticsearch](https://www.elastic.co/elasticsearch): 6.x, 7.x, 8.x                                                                                                                                                                                                                                                                                                                              | [Elasticsearch](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-elasticsearch) |
| [Fluss]({{< ref "docs/connectors/pipeline-connectors/fluss" >}})                  | Sink           | <li> [Fluss](https://fluss.apache.org/): 0.7, 0.8, 0.9                                                                                                                                                                                                                                                                                                                                                                | [Fluss](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-fluss) |
| [Hudi]({{< ref "docs/connectors/pipeline-connectors/hudi" >}})                   | Sink           | <li> [Apache Hudi](https://hudi.apache.org/)                                                                                                                                                                                                                                                                                                                                                           | [Apache Hudi](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-hudi) |
| [Iceberg]({{< ref "docs/connectors/pipeline-connectors/iceberg" >}})             | Sink           | <li> [Apache Iceberg](https://iceberg.apache.org/): 1.6, 1.7, 1.8, 1.9, 1.10                                                                                                                                                                                                                                                                                                                                                    | [Apache Iceberg](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-iceberg) |
| [Kafka]({{< ref "docs/connectors/pipeline-connectors/kafka" >}})                 | Sink           | <li> [Kafka](https://kafka.apache.org/)                                                                                                                                                                                                                                                                                                                                                                | [Kafka](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-kafka) |
| [MaxCompute]({{< ref "docs/connectors/pipeline-connectors/maxcompute" >}})       | Sink           | <li> [MaxCompute](https://www.aliyun.com/product/maxcompute)                                                                                                                                                                                                                                                                                                                                           | [MaxCompute](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-maxcompute) |
| [MySQL]({{< ref "docs/connectors/pipeline-connectors/mysql" >}})                 | Source         | <li> [MySQL](https://dev.mysql.com/doc): 5.7, 8.0.x, 8.4+ <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | [MySQL](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-mysql) |
| [OceanBase]({{< ref "docs/connectors/pipeline-connectors/oceanbase" >}})         | Sink           | <li> [OceanBase](https://www.oceanbase.com/): 3.x, 4.x                                                                                                                                                                                                                                                                                                                                                 | [OceanBase](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-oceanbase) |
| [Oracle]({{< ref "docs/connectors/pipeline-connectors/oracle" >}})               | Source         | <li> [Oracle](https://www.oracle.com/database/)                                                                                                                                                                                                                                                                                                                                                        | [Oracle](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-oracle) |
| [Paimon]({{< ref "docs/connectors/pipeline-connectors/paimon" >}})               | Sink           | <li> [Paimon](https://paimon.apache.org/): 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3                                                                                                                                                                                                                                                                                                                                     | [Paimon](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-paimon) |
| [Postgres]({{< ref "docs/connectors/pipeline-connectors/postgres" >}})           | Source         | <li> [PostgreSQL](https://www.postgresql.org/)                                                                                                                                                                                                                                                                                                                                                         | [Postgres](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-postgres) |
| [StarRocks]({{< ref "docs/connectors/pipeline-connectors/starrocks" >}})         | Sink           | <li> [StarRocks](https://www.starrocks.io/): 2.x, 3.x                                                                                                                                                                                                                                                                                                                                                  | [StarRocks](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-starrocks) |


## Supported Flink Versions

The following table shows the version mapping between Flink CDC Pipeline Connectors and Flink:

| Flink CDC Version |       Flink Version       |      Pipeline Source      |                                        Pipeline Sink                                        |                  Notes                   |
|:-----------------:|:-------------------------:|:-------------------------:|:-------------------------------------------------------------------------------------------:|:----------------------------------------:|
|     **3.6.x**     |      1.20.\*, 2.2.\*      | MySQL, PostgreSQL, Oracle | StarRocks, Doris, Paimon, Kafka, Elasticsearch, OceanBase, MaxCompute, Iceberg, Fluss, Hudi |                                          |
|     **3.5.x**     |     1.19.\*, 1.20.\*      |     MySQL, PostgreSQL     |    StarRocks, Doris, Paimon, Kafka, Elasticsearch, OceanBase, MaxCompute, Iceberg, Fluss    |                                          |
|     **3.4.x**     |     1.19.\*, 1.20.\*      |           MySQL           |       StarRocks, Doris, Paimon, Kafka, Elasticsearch, OceanBase, MaxCompute, Iceberg        |                                          |
|     **3.3.x**     |     1.19.\*, 1.20.\*      |           MySQL           |            StarRocks, Doris, Paimon, Kafka, Elasticsearch, OceanBase, MaxCompute            |                                          |
|     **3.2.x**     | 1.17.\*, 1.18.\*, 1.19.\* |           MySQL           |                       StarRocks, Doris, Paimon, Kafka, Elasticsearch                        |                                          |
|     **3.1.x**     | 1.17.\*, 1.18.\*, 1.19.\* |           MySQL           |                               StarRocks, Doris, Paimon, Kafka                               | Only flink-cdc 3.1.1 supports Flink 1.19 |
|     **3.0.x**     |     1.17.\*, 1.18.\*      |           MySQL           |                                      StarRocks, Doris                                       |                                          |


## Develop Your Own Connector

If provided connectors cannot fulfill your requirement, you can always develop
your own connector to get your external system involved in Flink CDC pipelines.
Check out [Flink CDC APIs]({{< ref "docs/developer-guide/understand-flink-cdc-api" >}})
to learn how to develop your own connectors.

{{< top >}}
