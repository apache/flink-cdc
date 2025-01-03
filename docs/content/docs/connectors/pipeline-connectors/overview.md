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

| Connector                                                                        | Supported Type | External System                                                                                                                                                                                                                                                                                                                                                                                        | 
|----------------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Apache Doris]({{< ref "docs/connectors/pipeline-connectors/doris" >}})          | Sink           | <li> [Apache Doris](https://doris.apache.org/): 1.2.x, 2.x.x                                                                                                                                                                                                                                                                                                                                           | 
| [Elasticsearch]({{< ref "docs/connectors/pipeline-connectors/elasticsearch" >}}) | Sink           | <li> [Elasticsearch](https://www.elastic.co/elasticsearch): 6.x, 7.x, 8.x                                                                                                                                                                                                                                                                                                                              |
| [Kafka]({{< ref "docs/connectors/pipeline-connectors/kafka" >}})                 | Sink           | <li> [Kafka](https://kafka.apache.org/)                                                                                                                                                                                                                                                                                                                                                                | 
| [MySQL]({{< ref "docs/connectors/pipeline-connectors/mysql" >}})                 | Source         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | 
| [Paimon]({{< ref "docs/connectors/pipeline-connectors/paimon" >}})               | Sink           | <li> [Paimon](https://paimon.apache.org/): 0.6, 0.7, 0.8                                                                                                                                                                                                                                                                                                                                               |
| [StarRocks]({{< ref "docs/connectors/pipeline-connectors/starrocks" >}})         | Sink           | <li> [StarRocks](https://www.starrocks.io/): 2.x, 3.x                                                                                                                                                                                                                                                                                                                                                  |

## Develop Your Own Connector

If provided connectors cannot fulfill your requirement, you can always develop
your own connector to get your external system involved in Flink CDC pipelines.
Check out [Flink CDC APIs]({{< ref "docs/developer-guide/understand-flink-cdc-api" >}})
to learn how to develop your own connectors.

{{< top >}}
