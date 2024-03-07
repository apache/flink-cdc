---
title: "Overview"
weight: 1
type: docs
aliases:
  - /connectors/pipeline-connectors/
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

# Pipeline Connectors Of CDC Streaming ELT Framework

## Supported Connectors

| Connector                                   | Database                                                                                                                                                                                                                                                                                                                                                                                               | 
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [doris-pipeline](doris-pipeline.md)         | <li> [Doris](https://doris.apache.org/): 1.2.x, 2.x.x                                                                                                                                                                                                                                                                                                                                                  | 
| [mysql-pipeline](mysql-pipeline.md)         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | 
| [starrocks-pipeline](starrocks-pipeline.md) | <li> [StarRocks](https://www.starrocks.io/): 2.x, 3.x                                                                                                                                                                                                                                                                                                                                                  | 

## Supported Flink Versions
The following table shows the version mapping between Flink<sup>®</sup> CDC Pipeline and Flink<sup>®</sup>:

|    Flink<sup>®</sup> CDC Version    |                                                                                                      Flink<sup>®</sup> Version                                                                                                       |
|:-----------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| <font color="DarkCyan">3.0.*</font> | <font color="MediumVioletRed">1.14.\*</font>, <font color="MediumVioletRed">1.15.\*</font>, <font color="MediumVioletRed">1.16.\*</font>, <font color="MediumVioletRed">1.17.\*</font>, <font color="MediumVioletRed">1.18.\*</font> |

{{< top >}}
