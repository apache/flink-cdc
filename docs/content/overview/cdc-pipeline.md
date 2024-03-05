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

# CDC Streaming ELT Framework

## What is CDC Streaming ELT Framework
CDC Streaming ELT Framework is a stream data integration framework that aims to provide users with a more robust API. It allows users to configure their data synchronization logic through customized Flink operators and job submission tools. The framework prioritizes optimizing the task submission process and offers enhanced functionalities such as whole database synchronization, sharding, and schema change synchronization.

## What can CDC Streaming ELT Framework do?
![CDC Architecture](/_static/fig/architecture.png "CDC Architecture")
* ✅ End-to-end data integration framework
* ✅ API for data integration users to build jobs easily
* ✅ Multi-table support in Source / Sink
* ✅ Synchronization of entire databases 
* ✅ Schema evolution capability


## Supported Connectors

| Connector                                                | Database                                                                                                                                                                                                                                                                                                                                                                                               | 
|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [doris-pipeline](../pipelines/doris-pipeline.md)         | <li> [Doris](https://doris.apache.org/): 1.2.x, 2.x.x                                                                                                                                                                                                                                                                                                                                                  | 
| [mysql-pipeline](../pipelines/mysql-pipeline.md)         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | 
| [starrocks-pipeline](../pipelines/starrocks-pipeline.md) | <li> [StarRocks](https://www.starrocks.io/): 2.x, 3.x                                                                                                                                                                                                                                                                                                                                                     | 

## Supported Flink Versions
The following table shows the version mapping between Flink<sup>®</sup> CDC Pipeline and Flink<sup>®</sup>:

|    Flink<sup>®</sup> CDC Version    |                                                                                                      Flink<sup>®</sup> Version                                                                                                      |
|:-----------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| <font color="DarkCyan">3.0.*</font> | <font color="MediumVioletRed">1.14.\*</font>, <font color="MediumVioletRed">1.15.\*</font>, <font color="MediumVioletRed">1.16.\*</font>, <font color="MediumVioletRed">1.17.\*</font>, <font color="MediumVioletRed">1.18.\*</font> |

## Core Concepts
![CDC Design](/_static/fig/design.png "CDC Design")

The data types flowing in the Flink CDC 3.0 framework are referred to as **Event**, which represent the change events generated by external systems.
Each event is marked with a **Table ID** for which the change occurred. Events are categorized into `SchemaChangeEvent` and `DataChangeEvent`, representing changes in table structure and data respectively.

**Data Source** Connector captures the changes in external systems and converts them into events as the output of the synchronization task. It also provides a `MetadataAccessor` for the framework to read the metadata of the external systems.

**Data Sink** connector receives the change events from **Data Source** and applies them to the external systems. Additionally, `MetadataApplier` is used to apply metadata changes from the source system to the target system.

Since events flow from the upstream to the downstream in a pipeline manner, the data synchronization task is referred as a **Data Pipeline**. A **Data Pipeline** consists of a **Data Source**, **Route**, **Transform** and **Data Sink**. The transform can add extra content to events, and the router can remap the `Table ID`s corresponding to events.

Now let's introduce more details about the concepts you need to know when using the CDC Streaming ELT Framework.

### Table ID
When connecting to external systems, it is necessary to establish a mapping relationship with the storage objects of the external system. This is what `Table Id` refers to.

To be compatible with most external systems, the `Table ID` is represented by a 3-tuple : (namespace, schemaName, table). Connectors need to establish the mapping between Table ID and storage objects in external systems.
For instance, a table in MySQL/Doris is mapped to (null, database, table) and a topic in a message queue system such as Kafka is mapped to (null, null, topic).

### Data Source
Data Source is used to access metadata and read the changed data from external systems.
A Data Source can read data from multiple tables simultaneously.

To describe a data source, the follows are required:
* Type: The type of the source, such as MySQL, Postgres.
* Name: The name of the source, which is user-defined (optional, with a default value provided).
* Other custom configurations for the source.

For example, we could use `yaml` files to define a mysql source
```yaml
source:
  type: mysql
  name: mysql-source   #optional，description information
  host: localhost
  port: 3306
  username: admin
  password: pass
  tables: adb.*, bdb.user_table_[0-9]+, [app|web]_order_\.*
```

### Data Sink
The Data Sink is used to apply schema changes and write change data to external systems. A Data Sink can write to multiple tables simultaneously.

To describe a data sink, the follows are required:
* Type: The type of the sink, such as MySQL or PostgreSQL.
* Name: The name of the sink, which is user-defined (optional, with a default value provided).
* Other custom configurations for the sink.

For example, we can use this `yaml` file to define a kafka sink:
```yaml
sink:
  type: kafka
  name: mysink-queue           	# Optional parameter for description purpose
  bootstrap-servers: localhost:9092
  auto-create-table: true      	# Optional parameter for advanced functionalities
```

### Route
Route specifies the target table ID of each event. 
The most typical scenario is the merge of sub-databases and sub-tables, routing multiple upstream source tables to the same sink table

To describe a route, the follows are required:
* source-table: Source table id, supports regular expressions
* sink-table: Sink table id, supports regular expressions
* description: Routing rule description(optional, default value provided)

For example, if synchronize the table 'web_order' in the database 'mydb' to a Kafka topic 'ods_web_order', we can use this yaml file to define this route：
```yaml
route:
  source-table: mydb.default.web_order
  sink-table: ods_web_order
  description: sync table to one destination table with given prefix ods_
```

### Data Pipeline
Since events flow from the upstream to the downstream in a pipeline manner, the data synchronization task is also referred as a Data Pipeline.

To describe a Data Pipeline, the follows are required:
* Name: The name of the pipeline, which will be submitted to the Flink cluster as the job name.
* Other advanced capabilities such as automatic table creation, schema evolution, etc., will be implemented.

For example, we can use this yaml file to define a pipeline:
```yaml
pipeline:
  name: mysql-to-kafka-pipeline
  parallelism: 1
```
