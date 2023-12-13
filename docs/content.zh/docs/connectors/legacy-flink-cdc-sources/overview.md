---
title: "概览"
weight: 1
type: docs
aliases:
- /connectors/legacy-flink-cdc-sources/
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

# Legacy CDC sources

Legacy CDC sources is a set of source connectors for <a href="https://flink.apache.org/">Apache Flink<sup>®</sup></a>, ingesting changes from different databases using change data capture (CDC).
Some CDC sources integrate Debezium as the engine to capture data changes. So it can fully leverage the ability of Debezium. See more about what is [Debezium](https://github.com/debezium/debezium).

You can also read [tutorials]({{< ref "docs/connectors/legacy-flink-cdc-sources/tutorials/build-streaming-etl-tutorial" >}}) about how to use these sources.

{{< img src="/fig/cdc-flow.png" width="600px" alt="Flink CDC" >}}

## Supported Connectors

| Connector                                                                             | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                    |
|---------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| [mongodb-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/mongodb-cdc" >}})     | <li> [MongoDB](https://www.mongodb.com): 3.6, 4.x, 5.0                                                                                                                                                                                                                                                                                                                                                 | MongoDB Driver: 4.3.4     |
| [mysql-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/mysql-cdc" >}})         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.28       |
| [oceanbase-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/oceanbase-cdc" >}}) | <li> [OceanBase CE](https://open.oceanbase.com): 3.1.x, 4.x <li> [OceanBase EE](https://www.oceanbase.com/product/oceanbase): 2.x, 3.x, 4.x                                                                                                                                                                                                                                                            | OceanBase Driver: 2.4.x   |
| [oracle-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/oracle-cdc" >}})       | <li> [Oracle](https://www.oracle.com/index.html): 11, 12, 19, 21                                                                                                                                                                                                                                                                                                                                       | Oracle Driver: 19.3.0.0   |
| [postgres-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/postgres-cdc" >}})   | <li> [PostgreSQL](https://www.postgresql.org): 9.6, 10, 11, 12, 13, 14                                                                                                                                                                                                                                                                                                                                 | JDBC Driver: 42.5.1       |
| [sqlserver-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/sqlserver-cdc" >}}) | <li> [Sqlserver](https://www.microsoft.com/sql-server): 2012, 2014, 2016, 2017, 2019                                                                                                                                                                                                                                                                                                                   | JDBC Driver: 9.4.1.jre8   | 
| [tidb-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/tidb-cdc" >}})           | <li> [TiDB](https://www.pingcap.com/): 5.1.x, 5.2.x, 5.3.x, 5.4.x, 6.0.0                                                                                                                                                                                                                                                                                                                               | JDBC Driver: 8.0.27       | 
| [db2-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/db2-cdc" >}})             | <li> [Db2](https://www.ibm.com/products/db2): 11.5                                                                                                                                                                                                                                                                                                                                                     | Db2 Driver: 11.5.0.0      |
| [vitess-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/vitess-cdc" >}})       | <li> [Vitess](https://vitess.io/): 8.0.x, 9.0.x                                                                                                                                                                                                                                                                                                                                                        | MySql JDBC Driver: 8.0.26 |

## Supported Flink Versions
The following table shows the version mapping between Flink<sup>®</sup> CDC Connectors and Flink<sup>®</sup>:

|    Flink<sup>®</sup> CDC Version    |                                                                                                      Flink<sup>®</sup> Version                                                                                                       |
|:-----------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| <font color="DarkCyan">1.0.0</font> |                                                                                             <font color="MediumVioletRed">1.11.*</font>                                                                                              |
| <font color="DarkCyan">1.1.0</font> |                                                                                             <font color="MediumVioletRed">1.11.*</font>                                                                                              |
| <font color="DarkCyan">1.2.0</font> |                                                                                             <font color="MediumVioletRed">1.12.*</font>                                                                                              |
| <font color="DarkCyan">1.3.0</font> |                                                                                             <font color="MediumVioletRed">1.12.*</font>                                                                                              |
| <font color="DarkCyan">1.4.0</font> |                                                                                             <font color="MediumVioletRed">1.13.*</font>                                                                                              |
| <font color="DarkCyan">2.0.*</font> |                                                                                             <font color="MediumVioletRed">1.13.*</font>                                                                                              |
| <font color="DarkCyan">2.1.*</font> |                                                                                             <font color="MediumVioletRed">1.13.*</font>                                                                                              |
| <font color="DarkCyan">2.2.*</font> |                                                                      <font color="MediumVioletRed">1.13.\*</font>, <font color="MediumVioletRed">1.14.\*</font>                                                                      |
| <font color="DarkCyan">2.3.*</font> |                        <font color="MediumVioletRed">1.13.\*</font>, <font color="MediumVioletRed">1.14.\*</font>, <font color="MediumVioletRed">1.15.\*</font>, <font color="MediumVioletRed">1.16.\*</font>                        |
| <font color="DarkCyan">2.4.*</font> | <font color="MediumVioletRed">1.13.\*</font>, <font color="MediumVioletRed">1.14.\*</font>, <font color="MediumVioletRed">1.15.\*</font>, <font color="MediumVioletRed">1.16.\*</font>, <font color="MediumVioletRed">1.17.\*</font> |
| <font color="DarkCyan">3.0.*</font> | <font color="MediumVioletRed">1.14.\*</font>, <font color="MediumVioletRed">1.15.\*</font>, <font color="MediumVioletRed">1.16.\*</font>, <font color="MediumVioletRed">1.17.\*</font>, <font color="MediumVioletRed">1.18.\*</font> |

## Features

1. Supports reading database snapshot and continues to read binlogs with **exactly-once processing** even failures happen.
2. CDC connectors for DataStream API, users can consume changes on multiple databases and tables in a single job without Debezium and Kafka deployed.
3. CDC connectors for Table/SQL API, users can use SQL DDL to create a CDC source to monitor changes on a single table.

The following table shows the current features of the connector:

| Connector                                                                             | No-lock Read | Parallel Read | Exactly-once Read | Incremental Snapshot Read |
|---------------------------------------------------------------------------------------|--------------|---------------|-------------------|---------------------------|
| [mongodb-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/mongodb-cdc" >}})     | ✅            | ✅             | ✅                 | ✅                         |
| [mysql-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/mysql-cdc" >}})         | ✅            | ✅             | ✅                 | ✅                         |
| [oracle-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/oracle-cdc" >}})       | ✅            | ✅             | ✅                 | ✅                         |
| [postgres-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/postgres-cdc" >}})   | ✅            | ✅             | ✅                 | ✅                         |
| [sqlserver-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/sqlserver-cdc" >}}) | ✅            | ✅             | ✅                 | ✅                         |
| [oceanbase-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/oceanbase-cdc" >}}) | ❌            | ❌             | ❌                 | ❌                         |
| [tidb-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/tidb-cdc" >}})           | ✅            | ❌             | ✅                 | ❌                         |
| [db2-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/db2-cdc" >}})             | ✅            | ✅             | ✅                 | ✅                         |
| [vitess-cdc]({{< ref "docs/connectors/legacy-flink-cdc-sources/vitess-cdc" >}})       | ✅            | ❌             | ✅                 | ❌                         |

## Usage for Table/SQL API

We need several steps to setup a Flink cluster with the provided connector.

1. Setup a Flink cluster with version 1.12+ and Java 8+ installed.
2. Download the connector SQL jars from the [Downloads](https://github.com/apache/flink-cdc/releases) page (or [build yourself](#building-from-source)).
3. Put the downloaded jars under `FLINK_HOME/lib/`.
4. Restart the Flink cluster.

The example shows how to create a MySQL CDC source in [Flink SQL Client](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sqlclient/) and execute queries on it.

```sql
-- creates a mysql cdc table source
CREATE TABLE mysql_binlog (
 id INT NOT NULL,
 name STRING,
 description STRING,
 weight DECIMAL(10,3),
 PRIMARY KEY(id) NOT ENFORCED
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'localhost',
 'port' = '3306',
 'username' = 'flinkuser',
 'password' = 'flinkpw',
 'database-name' = 'inventory',
 'table-name' = 'products'
);

-- read snapshot and binlog data from mysql, and do some transformation, and show on the client
SELECT id, UPPER(name), description, weight FROM mysql_binlog;
```

## Usage for DataStream API

Include following Maven dependency (available through Maven Central):

```
<dependency>
  <groupId>org.apache.flink</groupId>
  <!-- add the dependency matching your database -->
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependencies need to be built based on master or release branches by yourself. -->
  <version>3.0-SNAPSHOT</version>
</dependency>
```

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;

public class MySqlBinlogSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("yourHostname")
            .port(yourPort)
            .databaseList("yourDatabaseName") // set captured database
            .tableList("yourDatabaseName.yourTableName") // set captured table
            .username("yourUsername")
            .password("yourPassword")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    // enable checkpoint
    env.enableCheckpointing(3000);
    
    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
      // set 4 parallel source tasks
      .setParallelism(4)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
    
    env.execute("Print MySQL Snapshot + Binlog");
  }
}
```
### Deserialization
The following JSON data show the change event in JSON format.

```json
{
  "before": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.18
  },
  "after": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.15
  },
  "source": {...},
  "op": "u",  // the operation type, "u" means this this is an update event 
  "ts_ms": 1589362330904,  // the time at which the connector processed the event
  "transaction": null
}
```
**Note:** Please refer [Debezium documentation](https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-events
)  to know the meaning of each field.

In some cases, users can use the `JsonDebeziumDeserializationSchema(true)` Constructor to enabled include schema in the message. Then the Debezium JSON message may look like this:
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": false,
            "field": "id"
          },
          {
            "type": "string",
            "optional": false,
            "default": "flink",
            "field": "name"
          },
          {
            "type": "string",
            "optional": true,
            "field": "description"
          },
          {
            "type": "double",
            "optional": true,
            "field": "weight"
          }
        ],
        "optional": true,
        "name": "mysql_binlog_source.inventory_1pzxhca.products.Value",
        "field": "before"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int32",
            "optional": false,
            "field": "id"
          },
          {
            "type": "string",
            "optional": false,
            "default": "flink",
            "field": "name"
          },
          {
            "type": "string",
            "optional": true,
            "field": "description"
          },
          {
            "type": "double",
            "optional": true,
            "field": "weight"
          }
        ],
        "optional": true,
        "name": "mysql_binlog_source.inventory_1pzxhca.products.Value",
        "field": "after"
      },
      {
        "type": "struct",
        "fields": {...}, 
        "optional": false,
        "name": "io.debezium.connector.mysql.Source",
        "field": "source"
      },
      {
        "type": "string",
        "optional": false,
        "field": "op"
      },
      {
        "type": "int64",
        "optional": true,
        "field": "ts_ms"
      }
    ],
    "optional": false,
    "name": "mysql_binlog_source.inventory_1pzxhca.products.Envelope"
  },
  "payload": {
    "before": {
      "id": 111,
      "name": "scooter",
      "description": "Big 2-wheel scooter",
      "weight": 5.18
    },
    "after": {
      "id": 111,
      "name": "scooter",
      "description": "Big 2-wheel scooter",
      "weight": 5.15
    },
    "source": {...},
    "op": "u",  // the operation type, "u" means this this is an update event
    "ts_ms": 1589362330904,  // the time at which the connector processed the event
    "transaction": null
  }
}
```
Usually, it is recommended to exclude schema because schema fields makes the messages very verbose which reduces parsing performance.

The `JsonDebeziumDeserializationSchema` can also accept custom configuration of `JsonConverter`, for example if you want to obtain numeric output for decimal data,
you can construct `JsonDebeziumDeserializationSchema` as following:

```java
 Map<String, Object> customConverterConfigs = new HashMap<>();
 customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
 JsonDebeziumDeserializationSchema schema = 
      new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
```

## Building from source

Prerequisites:
- git
- Maven
- At least Java 8

```
git clone https://github.com/apache/flink-cdc.git
cd flink-cdc
mvn clean install -DskipTests
```

The dependencies are now available in your local `.m2` repository.

{{< top >}}
