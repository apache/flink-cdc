# CDC Connectors for Apache Flink<sup>®</sup>

CDC Connectors for Apache Flink<sup>®</sup> is a set of source connectors for Apache Flink<sup>®</sup>, ingesting changes from different databases using change data capture (CDC).
CDC Connectors for Apache Flink<sup>®</sup> integrates Debezium as the engine to capture data changes. So it can fully leverage the ability of Debezium. See more about what is [Debezium](https://github.com/debezium/debezium).

This README is meant as a brief walkthrough on the core features of CDC Connectors for Apache Flink<sup>®</sup>. For a fully detailed documentation, please see [Documentation](https://ververica.github.io/flink-cdc-connectors/master/).

## Supported (Tested) Databases

| Connector                                                  | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                  |
|------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [mongodb-cdc](docs/content/connectors/mongodb-cdc.md)      | <li> [MongoDB](https://www.mongodb.com): 3.6, 4.x, 5.0                                                                                                                                                                                                                                                                                                                                                 | MongoDB Driver: 4.3.1   |
| [mysql-cdc](docs/content/connectors/mysql-cdc.md)          | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.27     |
| [oceanbase-cdc](/docs/content/connectors/oceanbase-cdc.md) | <li> [OceanBase CE](https://open.oceanbase.com): 3.1.x <li> [OceanBase EE](https://www.oceanbase.com/product/oceanbase) (MySQL mode): 2.x, 3.x                                                                                                                                                                                                                                                         | JDBC Driver: 5.1.4x     |
| [oracle-cdc](docs/content/connectors/oracle-cdc.md)        | <li> [Oracle](https://www.oracle.com/index.html): 11, 12, 19                                                                                                                                                                                                                                                                                                                                           | Oracle Driver: 19.3.0.0 |
| [postgres-cdc](docs/content/connectors/postgres-cdc.md)    | <li> [PostgreSQL](https://www.postgresql.org): 9.6, 10, 11, 12                                                                                                                                                                                                                                                                                                                                         | JDBC Driver: 42.2.27    |
| [sqlserver-cdc](docs/content/connectors/sqlserver-cdc.md)  | <li> [Sqlserver](https://www.microsoft.com/sql-server): 2012, 2014, 2016, 2017, 2019                                                                                                                                                                                                                                                                                                                   | JDBC Driver: 7.2.2.jre8 |
| [tidb-cdc](docs/content/connectors/tidb-cdc.md)            | <li> [TiDB](https://www.pingcap.com): 5.1.x, 5.2.x, 5.3.x, 5.4.x, 6.0.0                                                                                                                                                                                                                                                                                                                                | JDBC Driver: 8.0.27     |
| [Db2-cdc](docs/content/connectors/db2-cdc.md)              | <li> [Db2](https://www.ibm.com/products/db2): 11.5                                                                                                                                                                                                                                                                                                                                                     | Db2 Driver: 11.5.0.0    |

## Features

1. Supports reading database snapshot and continues to read transaction logs with **exactly-once processing** even failures happen.
2. CDC connectors for DataStream API, users can consume changes on multiple databases and tables in a single job without Debezium and Kafka deployed.
3. CDC connectors for Table/SQL API, users can use SQL DDL to create a CDC source to monitor changes on a single table.

## Usage for Table/SQL API

We need several steps to setup a Flink cluster with the provided connector.

1. Setup a Flink cluster with version 1.12+ and Java 8+ installed.
2. Download the connector SQL jars from the [Download](https://github.com/ververica/flink-cdc-connectors/releases) page (or [build yourself](#building-from-source)).
3. Put the downloaded jars under `FLINK_HOME/lib/`.
4. Restart the Flink cluster.

The example shows how to create a MySQL CDC source in [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/dev/table/sqlClient.html) and execute queries on it.

```sql
-- creates a mysql cdc table source
CREATE TABLE mysql_binlog (
 id INT NOT NULL,
 name STRING,
 description STRING,
 weight DECIMAL(10,3)
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
  <groupId>com.ververica</groupId>
  <!-- add the dependency matching your database -->
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.4-SNAPSHOT</version>
</dependency>
```

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExample {
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

## Building from source

- Prerequisites:
    - git
    - Maven
    - At least Java 8

```
git clone https://github.com/ververica/flink-cdc-connectors.git
cd flink-cdc-connectors
mvn clean install -DskipTests
```

The dependencies are now available in your local `.m2` repository.

## License

The code in this repository is licensed under the [Apache Software License 2](https://github.com/ververica/flink-cdc-connectors/blob/master/LICENSE).

## Contributing

CDC Connectors for Apache Flink<sup>®</sup> welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. You can report problems to request features in the [GitHub Issues](https://github.com/ververica/flink-cdc-connectors/issues).

## Community

* [DingTalk](https://www.dingtalk.com/) Chinese User Group

  You can search the group number [**33121212**] or scan the following QR code to join in the group.
  
  <div align=center>
     <img src="https://user-images.githubusercontent.com/5163645/233297896-0195d0ae-eb1c-4604-977b-1d08e424c7e7.png" width=400 />
   </div>

## Documents
To get started, please see https://ververica.github.io/flink-cdc-connectors/
