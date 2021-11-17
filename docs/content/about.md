# What's Flink CDC

Flink CDC Connectors is a set of source connectors for <a href="https://flink.apache.org/">Apache Flink</a>, ingesting changes from different databases using change data capture (CDC).
The Flink CDC Connectors integrates Debezium as the engine to capture data changes. So it can fully leverage the ability of Debezium. See more about what is [Debezium](https://github.com/debezium/debezium).

![Flink_CDC](/_static/fig/flinkcdc.png "Flink CDC")

## Supported Connectors

| Database | Version |
| --- | --- |
| MySQL | Database: 5.7, 8.0.x <br/>JDBC Driver: 8.0.16 |
| PostgreSQL | Database: 9.6, 10, 11, 12 <br/>JDBC Driver: 42.2.12|
| MongoDB | Database: 3.6, 4.x, 5.0 <br/>MongoDB Driver: 4.3.1|
| Oracle | Database: 11, 12, 19 <br/>Oracle Driver: 19.3.0.0|

## Supported Formats

| Format | Supported Connector | Flink Version |
| --- | --- | --- |
| <a href="https://github.com/ververica/flink-cdc-connectors/wiki/Changelog-JSON-Format">Changelog Json</a> | <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.13/dev/table/connectors/kafka.html">Apache Kafka</a> | 1.11+ |

## Supported Flink Versions 
The version mapping between Flink CDC Connectors and Flink.
| Flink CDC Connector Version | Flink Version |
| --- | --- |
|1.0.0 | 1.11.* |
|1.1.0 | 1.11.* |
|1.2.0 | 1.12.* |
|1.3.0 | 1.12.* |
|1.4.0 | 1.13.* |
|2.0.* | 1.13.* |
|2.1.* | 1.13.* |

## Features

1. Supports reading database snapshot and continues to read binlogs with **exactly-once processing** even failures happen.
2. CDC connectors for DataStream API, users can consume changes on multiple databases and tables in a single job without Debezium and Kafka deployed.
3. CDC connectors for Table/SQL API, users can use SQL DDL to create a CDC source to monitor changes on a single table.

## Usage for Table/SQL API

We need several steps to setup a Flink cluster with the provided connector.

1. Setup a Flink cluster with version 1.12+ and Java 8+ installed.
2. Download the connector SQL jars from the [Downloads](downloads.md) page (or [build yourself](#building-from-source)).
3. Put the downloaded jars under `FLINK_HOME/lib/`.
4. Restart the Flink cluster.

The example shows how to create a MySQL CDC source in [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/dev/table/sqlClient.html) and execute queries on it.

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
  <groupId>com.ververica</groupId>
  <!-- add the dependency matching your database -->
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <!-- the dependency is available only for stable releases. -->
  <version>2.1.0</version>
</dependency>
```

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

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
**Note:** Please refer [Debezium documentation](https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-events
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

## Building from source

Prerequisites:
- git
- Maven
- At least Java 8

```
git clone https://github.com/ververica/flink-cdc-connectors.git
cd flink-cdc-connectors
mvn clean install -DskipTests
```

Flink CDC Connectors is now available at your local `.m2` repository.

## License

The code in this repository is licensed under the [Apache Software License 2](https://github.com/ververica/flink-cdc-connectors/blob/master/LICENSE).
