# Db2 CDC Connector

The Db2 CDC connector allows for reading snapshot data and incremental data from Db2 database. This document 
describes how to setup the db2 CDC connector to run SQL queries against Db2 databases.


## Supported Databases

| Connector             | Database                                                                                                                                                                                                                                                                                                                                                                                            | Driver               |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| [Db2-cdc](db2-cdc.md) | <li> [Db2](https://www.ibm.com/products/db2): 11.5 | Db2 Driver: 11.5.0.0 |

Dependencies
------------

In order to set up the Db2 CDC connector, the following table provides dependency information for both projects 
using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-db2-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.4.0</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-db2-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-db2-cdc/2.4.0/flink-sql-connector-db2-cdc-2.4.0.jar) and 
put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-db2-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users 
need to download the source code and compile the corresponding jar. Users should use the released version, such as 
[flink-sql-connector-db2-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-connector-db2-cdc), 
the released version will be available in the Maven central warehouse.

Setup Db2 server
----------------

Follow the steps in the [Debezium Db2 Connector](https://debezium.io/documentation/reference/1.9/connectors/db2.html#setting-up-db2).


Notes
----------------

###  Not support BOOLEAN type in SQL Replication on Db2

Only snapshots can be taken from tables with BOOLEAN type columns. Currently, SQL Replication on Db2 does not support BOOLEAN, so Debezium can not perform CDC on those tables.
Consider using another type to replace BOOLEAN type.


How to create a Db2 CDC table
----------------

The Db2 CDC table can be defined as following:

```sql
-- checkpoint every 3 seconds                     
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a Db2 table 'products' in Flink SQL
Flink SQL> CREATE TABLE products (
     ID INT NOT NULL,
     NAME STRING,
     DESCRIPTION STRING,
     WEIGHT DECIMAL(10,3)
     ) WITH (
     'connector' = 'db2-cdc',
     'hostname' = 'localhost',
     'port' = '50000',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'schema-name' = 'myschema',
     'table-name' = 'products');
  
-- read snapshot and binlogs from products table
Flink SQL> SELECT * FROM products;
```

Connector Options
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'db2-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the Db2 database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Db2 database to use when connecting to the Db2 database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the Db2 database server.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the Db2 server to monitor.</td>
    </tr> 
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name of the Db2 database to monitor.</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the Db2 database to monitor.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50000</td>
      <td>Integer</td>
      <td>Integer port number of the Db2 database server.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for Db2 CDC consumer, valid enumerations are "initial"
           and "latest-offset". Please see <a href="#startup-reading-position">Startup Reading Position</a> section 
for more detailed information.</td>
    </tr> 
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai". 
          It controls how the TIMESTAMP type in Db2 converted to STRING.
          See more <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-temporal-types">here</a>.
          If not set, then ZoneId.systemDefault() is used to determine the server time zone.
      </td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from 
Db2 server.
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-connector-properties">Debezium's Db2 Connector properties</a></td> 
    </tr>
    </tbody>
</table>
</div>

Features
--------
### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for DB2 CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the binlog which means only have the changes since the connector was started.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not using them together. If you speicifying both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### DataStream Source

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class Db2SourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> db2Source =
            Db2Source.<String>builder()
                    .hostname("yourHostname")
                    .port(50000)
                    .database("yourDatabaseName") // set captured database
                    .tableList("yourSchemaName.yourTableName") // set captured table
                    .username("yourUsername")
                    .password("yourPassword")
                    .deserializer(
                            new JsonDebeziumDeserializationSchema()) // converts SourceRecord to
                    // JSON String
                    .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    env.addSource(db2Source)
            .print()
            .setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print Db2 Snapshot + Change Stream");
  }
}
```

**Note:** Please refer [Deserialization](../about.html#deserialization) for more details about the JSON deserialization.

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;"><a href="https://www.ibm.com/docs/en/db2/11.5?topic=elements-data-types">Db2 type</a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>
        SMALLINT<br>
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INTEGER
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TIME</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHARACTER(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>VARBINARY(N)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BLOB<br>
        CLOB<br>
        DBCLOB<br>
      </td>
      <td>BYTES</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARGRAPHIC<br>
        XML
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
