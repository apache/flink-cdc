# TiDB CDC Connector

The TiDB CDC connector allows for reading snapshot data and incremental data from TiDB database. This document describes how to setup the TiDB CDC connector to run SQL queries against TiDB databases.

Dependencies
------------

In order to setup the TiDB CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-tidb-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.4.0</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-tidb-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-tidb-cdc/2.4.0/flink-sql-connector-tidb-cdc-2.4.0.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-tidb-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-tidb-cdc-2.2.1.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-tidb-cdc), the released version will be available in the Maven central warehouse.

How to create a TiDB CDC table
----------------

The TiDB CDC table can be defined as following:

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a TiDB table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(3),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'tidb-cdc',
     'tikv.grpc.timeout_in_ms' = '20000', 
     'pd-addresses' = 'localhost:2379',
     'database-name' = 'mydb',
     'table-name' = 'orders'
);
  
-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
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
      <td>Specify what connector to use, here should be <code>'tidb-cdc'</code>.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the TiDB server to monitor.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the TiDB database to monitor.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Optional startup mode for TiDB CDC consumer, valid enumerations are "initial" and "latest-offset".</td>
    </tr>
    <tr>
      <td>pd-addresses</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>TiKV cluster's PD address.</td>
    </tr>
    <tr>
      <td>tikv.grpc.timeout_in_ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>TiKV GRPC timeout in ms.</td>
    </tr>
    <tr>
      <td>tikv.grpc.scan_timeout_in_ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>TiKV GRPC scan timeout in ms.</td>
    </tr>
    <tr>
      <td>tikv.batch_get_concurrency</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>TiKV GRPC batch get concurrency.</td>
    </tr>
    <tr>
      <td>tikv.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through TiDB client's properties.</td> 
    </tr>
    </tbody>
</table>
</div>

Available Metadata
----------------

The following format metadata can be exposed as read-only (VIRTUAL) columns in a table definition.

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the table that contain the row.</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contain the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>It indicates the time that the change was made in the database. <br>If the record is read from snapshot of the table instead of the binlog, the value is always 0.</td>
    </tr>
  </tbody>
</table>

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id INT,
    order_date TIMESTAMP(0),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'tikv.grpc.timeout_in_ms' = '20000',
    'pd-addresses' = 'localhost:2379',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);
```

Features
--------
### Exactly-Once Processing

The TiDB CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **exactly-once processing** even failures happen.

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for TiDB CDC consumer. The valid enumerations are:

- `initial` (default): Takes a snapshot of structure and data of captured tables; useful if you want fetch a complete representation of the data from the captured tables.
- `latest-offset`: Takes a snapshot of the structure of captured tables only; useful if only changes happening from now onwards should be fetched.

### Multi Thread Reading

The TiDB CDC source can work in parallel reading, because there is multiple tasks can receive change events.

### DataStream Source

The TiDB CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

### DataStream Source

```java
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;

public class TiDBSourceExample {

    public static void main(String[] args) throws Exception {

        SourceFunction<String> tidbSource =
            TiDBSource.<String>builder()
                .database("mydb") // set captured database
                .tableName("products") // set captured table
                .tiConf(
                    TDBSourceOptions.getTiConfiguration(
                        "localhost:2399", new HashMap<>()))
                .snapshotEventDeserializer(
                    new TiKVSnapshotEventDeserializationSchema<String>() {
                        @Override
                        public void deserialize(
                            Kvrpcpb.KvPair record, Collector<String> out)
                            throws Exception {
                            out.collect(record.toString());
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    })
                .changeEventDeserializer(
                    new TiKVChangeEventDeserializationSchema<String>() {
                        @Override
                        public void deserialize(
                            Cdcpb.Event.Row record, Collector<String> out)
                            throws Exception {
                            out.collect(record.toString());
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    })
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }
}
```

Data Type Mapping
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">TiDB type<a href="https://dev.tidb.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        DOUBLE
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        DECIMAL(p, s)<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>The precision for DECIMAL data type is up to 65 in TiDB, but the precision for DECIMAL is limited to 38 in Flink.
  So if you define a decimal column whose precision is greater than 38, you should map it to STRING to avoid precision loss.</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
        TINYINT(1)<br>
        BIT(1)
        </td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]</td>
      <td>TIMESTAMP_LTZ [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME [(p)]</td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)
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
        BIT(n)
      </td>
      <td>BINARY(⌈n/8⌉)</td>
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
        TINYTEXT<br>
        TEXT<br>
        MEDIUMTEXT<br>
        LONGTEXT<br>
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYBLOB<br>
        BLOB<br>
        MEDIUMBLOB<br>
        LONGBLOB<br>
      </td>
      <td>BYTES</td>
      <td>Currently, for BLOB data type in TiDB, only the blob whose length isn't greater than 2,147,483,647(2 ** 31 - 1) is supported. </td>
    </tr>
    <tr>
      <td>
        YEAR
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td>The JSON data type  will be converted into STRING with JSON format in Flink.</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>ARRAY&lt;STRING&gt;</td>
      <td>As the SET data type in TiDB is a string object that can have zero or more values, 
          it should always be mapped to an array of string
      </td>
    </tr>
    </tbody>
</table>
</div>
