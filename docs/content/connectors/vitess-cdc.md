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

# Vitess CDC Connector

The Vitess CDC connector allows for reading of incremental data from Vitess cluster. The connector does not support snapshot feature at the moment. This document describes how to setup the Vitess CDC connector to run SQL queries against Vitess databases.
[Vitess debezium documentation](https://debezium.io/documentation/reference/connectors/vitess.html)

Dependencies
------------

In order to setup the Vitess CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-vitess-cdc</artifactId>
  <version>3.0-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

Download [flink-sql-connector-vitess-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-vitess-cdc/3.0-SNAPSHOT/flink-sql-connector-vitess-cdc-3.0-SNAPSHOT.jar) and put it under `<FLINK_HOME>/lib/`.

Setup Vitess server
----------------

You can follow the Local Install via [Docker guide](https://vitess.io/docs/get-started/local-docker/), or the Vitess Operator for [Kubernetes guide](https://vitess.io/docs/get-started/operator/) to install Vitess. No special setup is needed to support Vitess connector.

### Checklist
* Make sure that the VTGate host and its gRPC port (default is 15991) is accessible from the machine where the Vitess connector is installed

### gRPC authentication
Because Vitess connector reads change events from the VTGate VStream gRPC server, it does not need to connect directly to MySQL instances.
Therefore, no special database user and permissions are needed. At the moment, Vitess connector only supports unauthenticated access to the VTGate gRPC server.

How to create a Vitess CDC table
----------------

The Vitess CDC table can be defined as following:

```sql
-- checkpoint every 3000 milliseconds
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a Vitess table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'vitess-cdc',
     'hostname' = 'localhost',
     'port' = '3306',
     'keyspace' = 'mydb',
     'table-name' = 'orders');

-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
```

Connector Options
----------------


<div class="highlight">
    <table class="colwidths-auto">
        <thead>
            <tr>
                <th class="text-left">Option</th>
                <th class="text-left">Required</th>
                <th class="text-left">Default</th>
                <th class="text-left">Type</th>
                <th class="text-left">Description</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>connector</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>Specify what connector to use, here should be <code>&lsquo;vitess-cdc&rsquo;</code>.</td>
            </tr>
            <tr>
                <td>hostname</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>IP address or hostname of the Vitess database server (VTGate).</td>
            </tr>
            <tr>
                <td>keyspace</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>The name of the keyspace from which to stream the changes.</td>
            </tr>
            <tr>
                <td>username</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>An optional username of the Vitess database server (VTGate). If not configured, unauthenticated VTGate gRPC is used.</td>
            </tr>
            <tr>
                <td>password</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>An optional password of the Vitess database server (VTGate). If not configured, unauthenticated VTGate gRPC is used.</td>
            </tr>
            <tr>
                <td>shard</td>
                <td>optional</td>
                <td>(none)</td>
                <td>String</td>
                <td>An optional name of the shard from which to stream the changes. If not configured, in case of unsharded keyspace, the connector streams changes from the only shard, in case of sharded keyspace, the connector streams changes from all shards in the keyspace.</td>
            </tr>
            <tr>
                <td>gtid</td>
                <td>optional</td>
                <td>current</td>
                <td>String</td>
                <td>An optional GTID position for a shard to stream from.</td>
            </tr>
            <tr>
                <td>stopOnReshard</td>
                <td>optional</td>
                <td>false</td>
                <td>Boolean</td>
                <td>Controls Vitess flag stop_on_reshard.</td>
            </tr>
            <tr>
                <td>tombstonesOnDelete</td>
                <td>optional</td>
                <td>true</td>
                <td>Boolean</td>
                <td>Controls whether a delete event is followed by a tombstone event.</td>
            </tr>
            <tr>
                <td>tombstonesOnDelete</td>
                <td>optional</td>
                <td>true</td>
                <td>Boolean</td>
                <td>Controls whether a delete event is followed by a tombstone event.</td>
            </tr>
            <tr>
                <td>schemaNameAdjustmentMode</td>
                <td>optional</td>
                <td>avro</td>
                <td>String</td>
                <td>Specifies how schema names should be adjusted for compatibility with the message converter used by the connector.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>required</td>
                <td>(none)</td>
                <td>String</td>
                <td>Table name of the MySQL database to monitor.</td>
            </tr>
            <tr>
                <td>tablet.type</td>
                <td>optional</td>
                <td>RDONLY</td>
                <td>String</td>
                <td>The type of Tablet (hence MySQL) from which to stream the changes: MASTER represents streaming from the master MySQL instance REPLICA represents streaming from the replica slave MySQL instance RDONLY represents streaming from the read-only slave MySQL instance.</td>
            </tr>
        </tbody>
    </table>
</div>

Features
--------

### Incremental Reading

The Vitess connector spends all its time streaming changes from the VTGate’s VStream gRPC service to which it is subscribed. The client receives changes from VStream as they are committed in the underlying MySQL server’s binlog at certain positions, which are referred to as VGTID.

The VGTID in Vitess is the equivalent of GTID in MySQL, it describes the position in the VStream in which a change event happens. Typically, A VGTID has multiple shard GTIDs, each shard GTID is a tuple of (Keyspace, Shard, GTID), which describes the GTID position of a given shard.

When subscribing to a VStream service, the connector needs to provide a VGTID and a Tablet Type (e.g. MASTER, REPLICA). The VGTID describes the position from which VStream should starts sending change events; the Tablet type describes which underlying MySQL instance (master or replica) in each shard do we read change events from.

The first time the connector connects to a Vitess cluster, it gets and provides the current VGTID to VStream.

The Debezium Vitess connector acts as a gRPC client of VStream. When the connector receives changes it transforms the events into Debezium create, update, or delete events that include the VGTID of the event. The Vitess connector forwards these change events in records to the Kafka Connect framework, which is running in the same process. The Kafka Connect process asynchronously writes the change event records in the same order in which they were generated to the appropriate Kafka topic.

#### Checkpoint

Incremental snapshot reading provides the ability to perform checkpoint in chunk level. It resolves the checkpoint timeout problem in previous version with old snapshot reading mechanism.

### Exactly-Once Processing

The Vitess CDC connector is a Flink Source connector which will read table snapshot chunks first and then continues to read binlog,
both snapshot phase and binlog phase, Vitess CDC connector read with **exactly-once processing** even failures happen.

### DataStream Source

The Incremental Reading feature of Vitess CDC Source only exposes in SQL currently, if you're using DataStream, please use Vitess Source:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.vitess.VitessSource;

public class VitessSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = VitessSource.<String>builder()
      .hostname("localhost")
      .port(15991)
      .keyspace("inventory")
      .username("flinkuser")
      .password("flinkpw")
      .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute();
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
        <th class="text-left">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DECIMAL(20, 0)</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        DOUBLE<br>
        DOUBLE PRECISION</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
         DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
         TINYINT(1)</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        VARCHAR(n)<br>
        TEXT</td>
      <td>STRING</td>
    </tr>
    </tbody>
</table>
</div>
