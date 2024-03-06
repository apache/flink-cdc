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

# MongoDB CDC Connector

The MongoDB CDC connector allows for reading snapshot data and incremental data from MongoDB. This document describes how to setup the MongoDB CDC connector to run SQL queries against MongoDB.

Dependencies
------------

In order to setup the MongoDB CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency
```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mongodb-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself. -->
  <version>3.0-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-mongodb-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/3.0-SNAPSHOT/flink-sql-connector-mongodb-cdc-3.0-SNAPSHOT.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-mongodb-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-mongodb-cdc-2.2.1.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-mongodb-cdc), the released version will be available in the Maven central warehouse.

Setup MongoDB
----------------

### Availability
- MongoDB version

  MongoDB version >= 3.6 <br>
We use [change streams](https://docs.mongodb.com/manual/changeStreams/) feature (new in version 3.6) to capture change data.

- Cluster Deployment

  [replica sets](https://docs.mongodb.com/manual/replication/) or [sharded clusters](https://docs.mongodb.com/manual/sharding/) is required.

- Storage Engine

  [WiredTiger](https://docs.mongodb.com/manual/core/wiredtiger/#std-label-storage-wiredtiger) storage engine is required.

- [Replica set protocol version](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion)

  Replica set protocol version 1 [(pv1)](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion) is required. <br>
Starting in version 4.0, MongoDB only supports pv1. pv1 is the default for all new replica sets created with MongoDB 3.2 or later.

- Privileges

  `changeStream` and `read` privileges are required by MongoDB Kafka Connector. 

  You can use the following example for simple authorization.<br>
  For more detailed authorization, please refer to [MongoDB Database User Roles](https://docs.mongodb.com/manual/reference/built-in-roles/#database-user-roles).

  ```javascript
  use admin;
  db.createRole(
      {
          role: "flinkrole",
          privileges: [{
              // Grant privileges on all non-system collections in all databases
              resource: { db: "", collection: "" },
              actions: [
                  "splitVector",
                  "listDatabases",
                  "listCollections",
                  "collStats",
                  "find",
                  "changeStream" ]
          }],
          roles: [
              // Read config.collections and config.chunks
              // for sharded cluster snapshot splitting.
              { role: 'read', db: 'config' }
          ]
      }
  );

  db.createUser(
    {
        user: 'flinkuser',
        pwd: 'flinkpw',
        roles: [
           { role: 'flinkrole', db: 'admin' }
        ]
    }
  );
  ```


How to create a MongoDB CDC table
----------------

The MongoDB CDC table can be defined as following:

```sql
-- register a MongoDB table 'products' in Flink SQL
CREATE TABLE products (
  _id STRING, // must be declared
  name STRING,
  weight DECIMAL(10,3),
  tags ARRAY<STRING>, -- array
  price ROW<amount DECIMAL(10,2), currency STRING>, -- embedded document
  suppliers ARRAY<ROW<name STRING, address STRING>>, -- embedded documents
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = 'localhost:27017,localhost:27018,localhost:27019',
  'username' = 'flinkuser',
  'password' = 'flinkpw',
  'database' = 'inventory',
  'collection' = 'products'
);

-- read snapshot and change events from products collection
SELECT * FROM products;
```

**Note that**

MongoDB's change event record doesn't have updated before message. So, we can only convert it to Flink's UPSERT changelog stream.
An upsert stream requires a unique key, so we must declare `_id` as primary key.
We can't declare other column as primary key, because delete operation does not contain the key and value besides `_id` and `sharding key`.

Connector Options
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
   <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>connector</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>mongodb-cdc</code>.</td>
    </tr>
    <tr>
      <td>scheme</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">mongodb</td>
      <td>String</td>
      <td>The protocol connected to MongoDB. eg. <code>mongodb or mongodb+srv.</code></td>
    </tr>
    <tr>
      <td>hosts</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The comma-separated list of hostname and port pairs of the MongoDB servers.<br>
          eg. <code>localhost:27017,localhost:27018</code>
      </td>
    </tr>
    <tr>
      <td>username</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the database user to be used when connecting to MongoDB.<br>
          This is required only when MongoDB is configured to use authentication.
      </td>
    </tr>
    <tr>
      <td>password</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to be used when connecting to MongoDB.<br>
          This is required only when MongoDB is configured to use authentication.
      </td>
    </tr>
    <tr>
      <td>database</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the database to watch for changes. If not set then all databases will be captured. <br>
          The database also supports regular expressions to monitor multiple databases matching the regular expression.</td>
    </tr>
    <tr>
      <td>collection</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the collection in the database to watch for changes. If not set then all collections will be captured.<br>
          The collection also supports regular expressions to monitor multiple collections matching fully-qualified collection identifiers.</td>
    </tr>
    <tr>
      <td>connection.options</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The ampersand-separated <a href="https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options">connection options</a> of MongoDB. eg. <br>
          <code>replicaSet=test&connectTimeoutMS=300000</code>
      </td>
    </tr>
    <tr>
        <td>scan.startup.mode</td>
        <td>optional</td>
        <td style="word-wrap: break-word;">initial</td>
        <td>String</td>
        <td>Optional startup mode for MongoDB CDC consumer, valid enumerations are "initial", "latest-offset" and "timestamp".
            Please see <a href="#startup-reading-position">Startup Reading Position</a> section for more detailed information.</td>
    </tr>
    <tr>
        <td>scan.startup.timestamp-millis</td>
        <td>optional</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Long</td>
        <td>Timestamp in millis of the start point, only used for <code>'timestamp'</code> startup mode.</td>
    </tr>
    <tr>
      <td>copy.existing.queue.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10240</td>
      <td>Integer</td>
      <td>The max size of the queue to use when copying data.</td>
    </tr>
    <tr>
      <td>batch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>The cursor batch size.</td>
    </tr>
    <tr>
      <td>poll.max.batch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>Maximum number of change stream documents to include in a single batch when polling for new data.</td>
    </tr>
    <tr>
      <td>poll.await.time.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>The amount of time to wait before checking for new results on the change stream.</td>
    </tr>
    <tr>
      <td>heartbeat.interval.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>The length of time in milliseconds between sending heartbeat messages. Use 0 to disable.</td>
    </tr>
    <tr>
      <td>scan.full-changelog</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether try to generate full-mode changelog based on pre- and post-images in MongoDB. Refer to <a href="#a-name-id-003-a">Full Changelog</a>  for more details. Supports MongoDB 6.0 and above only.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether enable incremental snapshot. The incremental snapshot feature only supports after MongoDB 4.0.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size.mb</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">64</td>
      <td>Integer</td>
      <td>The chunk size mb of incremental snapshot.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.samples</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>The samples count per chunk when using sample partition strategy during incremental snapshot.</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. <br>
          The flink version is required to be greater than or equal to 1.14 when 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' is set to true.<br>
          If the flink version is greater than or equal to 1.15, the default value of 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' has been changed to true,
          so it does not need to be explicitly configured 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'
      </td>
    </tr>
    <tr>
      <td>scan.cursor.no-timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. Only available when parallelism snapshot is enabled.</td>
    </tr>
    </tbody>
</table>
</div>

Note: `heartbeat.interval.ms` is highly recommended setting a proper value larger than 0 **if the collection changes slowly**.
The heartbeat event can push the `resumeToken` forward to avoid `resumeToken` being expired when we recover the Flink job from a checkpoint or savepoint.

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
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contain the row.</td>
    </tr>
    <tr>
      <td>collection_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the collection that contain the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>It indicates the time that the change was made in the database. <br>If the record is read from snapshot of the table instead of the change stream, the value is always 0.</td>
    </tr>
  </tbody>
</table>

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    collection_name STRING METADATA  FROM 'collection_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    _id STRING, // must be declared
    name STRING,
    weight DECIMAL(10,3),
    tags ARRAY<STRING>, -- array
    price ROW<amount DECIMAL(10,2), currency STRING>, -- embedded document
    suppliers ARRAY<ROW<name STRING, address STRING>>, -- embedded documents
    PRIMARY KEY(_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb-cdc',
    'hosts' = 'localhost:27017,localhost:27018,localhost:27019',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database' = 'inventory',
    'collection' = 'products'
);
```

Features
--------

### Exactly-Once Processing

The MongoDB CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change stream events with **exactly-once processing** even failures happen. 

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for MongoDB CDC consumer. The valid enumerations are:

- `initial` (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest oplog.
- `latest-offset`: Never to perform snapshot on the monitored database tables upon first startup, just read from
  the end of the oplog which means only have the changes since the connector was started.
- `timestamp`: Skip snapshot phase and start reading oplog events from a specific timestamp.

For example in DataStream API:
```java
MongoDBSource.builder()
    .startupOptions(StartupOptions.latest()) // Start from latest offset
    .startupOptions(StartupOptions.timestamp(1667232000000L) // Start from timestamp
    .build()
```

and with SQL:

```SQL
CREATE TABLE mongodb_source (...) WITH (
    'connector' = 'mongodb-cdc',
    'scan.startup.mode' = 'latest-offset', -- Start from latest offset
    ...
    'scan.startup.mode' = 'timestamp', -- Start from timestamp
    'scan.startup.timestamp-millis' = '1667232000000' -- Timestamp under timestamp startup mode
    ...
)
```

### Change Streams

We integrate the [MongoDB's official Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/) to read snapshot or change events from MongoDB and drive it by Debezium's `EmbeddedEngine`.

Debezium's `EmbeddedEngine` provides a mechanism for running a single Kafka Connect `SourceConnector` within an application's process, and it can drive any standard Kafka Connect `SourceConnector` properly even which is not provided by Debezium.

We choose **MongoDB's official Kafka Connector** instead of the **Debezium's MongoDB Connector** because they use a different change data capture mechanism.

- For Debezium's MongoDB Connector, it reads the `oplog.rs` collection of each replica-set's master node.
- For MongoDB's Kafka Connector, it subscribes `Change Stream` of MongoDB.

MongoDB's `oplog.rs` collection doesn't keep the changed record's update before state, so it's hard to extract the full document state by a single `oplog.rs` record and convert it to change log stream accepted by Flink (Insert Only, Upsert, All).
Additionally, MongoDB 5 (released in July 2021) has changed the oplog format, so the current Debezium connector cannot be used with it.

**Change Stream** is a new feature provided by MongoDB 3.6 for replica sets and sharded clusters that allows applications to access real-time data changes without the complexity and risk of tailing the oplog.<br>
Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment, and immediately react to them.

**Lookup Full Document for Update Operations** is a feature provided by **Change Stream** which can configure the change stream to return the most current majority-committed version of the updated document. Because of this feature, we can easily collect the latest full document and convert the change log to Flink's **Upsert Changelog Stream**. 

By the way, Debezium's MongoDB change streams exploration mentioned by [DBZ-435](https://issues.redhat.com/browse/DBZ-435) is on roadmap.<br> 
If it's done, we can consider integrating two kinds of source connector for users to choose.

### DataStream Source

The MongoDB CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;

public class MongoDBSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .username("flink")
                .password("flinkpw")
                .databaseList("inventory") // set captured database, support regex
                .collectionList("inventory.products", "inventory.orders") //set captured collections, support regex
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
```

The MongoDB CDC incremental connector (after 2.3.0) can be used as the following shows:
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class MongoDBIncrementalSourceExample {
    public static void main(String[] args) throws Exception {
        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts("localhost:27017")
                        .databaseList("inventory") // set captured database, support regex
                        .collectionList("inventory.products", "inventory.orders") //set captured collections, support regex
                        .username("flink")
                        .password("flinkpw")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print MongoDB Snapshot + Change Stream");
    }
}
```

**Note:** 
- If database regex is used, `readAnyDatabase` role is required.
- The incremental snapshot feature only supports after MongoDB 4.0.

### Full Changelog<a name="Full Changelog" id="003" ></a>

MongoDB 6.0 and above supports emitting change stream events containing document before and after the change was made (aka. pre- and post-images).

- The pre-image is the document before it was replaced, updated, or deleted. There is no pre-image for an inserted document.

- The post-image is the document after it was inserted, replaced, or updated. There is no post-image for a deleted document.

MongoDB CDC could make uses of pre-image and post-images to generate full-mode changelog stream including Insert, Update Before, Update After, and Delete data rows, thereby avoiding additional `ChangelogNormalize` downstream node.

To enable this feature, here's some prerequisites:

- MongoDB version must be 6.0 or above;
- Enable `preAndPostImages` feature at the database level:
```javascript
db.runCommand({
  setClusterParameter: {
    changeStreamOptions: {
      preAndPostImages: {
        expireAfterSeconds: 'off' // replace with custom image expiration time
      }
    }
  }
})
```
- Enable `changeStreamPreAndPostImages` feature for collections to be monitored:
```javascript
db.runCommand({
  collMod: "<< collection name >>", 
  changeStreamPreAndPostImages: {
    enabled: true 
  } 
})
```
- Enable MongoDB CDC's `scan.full-changelog` feature:

```java
MongoDBSource.builder()
    .scanFullChangelog(true)
    ...
    .build()
```

or with Flink SQL:

```SQL
CREATE TABLE mongodb_source (...) WITH (
    'connector' = 'mongodb-cdc',
    'scan.full-changelog' = 'true',
    ...
)
```

Data Type Mapping
----------------
[BSON](https://docs.mongodb.com/manual/reference/bson-types/) short for **Binary JSON** is a binary-encoded serialization of JSON-like format used to store documents and make remote procedure calls in MongoDB.

[Flink SQL Data Type](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/types/) is similar to the SQL standard’s data type terminology which describes the logical type of a value in the table ecosystem. It can be used to declare input and/or output types of operations.

In order to enable Flink SQL to process data from heterogeneous data sources, the data types of heterogeneous data sources need to be uniformly converted to Flink SQL data types.

The following is the mapping of BSON type and Flink SQL type.


<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">BSON type<a href="https://docs.mongodb.com/manual/reference/bson-types/"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td></td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td></td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>
        Int<br>
      <td>INT</td>
    </tr>
    <tr>
      <td>Long</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td></td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>Double</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>Decimal128</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>Boolean</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>Date</br>Timestamp</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>Date</br>Timestamp</td>
      <td>TIME</td>
    </tr>
    <tr>
      <td>Date</td>
      <td>TIMESTAMP(3)</br>TIMESTAMP_LTZ(3)</td>
    </tr>
    <tr>
      <td>Timestamp</td>
      <td>TIMESTAMP(0)</br>TIMESTAMP_LTZ(0)
      </td>
    </tr>
    <tr>
      <td>
        String<br>
        ObjectId<br>
        UUID<br>
        Symbol<br>
        MD5<br>
        JavaScript</br>
        Regex</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BinData</td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>Object</td>
      <td>ROW</td>
    </tr>
    <tr>
      <td>Array</td>
      <td>ARRAY</td>
    </tr>
    <tr>
      <td>DBPointer</td>
      <td>ROW&lt;$ref STRING, $id STRING&gt;</td>
    </tr>
    <tr>
      <td>
        <a href="https://docs.mongodb.com/manual/reference/geojson/">GeoJSON</a>
      </td>
      <td>
        Point : ROW&lt;type STRING, coordinates ARRAY&lt;DOUBLE&gt;&gt;</br>
        Line  : ROW&lt;type STRING, coordinates ARRAY&lt;ARRAY&lt; DOUBLE&gt;&gt;&gt;</br>
        ...
      </td>
    </tr>
    </tbody>
</table>
</div>


Reference
--------

- [MongoDB Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/)
- [Change Streams](https://docs.mongodb.com/manual/changeStreams/)
- [Replication](https://docs.mongodb.com/manual/replication/)
- [Sharding](https://docs.mongodb.com/manual/sharding/)
- [Database User Roles](https://docs.mongodb.com/manual/reference/built-in-roles/#database-user-roles)
- [WiredTiger](https://docs.mongodb.com/manual/core/wiredtiger/#std-label-storage-wiredtiger)
- [Replica set protocol](https://docs.mongodb.com/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.protocolVersion)
- [Connection String Options](https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options)
- [Document Pre- and Post-Images](https://www.mongodb.com/docs/v6.0/changeStreams/#change-streams-with-document-pre--and-post-images)
- [BSON Types](https://docs.mongodb.com/manual/reference/bson-types/)
- [Flink DataTypes](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/types/)

FAQ
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))