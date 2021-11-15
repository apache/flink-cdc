# MongoDB CDC Connector

The MongoDB CDC connector allows for reading snapshot data and incremental data from MongoDB. This document describes how to setup the MongoDB CDC connector to run SQL queries against MongoDB.

Dependencies
------------

In order to setup the MongoDB CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency
<!-- fixme: correct the version -->
```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mongodb-cdc</artifactId>
  <!-- the dependency is available only for stable releases. -->
  <version>2.1.0</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-mongodb-cdc-2.1.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/2.1.0/flink-sql-connector-mongodb-cdc-2.1.0.jar) and put it under `<FLINK_HOME>/lib/`.

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
  db.createUser({
    user: "flinkuser",
    pwd: "flinkpw",
    roles: [
      { role: "read", db: "admin" }, //read role includes changeStream privilege 
      { role: "readAnyDatabase", db: "admin" } //for snapshot reading
    ]
  });
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

MongoDB's change event record doesn't have update before message. So, we can only convert it to Flink's UPSERT changelog stream.
An upsert stream requires a unique key, so we must declare `_id` as primary key. 
We can't declare other column as primary key, becauce delete operation do not contain's the key and value besides `_id` and `sharding key`.

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
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the database to watch for changes.</td>
    </tr>
    <tr>
      <td>collection</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the collection in the database to watch for changes.</td>
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
      <td>errors.tolerance</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">none</td>
      <td>String</td>
      <td>Whether to continue processing messages if an error is encountered.
          Accept <code>none</code> or <code>all</code>.
          When set to <code>none</code>, the connector reports an error and blocks further processing of the rest of the records
          when it encounters an error. When set to <code>all</code>, the connector silently ignores any bad messages.
      </td>
    </tr> 
    <tr>
      <td>errors.log.enable</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether details of failed operations should be written to the log file.</td>
    </tr>
    <tr>
      <td>copy.existing</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether copy existing data from source collections.</td>
    </tr>
    <tr>
      <td>copy.existing.pipeline</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> An array of JSON objects describing the pipeline operations to run when copying existing data.<br>
           This can improve the use of indexes by the copying manager and make copying more efficient.
           eg. <code>[{"$match": {"closed": "false"}}]</code> ensures that 
           only documents in which the closed field is set to false are copied.
      </td>
    </tr>
    <tr>
      <td>copy.existing.max.threads</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">Processors Count</td>
      <td>Integer</td>
      <td>The number of threads to use when performing the data copy.</td>
    </tr>
    <tr>
      <td>copy.existing.queue.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16000</td>
      <td>Integer</td>
      <td>The max size of the queue to use when copying data.</td>
    </tr>
    <tr>
      <td>poll.max.batch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>Maximum number of change stream documents to include in a single batch when polling for new data.</td>
    </tr>
    <tr>
      <td>poll.await.time.ms</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1500</td>
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
    </tbody>
</table>
</div>

Note: `heartbeat.interval.ms` is highly recommended to set a proper value larger than 0 **if the collection changes slowly**.
The heartbeat event can pushing the `resumeToken` forward to avoid `resumeToken` being expired when we recover the Flink job from a checkpoint or savepoint.

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
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
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

### Snapshot When Startup Or Not

The config option `copy.existing` specifies whether do snapshot when MongoDB CDC consumer startup. <br>Defaults to `true`.

### Snapshot Data Filters

The config option `copy.existing.pipeline` describing the filters when copying existing data.<br>
This can filter only required data and improve the use of indexes by the copying manager.

In the following example, the `$match` aggregation operator ensures that only documents in which the closed field is set to false are copied.

```
copy.existing.pipeline=[ { "$match": { "closed": "false" } } ]
```

### Change Streams

We integrates the [MongoDB's official Kafka Connector](https://docs.mongodb.com/kafka-connector/current/kafka-source/) to read snapshot or change events from MongoDB and drive it by Debezium's `EmbeddedEngine`.

Debezium's `EmbeddedEngine` provides a mechanism for running a single Kafka Connect `SourceConnector` within an application's process and it can drive any standard Kafka Connect `SourceConnector` properly even which is not provided by Debezium.

We choose **MongoDB's official Kafka Connector** instead of the **Debezium's MongoDB Connector** cause they use a different change data capture mechanism.

- For Debezium's MongoDB Connector, it read the `oplog.rs` collection of each replica-set's master node.
- For MongoDB's Kafka Connector, it subscribe `Change Stream` of MongoDB.

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
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;

public class MongoDBSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts("localhost:27017")
                .username("flink")
                .password("flinkpw")
                .database("inventory")
                .collection("products")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
```


Data Type Mapping
----------------
[BSON](https://docs.mongodb.com/manual/reference/bson-types/) short for **Binary JSON** is a bin­ary-en­coded seri­al­iz­a­tion of JSON-like format used to store documents and make remote procedure calls in MongoDB.

[Flink SQL Data Type](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/types/) is similar to the SQL standard’s data type terminology which describes the logical type of a value in the table ecosystem. It can be used to declare input and/or output types of operations.

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
- [BSON Types](https://docs.mongodb.com/manual/reference/bson-types/)
- [Flink DataTypes](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/types/)
