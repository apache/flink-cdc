---
title: "TiDB"
weight: 8
type: docs
aliases:
- /connectors/flink-sources/tidb-cdc
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

# TiDB CDC Connector

The TiDB CDC connector reads a consistent snapshot of a TiDB table and then continuously reads row-level changes from TiKV CDC. It supports the Flink SQL/Table API and the unified DataStream Source API.

Dependencies
------------

### Maven dependency

{{< artifact flink-connector-tidb-cdc >}}

### SQL Client JAR

Download [flink-sql-connector-tidb-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-tidb-cdc) and place the JAR in `<FLINK_HOME>/lib/`.

The download link is available only for released versions. Use a connector artifact built for the Flink major version running the job.

Create a TiDB CDC table
-----------------------

The connector uses two endpoints:

- `hostname` and `port` connect to the TiDB SQL endpoint for schema discovery and snapshot reads.
- `pd-addresses` connects to PD and TiKV for incremental change reads.

All addresses advertised by PD and TiKV must be reachable from every TaskManager. Use `host-mapping` when the cluster advertises internal host names or addresses that Flink cannot resolve directly.

```sql
-- Checkpoint periodically so source progress can be recovered after a failure.
SET 'execution.checkpointing.interval' = '3s';

CREATE TABLE orders (
    order_id INT,
    order_date TIMESTAMP(3),
    customer_name STRING,
    price DECIMAL(10, 5),
    product_id INT,
    order_status BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'hostname' = 'tidb.example.com',
    'port' = '4000',
    'username' = 'root',
    'password' = '',
    'pd-addresses' = 'pd-0.example.com:2379,pd-1.example.com:2379',
    'database-name' = 'mydb',
    'table-name' = 'orders',
    'scan.startup.mode' = 'initial'
);

SELECT * FROM orders;
```

{{< hint info >}}
Define a primary key in the Flink table whenever the TiDB table has one. The key lets downstream operators and sinks interpret updates and deletes correctly. The connector currently creates one stream reader for one captured table, so use one SQL source table per TiDB table.
{{< /hint >}}

Connector options
-----------------

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `connector` | Yes | (none) | String | Must be `tidb-cdc`. |
| `hostname` | Yes | (none) | String | Host name or IP address of the TiDB SQL endpoint. |
| `port` | Yes | `4000` | Integer | Port of the TiDB SQL endpoint. Although a default exists in the configuration object, the table factory requires this option. |
| `username` | Yes | (none) | String | User name used for the TiDB JDBC connection. |
| `password` | Yes | (none) | String | Password used for the TiDB JDBC connection. Specify an empty string when the account has no password. |
| `pd-addresses` | Yes | (none) | String | Comma-separated PD endpoints used by the TiKV client. |
| `database-name` | Yes | (none) | String | Database containing the captured table. |
| `table-name` | Yes | (none) | String | Name of the single table to capture. |
| `scan.startup.mode` | No | `initial` | String | Startup mode: `initial`, `snapshot`, `latest-offset`, or `timestamp`. |
| `scan.startup.timestamp-millis` | Conditional | (none) | Long | Start timestamp in epoch milliseconds. Required when `scan.startup.mode` is `timestamp`. |
| `server-time-zone` | No | `UTC` | String | TiDB session time zone used when converting temporal values. |
| `connect.timeout` | No | `30s` | Duration | Maximum time to wait when opening a TiDB JDBC connection. |
| `connect.max-retries` | No | `3` | Integer | Maximum number of retries when opening a TiDB JDBC connection. |
| `connection.pool.size` | No | `20` | Integer | JDBC connection pool size. |
| `jdbc.driver` | No | `com.mysql.cj.jdbc.Driver` | String | JDBC driver class used to connect to TiDB. |
| `scan.incremental.snapshot.enabled` | No | `true` | Boolean | Accepted by the factory. The current SQL runtime always builds the incremental source and does not branch on this value. |
| `scan.incremental.snapshot.chunk.size` | No | `8096` | Integer | Approximate number of rows in each snapshot chunk. |
| `scan.snapshot.fetch.size` | No | `1024` | Integer | Maximum number of rows fetched by one snapshot poll. |
| `chunk-meta.group.size` | No | `1000` | Integer | Number of chunk metadata entries in each metadata group. |
| `scan.incremental.snapshot.chunk.key-column` | No | first primary-key column | String | Column used to split snapshot chunks. Use a comparable, evenly distributed column when possible. |
| `chunk-key.even-distribution.factor.upper-bound` | No | `1000.0` | Double | Upper bound used to decide whether the chunk key is evenly distributed. |
| `chunk-key.even-distribution.factor.lower-bound` | No | `0.05` | Double | Lower bound used to decide whether the chunk key is evenly distributed. |
| `host-mapping` | No | (none) | String | Maps advertised TiKV hosts to addresses reachable by Flink. Format: `internalHost:externalHost;internalHost2:externalHost2`. The port is preserved. |
| `heartbeat.interval.ms` | No | `30s` | Duration | Configured heartbeat interval. The current TiDB SQL runtime stores this value but does not attach a heartbeat setting to the source builder. |
| `table-list` | No | (none) | String | Accepted by the factory, but the current SQL runtime builds the capture list from `database-name` and `table-name`; do not use it as a replacement for those options. |

Options prefixed with `jdbc.properties.`, `debezium.`, and `tikv.` are accepted during table validation. In the current SQL runtime, arbitrary `tikv.*` keys are not copied into `TiConfiguration` and therefore have no runtime effect.

Startup modes
-------------

- `initial` (default): reads the table snapshot and then continues from the corresponding change position.
- `snapshot`: reads the snapshot only and stops before streaming changes.
- `latest-offset`: skips existing rows and reads changes produced after the source starts.
- `timestamp`: starts the change stream at `scan.startup.timestamp-millis`.

The source offset is stored in Flink checkpoint state. Enable checkpointing in production so the source can resume from a completed checkpoint after recovery.

Changelog semantics
-------------------

The table source declares the full Flink changelog mode:

| Row kind | Meaning |
| --- | --- |
| `+I` | Insert or snapshot row |
| `-U` | Value before an update |
| `+U` | Value after an update |
| `-D` | Deleted value |

TiKV must provide the old row value for updates and deletes. Downstream sinks that have a primary key can use these events to maintain the latest table state.

Available metadata
------------------

Metadata columns are read-only and must be declared with `METADATA ... VIRTUAL`.

| Key | Data type | Description |
| --- | --- | --- |
| `table_name` | `STRING NOT NULL` | Name of the source table. |
| `database_name` | `STRING NOT NULL` | Name of the source database. |
| `op_ts` | `TIMESTAMP_LTZ(3) NOT NULL` | Database change timestamp. Snapshot records use timestamp `0`. |
| `row_kind` | `STRING NOT NULL` | Flink row kind: `+I`, `-U`, `+U`, or `-D`. |

```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    source_table STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    change_kind STRING METADATA FROM 'row_kind' VIRTUAL,
    id BIGINT,
    name STRING,
    weight DECIMAL(10, 3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'tidb-cdc',
    'hostname' = 'tidb.example.com',
    'port' = '4000',
    'username' = 'root',
    'password' = '',
    'pd-addresses' = 'pd.example.com:2379',
    'database-name' = 'inventory',
    'table-name' = 'products'
);
```

DataStream Source
-----------------

Use `TiDBSourceBuilder.TiDBIncrementalSource` with `StreamExecutionEnvironment#fromSource`. The legacy `SourceFunction`-based `TiDBSource` API shown in older documentation is not part of the current connector.

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.source.TiDBSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

JdbcIncrementalSource<String> source =
        TiDBSourceBuilder.TiDBIncrementalSource.<String>builder()
                .hostname("tidb.example.com")
                .port(4000)
                .username("root")
                .password("")
                .pdAddresses("pd.example.com:2379")
                .databaseList("inventory")
                .tableList("inventory.products")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000L);
env.fromSource(source, WatermarkStrategy.noWatermarks(), "TiDB CDC")
        .print();
env.execute("TiDB CDC example");
```

Data type mapping
-----------------

| TiDB type | Flink SQL type | Notes |
| --- | --- | --- |
| `TINYINT` | `TINYINT` | |
| `TINYINT UNSIGNED`, `SMALLINT` | `SMALLINT` | |
| `SMALLINT UNSIGNED`, `MEDIUMINT`, `MEDIUMINT UNSIGNED`, `INT` | `INT` | |
| `INT UNSIGNED`, `BIGINT` | `BIGINT` | |
| `BIGINT UNSIGNED` | `DECIMAL(20, 0)` | |
| `FLOAT` | `FLOAT` | |
| `REAL`, `DOUBLE` | `DOUBLE` | |
| `NUMERIC(p,s)`, `DECIMAL(p,s)`, `p <= 38` | `DECIMAL(p,s)` | |
| `NUMERIC(p,s)`, `DECIMAL(p,s)`, `38 < p <= 65` | `STRING` | Flink decimals support at most 38 digits; use `STRING` to avoid precision loss. |
| `BOOLEAN`, `TINYINT(1)`, `BIT(1)` | `BOOLEAN` | |
| `DATE` | `DATE` | |
| `TIME(p)` | `TIME(p)` | |
| `TIMESTAMP(p)` | `TIMESTAMP_LTZ(p)` | Interpreted using `server-time-zone`. |
| `DATETIME(p)` | `TIMESTAMP(p)` | |
| `CHAR(n)` | `CHAR(n)` | |
| `VARCHAR(n)` | `VARCHAR(n)` | |
| `BIT(n)` | `BINARY(ceil(n / 8))` | |
| `BINARY(n)` | `BINARY(n)` | |
| `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `STRING` | |
| `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | `BYTES` | Values larger than the maximum Java array size are not supported. |
| `YEAR` | `INT` | |
| `ENUM` | `STRING` | |
| `SET` | `ARRAY<STRING>` | Values are split into string elements. |
| `JSON` | `STRING` | Serialized as JSON text. |

{{< top >}}
