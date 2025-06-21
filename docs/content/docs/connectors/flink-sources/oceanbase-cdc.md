---
title: "OceanBase"
weight: 9
type: docs
aliases:
- /connectors/flink-sources/oceanbase-cdc
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

# OceanBase CDC Connector

The OceanBase CDC connector allows for reading snapshot data and incremental data from OceanBase. This document describes how to set up the OceanBase CDC connector to run SQL queries against OceanBase.

### OceanBase CDC Solutions

Glossary:

- *OceanBase CE*: OceanBase Community Edition. It's compatible with MySQL and has been open sourced at https://github.com/oceanbase/oceanbase.
- *OceanBase EE*: OceanBase Enterprise Edition. It supports two compatibility modes: MySQL and Oracle. See https://en.oceanbase.com.
- *OceanBase Cloud*: OceanBase Enterprise Edition on Cloud. See https://en.oceanbase.com/product/cloud.
- *Log Proxy CE*: OceanBase Log Proxy Community Edition (CDC mode). It's a proxy service which can fetch the commit log data of OceanBase CE. It has been open sourced at https://github.com/oceanbase/oblogproxy.
- *Log Proxy EE*: OceanBase Log Proxy Enterprise Edition (CDC mode). It's a proxy service which can fetch the commit log data of OceanBase EE. Limited support is available on OceanBase Cloud only, you can contact the provider support for more details.
- *Binlog Service CE*: OceanBase Binlog Service Community Edition. It is a solution of OceanBase CE that is compatible with the MySQL replication protocol. See the docs of Log Proxy CE (Binlog mode) for details.
- *Binlog Service EE*: OceanBase Binlog Service Enterprise Edition. It is a solution of OceanBase EE MySQL mode that is compatible with the MySQL replication protocol, and it's only available for users of Alibaba Cloud, see [User Guide](https://www.alibabacloud.com/help/en/apsaradb-for-oceanbase/latest/binlog-overview).
- *MySQL Driver*: `mysql-connector-java` which can be used with OceanBase CE and OceanBase EE MySQL mode.
- *OceanBase Driver*: The Jdbc driver for OceanBase, which supports both MySQL mode and Oracle mode of all OceanBase versions. It's open sourced at https://github.com/oceanbase/obconnector-j.

CDC Source Solutions for OceanBase:

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">Database</th>
                <th class="text-left">Supported Driver</th>
                <th class="text-left">CDC Source Connector</th>
                <th class="text-left">Other Required Components</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td rowspan="2">OceanBase CE</td>
                <td>
                    MySQL Driver: 5.1.4x, 8.0.x <br>
                    OceanBase Driver: 2.4.x
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Log Proxy CE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service CE</td>
            </tr>
            <tr>
                <td rowspan="2">OceanBase EE (MySQL Mode)</td>
                <td>
                    MySQL Driver: 5.1.4x, 8.0.x <br>
                    OceanBase Driver: 2.4.x
                </td>
                <td>OceanBase CDC Connector</td>
                <td>Log Proxy EE</td>
            </tr>
            <tr>
                <td>MySQL Driver: 8.0.x</td>
                <td>MySQL CDC Connector</td>
                <td>Binlog Service EE</td>
            </tr>
            <tr>
                <td>OceanBase EE (Oracle Mode)</td>
                <td>OceanBase Driver: 2.4.x</td>
                <td>OceanBase CDC Connector</td>
                <td>Log Proxy EE (CDC Mode)</td>
            </tr>
        </tbody>
    </table>
</div>

Note: For users of OceanBase CE or OceanBase EE MySQL Mode, we recommend that you follow the [MySQL CDC documentation](mysql-cdc.md) to use the MySQL CDC source connector with the Binlog service.

Dependencies
------------

In order to set up the OceanBase CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

{{< artifact flink-connector-oceanbase-cdc >}}

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oceanbase-cdc/{{< param Version >}}/flink-sql-connector-oceanbase-cdc-{{< param Version >}}.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** Refer to [flink-sql-connector-oceanbase-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oceanbase-cdc), more released versions will be available in the Maven central warehouse.

Since the licenses of MySQL Driver and OceanBase Driver are incompatible with Flink CDC project, we can't provide them in prebuilt connector jar packages. You may need to configure the following dependencies manually.

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Dependency Item</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.27">mysql:mysql-connector-java:8.0.27</a></td>
        <td>Used for connecting to MySQL tenant of OceanBase.</td>
      </tr>
    </tbody>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client/2.4.9">com.oceanbase:oceanbase-client:2.4.9</a></td>
        <td>Used for connecting to MySQL or Oracle tenant of OceanBase.</td>
      </tr>
    </tbody>
</table>
</div>

Setup OceanBase and LogProxy Server
----------------------

1. Set up the OceanBase cluster following the [doc](https://github.com/oceanbase/oceanbase#quick-start).

2. Create a user with password in `sys` tenant, this user is used in OceanBase LogProxy.

   ```shell
   mysql -h${host} -P${port} -uroot
   
   mysql> SHOW TENANT;
   mysql> CREATE USER ${sys_username} IDENTIFIED BY '${sys_password}';
   mysql> GRANT ALL PRIVILEGES ON *.* TO ${sys_username} WITH GRANT OPTION;
   ```

3. Create a user in the tenant you want to monitor, this is used to read data for snapshot and change event.

4. For users of OceanBase Community Edition, you need to get the `rootserver-list`. You can use the following command to get the value:

    ```shell
    mysql> show parameters like 'rootservice_list';
    ```

   For users of OceanBase Enterprise Edition, you need to get the `config-url`. You can use the following command to get the value:

    ```shell
    mysql> show parameters like 'obconfig_url';
    ```

5. Setup OceanBase LogProxy. For users of OceanBase Community Edition, you can follow the [docs (Chinese)](https://www.oceanbase.com/docs/community-oblogproxy-doc-1000000000531984).

How to create a OceanBase CDC table
----------------

The OceanBase CDC table can be defined as following:

```sql
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';

-- register a OceanBase table 'orders' in Flink SQL
Flink SQL> CREATE TABLE orders (
    order_id     INT,
    order_date   TIMESTAMP(0),
    customer_name STRING,
    price        DECIMAL(10, 5),
    product_id   INT,
    order_status BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'oceanbase-cdc',
    'scan.startup.mode' = 'initial',
    'username' = 'user@test_tenant#cluster_name',
    'password' = 'pswd',
    'tenant-name' = 'test_tenant',
    'database-name' = '^test_db$',
    'table-name' = '^orders$',
    'hostname' = '127.0.0.1',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = '127.0.0.1',
    'logproxy.port' = '2983',
    'working-mode' = 'memory'
);

-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM orders;
```

If you want to use OceanBase Oracle mode, you need to add the OceanBase jdbc jar file to Flink and set up the enterprise edition of oblogproxy, then you can create a table in Flink as following:

```sql
Flink SQL> CREATE TABLE orders (
    order_id     INT,
    order_date   TIMESTAMP(0),
    customer_name STRING,
    price        DECIMAL(10, 5),
    product_id   INT,
    order_status BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'oceanbase-cdc',
    'scan.startup.mode' = 'initial',
    'username' = 'user@test_tenant#cluster_name',
    'password' = 'pswd',
    'tenant-name' = 'test_tenant',
    'database-name' = '^test_db$',
    'table-name' = '^orders$',
    'hostname' = '127.0.0.1',
    'port' = '2881',
    'compatible-mode' = 'oracle',
    'jdbc.driver' = 'com.oceanbase.jdbc.Driver',
    'config-url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&User_ID=xxx&UID=xxx&ObRegion=xxx',
    'logproxy.host' = '127.0.0.1',
    'logproxy.port' = '2983',
    'working-mode' = 'memory'
);
```

You can also try the quickstart tutorial that sync data from OceanBase to Elasticsearch, please refer [Flink CDC Tutorial]({{< ref "docs/connectors/flink-sources/tutorials/oceanbase-tutorial" >}}) for more information.


Connector Options
----------------

The OceanBase CDC Connector contains some options for both sql and stream api as the following sheet. 

*Note*: The connector supports two ways to specify the table list to listen to, and will get the union of the results when both way are used at the same time.
1. Use `database-name` and `table-name` to match database and table names in regex.
2. Use `table-list` to match the exact value of database and table names.

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
                <td>Specify what connector to use, here should be <code>'oceanbase-cdc'</code>.</td>
            </tr>
            <tr>
                <td>scan.startup.mode</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">initial</td>
                <td>String</td>
                <td>Specify the startup mode for OceanBase CDC consumer, valid enumerations are
                    <code>'initial'</code>,<code>'latest-offset'</code>,<code>'timestamp'</code> or <code>'snapshot'</code>.
                </td>
            </tr>
            <tr>
                <td>scan.startup.timestamp</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>Long</td>
                <td>Timestamp in seconds of the start point, only used for <code>'timestamp'</code> startup mode.</td>
            </tr>
            <tr>
                <td>username</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Username to be used when connecting to OceanBase.</td>
            </tr>
            <tr>
                <td>password</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Password to be used when connecting to OceanBase.</td>
            </tr>
            <tr>
                <td>tenant-name</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Tenant name of OceanBase to monitor, should be exact value. Required when 'scan.startup.mode' is not 'snapshot'.</td>
            </tr>
            <tr>
                <td>database-name</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Database name of OceanBase to monitor, should be regular expression.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Table name of OceanBase to monitor, should be regular expression.</td>
            </tr>
            <tr>
                <td>table-list</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>List of full names of tables, separated by commas, e.g. "db1.table1, db2.table2".</td>
            </tr>
            <tr>
                <td>hostname</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>IP address or hostname of the OceanBase database server or OceanBase Proxy server.</td>
            </tr>
            <tr>
                <td>port</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>Integer</td>
                <td>Integer port number to connect to OceanBase. It can be the SQL port of OceanBase server, which is 2881 by default, or the port of OceanBase proxy service, which is 2883 by default.</td>
            </tr>
            <tr>
                <td>connect.timeout</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">30s</td>
                <td>Duration</td>
                <td>The maximum time that the connector should wait after trying to connect to the OceanBase database server before timing out.</td>
            </tr>
            <tr>
                <td>server-time-zone</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">+00:00</td>
                <td>String</td>
                <td>The session timezone which controls how temporal types are converted to STRING in OceanBase. Can be UTC offset in format "±hh:mm", or named time zones if the time zone information tables in the mysql database have been created and populated.</td>
            </tr>
            <tr>
                <td>logproxy.host</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Hostname or IP address of OceanBase log proxy service. Required when 'scan.startup.mode' is not 'snapshot'.</td>
            </tr>
            <tr>
                <td>logproxy.port</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>Integer</td>
                <td>Port number of OceanBase log proxy service. Required when 'scan.startup.mode' is not 'snapshot'.</td>
            </tr>
            <tr>
                <td>logproxy.client.id</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">By rule.</td>
                <td>String</td>
                <td>Id of a log proxy client connection, will be in format {flink_ip}_{process_id}_{timestamp}_{thread_id}_{tenant} by default.</td>
            </tr>
            <tr>
                <td>rootserver-list</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>The semicolon-separated list of OceanBase root servers in format `ip:rpc_port:sql_port`, required for OceanBase CE.</td>
            </tr>
            <tr>
                <td>config-url</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>The url to get the server info from the config server, required for OceanBase EE.</td>
            </tr>
            <tr>
                <td>working-mode</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">storage</td>
                <td>String</td>
                <td>Working mode of `obcdc` in LogProxy, can be `storage` or `memory`.</td>
            </tr>
            <tr>
                <td>compatible-mode</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">mysql</td>
                <td>String</td>
                <td>Compatible mode of OceanBase, can be `mysql` or `oracle`.</td>
            </tr>
            <tr>
                <td>jdbc.driver</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">com.mysql.cj.jdbc.Driver</td>
                <td>String</td>
                <td>JDBC driver class for snapshot reading.</td>
            </tr>
            <tr>
                <td>jdbc.properties.*</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Option to pass custom JDBC URL properties. User can pass custom properties like 'jdbc.properties.useSSL' = 'false'.</td>
            </tr>
            <tr>
                <td>obcdc.properties.*</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Option to pass custom configurations to the <code>libobcdc</code>, eg: 'obcdc.properties.sort_trans_participants' = '1'. Please refer to <a href="https://en.oceanbase.com/docs/common-oceanbase-database-10000000000872541">obcdc parameters</a> for more details.</td>
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
            <td>tenant_name</td>
            <td>STRING</td>
            <td>Name of the tenant that contains the row.</td>
        </tr>
        <tr>
            <td>database_name</td>
            <td>STRING</td>
            <td>Name of the database that contains the row.</td>
        </tr>
        <tr>
            <td>schema_name</td>
            <td>STRING</td>
            <td>Name of the schema that contains the row.</td>
        </tr>
        <tr>
            <td>table_name</td>
            <td>STRING NOT NULL</td>
            <td>Name of the table that contains the row.</td>
        </tr>
        <tr>
            <td>op_ts</td>
            <td>TIMESTAMP_LTZ(3) NOT NULL</td>
            <td>It indicates the time that the change was made in the database. <br>
                If the record is read from snapshot of the table instead of the change stream, the value is always 0.</td>
        </tr>
    </tbody>
</table>

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE products (
    tenant_name STRING METADATA FROM 'tenant_name' VIRTUAL,
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
   'connector' = 'oceanbase-cdc',
   'scan.startup.mode' = 'initial',
   'username' = 'user@test_tenant',
   'password' = 'pswd',
   'tenant-name' = 'test_tenant',
   'database-name' = '^test_db$',
   'table-name' = '^orders$',
   'hostname' = '127.0.0.1',
   'port' = '2881',
   'rootserver-list' = '127.0.0.1:2882:2881',
   'logproxy.host' = '127.0.0.1',
   'logproxy.port' = '2983',
   'working-mode' = 'memory'
);
```

Features
--------

### At-Least-Once Processing

The OceanBase CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **at-least-once processing**.

OceanBase is a kind of distributed database whose log files are distributed on different servers. As there is no position information like MySQL binlog offset, we can only use timestamp as the position mark. In order to ensure the completeness of reading data, `liboblog` (a C++ library to read OceanBase log record) might read some log data before the given timestamp. So in this way we may read duplicate data whose timestamp is around the start point, and only 'at-least-once' can be guaranteed.

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for OceanBase CDC consumer. The valid enumerations are:

- `initial`: Performs an initial snapshot on the monitored table upon first startup, and continue to read the latest commit log.
- `latest-offset`: Never to perform snapshot on the monitored table upon first startup and just read the latest commit log since the connector is started.
- `timestamp`: Never to perform snapshot on the monitored table upon first startup and just read the commit log from the given `scan.startup.timestamp`.
- `snapshot`: Only perform snapshot on the monitored table.

### Consume Commit Log

The OceanBase CDC Connector using [oblogclient](https://github.com/oceanbase/oblogclient) to consume commit log from OceanBase LogProxy.

### DataStream Source

The OceanBase CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oceanbase.OceanBaseSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class OceanBaseSourceExample {
   public static void main(String[] args) throws Exception {
      SourceFunction<String> oceanBaseSource =
              OceanBaseSource.<String>builder()
                      .startupOptions(StartupOptions.initial())
                      .hostname("127.0.0.1")
                      .port(2881)
                      .username("user@test_tenant")
                      .password("pswd")
                      .compatibleMode("mysql")
                      .jdbcDriver("com.mysql.cj.jdbc.Driver")
                      .tenantName("test_tenant")
                      .databaseName("^test_db$")
                      .tableName("^test_table$")
                      .logProxyHost("127.0.0.1")
                      .logProxyPort(2983)
                      .rsList("127.0.0.1:2882:2881")
                      .serverTimeZone("+08:00")
                      .deserializer(new JsonDebeziumDeserializationSchema())
                      .build();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      // enable checkpoint
      env.enableCheckpointing(3000);

      env.addSource(oceanBaseSource).print().setParallelism(1);
      env.execute("Print OceanBase Snapshot + Change Events");
   }
}
```

### Available Source metrics

Metrics can help understand the progress of assignments, and the following are the supported [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description                                         |
|------------------------|----------------------------|-------|-----------------------------------------------------|
| namespace.schema.table | isSnapshotting             | Gauge | Weather the table is snapshotting or not            |
| namespace.schema.table | isStreamReading            | Gauge | Weather the table is stream reading or not          |
| namespace.schema.table | numTablesSnapshotted       | Gauge | The number of tables that have been snapshotted     |
| namespace.schema.table | numTablesRemaining         | Gauge | The number of tables that have not been snapshotted |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | The number of splits that is being processed        |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | The number of splits that have not been processed   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | The number of splits that have been processed       |
| namespace.schema.table | snapshotStartTime          | Gauge | The time when the snapshot started                  |
| namespace.schema.table | snapshotEndTime            | Gauge | The time when the snapshot ended                    |

Notice:
1. The group name is `namespace.schema.table`, where `namespace` is the actual database name, `schema` is the actual schema name, and `table` is the actual table name.
2. For OceanBase, the `namespace` will be set to the default value "", and the group name will be like `test_database.test_table`.

Data Type Mapping
----------------

### Mysql Mode

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">OceanBase type</th>
                <th class="text-left">Flink SQL type</th>
                <th class="text-left">NOTE</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>BOOLEAN<br>
                    TINYINT(1)<br>
                    BIT(1)</td>
                <td>BOOLEAN</td>
                <td></td>
            </tr>
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
                    REAL<br>
                    FLOAT<br>
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
                    where 38 < p <=65<br>
                </td>
                <td>STRING</td>
                <td>DECIMAL is equivalent to NUMERIC. The precision for DECIMAL data type is up to 65 in OceanBase, but
                    the precision for DECIMAL is limited to 38 in Flink.
                    So if you define a decimal column whose precision is greater than 38, you should map it to STRING to
                    avoid precision loss.</td>
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
                <td>DATETIME [(p)]</td>
                <td>TIMESTAMP [(p)]</td>
                <td></td>
            </tr>
            <tr>
                <td>TIMESTAMP [(p)]</td>
                <td>TIMESTAMP_LTZ [(p)]</td>
                <td></td>
            </tr>
            <tr>
                <td>CHAR(n)</td>
                <td>CHAR(n)</td>
                <td></td>
            </tr>
            <tr>
                <td>VARCHAR(n)</td>
                <td>VARCHAR(n)</td>
                <td></td>
            </tr>
            <tr>
                <td>BIT(n)</td>
                <td>BINARY(⌈(n + 7) / 8⌉)</td>
                <td></td>
            </tr>
            <tr>
                <td>BINARY(n)</td>
                <td>BINARY(n)</td>
                <td></td>
            </tr>
            <tr>
                <td>VARBINARY(N)</td>
                <td>VARBINARY(N)</td>
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
                <td></td>
            </tr>
            <tr>
                <td>YEAR</td>
                <td>INT</td>
                <td></td>
            </tr>
            <tr>
                <td>ENUM</td>
                <td>STRING</td>
                <td></td>
            </tr>
            <tr>
                <td>SET</td>
                <td>ARRAY&lt;STRING&gt;</td>
                <td>As the SET data type in OceanBase is a string object that can have zero or more values, it should always be mapped to an array of string</td>
            </tr>
            <tr>
                <td>JSON</td>
                <td>STRING</td>
                <td>The JSON data type  will be converted into STRING with JSON format in Flink.</td>
            </tr>
        </tbody>
    </table>
</div>

### Oracle Mode

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">OceanBase type</th>
                <th class="text-left">Flink SQL type</th>
                <th class="text-left">NOTE</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>NUMBER(1)</td>
                <td>BOOLEAN</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), p - s < 3 </td>
                <td>TINYINT</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), p - s < 5 </td>
                <td>SMALLINT</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), p - s < 10 </td>
                <td>INT</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), p - s < 19 </td>
                <td>BIGINT</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), 19 <=p - s <=38</td>
                <td>DECIMAL(p - s, 0)</td>
                <td></td>
            </tr>
            <tr>
                <td>NUMBER(p, s > 0)</td>
                <td>DECIMAL(p, s)</td>
            </tr>
            <tr>
                <td>NUMBER(p, s <= 0), p - s> 38 </td>
                <td>STRING</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    FLOAT<br>
                    BINARY_FLOAT
                </td>
                <td>FLOAT</td>
                <td></td>
            </tr>
            <tr>
                <td>BINARY_DOUBLE</td>
                <td>DOUBLE</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    DATE<br>
                    TIMESTAMP [(p)]
                </td>
                <td>TIMESTAMP [(p)]</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    CHAR(n)<br>
                    NCHAR(n)<br>
                    VARCHAR(n)<br>
                    VARCHAR2(n)<br>
                    NVARCHAR2(n)<br>
                    CLOB<br>
                </td>
                <td>STRING</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    RAW<br>
                    BLOB<br>
                    ROWID
                </td>
                <td>BYTES</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>

{{< top >}}
