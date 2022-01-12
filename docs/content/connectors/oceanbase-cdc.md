# OceanBase CDC Connector

The OceanBase CDC connector allows for reading snapshot data and incremental data from OceanBase. This document describes how to setup the OceanBase CDC connector to run SQL queries against OceanBase.

Dependencies
------------

In order to setup the OceanBase CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

```xml
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-oceanbase-cdc</artifactId>
  <!-- the dependency is available only for stable releases. -->
  <version>2.2-SNAPSHOT</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-oceanbase-cdc-2.2-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.2-SNAPSHOT/flink-sql-connector-oceanbase-cdc-2.2-SNAPSHOT.jar) and put it under `<FLINK_HOME>/lib/`.

Setup OceanBase and LogProxy Server
----------------------

1. Setup the OceanBase cluster following the [deployment doc](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/deploy-the-distributed-oceanbase-cluster).

2. Create a user with password in `sys` tenant, this user is used in OceanBase LogProxy. See [user management doc](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/create-user-3).

3. Create a user in the tenant you want to monitor, this is used to read data for snapshot and change event.

4. Get the `rootservice_list`. You can use the following command to get the value:

    ```mysql
    show parameters like 'rootservice_list';
    ```

5. Setup OceanBase LogProxy following the [quick start](https://github.com/oceanbase/oblogproxy#quick-start).

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
    'username' = 'user@test_tenant',
    'password' = 'pswd',
    'tenant_name' = 'test_tenant',
    'database_name' = 'test_db',
    'table_name' = 'orders',
    'rootserver_list' = '127.0.0.1:2882:2881',
    'log_proxy.host' = '127.0.0.1',
    'log_proxy.port' = '2983',
    'jdbc.url' = 'jdbc:mysql://127.0.0.1:2881/test_db');

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
                <td>Specify what connector to use, here should be <code>'oceanbase-cdc'</code>.</td>
            </tr>
            <tr>
                <td>scan.startup.mode</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Specify the startup mode for OceanBase CDC consumer, valid enumerations are
                    <code>'initial'</code>,<code>'latest'</code> or <code>'timestamp'</code>.
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
                <td>tenant_name</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Tenant name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>database_name</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Database name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>table_name</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Table name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>rootserver_list</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>The semicolon-separated list of OceanBase root servers in format `ip:rpc_port:sql_port`.</td>
            </tr>
            <tr>
                <td>log_proxy.host</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Hostname or IP address of OceanBase log proxy service.</td>
            </tr>
            <tr>
                <td>log_proxy.port</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">2983</td>
                <td>Integer</td>
                <td>Port number of OceanBase log proxy service.</td>
            </tr>
            <tr>
                <td>jdbc.url</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>JDBC url, only used for snapshot.</td>
            </tr>
            <tr>
                <td>jdbc.driver</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">'com.mysql.jdbc.Driver'</td>
                <td>String</td>
                <td>JDBC driver class, only used for snapshot.</td>
            </tr>
        </tbody>
    </table>
</div>

Features
--------

### At-Least-Once Processing

The OceanBase CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **at-least-once processing**.

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for OceanBase CDC consumer. The valid enumerations are:

- `initial`: Performs an initial snapshot on the monitored table upon first startup, and continue to read the latest commit log.
- `latest`: Never to perform snapshot on the monitored table upon first startup and just read the latest commit log since the connector is started.
- `timestamp`: Never to perform snapshot on the monitored table upon first startup and just read the commit log from the given `scan.startup.timestamp`.

### Consume Commit Log

The OceanBase CDC Connector using [oblogclient](https://github.com/oceanbase/oblogclient) to consume commit log from OceanBase LogProxy.

### DataStream Source

The OceanBase CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
public class OceanBaseSourceExample {

  public static void main(String[] args) throws Exception {

    ResolvedSchema schema =
        new ResolvedSchema(
            Arrays.asList(
                Column.physical("id", DataTypes.INT()),
                Column.physical("name", DataTypes.STRING())),
            new ArrayList<>(),
            UniqueConstraint.primaryKey("id", Collections.singletonList("id")));
    RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

    SourceFunction<RowData> oceanBaseSource =
        OceanBaseSource.<RowData>builder()
            .rsList("127.0.0.1:2882:2881")
            .startupMode(OceanBaseTableSourceFactory.StartupMode.INITIAL)
            .username("user@test_tenant")
            .password("pswd")
            .tenantName("test_tenant")
            .databaseName("test_db")
            .tableName("test_table")
            .logProxyHost("127.0.0.1")
            .logProxyPort(2983)
            .jdbcUrl("jdbc:mysql://127.0.0.1:2881")
            .snapshotEventDeserializer(new OceanBaseSnapshotEventDeserializer(rowType))
            .changeEventDeserializer(new OceanBaseChangeEventDeserializer())
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    TypeInformation<RowData> typeInformation =
        InternalTypeInfo.of(schema.toPhysicalRowDataType().getLogicalType());
    env.addSource(oceanBaseSource).returns(typeInformation).print().setParallelism(1);

    env.execute("Print OceanBase Snapshot + Commit Log");
  }
}
```

<!-- TODO add data type mapping -->
