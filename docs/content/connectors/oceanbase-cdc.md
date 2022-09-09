# OceanBase CDC Connector

The OceanBase CDC connector allows for reading snapshot data and incremental data from OceanBase. This document describes how to setup the OceanBase CDC connector to run SQL queries against OceanBase.

Dependencies
------------

In order to setup the OceanBase CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

```xml
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-oceanbase-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.2.1</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-oceanbase-cdc-2.2.1.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.2.1/flink-sql-connector-oceanbase-cdc-2.2.1.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-oceanbase-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-oceanbase-cdc-2.2.1.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-oceanbase-cdc), the released version will be available in the Maven central warehouse.

Setup OceanBase and LogProxy Server
----------------------

1. Setup the OceanBase cluster following the [deployment doc](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/deploy-the-distributed-oceanbase-cluster).

2. Create a user with password in `sys` tenant, this user is used in OceanBase LogProxy. See [user management doc](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/create-user-3).

   ```shell
   mysql -h${host} -P${port} -uroot
   
   mysql> SHOW TENANT;
   mysql> CREATE USER ${sys_username} IDENTIFIED BY '${sys_password}';
   mysql> GRANT ALL PRIVILEGES ON *.* TO ${sys_username} WITH GRANT OPTION;
   ```

3. Create a user in the tenant you want to monitor, this is used to read data for snapshot and change event.

4. Get the `rootservice_list`. You can use the following command to get the value:

    ```shell
    mysql> show parameters like 'rootservice_list';
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
    'tenant-name' = 'test_tenant',
    'database-name' = 'test_db',
    'table-name' = 'orders',
    'hostname' = '127.0.0.1',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = '127.0.0.1',
    'logproxy.port' = '2983');

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
                    <code>'initial'</code>,<code>'latest-offset'</code> or <code>'timestamp'</code>.
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
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Tenant name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>database-name</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Database name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Table name of OceanBase to monitor.</td>
            </tr>
            <tr>
                <td>hostname</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>IP address or hostname of the OceanBase database server or OceanBase Proxy server.</td>
            </tr>
            <tr>
                <td>port</td>
                <td>optional</td>
                <td style="word-wrap: break-word;">(none))</td>
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
                <td style="word-wrap: break-word;">UTC</td>
                <td>String</td>
                <td>The session time zone in database server, e.g. "Asia/Shanghai". It controls how the TIMESTAMP type in OceanBase converted to STRING in snapshot reading, please make sure to set it same with the timezone of `oblogproxy` deployment. </td>
            </tr>
            <tr>
                <td>rootserver-list</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>The semicolon-separated list of OceanBase root servers in format `ip:rpc_port:sql_port`.</td>
            </tr>
            <tr>
                <td>logproxy.host</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>String</td>
                <td>Hostname or IP address of OceanBase log proxy service.</td>
            </tr>
            <tr>
                <td>logproxy.port</td>
                <td>required</td>
                <td style="word-wrap: break-word;">(none)</td>
                <td>Integer</td>
                <td>Port number of OceanBase log proxy service.</td>
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
            <td>STRING NOT NULL</td>
            <td>Name of the tenant that contains the row.</td>
        </tr>
        <tr>
            <td>database_name</td>
            <td>STRING NOT NULL</td>
            <td>Name of the database that contains the row.</td>
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
   'database-name' = 'test_db',
   'table-name' = 'orders',
   'hostname' = '127.0.0.1',
   'port' = '2881',
   'rootserver-list' = '127.0.0.1:2882:2881',
   'logproxy.host' = '127.0.0.1',
   'logproxy.port' = '2983');
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

### Consume Commit Log

The OceanBase CDC Connector using [oblogclient](https://github.com/oceanbase/oblogclient) to consume commit log from OceanBase LogProxy.

### DataStream Source

The OceanBase CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseTableSourceFactory;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class OceanBaseSourceExample {

  public static void main(String[] args) throws Exception {
    SourceFunction<String> oceanBaseSource =
        OceanBaseSource.<String>builder()
            .rsList("127.0.0.1:2882:2881")  // set root server list
            .startupMode(StartupMode.INITIAL) // set startup mode
            .username("user@test_tenant")  // set cluster username
            .password("pswd")  // set cluster password
            .tenantName("test_tenant")  // set captured tenant name, do not support regex
            .databaseName("test_db")  // set captured database, support regex
            .tableName("test_table")  // set captured table, support regex
            .hostname("127.0.0.1")  // set hostname of OceanBase server or proxy
            .port(2881)  // set the sql port for OceanBase server or proxy
            .logProxyHost("127.0.0.1")  // set the hostname of log proxy
            .logProxyPort(2983)  // set the port of log proxy
            .deserializer(new JsonDebeziumDeserializationSchema())  // converts SourceRecord to JSON String
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);
    
    env.addSource(oceanBaseSource).print().setParallelism(1);

    env.execute("Print OceanBase Snapshot + Commit Log");
  }
}
```
Data Type Mapping
----------------

When the startup mode is not `INITIAL`, we will not be able to get the precision and scale of a column. In order to be compatible with different startup modes, we will not map one OceanBase type of different precision to different FLink types.

For example, you can get a boolean from a column with type BOOLEAN, TINYINT(1) or BIT(1). BOOLEAN is equivalent to TINYINT(1) in OceanBase, so columns of BOOLEAN and TINYINT types will be mapped to TINYINT in Flink, and BIT(1) will be mapped to BINARY(1) in Flink.

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
                    TINYINT</td>
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
                <td>TIMESTAMP [(p)]<br>
                    DATETIME [(p)]
                </td>
                <td>TIMESTAMP [(p)]
                </td>
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
                <td>BINARY(⌈n/8⌉)</td>
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
                <td>STRING</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</div>
