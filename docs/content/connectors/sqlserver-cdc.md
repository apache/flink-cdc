# SQLServer CDC Connector

The SQLServer CDC connector allows for reading snapshot data and incremental data from SQLServer database. This document describes how to setup the SQLServer CDC connector to run SQL queries against SQLServer databases.

Dependencies
------------

In order to setup the SQLServer CDC connector, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-sqlserver-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.3.0</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-sql-connector-sqlserver-cdc-2.3.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-sqlserver-cdc/2.3.0/flink-sql-connector-sqlserver-cdc-2.3.0.jar) and put it under `<FLINK_HOME>/lib/`.

**Note:** flink-sql-connector-sqlserver-cdc-XXX-SNAPSHOT version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as [flink-sql-connector-sqlserver-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-sqlserver-cdc), the released version will be available in the Maven central warehouse.

Setup SQLServer Database
----------------
A SQL Server administrator must enable change data capture on the source tables that you want to capture. The database must already be enabled for CDC. To enable CDC on a table, a SQL Server administrator runs the stored procedure ```sys.sp_cdc_enable_table``` for the table.

**Prerequisites:**
* CDC is enabled on the SQL Server database.
* The SQL Server Agent is running.
* You are a member of the db_owner fixed database role for the database.

**Procedure:**
* Connect to the SQL Server database by database management studio.
* Run the following SQL statement to enable CDC on the table.
```sql
USE MyDB
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',     -- Specifies the schema of the source table.
@source_name   = N'MyTable', -- Specifies the name of the table that you want to capture.
@role_name     = N'MyRole',  -- Specifies a role MyRole to which you can add users to whom you want to grant SELECT permission on the captured columns of the source table. Users in the sysadmin or db_owner role also have access to the specified change tables. Set the value of @role_name to NULL, to allow only members in the sysadmin or db_owner to have full access to captured information.
@filegroup_name = N'MyDB_CT',-- Specifies the filegroup where SQL Server places the change table for the captured table. The named filegroup must already exist. It is best not to locate change tables in the same filegroup that you use for source tables.
@supports_net_changes = 0
GO
```
* Verifying that the user has access to the CDC table
```sql
--The following example runs the stored procedure sys.sp_cdc_help_change_data_capture on the database MyDB:
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture
GO
```
The query returns configuration information for each table in the database that is enabled for CDC and that contains change data that the caller is authorized to access. If the result is empty, verify that the user has privileges to access both the capture instance and the CDC tables.

How to create a SQLServer CDC table
----------------

The SqlServer CDC table can be defined as following:

```sql
-- register a SqlServer table 'orders' in Flink SQL
CREATE TABLE orders (
    id INT,
    order_date DATE,
    purchaser INT,
    quantity INT,
    product_id INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'schema-name' = 'dbo',
    'table-name' = 'orders'
);

-- read snapshot and binlogs from orders table
SELECT * FROM orders;
```

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
      <td>Specify what connector to use, here should be <code>'sqlserver-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the SQLServer database.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Username to use when connecting to the SQLServer database.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the SQLServer database.</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Database name of the SQLServer database to monitor.</td>
    </tr> 
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name of the SQLServer database to monitor.</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table name of the SQLServer database to monitor.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>Integer port number of the SQLServer database.</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">UTC</td>
      <td>String</td>
      <td>The session time zone in database server, e.g. "Asia/Shanghai".</td>
    </tr>
   <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from SQLServer.
          For example: <code>'debezium.snapshot.mode' = 'initial_only'</code>.
          See more about the <a href="https://debezium.io/documentation/reference/1.6/connectors/sqlserver.html#sqlserver-required-connector-configuration-properties">Debezium's SQLServer Connector properties</a></td> 
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
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the schema that contain the row.</td>
    </tr>    
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contain the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>It indicates the time that the change was made in the database. <br>If the record is read from snapshot of the table instead of the change stream, the value is always 0.</td>
    </tr>
  </tbody>
</table>

Limitation
--------

### Can't perform checkpoint during scanning snapshot of tables
During scanning snapshot of database tables, since there is no recoverable position, we can't perform checkpoints. In order to not perform checkpoints, SqlServer CDC source will keep the checkpoint waiting to timeout. The timeout checkpoint will be recognized as failed checkpoint, by default, this will trigger a failover for the Flink job. So if the database table is large, it is recommended to add following Flink configurations to avoid failover because of the timeout checkpoints:

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

The extended CREATE TABLE example demonstrates the syntax for exposing these metadata fields:
```sql
CREATE TABLE products (
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    schema_name STRING METADATA  FROM 'schema_name' VIRTUAL,
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    id INT NOT NULL,
    name STRING,
    description STRING,
    weight DECIMAL(10,3)
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'schema-name' = 'dbo',
    'table-name' = 'products'
);
```

Features
--------

### Exactly-Once Processing

The SQLServer CDC connector is a Flink Source connector which will read database snapshot first and then continues to read change events with **exactly-once processing** even failures happen. Please read [How the connector works](https://debezium.io/documentation/reference/1.6/connectors/sqlserver.html#how-the-sqlserver-connector-works).

### Startup Reading Position

The config option `scan.startup.mode` specifies the startup mode for SQLServer CDC consumer. The valid enumerations are:

- `initial` (default): Takes a snapshot of structure and data of captured tables; useful if topics should be populated with a complete representation of the data from the captured tables.
- `initial-only`: Takes a snapshot of structure and data like initial but instead does not transition into streaming changes once the snapshot has completed.
- `latest-offset`: Takes a snapshot of the structure of captured tables only; useful if only changes happening from now onwards should be propagated to topics.

_Note: the mechanism of `scan.startup.mode` option relying on Debezium's `snapshot.mode` configuration. So please do not use them together. If you specific both `scan.startup.mode` and `debezium.snapshot.mode` options in the table DDL, it may make `scan.startup.mode` doesn't work._

### Single Thread Reading

The SQLServer CDC source can't work in parallel reading, because there is only one task can receive change events.

### DataStream Source

The SQLServer CDC connector can also be a DataStream source. You can create a SourceFunction as the following shows:

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;

public class SqlServerSourceExample {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
      .hostname("localhost")
      .port(1433)
      .database("sqlserver") // monitor sqlserver database
      .tableList("dbo.products") // monitor products table
      .username("sa")
      .password("Password!")
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
        <th class="text-left"><a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql">SQLServer type</a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>char(n)</td>
      <td>CHAR(n)</td>
    </tr>
    <tr>
      <td>
        varchar(n)<br>
        nvarchar(n)<br>
        nchar(n)
      </td>
      <td>VARCHAR(n)</td>
    </tr>
    <tr>
      <td>
        text<br>
        ntext<br>
        xml
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>
        decimal(p, s)<br>
        money<br>
        smallmoney
      </td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>numeric</td>
      <td>NUMERIC</td>
    </tr>
    <tr>
      <td>
        float<br>
        real
      </td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>bit</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>int</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>tinyint</td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>smallint</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>bigint</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>date</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>time(n)</td>
      <td>TIME(n)</td>
    </tr>
    <tr>
      <td>
        datetime2<br>
        datetime<br>
        smalldatetime
      </td>
      <td>TIMESTAMP(n)</td>
    </tr>
    <tr>
      <td>datetimeoffset</td>
      <td>TIMESTAMP_LTZ(3)</td>
    </tr>
    </tbody>
</table>
</div>
