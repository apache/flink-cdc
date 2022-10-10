# Flink OceanBase CDC 连接器

OceanBase CDC 连接器允许从 OceanBase 读取快照数据和增量数据。本文介绍了如何设置 OceanBase CDC 连接器以对 OceanBase 进行 SQL 查询。

## 依赖

要设置 OceanBase CDC 连接器，您必须提供相关的依赖信息。以下依赖信息适用于使用自动构建工具（如 Maven 或 SBT）构建的项目和使用 SQL JAR 包捆绑的 SQL 客户端。

```xml
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-oceanbase-cdc</artifactId>
  <!-- The dependency is available only for stable releases, SNAPSHOT dependency need build by yourself. -->
  <version>2.2.1</version>
</dependency>
```

## 下载 SQL 客户端 JAR 包

点击 [Maven 中央仓库](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.2.1/flink-sql-connector-oceanbase-cdc-2.2.1.jar) 下载 JAR 包至 `<FLINK_HOME>/lib/`.

> **说明：**
>
> 下载链接仅适用于稳定发行版本。

`flink-sql-connector-oceanbase-cdc-XXX-SNAPSHOT` 快照版本与开发分支的版本对应。要使用快照版本，您必须自行下载并编译源代码。推荐使用稳定发行版本，例如 `flink-sql-connector-oceanbase-cdc-2.2.1.jar`。您可以在 Maven 中央仓库中找到使用稳定发行版本。

### 配置 OceanBase 数据库和 oblogproxy

1. 按照 [部署文档配置](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/deploy-the-distributed-oceanbase-cluster) OceanBase 集群。
2. 在 sys 租户中，为 oblogproxy 创建用户。更多信息，参考 [用户管理文档](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/create-user-3)。

    ```bash
    mysql -h${host} -P${port} -uroot
    mysql> SHOW TENANT;
    mysql> CREATE USER ${sys_username} IDENTIFIED BY '${sys_password}';
    mysql> GRANT ALL PRIVILEGES ON *.* TO ${sys_username} WITH GRANT OPTION;
    ```

3. 为待监控有租户创建一个租户。此租户用来读取快照数据和变化事件数据。
4. 使用以下命令，获取 `rootservice_list` 的值。

    ```bash
    mysql> SHOW PARAMETERS LIKE 'rootservice_list';
    ```

5. 按照 [oblogproxy 文档](todo) 配置 oblogproxy。

## 创建 OceanBase CDC 表

使用以下命令，创建 OceanBase CDC 表：

```sql
-- 每 3000 毫秒检查一次                   
Flink SQL> SET 'execution.checkpointing.interval' = '3s';

-- 在 Flink SQL 中创建 OceanBase 表 `orders`
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

-- 从表 orders 中读取快照数据和 binlogs
Flink SQL> SELECT * FROM orders;
```

您也可以访问 Flink 官网文档，快速体验将数据从 OceanBase 导入到 Elasticsearch。更多信息，参考 [Flink 官网文档](https://ververica.github.io/flink-cdc-connectors/release-2.2/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/oceanbase-tutorial-zh.html)。

## Flink OceanBase CDC 连接器选项

选项 | 是否必选 | 默认值 | 类型 | 描述
---- | ----- | ------ | ----- | ----
connector | 是 | 无 | String | 指定要使用的连接器。此处为 `mysql-cdc`。
scan.startup.mode | 是 | 无 | String | 指定 Flink OceanBase CDC 消费者的启动模式。可取值为 `initial`、`latest-offset` 或`timestamp`。
scan.startup.timestamp | 否 | 无 | Long | 起始点的时间戳，单位为秒。仅在 `scan.startup.mode` 的值为 `timestamp` 时适用。
username | 是 | 无 | String | 连接 OceanBase 数据库的用户的名称。
password | 是 | 无 | String | 连接 OceanBase 数据库时使用的密码。
tenant-name | 是 | 无 | String | 待监控 OceanBase 数据库的租户名。
database-name | 是 | 无 | String | 待监控 OceanBase 数据库的数据库名。
table-name | 是 | 无 | String | 待监控 OceanBase 数据库的表名。
hostname | 否 | 无 | String | OceanBase 数据库或 OceanBbase 代理 ODP 的 IP 地址或主机名。
port | 否 | 无 | String | Integer | OceanBase 数据库服务器的整数端口号。可以是 OceanBase 服务器的 SQL 端口号，默认值为 `2881`。或 ODP 的端口号默认值为 `2883`。
connect.timeout |  否 | 30s | Duration | 连接器在尝试连接到 OceanBase 数据库服务器后的超时时间。
server-time-zone |  否 | UTC | String | 数据库服务器中的会话时区，例如 `"Asia/Shanghai"`。此选项控制 OceanBase 数据库中的 `TIMESTAMP` 类型在快照读取时如何转换为`STRING`。确保此选项与 `oblogproxy` 的时区设置相同。
rootserver-list | 是 | 无 | String | 格式为 `ip:rpc_port:sql_port`。多个服务器地址使用英文分号 `;` 隔开。
logproxy.host | 是 | 无 | String | oblogproxy 的 IP 地址或主机名。
logproxy.port | 是 | 无 | Integer | oblogproxy 的端口号。

## 支持的元数据

在创建表时，您可以使用以下格式的元数据作为只读列（VIRTUAL）。

Key | 数据类型 | 描述
----- | ----- | -----
tenant_name | STRING NOT NULL | 当前记录所属的租户名称。
database_name | STRING NOT NULL | 当前记录所属的数据库名称。
table_name | STRING NOT NULL | 当前记录所属的表名称。
op_ts | TIMESTAMP_LTZ(3) NOT NULL | 当前记录表在数据库中更新的时间。
如果从表的快照而不是 binlog 读取记录，该值将始终为 0。

例如：

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

## 特性

### At-Least-Once 处理

OceanBase CDC 连接器是一个 Flink 源连接器。它将首先读取数据库快照，然后再读取变化事件，并进行 **At-Least-Once 处理**。

OceanBase 数据库是一个分布式数据库，它的日志也分散在不同的服务器上。由于没有类似 MySQL binlog 偏移量的位置信息，OceanBase 数据库用时间戳作为位置标记。为确保读取完整的数据，liboblog（读取 OceanBase 日志记录的 C++ 库）可能会在给定的时间戳之前读取一些日志数据。因此，OceanBase 数据库可能会读到起始点附近时间戳的重复数据，从而保证了 **At-Least-Once 处理**。

### 启动模式

配置选项 `scan.startup.mode` 指定 OceanBase CDC 使用者的启动模式。有效枚举包括：

- initial（默认）：在首次启动时对受监视的数据库表执行初始快照，并继续读取最新的事务日志。
- `latest-offset`：首次启动时，不对受监视的数据库表执行快照，仅从连接器启动时读取事务日志。
- `timestamp`：在首次启动时不对受监视的数据库表执行初始快照，仅从指定的 `scan.startup.timestamp` 读取事务日志。

### 消费事务日志

OceanBase CDC 连接器使用 [oblogclient](https://github.com/oceanbase/oblogclient) 消费 oblogproxy 中的事务日志。

### DataStream Source

OceanBase CDC 连接器也可以作为 DataStream Source 使用。您可以创建一个 SourceFunction，例如：

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

## 数据类型映射

当启动模式不是 `INITIAL` 时，连接器无法获得一个列的精度和比例。为兼容不同的启动模式，连接器不会将一个不同精度的 OceanBase 类型映射到不同的FLink 类型。例如，`BOOLEAN`、`TINYINT(1)` 或 `BIT(1)` 均会转换成 `BOOLEAN`。在 OceanBase 数据库中，`BOOLEAN` 等同于 `TINYINT(1)`，所以 `BOOLEAN` 和 `TINYINT` 类型的列在 Flink 中会被映射为 `TINYINT`，而 `BIT(1)` 在 Flink 中会被映射为 `BINARY(1)`。

OceanBase 数据类型 | Flink SQL 类型 | 描述
----- | ----- | ------
BOOLEAN <br>TINYINT | TINYINT |
SMALLINT<br>TINYINT <br>UNSIGNED | SMALLINT |
INT<br>MEDIUMINT<br>SMALLINT<br>UNSIGNED | INT |
BIGINT<br>INT UNSIGNED | BIGINT |
BIGINT UNSIGNED | DECIMAL(20, 0) |
REAL FLOAT | FLOAT |
DOUBLE | DOUBLE |
NUMERIC(p, s)<br>DECIMAL(p, s)<br>where p <= 38 | DECIMAL(p, s) |
NUMERIC(p, s)<br>DECIMAL(p, s)<br>where 38 < p <=65 | STRING | DECIMAL 等同于NUMERIC。在 OceanBase 数据库中，DECIMAL 数据类型的精度最高为 65。但在 Flink 中，DECIMAL 的精度为 38。因此，如果你定义了一个精度大于 38 的 DECIMAL 列，你要把它映射为 STRING，以避免精度损失。 |
DATE | DATE |
TIME [(p)] | TIME [(p)] |
TIMESTAMP [(p)]<br>DATETIME [(p)] | TIMESTAMP [(p)] |
CHAR(n) | CHAR(n) |
VARCHAR(n) | VARCHAR(n) |
BIT(n) | BINARY(⌈n/8⌉) |
BINARY(n) | BINARY(n) |
VARBINARY(N) | VARBINARY(N) |
TINYTEXT<br>TEXT<br>MEDIUMTEXT<br>LONGTEXT | STRING |
TINYBLOB<br>BLOB<br>MEDIUMBLOB<br>LONGBLOB | BYTES |
YEAR | INT |
ENUM | STRING |
SET | STRING
