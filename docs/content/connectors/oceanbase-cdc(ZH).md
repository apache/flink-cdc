# OceanBase CDC 连接器

OceanBase CDC 连接器允许从 OceanBase 读取快照数据和增量数据。本文介绍了如何设置 OceanBase CDC 连接器以对 OceanBase 进行 SQL 查询。

## 依赖

为了使用 OceanBase CDC 连接器，您必须提供相关的依赖信息。以下依赖信息适用于使用自动构建工具（如 Maven 或 SBT）构建的项目和带有 SQL JAR 包的 SQL 客户端。

```xml
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-oceanbase-cdc</artifactId>
  <!--  请使用已发布的版本依赖，snapshot 版本的依赖需要本地自行编译。 -->
  <version>2.3.0</version>
</dependency>
```

## 下载 SQL 客户端 JAR 包

```下载链接仅在已发布版本可用，请在文档网站左下角选择浏览已发布的版本。```

下载  [flink-sql-connector-oceanbase-cdc-2.3.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.3.0/flink-sql-connector-oceanbase-cdc-2.3.0.jar) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** flink-sql-connector-oceanbase-cdc-XXX-SNAPSHOT 版本是开发分支`release-XXX`对应的快照版本，快照版本用户需要下载源代码并编译相应的 jar。用户应使用已经发布的版本，例如 [flink-sql-connector-oceanbase-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-oceanbase-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

### 配置 OceanBase 数据库和 oblogproxy 服务

1. 按照 [部署文档](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/deploy-the-distributed-oceanbase-cluster) 配置 OceanBase 集群。
2. 在 sys 租户中，为 oblogproxy 创建一个带密码的用户。更多信息，参考 [用户管理文档](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.1/create-user-3)。

    ```bash
    mysql -h${host} -P${port} -uroot
    mysql> SHOW TENANT;
    mysql> CREATE USER ${sys_username} IDENTIFIED BY '${sys_password}';
    mysql> GRANT ALL PRIVILEGES ON *.* TO ${sys_username} WITH GRANT OPTION;
    ```

3. 为你想要监控的租户创建一个用户，这个用户用来读取快照数据和变化事件数据。
4. OceanBase 社区版用户需要获取`rootserver-list`，可以使用以下命令获取：

    ```bash
    mysql> SHOW PARAMETERS LIKE 'rootservice_list';
    ```
   OceanBase 企业版用户需要获取 `config-url`，可以使用以下命令获取：

    ```shell
    mysql> show parameters like 'obconfig_url';
    ```

5. 按照 [快速入门 文档](https://github.com/oceanbase/oblogproxy#quick-start) 配置 oblogproxy。

## 创建 OceanBase CDC 表

使用以下命令，创建 OceanBase CDC 表：

```sql
-- 每 3 秒做一次 checkpoint，用于测试，生产配置建议 5 到 10 分钟                  
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
    'logproxy.port' = '2983',
    'working-mode' = 'memory'
);

-- 从表 orders 中读取快照数据和 binlog 数据
Flink SQL> SELECT * FROM orders;
```

您也可以访问 Flink CDC 官网文档，快速体验将数据从 OceanBase 导入到 Elasticsearch。更多信息，参考 [Flink CDC 官网文档](https://ververica.github.io/flink-cdc-connectors/release-2.2/content/%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B/oceanbase-tutorial-zh.html)。

## OceanBase CDC 连接器选项

OceanBase CDC 连接器包括用于 SQL 和 DataStream API 的选项，如下表所示。

*注意*：连接器支持两种方式来指定需要监听的表，两种方式同时使用时会监听两种方式匹配的所有表。
1. 使用 `database-name` 和 `table-name` 匹配正则表达式中的数据库和表名。 由于`obcdc`（以前的`liboblog`）现在只支持`fnmatch`匹配，我们不能直接使用正则过滤 changelog 事件，所以通过两个选项去匹配去指定监听表只能在`initial`启动模式下使用。
2. 使用 `table-list` 去匹配数据库名和表名的准确列表。


配置项 | 是否必选 | 默认值 | 类型 | 描述
---- | ----- | ------ | ----- | ----
connector | 是 | 无 | String | 指定要使用的连接器。此处为 `oceanbase-cdc`。
scan.startup.mode | 是 | 无 | String | 指定 OceanBase CDC 消费者的启动模式。可取值为 `initial`、`latest-offset` 或`timestamp`。
scan.startup.timestamp | 否 | 无 | Long | 起始点的时间戳，单位为秒。仅在 `scan.startup.mode` 的值为 `timestamp` 时适用。
username | 是 | 无 | String | 连接 OceanBase 数据库的用户的名称。
password | 是 | 无 | String | 连接 OceanBase 数据库时使用的密码。
tenant-name | 是 | 无 | String | 待监控 OceanBase 数据库的租户名，应该填入精确值。
database-name | 是 | 无 | String | 待监控 OceanBase 数据库的数据库名，应该是正则表达式，该选项只支持和 'initial' 模式一起使用。
table-name | 否 | 无 | String | 待监控 OceanBase 数据库的表名，应该是正则表达式，该选项只支持和 'initial' 模式一起使用。
table-list | 否 | 无 | String | 待监控 OceanBase 数据库的全路径的表名列表，逗号分隔，如："db1.table1, db2.table2"。
hostname | 否 | 无 | String | OceanBase 数据库或 OceanBbase 代理 ODP 的 IP 地址或主机名。
port | 否 | 无 | Integer | OceanBase 数据库服务器的整数端口号。可以是 OceanBase 服务器的 SQL 端口号（默认值为 2881）<br>或 OceanBase代理服务的端口号（默认值为 2883）。
connect.timeout |  否 | 30s | Duration | 连接器在尝试连接到 OceanBase 数据库服务器超时前的最长时间。
server-time-zone |  否 | +00:00 | String | 数据库服务器中的会话时区，用户控制 OceanBase 的时间类型如何转换为 STRING。<br>合法的值可以是格式为"±hh:mm"的 UTC 时区偏移量，<br>如果 mysql 数据库中的时区信息表已创建，合法的值则可以是创建的时区。
logproxy.host | 是 | 无 | String | OceanBase 日志代理服务 的 IP 地址或主机名。
logproxy.port | 是 | 无 | Integer | OceanBase 日志代理服务 的端口号。
logproxy.client.id | 否 | 按规则生成 | String |	OceanBase日志代理服务的客户端链接，默认值的生成规则是 {flink_ip}_{process_id}_{timestamp}_{thread_id}_{tenant}。
rootserver-list | 是 | 无 | String | OceanBase root 服务器列表，服务器格式为 `ip:rpc_port:sql_port`，<br>多个服务器地址使用英文分号 `;` 隔开，OceanBase 社区版本必填。
config-url | 否 |  无 | String |	从配置服务器获取服务器信息的 url, OceanBase 企业版本必填。
working-mode | 否 | storage | String | 日志代理中 `obcdc` 的工作模式 , 可以是 `storage` 或 `memory`。

## 支持的元数据

在创建表时，您可以使用以下格式的元数据作为只读列（VIRTUAL）。

列名 | 数据类型 | 描述
----- | ----- | -----
tenant_name | STRING NOT NULL | 当前记录所属的租户名称。
database_name | STRING NOT NULL | 当前记录所属的数据库名称。
table_name | STRING NOT NULL | 当前记录所属的表名称。
op_ts | TIMESTAMP_LTZ(3) NOT NULL | 该值表示此修改在数据库中发生的时间。如果这条记录是该表在快照阶段读取的记录，则该值返回 0。

如下 SQL 展示了如何在表中使用这些元数据列：

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

OceanBase CDC 连接器是一个 Flink Source 连接器。它将首先读取数据库快照，然后再读取变化事件，并进行 **At-Least-Once 处理**。

OceanBase 数据库是一个分布式数据库，它的日志也分散在不同的服务器上。由于没有类似 MySQL binlog 偏移量的位置信息，OceanBase 数据库用时间戳作为位置标记。为确保读取完整的数据，liboblog（读取 OceanBase 日志记录的 C++ 库）可能会在给定的时间戳之前读取一些日志数据。因此，OceanBase 数据库可能会读到起始点附近时间戳的重复数据，可保证 **At-Least-Once 处理**。

### 启动模式

配置选项 `scan.startup.mode` 指定 OceanBase CDC 连接器的启动模式。可用取值包括：

- `initial`（默认）：在首次启动时对受监视的数据库表执行初始快照，并继续读取最新的提交日志。
- `latest-offset`：首次启动时，不对受监视的数据库表执行快照，仅从连接器启动时读取提交日志。
- `timestamp`：在首次启动时不对受监视的数据库表执行初始快照，仅从指定的 `scan.startup.timestamp` 读取最新的提交日志。

### 消费提交日志

OceanBase CDC 连接器使用 [oblogclient](https://github.com/oceanbase/oblogclient) 消费 OceanBase日志代理服务 中的事务日志。

### DataStream Source

OceanBase CDC 连接器也可以作为 DataStream Source 使用。您可以按照如下创建一个 SourceFunction：

```java
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.connectors.oceanbase.source.RowDataOceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

public class OceanBaseSourceExample {
   public static void main(String[] args) throws Exception {
      ResolvedSchema resolvedSchema =
              new ResolvedSchema(
                      Arrays.asList(
                              Column.physical("id", DataTypes.INT().notNull()),
                              Column.physical("name", DataTypes.STRING().notNull())),
                      Collections.emptyList(),
                      UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

      RowType physicalDataType =
              (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
      TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(physicalDataType);
      String serverTimeZone = "+00:00";

      OceanBaseDeserializationSchema<RowData> deserializer =
              RowDataOceanBaseDeserializationSchema.newBuilder()
                      .setPhysicalRowType(physicalDataType)
                      .setResultTypeInfo(resultTypeInfo)
                      .setServerTimeZone(ZoneId.of(serverTimeZone))
                      .build();

      SourceFunction<RowData> oceanBaseSource =
              OceanBaseSource.<RowData>builder()
                      .rsList("127.0.0.1:2882:2881")
                      .startupMode(StartupMode.INITIAL)
                      .username("user@test_tenant")
                      .password("pswd")
                      .tenantName("test_tenant")
                      .databaseName("test_db")
                      .tableName("test_table")
                      .hostname("127.0.0.1")
                      .port(2881)
                      .logProxyHost("127.0.0.1")
                      .logProxyPort(2983)
                      .serverTimeZone(serverTimezone)
                      .deserializer(deserializer)
                      .build();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      // enable checkpoint
      env.enableCheckpointing(3000);

      env.addSource(oceanBaseSource).print().setParallelism(1);
      env.execute("Print OceanBase Snapshot + Change Events");
   }
}
```

## 数据类型映射

OceanBase 数据类型 | Flink SQL 类型 | 描述
----- | ----- | ------
BOOLEAN <br>TINYINT(1) <br>BIT(1)&nbsp;&nbsp;&nbsp; | BOOLEAN |
TINYINT | TINYINT |
SMALLINT <br>TINYINT UNSIGNED | SMALLINT |
INT <br>MEDIUMINT <br>SMALLINT UNSIGNED | INT |
BIGINT <br>INT UNSIGNED | BIGINT |
BIGINT UNSIGNED | DECIMAL(20, 0) |
REAL <br>FLOAT | FLOAT |
DOUBLE | DOUBLE |
NUMERIC(p, s)<br>DECIMAL(p, s)<br>where p <= 38 | DECIMAL(p, s) |
NUMERIC(p, s)<br>DECIMAL(p, s)<br>where 38 < p <=65 | STRING | DECIMAL 等同于 NUMERIC。在 OceanBase 数据库中，DECIMAL 数据类型的精度最高为 65。<br> 但在 Flink 中，DECIMAL 的最高精度为 38。因此，<br> 如果你定义了一个精度大于 38 的 DECIMAL 列，你应当将其映射为 STRING，以避免精度损失。 |
DATE | DATE |
TIME [(p)] | TIME [(p)] |
DATETIME [(p)] | TIMESTAMP [(p)] |
TIMESTAMP [(p)] | TIMESTAMP_LTZ [(p)] |
CHAR(n) | CHAR(n) |
VARCHAR(n) | VARCHAR(n) |
BIT(n) | BINARY(⌈n/8⌉) |
BINARY(n) | BINARY(n) |
VARBINARY(N) | VARBINARY(N) |
TINYTEXT<br>TEXT<br>MEDIUMTEXT<br>LONGTEXT | STRING |
TINYBLOB<br>BLOB<br>MEDIUMBLOB<br>LONGBLOB | BYTES |
YEAR | INT |
ENUM | STRING |
SET | ARRAY&lt;STRING&gt; | 因为 OceanBase 的 SET 类型是用包含一个或多个值的字符串对象表示，<br> 所以映射到 Flink 时是一个字符串数组
JSON | STRING | JSON 类型的数据在 Flink 中会转化为 JSON 格式的字符串
