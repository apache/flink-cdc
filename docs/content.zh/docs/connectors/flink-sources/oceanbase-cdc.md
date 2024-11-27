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

# OceanBase CDC 连接器

OceanBase CDC 连接器允许从 OceanBase 读取快照数据和增量数据。本文介绍了如何设置 OceanBase CDC 连接器以对 OceanBase 进行 SQL 查询。


### OceanBase CDC 方案

名词解释:

- *OceanBase CE*: OceanBase 社区版。OceanBase 的开源版本，兼容 MySQL https://github.com/oceanbase/oceanbase 。
- *OceanBase EE*: OceanBase 企业版。OceanBase 的商业版本，支持 MySQL 和 Oracle 两种兼容模式 https://www.oceanbase.com 。
- *OceanBase Cloud*: OceanBase 云数据库 https://www.oceanbase.com/product/cloud 。
- *Log Proxy CE*: OceanBase 日志代理服务社区版。单独使用时支持 CDC 模式，是一个获取 OceanBase 社区版事务日志（commit log）的代理服务 https://github.com/oceanbase/oblogproxy 。
- *Log Proxy EE*: OceanBase 日志代理服务企业版。单独使用时支持 CDC 模式，是一个获取 OceanBase 企业版事务日志（commit log）的代理服务，目前仅在 OceanBase Cloud 上提供有限的支持, 详情请咨询相关技术支持。
- *Binlog Service CE*: OceanBase Binlog 服务社区版。OceanBase 社区版的一个兼容 MySQL 复制协议的解决方案，详情参考 Log Proxy CE Binlog 模式的文档。
- *Binlog Service EE*: OceanBase Binlog 服务企业版。OceanBase 企业版 MySQL 模式的一个兼容 MySQL 复制协议的解决方案，仅可在阿里云使用，详情见[操作指南](https://www.alibabacloud.com/help/zh/apsaradb-for-oceanbase/latest/binlog-overview)。
- *MySQL Driver*: `mysql-connector-java`，可用于 OceanBase 社区版和 OceanBase 企业版 MySQL 模式。
- *OceanBase Driver*: OceanBase JDBC 驱动，支持所有版本的 MySQL 和 Oracle 兼容模式 https://github.com/oceanbase/obconnector-j 。

OceanBase CDC 源端读取方案：

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">数据库类型</th>
                <th class="text-left">支持的驱动</th>
                <th class="text-left">CDC 连接器</th>
                <th class="text-left">其他用到的组件</th>
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
                <td rowspan="2">OceanBase EE (MySQL 模式)</td>
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
                <td>OceanBase EE (Oracle 模式)</td>
                <td>OceanBase Driver: 2.4.x</td>
                <td>OceanBase CDC Connector</td>
                <td>Log Proxy EE (CDC 模式)</td>
            </tr>
        </tbody>
    </table>
</div>

注意: 对于使用 OceanBase 社区版或 OceanBase 企业版 MySQL 模式的用户，我们推荐参考  [MySQL CDC 的文档](mysql-cdc.md)，使用 MySQL CDC 连接器搭配 Binlog 服务。

依赖
------------

为了使用 OceanBase CDC 连接器，您必须提供相关的依赖信息。以下依赖信息适用于使用自动构建工具（如 Maven 或 SBT）构建的项目和带有 SQL JAR 包的 SQL 客户端。

### Maven dependency

{{< artifact flink-connector-oceanbase-cdc >}}

### SQL Client JAR

```下载链接仅在已发布版本可用，请在文档网站左下角选择浏览已发布的版本。```

下载[flink-sql-connector-oceanbase-cdc-3.1.0.jar](https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oceanbase-cdc/3.1.0/flink-sql-connector-oceanbase-cdc-3.1.0.jar)  到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-oceanbase-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oceanbase-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

由于 MySQL Driver 和 OceanBase Driver 使用的开源协议都与 Flink CDC 项目不兼容，我们无法在 jar 包中提供驱动。 您可能需要手动配置以下依赖：

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">依赖名称</th>
        <th class="text-left">说明</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.27">mysql:mysql-connector-java:8.0.27</a></td>
        <td>用于连接到 OceanBase 数据库的 MySQL 租户。</td>
      </tr>
    </tbody>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client/2.4.9">com.oceanbase:oceanbase-client:2.4.9</a></td>
        <td>用于连接到 OceanBase 数据库的 MySQL 或 Oracle 租户。</td>
      </tr>
    </tbody>
</table>
</div>

配置 OceanBase 数据库和 Log Proxy 服务
----------------------

1. 按照 [文档](https://github.com/oceanbase/oceanbase#quick-start) 配置 OceanBase 集群。
2. 在 sys 租户中，为 oblogproxy 创建一个带密码的用户。

   ```shell
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

5. 设置 OceanBase LogProxy。 对于OceanBase社区版的用户，您可以按照[此文档](https://www.oceanbase.com/docs/community-oblogproxy-doc-1000000000531984)进行操作。

创建 OceanBase CDC 表
----------------

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

-- 从表 orders 中读取快照数据和 binlog 数据
Flink SQL> SELECT * FROM orders;
```

如果您使用的是企业版的 OceanBase Oracle 模式，您需要先添加 OceanBase 的官方 JDBC 驱动 jar 包到 Flink 环境，并且部署企业版的 oblogproxy 服务，然后通过以下命令创建 OceanBase CDC 表：

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

您也可以访问 Flink CDC 官网文档，快速体验将数据从 OceanBase 导入到 Elasticsearch。更多信息，参考 [Flink CDC 官网文档]({{< ref "docs/connectors/flink-sources/tutorials/oceanbase-tutorial" >}})。

OceanBase CDC 连接器选项
----------------

OceanBase CDC 连接器包括用于 SQL 和 DataStream API 的选项，如下表所示。

*注意*：连接器支持两种方式来指定需要监听的表，两种方式同时使用时会监听两种方式匹配的所有表。
1. 使用 `database-name` 和 `table-name` 匹配正则表达式中的数据库和表名。
2. 使用 `table-list` 去匹配数据库名和表名的准确列表。

<div class="highlight">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left" style="width: 10%">配置项</th>
                <th class="text-left" style="width: 8%">是否必选</th>
                <th class="text-left" style="width: 7%">默认值</th>
                <th class="text-left" style="width: 10%">类型</th>
                <th class="text-left" style="width: 65%">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>connector</td>
                <td>是</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>指定要使用的连接器，此处为 <code>'oceanbase-cdc'</code>。</td>
            </tr>
            <tr>
                <td>scan.startup.mode</td>
                <td>否</td>
                <td style="word-wrap: break-word;">initial</td>
                <td>String</td>
                <td>指定 OceanBase CDC 消费者的启动模式。可取值为
                    <code>'initial'</code>，<code>'latest-offset'</code>，<code>'timestamp'</code> 或 <code>'snapshot'</code>。
                </td>
            </tr>
            <tr>
                <td>scan.startup.timestamp</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>Long</td>
                <td>起始点的时间戳，单位为秒。仅在启动模式为 <code>'timestamp'</code> 时可用。</td>
            </tr>
            <tr>
                <td>username</td>
                <td>是</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>连接 OceanBase 数据库的用户的名称。</td>
            </tr>
            <tr>
                <td>password</td>
                <td>是</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>连接 OceanBase 数据库时使用的密码。</td>
            </tr>
            <tr>
                <td>tenant-name</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>待监控 OceanBase 数据库的租户名，应该填入精确值。</td>
            </tr>
            <tr>
                <td>database-name</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>待监控 OceanBase 数据库的数据库名，应该是正则表达式。</td>
            </tr>
            <tr>
                <td>table-name</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>待监控 OceanBase 数据库的表名，应该是正则表达式。</td>
            </tr>
            <tr>
                <td>table-list</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>待监控 OceanBase 数据库的全路径的表名列表，逗号分隔，如："db1.table1, db2.table2"。</td>
            </tr>
            <tr>
                <td>hostname</td>
                <td>是</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>OceanBase 数据库或 OceanBbase 代理 ODP 的 IP 地址或主机名。</td>
            </tr>
            <tr>
                <td>port</td>
                <td>是</td>
                <td style="word-wrap: break-word;">无</td>
                <td>Integer</td>
                <td>
                    OceanBase 数据库服务器的整数端口号。可以是 OceanBase 服务器的 SQL 端口号（默认值为 2881）<br>
                    或 OceanBase代理服务的端口号（默认值为 2883）</td>
            </tr>
            <tr>
                <td>connect.timeout</td>
                <td>否</td>
                <td style="word-wrap: break-word;">30s</td>
                <td>Duration</td>
                <td>连接器在尝试连接到 OceanBase 数据库服务器超时前的最长时间。</td>
            </tr>
            <tr>
                <td>server-time-zone</td>
                <td>否</td>
                <td style="word-wrap: break-word;">+00:00</td>
                <td>String</td>
                <td>
                    数据库服务器中的会话时区，用户控制 OceanBase 的时间类型如何转换为 STRING。<br>
                    合法的值可以是格式为"±hh:mm"的 UTC 时区偏移量，<br>
                    如果 mysql 数据库中的时区信息表已创建，合法的值则可以是创建的时区。
                </td>
            </tr>
            <tr>
                <td>logproxy.host</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>OceanBase 日志代理服务 的 IP 地址或主机名。</td>
            </tr>
            <tr>
                <td>logproxy.port</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>Integer</td>
                <td>OceanBase 日志代理服务 的端口号。</td>
            </tr>
            <tr>
                <td>logproxy.client.id</td>
                <td>否</td>
                <td style="word-wrap: break-word;">规则生成</td>
                <td>String</td>
                <td>OceanBase日志代理服务的客户端连接 ID，默认值的生成规则是 {flink_ip}_{process_id}_{timestamp}_{thread_id}_{tenant}。</td>
            </tr>
            <tr>
                <td>rootserver-list</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>OceanBase root 服务器列表，服务器格式为 `ip:rpc_port:sql_port`，<br>多个服务器地址使用英文分号 `;` 隔开，OceanBase 社区版本必填。</td>
            </tr>
            <tr>
                <td>config-url</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>从配置服务器获取服务器信息的 url, OceanBase 企业版本必填。</td>
            </tr>
            <tr>
                <td>working-mode</td>
                <td>否</td>
                <td style="word-wrap: break-word;">storage</td>
                <td>String</td>
                <td>日志代理中 `libobcdc` 的工作模式 , 可以是 `storage` 或 `memory`。</td>
            </tr>
            <tr>
                <td>compatible-mode</td>
                <td>否</td>
                <td style="word-wrap: break-word;">mysql</td>
                <td>String</td>
                <td>OceanBase 的兼容模式，可以是 `mysql` 或 `oracle`。</td>
            </tr>
            <tr>
                <td>jdbc.driver</td>
                <td>否</td>
                <td style="word-wrap: break-word;">com.mysql.cj.jdbc.Driver</td>
                <td>String</td>
                <td>全量读取时使用的 jdbc 驱动类名。</td>
            </tr>
            <tr>
                <td>jdbc.properties.*</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'。</td>
            </tr>
            <tr>
                <td>obcdc.properties.*</td>
                <td>否</td>
                <td style="word-wrap: break-word;">无</td>
                <td>String</td>
                <td>传递自定义 <code>libobcdc</code> 属性的选项，如 'obcdc.properties.sort_trans_participants' = '1'。详情参见 <a href="https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000000510698">obcdc 配置项说明</a>。</td>
            </tr>
        </tbody>
    </table>
</div>

支持的元数据
----------------

在创建表时，您可以使用以下格式的元数据作为只读列（VIRTUAL）。

<table class="colwidths-auto docutils">
    <thead>
        <tr>
            <th class="text-left" style="width: 15%">列名</th>
            <th class="text-left" style="width: 30%">数据类型</th>
            <th class="text-left" style="width: 55%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>tenant_name</td>
            <td>STRING</td>
            <td>当前记录所属的租户名称。</td>
        </tr>
        <tr>
            <td>database_name</td>
            <td>STRING</td>
            <td>当前记录所属的 db 名。</td>
        </tr>
        <tr>
            <td>schema_name</td>
            <td>STRING</td>
            <td>当前记录所属的 schema 名。</td>
        </tr>
        <tr>
            <td>table_name</td>
            <td>STRING NOT NULL</td>
            <td>当前记录所属的表名称。</td>
        </tr>
        <tr>
            <td>op_ts</td>
            <td>TIMESTAMP_LTZ(3) NOT NULL</td>
            <td>该值表示此修改在数据库中发生的时间。如果这条记录是该表在快照阶段读取的记录，则该值返回 0。</td>
        </tr>
    </tbody>
</table>

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

特性
--------

### At-Least-Once 处理

OceanBase CDC 连接器是一个 Flink Source 连接器。它将首先读取数据库快照，然后再读取变化事件，并进行 **At-Least-Once 处理**。

OceanBase 数据库是一个分布式数据库，它的日志也分散在不同的服务器上。由于没有类似 MySQL binlog 偏移量的位置信息，OceanBase 数据库用时间戳作为位置标记。为确保读取完整的数据，liboblog（读取 OceanBase 日志记录的 C++ 库）可能会在给定的时间戳之前读取一些日志数据。因此，OceanBase 数据库可能会读到起始点附近时间戳的重复数据，可保证 **At-Least-Once 处理**。

### 启动模式

配置选项 `scan.startup.mode` 指定 OceanBase CDC 连接器的启动模式。可用取值包括：

- `initial`（默认）：在首次启动时对受监视的数据库表执行初始快照，并继续读取最新的提交日志。
- `latest-offset`：首次启动时，不对受监视的数据库表执行快照，仅从连接器启动时读取提交日志。
- `timestamp`：在首次启动时不对受监视的数据库表执行初始快照，仅从指定的 `scan.startup.timestamp` 读取最新的提交日志。
- `snapshot`: 仅对受监视的数据库表执行初始快照。

### 消费提交日志

OceanBase CDC 连接器使用 [oblogclient](https://github.com/oceanbase/oblogclient) 消费 OceanBase日志代理服务 中的事务日志。

### DataStream Source

OceanBase CDC 连接器也可以作为 DataStream Source 使用。您可以按照如下创建一个 SourceFunction：

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

数据类型映射
----------------

### Mysql 模式

<div class="wy-table-responsive">
    <table class="colwidths-auto docutils">
        <thead>
            <tr>
                <th class="text-left">OceanBase 数据类型</th>
                <th class="text-left">Flink SQL 类型</th>
                <th class="text-left">描述</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>
                    BOOLEAN<br>
                    TINYINT(1)<br>
                    BIT(1)
                </td>
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
                    TINYINT UNSIGNED
                </td>
                <td>SMALLINT</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    INT<br>
                    MEDIUMINT<br>
                    SMALLINT UNSIGNED
                </td>
                <td>INT</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    BIGINT<br>
                    INT UNSIGNED
                </td>
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
                    FLOAT
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
                    where p <= 38 </td>
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
                <td>
                    DECIMAL 等同于 NUMERIC。在 OceanBase 数据库中，DECIMAL 数据类型的精度最高为 65。<br>
                    但在 Flink 中，DECIMAL 的最高精度为 38。因此，<br>
                    如果你定义了一个精度大于 38 的 DECIMAL 列，你应当将其映射为 STRING，以避免精度损失。
                </td>
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
                    LONGTEXT
                </td>
                <td>STRING</td>
                <td></td>
            </tr>
            <tr>
                <td>
                    TINYBLOB<br>
                    BLOB<br>
                    MEDIUMBLOB<br>
                    LONGBLOB
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
                <td>
                    因为 OceanBase 的 SET 类型是用包含一个或多个值的字符串对象表示，<br>
                    所以映射到 Flink 时是一个字符串数组
                </td>
            </tr>
            <tr>
                <td>JSON</td>
                <td>STRING</td>
                <td>JSON 类型的数据在 Flink 中会转化为 JSON 格式的字符串</td>
            </tr>
        </tbody>
    </table>
</div>

### Oracle 模式

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
