# MySQL CDC 连接器

MySQL CDC 连接器允许从 MySQL 数据库读取快照数据和增量数据。本文描述了如何设置 MySQL CDC 连接器来对 MySQL 数据库运行 SQL 查询。

## 支持的数据库

| Connector                                                | Database                                                                                                                                                                                                                                                                                                                                                                                               | Driver                  |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [mysql-cdc](mysql-cdc(ZH).md)         | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x <li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x <li> [PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x <li> [Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x <li> [MariaDB](https://mariadb.org): 10.x <li> [PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.27     |

依赖
------------

为了设置 MySQL CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-connector-mysql-cdc</artifactId>
  <!-- 请使用已发布的版本依赖，snapshot版本的依赖需要本地自行编译。 -->
  <version>2.3.0</version>
</dependency>
```

### SQL Client JAR

```下载链接仅在已发布版本可用，请在文档网站左下角选择浏览已发布的版本。```

下载 [flink-sql-connector-mysql-cdc-2.3.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.3.0/flink-sql-connector-mysql-cdc-2.3.0.jar) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** flink-sql-connector-mysql-cdc-XXX-SNAPSHOT 版本是开发分支`release-XXX`对应的快照版本，快照版本用户需要下载源代码并编译相应的 jar。用户应使用已经发布的版本，例如 [flink-sql-connector-mysql-cdc-2.3.0.jar](https://mvnrepository.com/artifact/com.ververica/flink-sql-connector-mysql-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。

配置 MySQL 服务器
----------------

你必须定义一个 MySQL 用户，该用户对 MySQL CDC 连接器监视的所有数据库都应该具有所需的权限。

1. 创建 MySQL 用户：

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. 向用户授予所需的权限：

```sql
mysql> GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
**注意:** 在 `scan.incremental.snapshot.enabled` 参数已启用时（默认情况下已启用）时，不再需要授予 reload 权限。

3. 刷新用户权限：

```sql
mysql> FLUSH PRIVILEGES;
```

查看更多用户权限问题请参考 [权限说明](https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-creating-user).


注意事项
----------------

### 为每个 Reader 设置不同的 Server id

每个用于读取 binlog 的 MySQL 数据库客户端都应该有一个唯一的 id，称为 Server id。 MySQL 服务器将使用此 id 来维护网络连接和 binlog 位置。 因此，如果不同的作业共享相同的 Server id， 则可能导致从错误的 binlog 位置读取数据。
因此，建议通过为每个 Reader 设置不同的 Server id  [SQL Hints](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/hints.html),
假设 Source 并行度为 4, 我们可以使用 `SELECT * FROM source_table /*+ OPTIONS('server-id'='5401-5404') */ ;` 来为 4 个 Source readers 中的每一个分配唯一的 Server id。


### 设置 MySQL 会话超时时间

当为大型数据库创建初始一致快照时，你建立的连接可能会在读取表时碰到超时问题。你可以通过在 MySQL 侧配置 interactive_timeout 和 wait_timeout 来缓解此类问题。
- `interactive_timeout`: 服务器在关闭交互连接之前等待活动的秒数。 更多信息请参考 [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: 服务器在关闭非交互连接之前等待活动的秒数。 更多信息请参考 [MySQL documentations](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).


如何创建 MySQL CDC 表
----------------

MySQL CDC 表可以定义如下：

```sql
-- 每 3 秒做一次 checkpoint，用于测试，生产配置建议5到10分钟                      
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- 在 Flink SQL中注册 MySQL 表 'orders'
Flink SQL> CREATE TABLE orders (
     order_id INT,
     order_date TIMESTAMP(0),
     customer_name STRING,
     price DECIMAL(10, 5),
     product_id INT,
     order_status BOOLEAN,
     PRIMARY KEY(order_id) NOT ENFORCED
     ) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'localhost',
     'port' = '3306',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'mydb',
     'table-name' = 'orders');
  
-- 从订单表读取全量数据(快照)和增量数据(binlog)
Flink SQL> SELECT * FROM orders;
```

连接器选项
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
      <td>指定要使用的连接器, 这里应该是 <code>'mysql-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> MySQL 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 MySQL 数据库服务器时要使用的 MySQL 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 MySQL 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 MySQL 服务器的数据库名称。数据库名称还支持正则表达式，以监视多个与正则表达式匹配的表。</td>
    </tr> 
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 MySQL 数据库的表名。表名还支持正则表达式，以监视多个表与正则表达式匹配。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td> MySQL 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408', 
      建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。因为在当前 MySQL 集群中运行的所有 slave 节点，标记每个 salve 节点的 id 都必须是唯一的。 所以当连接器加入 MySQL 集群作为另一个 slave 节点（并且具有唯一 id 的情况下），它就可以读取 binlog。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。
      </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.enabled</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">true</td>
          <td>Boolean</td>
          <td>增量快照是一种读取表快照的新机制，与旧的快照机制相比，
              增量快照有许多优点，包括：
              （1）在快照读取期间，Source 支持并发读取，
              （2）在快照读取期间，Source 支持进行 chunk 粒度的 checkpoint，
              （3）在快照读取之前，Source 不需要数据库锁权限。
              如果希望 Source 并行运行，则每个并行 Readers 都应该具有唯一的 Server id，所以
              Server id 必须是类似 `5400-6400` 的范围，并且该范围必须大于并行度。
              请查阅 <a href="#a-name-id-001-a">增量快照读取</a> 章节了解更多详细信息。
          </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> MySQL CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"earliest-offset"，"latest-offset"，"specific-offset" 和 "timestamp"。
           请查阅 <a href="#a-name-id-002-a">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.file</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件名。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.pos</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件位置。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.gtid-set</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 GTID 集合。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-events</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的事件数量。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的数据行数量。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区， 例如： "Asia/Shanghai". 
          它控制 MYSQL 中的时间戳类型如何转换为字符串。
          更多请参考 <a href="https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-temporal-types"> 这里</a>.
          如果没有设置，则使用ZoneId.systemDefault()来确定服务器时区。
      </td>
    </tr>
    <tr>
      <td>debezium.min.row.
      count.to.stream.result</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>	
       在快照操作期间，连接器将查询每个包含的表，以生成该表中所有行的读取事件。 此参数确定 MySQL 连接是否将表的所有结果拉入内存（速度很快，但需要大量内存）， 或者结果是否需要流式传输（传输速度可能较慢，但适用于非常大的表）。 该值指定了在连接器对结果进行流式处理之前，表必须包含的最小行数，默认值为1000。将此参数设置为`0`以跳过所有表大小检查，并始终在快照期间对所有结果进行流式处理。</td>
    </tr>
    <tr>
          <td>connect.timeout</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>连接器在尝试连接到 MySQL 数据库服务器后超时前应等待的最长时间。</td>
    </tr>    
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>连接器应重试以建立 MySQL 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
          <td>connection.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>Integer</td>
          <td>连接池大小。</td>
    </tr>
    <tr>
          <td>jdbc.properties.*</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>String</td>
          <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
          <td>heartbeat.interval</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">30s</td>
          <td>Duration</td>
          <td>用于跟踪最新可用 binlog 偏移的发送心跳事件的间隔。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 MySQL 服务器捕获数据更改。
          For example: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-connector-properties"> Debezium 的  MySQL 连接器属性</a></td> 
    </tr>
    </tbody>
</table>
</div>

支持的元数据
----------------

下表中的元数据可以在 DDL 中作为只读（虚拟）meta 列声明。

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
      <td>当前记录所属的表名称。</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是 binlog 读取记录，该值将始终为0。</td>
    </tr>
  </tbody>
</table>

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
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
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);
```

下述创建表示例展示使用正则表达式匹配多张库表的用法：
```sql
CREATE TABLE products (
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
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = '(^(test).*|^(tpc).*|txc|.*[p$]|t{2})',
    'table-name' = '(t[5-8]|tt)'
);
```
<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">匹配示例</th>
       <th class="text-left" style="width: 30%">表达式</th>
       <th class="text-left" style="width: 55%">描述</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>前缀匹配</td>
      <td>^(test).*</td>
      <td>匹配前缀为test的数据库名或表名，例如test1、test2等。</td>
    </tr>
    <tr>
      <td>后缀匹配</td>
      <td>.*[p$]</td>
      <td>匹配后缀为p的数据库名或表名，例如cdcp、edcp等。</td>
    </tr>
    <tr>
      <td>特定匹配</td>
      <td>txc</td>
      <td>匹配具体的数据库名或表名。</td>
    </tr>
  </tbody>
</table>
进行库表匹配时,使用的模式是database-name.table-name，所以该例子使用(^(test).*|^(tpc).*|txc|.*[p$]|t{2}).(t[5-8]|tt)，匹配txc.tt、test2.test5。

支持的特性
--------

### 增量快照读取<a name="增量快照读取" id="001" ></a>

增量快照读取是一种读取表快照的新机制。与旧的快照机制相比，增量快照具有许多优点，包括：
* （1）在快照读取期间，Source 支持并发读取，
* （2）在快照读取期间，Source 支持进行 chunk 粒度的 checkpoint，
* （3）在快照读取之前，Source 不需要数据库锁权限。

如果希望 source 并行运行，则每个并行 reader 都应该具有唯一的 server id，因此`server id`的范围必须类似于 `5400-6400`，
且范围必须大于并行度。在增量快照读取过程中，MySQL CDC Source 首先通过表的主键将表划分成多个块（chunk），
然后 MySQL CDC Source 将多个块分配给多个 reader 以并行读取表的数据。

#### 并发读取

增量快照读取提供了并行读取快照数据的能力。
你可以通过设置作业并行度的方式来控制 Source 的并行度 `parallelism.default`. For example, in SQL CLI:

```sql
Flink SQL> SET 'parallelism.default' = 8;
```

#### 全量阶段支持 checkpoint

增量快照读取提供了在区块级别执行检查点的能力。它使用新的快照读取机制解决了以前版本中的检查点超时问题。

#### 无锁算法

MySQL CDC source 使用 增量快照算法, 避免了数据库锁的使用，因此不需要 “RELOAD” 权限。

#### MySQL高可用性支持

```mysql cdc``` 连接器通过使用 [GTID](https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-concepts.html) 提供 MySQL 高可用集群的高可用性信息。为了获得高可用性， MySQL集群需要启用 GTID 模式，MySQL 配置文件中的 GTID 模式应该包含以下设置：

```yaml
gtid_mode = on
enforce_gtid_consistency = on
```

如果监控的MySQL服务器地址包含从实例，则需要对MySQL配置文件设置以下设置。设置 ```log slave updates=1``` 允许从实例也将从主实例同步的数据写入其binlog， 这确保了```mysql cdc```连接器可以使用从实例中的全部数据。

```yaml
gtid_mode = on
enforce_gtid_consistency = on
log-slave-updates = 1
```

MySQL 集群中你监控的服务器出现故障后, 你只需将受监视的服务器地址更改为其他可用服务器，然后从最新的检查点/保存点重新启动作业, 作业将从 checkpoint/savepoint 恢复，不会丢失任何记录。

建议为 MySQL 集群配置 DNS（域名服务）或 VIP（虚拟 IP 地址）， 使用```mysql cdc```连接器的 DNS 或 VIP 地址， DNS或VIP将自动将网络请求路由到活动MySQL服务器。 这样，你就不再需要修改地址和重新启动管道。

#### MySQL心跳事件支持

如果表不经常更新，则 binlog 文件或 GTID 集可能已在其最后提交的 binlog 位置被清理。
在这种情况下，CDC 作业可能会重新启动失败。因此心跳事件将帮助更新 binlog 位置。 默认情况下，MySQL CDC Source 启用心跳事件，间隔设置为30秒。 可以使用表选项```heartbeat```指定间隔。或将选项设置为```0s```以禁用心跳事件。

#### 增量快照读取的工作原理

当 MySQL CDC Source 启动时，它并行读取表的快照，然后以单并行度的方式读取表的 binlog。

在快照阶段，根据表的主键和表行的大小将快照切割成多个快照块。
快照块被分配给多个快照读取器。每个快照读取器使用 [区块读取算法](#snapshot-chunk-reading) 并将读取的数据发送到下游。
Source 会管理块的进程状态（完成或未完成），因此快照阶段的 Source 可以支持块级别的 checkpoint。
如果发生故障，可以恢复 Source 并继续从最后完成的块中读取块。

所有快照块完成后，Source 将继续在单个任务中读取 binlog。
为了保证快照记录和 binlog 记录的全局数据顺序，binlog reader 将开始读取数据直到快照块完成后并有一个完整的 checkpoint，以确保所有快照数据已被下游消费。
binlog reader 在状态中跟踪所使用的 binlog 位置，因此 binlog 阶段的 Source 可以支持行级别的 checkpoint。

Flink 定期为 Source 执行 checkpoint，在故障转移的情况下，作业将重新启动并从最后一个成功的 checkpoint 状态恢复，并保证只执行一次语义。

##### 全量阶段分片算法

在执行增量快照读取时，MySQL CDC source 需要一个用于分片的的算法。
MySQL CDC Source 使用主键列将表划分为多个分片（chunk）。 默认情况下，MySQL CDC source 会识别表的主键列，并使用主键中的第一列作为用作分片列。
如果表中没有主键， 增量快照读取将失败，你可以禁用 `scan.incremental.snapshot.enabled` 来回退到旧的快照读取机制。

对于数值和自动增量拆分列，MySQL CDC Source 按固定步长高效地拆分块。
例如，如果你有一个主键列为`id`的表，它是自动增量 BIGINT 类型，最小值为`0`，最大值为`100`，
和表选项 `scan.incremental.snapshot.chunk.size` 大小 `value`为`25`，表将被拆分为以下块：

```
 (-∞, 25),
 [25, 50),
 [50, 75),
 [75, 100),
 [100, +∞)
```

对于其他主键列类型， MySQL CDC Source 将以下形式执行语句： `SELECT MAX(STR_ID) AS chunk_high FROM (SELECT * FROM TestTable WHERE STR_ID > 'uuid-001' limit 25)` 来获得每个区块的低值和高值，
分割块集如下所示：

 ```
 (-∞, 'uuid-001'),
 ['uuid-001', 'uuid-009'),
 ['uuid-009', 'uuid-abc'),
 ['uuid-abc', 'uuid-def'),
 [uuid-def, +∞).
```

##### Chunk 读取算法

对于上面的示例`MyTable`，如果 MySQL CDC Source 并行度设置为 4，MySQL CDC Source 将在每一个 executes 运行 4 个 Readers **通过偏移信号算法**
获取快照区块的最终一致输出。 **偏移信号算法**简单描述如下：

* (1) 将当前 binlog 位置记录为`LOW`偏移量
* (2) 通过执行语句读取并缓冲快照区块记录 `SELECT * FROM MyTable WHERE id > chunk_low AND id <= chunk_high`
* (3) 将当前 binlog 位置记录为`HIGH`偏移量
* (4) 从`LOW`偏移量到`HIGH`偏移量读取属于快照区块的 binlog 记录
* (5) 将读取的 binlog 记录向上插入缓冲区块记录，并发出缓冲区中的所有记录作为快照区块的最终输出（全部作为插入记录）
* (6) 继续读取并发出属于 *单个 binlog reader* 中`HIGH`偏移量之后的区块的 binlog 记录。

该算法的是基于 [DBLog Paper](https://arxiv.org/pdf/2010.12597v1.pdf) 并结合 Flink 的一个变种, 请参考它了解更多详细信息。

**注意:** 如果主键的实际值在其范围内分布不均匀，则在增量快照读取时可能会导致任务不平衡。

### Exactly-Once 处理

MySQL CDC 连接器是一个 Flink Source 连接器，它将首先读取表快照块，然后继续读取 binlog，
无论是在快照阶段还是读取 binlog 阶段，MySQL CDC 连接器都会在处理时**准确读取数据**，即使任务出现了故障。

### 启动模式<a name="启动模式" id="002" ></a>

配置选项```scan.startup.mode```指定 MySQL CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 binlog。
- `earliest-offset`：跳过快照阶段，从可读取的最早 binlog 位点开始读取
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 binlog 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
- `specific-offset`：跳过快照阶段，从指定的 binlog 位点开始读取。位点可通过 binlog 文件名和位置指定，或者在 GTID 在集群上启用时通过 GTID 集合指定。
- `timestamp`：跳过快照阶段，从指定的时间戳开始读取 binlog 事件。

例如使用 DataStream API:
```java
MySQLSource.builder()
    .startupOptions(StartupOptions.earliest()) // 从最早位点启动
    .startupOptions(StartupOptions.latest()) // 从最晚位点启动
    .startupOptions(StartupOptions.specificOffset("mysql-bin.000003", 4L) // 从指定 binlog 文件名和位置启动
    .startupOptions(StartupOptions.specificOffset("24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")) // 从 GTID 集合启动
    .startupOptions(StartupOptions.timestamp(1667232000000L) // 从时间戳启动
    ...
    .build()
```

使用 SQL:

```SQL
CREATE TABLE mysql_source (...) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset', -- 从最早位点启动
    'scan.startup.mode' = 'latest-offset', -- 从最晚位点启动
    'scan.startup.mode' = 'specific-offset', -- 从特定位点启动
    'scan.startup.mode' = 'timestamp', -- 从特定位点启动
    'scan.startup.specific-offset.file' = 'mysql-bin.000003', -- 在特定位点启动模式下指定 binlog 文件名
    'scan.startup.specific-offset.pos' = '4', -- 在特定位点启动模式下指定 binlog 位置
    'scan.startup.specific-offset.gtid-set' = '24DA167-0C0C-11E8-8442-00059A3C7B00:1-19', -- 在特定位点启动模式下指定 GTID 集合
    'scan.startup.timestamp-millis' = '1667232000000' -- 在时间戳启动模式下指定启动时间戳
    ...
)
```

**注意**：
1. MySQL source 会在 checkpoint 时将当前位点以 INFO 级别打印到日志中，日志前缀为 "Binlog offset on checkpoint {checkpoint-id}"。
该日志可以帮助将作业从某个 checkpoint 的位点开始启动的场景。
2. 如果捕获变更的表曾经发生过表结构变化，从最早位点、特定位点或时间戳启动可能会发生错误，因为 Debezium 读取器会在内部保存当前的最新表结构，结构不匹配的早期数据无法被正确解析。


### DataStream Source

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .databaseList("yourDatabaseName") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
        .tableList("yourDatabaseName.yourTableName") // 设置捕获的表
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置 3s 的 checkpoint 间隔
    env.enableCheckpointing(3000);

    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
      // 设置 source 节点的并行度为 4
      .setParallelism(4)
      .print().setParallelism(1); // 设置 sink 节点并行度为 1 

    env.execute("Print MySQL Snapshot + Binlog");
  }
}
```

**注意:** 请参考 [Deserialization](../about.html#deserialization) 有关 JSON 反序列化的更多详细信息。

### 动态加表

扫描新添加的表功能使你可以添加新表到正在运行的作业中，新添加的表将首先读取其快照数据，然后自动读取其变更日志。

想象一下这个场景：一开始， Flink 作业监控表 `[product, user, address]`, 但几天后，我们希望这个作业还可以监控表 `[order, custom]`，这些表包含历史数据，我们需要作业仍然可以复用作业的已有状态，动态加表功能可以优雅地解决此问题。

以下操作显示了如何启用此功能来解决上述场景。 使用现有的 Flink CDC Source 作业，如下：

```java
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .scanNewlyAddedTableEnabled(true) // 启用扫描新添加的表功能
        .databaseList("db") // 设置捕获的数据库
        .tableList("db.product, db.user, db.address") // 设置捕获的表 [product, user, address]
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();
   // 你的业务代码
```

如果我们想添加新表 `[order, custom]` 对于现有的 Flink 作业，只需更新 `tableList()` 将新增表 `[order, custom]` 加入并从已有的 savepoint 恢复作业。

_Step 1_: 使用 savepoint 停止现有的 Flink 作业。
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_Step 2_: 更新现有 Flink 作业的表列表选项。
1. 更新 `tableList()` 参数.
2. 编译更新后的作业，示例如下：
```java
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("yourHostname")
        .port(yourPort)
        .scanNewlyAddedTableEnabled(true) 
        .databaseList("db") 
        .tableList("db.product, db.user, db.address, db.order, db.custom") // 设置捕获的表 [product, user, address ,order, custom]
        .username("yourUsername")
        .password("yourPassword")
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();
   // 你的业务代码
```
_Step 3_: 从 savepoint 还原更新后的 Flink 作业。
```shell
$ ./bin/flink run \
      --detached \ 
      --fromSavepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**注意:** 请参考文档 [Restore the job from previous savepoint](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/cli/#command-line-interface) 了解更多详细信息。

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;"><a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html">MySQL type</a></th>
        <th class="text-left" style="width:10%;">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED<br>
        TINYINT UNSIGNED ZEROFILL
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        MEDIUMINT<br>
        SMALLINT UNSIGNED<br>
        SMALLINT UNSIGNED ZEROFILL
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED<br>
        INT UNSIGNED ZEROFILL<br>
        MEDIUMINT UNSIGNED<br>
        MEDIUMINT UNSIGNED ZEROFILL
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>
        BIGINT UNSIGNED<br>
        BIGINT UNSIGNED ZEROFILL<br>
        SERIAL
      </td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        FLOAT UNSIGNED<br>
        FLOAT UNSIGNED ZEROFILL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        REAL UNSIGNED<br>
        REAL UNSIGNED ZEROFILL<br>
        DOUBLE<br>
        DOUBLE UNSIGNED<br>
        DOUBLE UNSIGNED ZEROFILL<br>
        DOUBLE PRECISION<br>
        DOUBLE PRECISION UNSIGNED<br>
        DOUBLE PRECISION UNSIGNED ZEROFILL
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>在 MySQL 中，十进制数据类型的精度高达 65，但在 Flink 中，十进制数据类型的精度仅限于 38。所以，如果定义精度大于 38 的十进制列，则应将其映射到字符串以避免精度损失。在 MySQL 中，十进制数据类型的精度高达65，但在Flink中，十进制数据类型的精度仅限于38。所以，如果定义精度大于 38 的十进制列，则应将其映射到字符串以避免精度损失。</td>
    </tr>
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
      <td>
        CHAR(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIT(n)
      </td>
      <td>BINARY(⌈n/8⌉)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
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
      <td>目前，对于 MySQL 中的 BLOB 数据类型，仅支持长度不大于 2147483647（2**31-1）的 blob。 </td>
    </tr>
    <tr>
      <td>
        YEAR
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td> JSON 数据类型将在 Flink 中转换为 JSON 格式的字符串。</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>ARRAY&lt;STRING&gt;</td>
      <td>因为 MySQL 中的 SET 数据类型是一个字符串对象，可以有零个或多个值 
          它应该始终映射到字符串数组。
      </td>
    </tr>
    <tr>
      <td>
       GEOMETRY<br>
       POINT<br>
       LINESTRING<br>
       POLYGON<br>
       MULTIPOINT<br>
       MULTILINESTRING<br>
       MULTIPOLYGON<br>
       GEOMETRYCOLLECTION<br>
      </td>
      <td>
        STRING
      </td>
      <td>
      MySQL 中的空间数据类型将转换为具有固定 Json 格式的字符串。
      请参考 MySQL <a href="#a-name-id-003-a">空间数据类型映射</a> 章节了解更多详细信息。
      </td>
    </tr>
    </tbody>
</table>
</div>

### 空间数据类型映射<a name="空间数据类型映射" id="003"></a>

MySQL中除`GEOMETRYCOLLECTION`之外的空间数据类型都会转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "xxx", "coordinates": [0, 0]}
```
字段`srid`标识定义几何体的 SRS，如果未指定 SRID，则 SRID 0 是新几何体值的默认值。
由于 MySQL 8+ 在定义空间数据类型时只支持特定的 SRID，因此在版本较低的MySQL中，字段`srid`将始终为 0。

字段`type`标识空间数据类型，例如`POINT`/`LINESTRING`/`POLYGON`。

字段`coordinates`表示空间数据的`坐标`。

对于`GEOMETRYCOLLECTION`，它将转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "GeometryCollection", "geometries": [{"type":"Point","coordinates":[10,10]}]}
```

`Geometrics`字段是一个包含所有空间数据的数组。

不同空间数据类型映射的示例如下：
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data in MySQL</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>POINT(1 1)</td>
        <td>{"coordinates":[1,1],"type":"Point","srid":0}</td>
      </tr>
      <tr>
        <td>LINESTRING(3 0, 3 3, 3 5)</td>
        <td>{"coordinates":[[3,0],[3,3],[3,5]],"type":"LineString","srid":0}</td>
      </tr>
      <tr>
        <td>POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))</td>
        <td>{"coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],"type":"Polygon","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOINT((1 1),(2 2))</td>
        <td>{"coordinates":[[1,1],[2,2]],"type":"MultiPoint","srid":0}</td>
      </tr>
      <tr>
        <td>MultiLineString((1 1,2 2,3 3),(4 4,5 5))</td>
        <td>{"coordinates":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],"type":"MultiLineString","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))</td>
        <td>{"coordinates":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],"type":"MultiPolygon","srid":0}</td>
      </tr>
      <tr>
        <td>GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))</td>
        <td>{"geometries":[{"type":"Point","coordinates":[10,10]},{"type":"Point","coordinates":[30,30]},{"type":"LineString","coordinates":[[15,15],[20,20]]}],"type":"GeometryCollection","srid":0}</td>
      </tr>
    </tbody>
</table>
</div>

常见问题
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
