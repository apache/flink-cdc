---
title: "通用 FAQ"
weight: 1
type: docs
aliases:
- /faq/faq
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
## 通用 FAQ {#通用faq}

### Q1：为啥没法下载 flink-sql-connector-mysql-cdc-2.2-SNAPSHOT.jar，maven 仓库为啥没有 xxx-SNAPSHOT 依赖？ {#q1-为啥没法下载-flink-sql-connector-mysql-cdc-22-snapshotjar-maven-仓库为啥没有-xxx-snapshot-依赖}

和主流的 maven 项目版本管理相同，xxx-SNAPSHOT 版本都是对应开发分支的代码，需要用户自己下载源码并编译对应的 jar，用户应该使用已经 release 过的版本，比如 flink-sql-connector-mysql-cdc-2.1.0.jar，release 过的版本 maven 中心仓库才会有。

### Q2：什么时候使用 flink-sql-connector-xxx.jar，什么时候使用 flink-connector-xxx.jar，两者有啥区别？ {#q2-什么时候使用-flink-sql-connector-xxxjar什么时候使用-flink-connector-xxxjar两者有啥区别}

Flink CDC 项目中各个 connector 的依赖管理和 Flink 项目中 connector 保持一致。flink-sql-connector-xx 是胖包，除了 connector 的代码外，还把 connector 依赖的所有三方包 shade 后打入，提供给 SQL 作业使用，用户只需要在 lib 目录下添加该胖包即可。flink-connector-xx 只有该 connector 的代码，不包含其所需的依赖，提供 datastream 作业使用，用户需要自己管理所需的三方包依赖，有冲突的依赖需要自己做 exclude、shade 处理。

### Q3：为啥把包名从 com.alibaba.ververica 改成 org.apache.flink? 为啥 maven 仓库里找不到 2.x 版本？ {#q3-为啥把包名从-comalibabaververica-改成-orgapacheflink-为啥-maven-仓库里找不到-2x-版本}


Flink CDC 项目从 2.0.0 版本将 group id 从 com.alibaba.ververica 改成 com.ververica，自 3.1 版本将 group id 从 com.ververica 改成 org.apache.flink。
这是为了让项目更加社区中立，让各个公司的开发者共建时更方便。所以在 maven 仓库找 2.x 的包时，路径是 /com/ververica；找 3.1 及以上版本的包时，路径是 /org/apache/flink

## MySQL CDC FAQ

### Q1：使用 CDC 2.x 版本，只能读取全量数据，无法读取增量（binlog）数据，怎么回事？ {#q1-使用cdc-2x版本只能读取全量数据无法读取增量binlog-数据怎么回事}

CDC 2.0 支持了无锁算法，支持并发读取，为了保证全量数据 + 增量数据的顺序性，依赖 Flink 的 checkpoint 机制，所以作业需要配置 checkpoint。SQL 作业中配置方式：

```sql
Flink SQL> SET 'execution.checkpointing.interval' = '3s';    
```

DataStream 作业配置方式：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);  
```

此外，如果某些数据库的只读实例是简化过 binlog 的，比如阿里云 RDS MySQL 5.6 只读实例，其 binlog 不含有变更数据，自然无法获得所需增量数据

### Q2：使用 MySQL CDC，增量阶段读取出来的 timestamp 字段时区相差 8 小时，怎么回事呢？ {#q2-使用-mysql-cdc增量阶段读取出来的-timestamp-字段时区相差8小时怎么回事呢}

在解析 binlog 数据中的 timestamp 字段时，cdc 会使用到作业里配置的 server-time-zone 信息，也就是 MySQL 服务器的时区，如果这个时区没有和你的 MySQL 服务器时区一致，就会出现这个问题。

此外，如果是在 DataStream 作业中自定义反序列化器如 MyDeserializer implements DebeziumDeserializationSchema, 自定义的序列化器在解析 timestamp 类型数据时，需要参考下 RowDataDebeziumDeserializeSchema 中对 timestamp 类型的解析，使用给定的时区信息。

```
private TimestampData convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromEpochMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000));
            }
        }
        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
        return TimestampData.fromLocalDateTime(localDateTime);
    }
```

### Q3：mysql cdc 支持监听从库吗？从库需要如何配置？ {#q3-mysql-cdc支持监听从库吗从库需要如何配置}

支持的，从库需要配置 log-slave-updates = 1 使从实例也能将从主实例同步的数据写入从库的 binlog 文件中，如果主库开启了 gtid mode，从库也需要开启。

```
log-slave-updates = 1
gtid_mode = on 
enforce_gtid_consistency = on 
```

### Q4：我想同步分库分表，应该如何配置？ {#q4-我想同步分库分表应该如何配置}

通过 mysql cdc 表的 with 参数中，表名和库名均支持正则配置，比如 'table-name' ='user_.' 可以匹配表名 user_1, user_2,user_a 表，注意正则匹配任意字符是 '.' 而不是 '*'，其中点号表示任意字符，星号表示 0 个或多个，database-name 也如此。

### Q5：我想跳过存量读取阶段，只读取 binlog 数据，怎么配置？ {#q5-我想跳过存量读取阶段只读取-binlog-数据怎么配置}

在 mysql cdc 表的 with 参数中指定 'scan.startup.mode' = 'latest-offset' 即可。

```
'scan.startup.mode' = 'latest-offset'.
```

### Q6：我想获取数据库中的 DDL 事件，怎么办，有 demo 吗？ {#q6-我想获取数据库中的-ddl事件怎么办有demo吗}

CDC 2.1 版本提供了 DataStream API：`MySqlSource`，用户可以配置 includeSchemaChanges 表示是否需要 DDL 事件，获取到 DDL 事件后自己写代码处理。

```java
 public void consumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // Configure here and output DDL events
                        .build();
				... // Other processing logic                        
    }
```

### Q7：MySQL 整库同步怎么做，Flink CDC 支持吗？ {#q7-mysql-整库同步怎么做-flink-cdc-支持吗}

Flink CDC 支持的。
1. Q6 中提供的 DataStream API 已经可以让用户获取 DDL 变更事件和数据变更事件，用户需要在此基础上，根据自己的业务逻辑和下游存储进行 DataStream 作业开发。
2. Flink CDC 3.0 以上版本支持以 Pipeline 的形式对 MySQL 整库同步。

### Q8：同一个实例下，某个库的表无法同步增量数据，其他库都可以，这是为啥？ {#q8-同一个实例下某个库的表无法同步增量数据其他库都可以这是为啥}

这个问题是因为 mysql 服务器 可以配置 binlog 过滤器，忽略了某些库的 binlog。用户可以通过 show master status 命令查看 Binlog_Ignore_DB 和 Binlog_Do_DB。

```mysql
mysql> show master status;
+------------------+----------+--------------+------------------+----------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set    |
+------------------+----------+--------------+------------------+----------------------+
| mysql-bin.000006 |     4594 |              |                  | xxx:1-15             |
+------------------+----------+--------------+------------------+----------------------+
```

### Q9：作业报错 The connector is trying to read binlog starting at GTIDs xxx and binlog file 'binlog.000064', pos=89887992, skipping 4 events plus 1 rows, but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed，怎么办呢？ {#q9-作业报错-the-connector-is-trying-to-read-binlog-starting-at-gtids-xxx-and-binlog-file-binlog000064-pos89887992-skipping-4-events-plus-1-rows-but-this-is-no-longer-available-on-the-server-reconfigure-the-connector-to-use-a-snapshot-when-needed怎么办呢}

出现这种错误是作业正在读取的 binlog 文件在 MySQL 服务器已经被清理掉，这种情况一般是 MySQL 服务器上保留的 binlog 文件过期时间太短，可以将该值设置大一点，比如 7 天。

```mysql
mysql> show variables like 'expire_logs_days';
mysql> set global expire_logs_days=7;
```

还有种情况是 flink cdc 作业消费 binlog 太慢，这种一般分配足够的资源即可。

### Q10：作业报错 ConnectException: A slave with the same server_uuid/server_id as this slave has connected to the master，怎么办呢？ {#q10-作业报错-connectexception-a-slave-with-the-same-server_uuidserver_id-as-this-slave-has-connected-to-the-master怎么办呢}

出现这种错误是作业里使用的 server id 和其他作业或其他同步工具使用的 server id 冲突了，server id 需要全局唯一，server id 是一个 int 类型整数。在 CDC 2.x 版本中，source 的每个并发都需要一个 server id，建议合理规划好 server id，比如作业的 source 设置成了四个并发，可以配置 'server-id' = '5001-5004'，这样每个 source task 就不会冲突了。

### Q11：作业报错 ConnectException: Received DML ‘…’ for processing, binlog probably contains events generated with statement or mixed based replication format，怎么办呢？ {#q11-作业报错-connectexception-received-dml--for-processing-binlog-probably-contains-events-generated-with-statement-or-mixed-based-replication-format怎么办呢}

出现这种错误是 MySQL 服务器配置不对，需要检查下 binlog_format 是不是 ROW？可以通过下面的命令查看
```mysql
mysql> show variables like '%binlog_format%'; 
```

### Q12：作业报错 MySQL 8.0 Public Key Retrieval is not allowed，怎么办呢？ {#q12-作业报错-mysql80-public-key-retrieval-is-not-allowed-怎么办呢}

这是因为用户配置的 MySQL 用户使用的是 sha256 密码认证，需要 TLS 等协议传输密码。一种简单的方法是允许 MySQL 用户支持原始密码方式访问。

```mysql
mysql> ALTER USER 'username'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
mysql> FLUSH PRIVILEGES; 
```

### Q13：作业报错 EventDataDeserializationException: Failed to deserialize data of EventHeaderV4 .... Caused by: java.net.SocketException: Connection reset，怎么办呢？ {#q13-作业报错-eventdatadeserializationexception-failed-to-deserialize-data-of-eventheaderv4--caused-by-javanetsocketexception-connection-reset-怎么办呢-}

这个问题一般是网络原因或者数据库繁忙引起，首先排查 flink 集群到数据库之间的网络情况，其次可以调大 MySQL 服务器的网络参数。

```mysql
mysql> set global slave_net_timeout = 120; 
mysql> set global thread_pool_idle_timeout = 120;
```

或者采用下面的 Flink 配置：
```
execution.checkpointing.interval=10min
execution.checkpointing.tolerable-failed-checkpoints=100
restart-strategy=fixed-delay
restart-strategy.fixed-delay.attempts=2147483647
restart-strategy.fixed-delay.delay= 30s
```

如果作业存在反压，也可能出现这个问题。你需要先处理作业的反压。

### Q14：作业报错 The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires. 怎么办呢？ {#q14-作业报错-the-slave-is-connecting-using-change-master-to-master_auto_position--1-but-the-master-has-purged-binary-logs-containing-gtids-that-the-slave-requires-怎么办呢-}

出现这个问题的原因是作业全量阶段读取太慢，在全量阶段读完后，之前记录的全量阶段开始时的 gtid 位点已经被 mysql 清理掉了。这种可以增大 mysql 服务器上 binlog 文件的保存时间，也可以调大 source 的并发，让全量阶段读取更快。

### Q15：在 DataStream API 中构建 MySQL CDC 源时如何配置 tableList 选项？ {#q15-在-datastream-api中构建mysql-cdc源时如何配置tablelist选项}

1. tableList 选项要求表名使用数据库名，而不是 DataStream API 中的表名。对于 MySQL CDC 源代码，tableList 选项值应该类似于 `my_db.my_table`。
2. 如果要同步排除 products 和 orders 表之外的整个 my_db 库，tableList 选项值应该类似于 `my_db.(?!products|orders).*`。

### Q16：MySQL 源表中存在 TINYINT(1) 类型的列，且部分行的数值>1，Pipeline 作业下游接收到的数据却是 true/false，为什么？ {#q16-mysql源表中存在tinyint1类型的列且部分行的数值1pipeline作业下游接收到的数据却是truefalse为什么}
这是由于 MySQL 连接参数 `tinyInt1isBit` 默认值为 `true`，Flink CDC 3.3.0 之前的版本未处理该参数，导致 TINYINT(1) 类型的数据被解析为布尔值。
若需将其转换为实际值，请将 CDC 升级至 3.3.0+，并在 source 节点添加配置 `treat-tinyint1-as-boolean.enabled: false`。\
例如：
```yaml
source:
  type: mysql
  ...
  treat-tinyint1-as-boolean.enabled: false

sink:
  type: ...
```
## Postgres CDC FAQ

### Q1：发现 PG 服务器磁盘使用率高，WAL 不释放 是什么原因？ {#q1-发现-pg-服务器磁盘使用率高wal-不释放-是什么原因}

Flink Postgres CDC 只会在 checkpoint 完成的时候更新 Postgres slot 中的 LSN。因此如果发现磁盘使用率高的情况下，请先确认 checkpoint 是否开启。

### Q2：Flink Postgres CDC 同步 Postgres 中将 超过最大精度（38，18）的 DECIMAL 类型返回 NULL {#q2-flink-postgres-cdc-同步-postgres-中将-超过最大精度3818的-decimal-类型返回-null}

Flink 中如果收到数据的 precision 大于在 Flink 中声明的类型的 precision 时，会将数据处理成 NULL。此时可以配置相应 'debezium.decimal.handling.mode' = 'string' 将读取的数据用 STRING 类型 来处理。

### Q3：Flink Postgres CDC 提示未传输 TOAST 数据，是什么原因？ {#q3-flink-postgres-cdc-提示未传输-toast-数据是什么原因}

请先确保 REPLICA IDENTITY 是 FULL。TOAST 的数据比较大，为了节省 wal 的大小，如果 TOAST 数据没有变更，那么 wal2json plugin 就不会在更新后的数据中带上 toast 数据。为了避免这个问题，可以通过 'debezium.schema.refresh.mode'='columns_diff_exclude_unchanged_toast' 来解决。

### Q4：作业报错 Replication slot "xxxx" is active，怎么办？ {#q4-作业报错-replication-slot-xxxx-is-active-怎么办}

当前 Flink Postgres CDC 在作业退出后并不会手动释放 slot。前往 Postgres 中手动执行以下命令：

```
select pg_drop_replication_slot('rep_slot');
    ERROR:  replication slot "rep_slot" is active for PID 162564
select pg_terminate_backend(162564); select pg_drop_replication_slot('rep_slot');
```

### Q5：作业有脏数据，比如非法的日期，有参数可以配置可以过滤吗？ {#q5-作业有脏数据比如非法的日期有参数可以配置可以过滤吗}

可以的，可以在 Flink CDC 表的 with 参数里 加下 'debezium.event.deserialization.failure.handling.mode'='warn' 参数，跳过脏数据，将脏数据打印到 WARN 日志里。也可以配置 'debezium.event.deserialization.failure.handling.mode'='ignore'，直接跳过脏数据，不打印脏数据到日志。


### Q6：在 DataStream API 中构建 Postgres CDC 源时如何配置 tableList 选项？ {#q6-在datastream-api中构建postgres-cdc源时如何配置tablelist选项}

tableList 选项要求表名使用架构名，而不是 DataStream API 中的表名。对于 Postgres CDC source，tableList 选项值应为 `my_schema.my_table`。

## MongoDB CDC FAQ

### Q1：MongoDB CDC 支持 全量 + 增量读 和 只读增量吗？ {#q1-mongodb-cdc-支持-全量增量读-和-只读增量吗}

支持，默认为 全量 + 增量 读取；使用 'scan.startup.mode' = 'latest-offset' 参数设置为只读增量。

### Q2：MongoDB CDC 支持从 checkpoint 恢复吗？原理是怎么样的呢？ {#q2-mongodb-cdc-支持从-checkpoint-恢复吗-原理是怎么样的呢}

支持，checkpoint 会记录 ChangeStream 的 resumeToken，恢复的时候可以通过 resumeToken 重新恢复 ChangeStream。其中 resumeToken 对应 `oplog.rs`（MongoDB 变更日志 collection）的位置，`oplog.rs` 是一个固定容量的 collection。当 resumeToken 对应的记录在 `oplog.rs` 中不存在的时候，可能会出现 Invalid resumeToken 的异常。这种情况，在使用时可以设置合适 `oplog.rs` 的集合大小，避免 `oplog.rs` 保留时间过短，可以参考 https://docs.mongodb.com/manual/tutorial/change-oplog-size/。另外，resumeToken 可以通过新到的变更记录和 heartbeat 记录来刷新。

### Q3：MongoDB CDC 支持输出 -U（update_before，更新前镜像值）消息吗？ {#q3-mongodb-cdc-支持输出--uupdate_before更新前镜像值消息吗}

在 MongoDB 6.0 及以上版本，若数据库开启了[前像或后像功能](https://www.mongodb.com/docs/atlas/app-services/mongodb/preimages/)，可以在 SQL 作业中配置参数 'scan.full-changelog' = 'true'，使得数据源能够输出 -U 消息，从而省去 ChangelogNormalize。

在 MongoDB 6.0 版本前，MongoDB 原始的 `oplog.rs` 只有 INSERT、UPDATE、REPLACE、DELETE 这几种操作类型，没有保留更新前的信息，不能输出 -U 消息，在 Flink 中只能实现 UPSERT 语义。在使用 MongoDBTableSource 时，Flink planner 会自动进行 ChangelogNormalize 优化，补齐缺失的 -U 消息，输出完整的 +I、-U、+U、-D 四种消息，代价是该节点会保存之前所有 key 的状态。所以，如果是 DataStream 作业直接使用 MongoDBSource，如果没有 Flink planner 的优化，将不会自动进行 ChangelogNormalize，所以不能直接获取 -U 消息。想要获取更新前镜像值，需要自己管理状态，如果不希望自己管理状态，可以将 MongoDBTableSource 转换为 ChangelogStream 或者 RetractStream，借助 Flink planner 的优化能力补齐更新前镜像值，示例如下：

```
    tEnv.executeSql("CREATE TABLE orders ( ... ) WITH ( 'connector'='mongodb-cdc',... )");

    Table table = tEnv.from("orders")
            .select($("*"));

    tEnv.toChangelogStream(table)
            .print()
            .setParallelism(1);

    env.execute();
```

### Q4：MongoDB CDC 支持订阅多个集合吗？ {#q4-mongodb-cdc-支持订阅多个集合吗}

支持订阅整库的 collection，例如配置 database 为 'mgdb'，并且配置 collection 为空字符串，则会订阅 'mgdb' 库下所有 collection。

也支持通过正则表达式匹配 collection，如果要监控的集合名称中包含正则表达式特殊字符，则 collection 参数必须配置为完全限定的名字空间（数据库名称.集合名称），否则无法捕获对应 collection 的变更。

### Q5：MongoDB CDC 支持 MongoDB 的版本是哪些？ {#q5-mongodb-cdc-支持-mongodb-的版本是哪些}

MongoDB CDC 基于 ChangeStream 特性实现，ChangeStream 是 MongoDB 3.6 推出的新特性。MongoDB CDC 理论上支持 3.6 及以上版本，建议运行版本 >= 4.0，在低于 3.6 版本执行时，会出现错误：Unrecognized pipeline stage name: '$changeStream'。

### Q6：MongoDB CDC 支持 MongoDB 的运行模式是什么？ {#q6-mongodb-cdc-支持-mongodb-的运行模式是什么}

ChangeStream 需要 MongoDB 以副本集或者分片模式运行，本地测试可以使用单机版副本集 `rs.initiate()`。在 standalone 模式下会出现错误：`The $changeStream stage is only supported on replica sets.`

### Q7：MongoDB CDC 报错用户名密码错误，但其他组件使用该用户名密码都能正常连接，这是什么原因？ {#q7-mongodb-cdc-报错用户名密码错误-但其他组件使用该用户名密码都能正常连接这是什么原因}

如果用户不是在默认的 admin 数据库下创建的，需要在 with 参数里加下 'connection.options' = 'authSource=用户所在的 db'。

### Q8：MongoDB CDC 是否支持 debezium 相关的参数？ {#q8--mongodb-cdc-是否支持-debezium-相关的参数}

不支持的，因为 MongoDB CDC 连接器是在 Flink CDC 项目中独立开发，并不依赖 Debezium 项目，所以不支持。

## Oracle CDC FAQ

### Q1：Oracle CDC 的归档日志增长很快，且读取 log 慢？ {#q1-oracle-cdc-的归档日志增长很快且读取-log-慢}

可以使用在线挖掘的模式，不写入数据字典到 redo log 中，但是这样无法处理 DDL 语句。生产环境默认策略读取 log 较慢，且默认策略会写入数据字典信息到 redo log 中导致日志量增加较多，可以添加如下 debezium 的配置项。'log.mining.strategy' = 'online_catalog','log.mining.continuous.mine' = 'true'。如果使用 SQL 的方式，则需要在配置项中加上前缀 `debezium.`，即：

```
'debezium.log.mining.strategy' = 'online_catalog',
'debezium.log.mining.continuous.mine' = 'true'
```


### Q2：作业报错 Caused by: io.debezium.DebeziumException: Supplemental logging not configured for table xxx. Use command: ALTER TABLE xxx ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS，怎么办呢？ {#q2-作业报错-caused-by-iodebeziumdebeziumexception-supplemental-logging-not-configured-for-table-xxx-use-command-alter-table-xxx-add-supplemental-log-data-all-columns-怎么办呢}

对于 Oracle 11 版本，debezium 会默认把 tableIdCaseInsensitive 设置为 true，导致表名被更新为小写，因此在 Oracle 中查询不到 这个表补全日志设置，导致误报这个 「Supplemental logging not configured for table」错误。添加 debezium 的配置项 'database.tablename.case.insensitive' = 'false'，如果使用 SQL 的方式，则在表的 option 中添加配置项 'debezium.database.tablename.case.insensitive' = 'false'

### Q3：Oracle CDC 如何切换成 XStream 的方式？ {#q3-oracle-cdc-如何切换成-xstream-的方式}

添加 debezium 的配置项 'database.connection.adapter' = 'xstream'，如果使用 SQL 的方式，则在表的 option 中添加配置项 'debezium.database.connection.adapter' = 'xstream'

### Q4：Oracle CDC 的 database-name 和 schema-name 分别是什么？ {#q4-oracle-cdc-的-database-name-和-schema-name-分别是什么}

database-name 是数据库实例的名字，也就是 Oracle 的 SID；schema-name 是表对应的 schema，一般而言，一个用户就对应一个 schema，该用户的 schema 名等于用户名，并作为该用户缺省 schema。所以 schema-name 一般都是创建这个表的用户名，但是如果创建表的时候指定了 schema，则指定的 schema 则为 schema-name。比如用 CREATE TABLE aaaa.testtable(xxxx) 的方式成功创建了表 testtable，则 aaaa 为 schema-name。
