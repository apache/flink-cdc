---
title: "FAQ"
weight: 1
type: docs
aliases:
- /faq/faq.html
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
## General FAQ

### Q1: Why can't I download Flink-sql-connector-mysql-cdc-2.2-snapshot jar, why doesn't Maven warehouse rely on XXX snapshot?

Like the mainstream Maven project version management, XXX snapshot version is the code corresponding to the development branch. Users need to download the source code and compile the corresponding jar. Users should use the released version, such as flink-sql-connector-mysql-cdc-2.1 0.jar, the released version will be available in the Maven central warehouse.

### Q2: When should I use Flink SQL connector XXX Jar? When should I  Flink connector XXX jar? What's the difference between the two?

The dependency management of each connector in Flink CDC project is consistent with that in Flink project. Flink SQL connector XX is a fat jar. In addition to the code of connector, it also enters all the third-party packages that connector depends on into the shade and provides them to SQL jobs. Users only need to add the fat jar in the flink/lib directory. The Flink connector XX has only the code of the connector and does not contain the required dependencies. It is used by DataStream jobs. Users need to manage the required three-party package dependencies. Conflicting dependencies need to be excluded and shaded by themselves.

### Q3: Why change the package name from com.alibaba.ververica changed to org.apache.flink?  Why can't the 2. X version be found in Maven warehouse?

Flink CDC project changes the group ID from com.alibaba.ververica changed to org.apache.flink since 2.0.0 version, this is to make the project more community neutral and more convenient for developers of various companies to build. So look for 2.x in Maven warehouse package, the path is /org/apache/flink.

## MySQL CDC FAQ

### Q1: I use CDC 2.x version , only full data can be read, but binlog data cannot be read. What's the matter?

CDC 2.0 supports lock free algorithm and concurrent reading. In order to ensure the order of full data + incremental data, it relies on Flink's checkpoint mechanism, so the job needs to be configured with checkpoint.

Configuration method in SQL job:

```sql
Flink SQL> SET 'execution.checkpointing.interval' = '3s';    
```

DataStream job configuration mode:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);  
```

### Q2: Using MySQL CDC DataStream API, the timestamp field read in the incremental phase has a time zone difference of 8 hours. What's the matter?

When parsing the timestamp field in binlog data, CDC will use the server time zone information configured in the job, that is, the time zone of the MySQL server. If this time zone is not consistent with the time zone of your MySQL server, this problem will occur.

In addition, if the serializer is customized in the DataStream job.

such as MyDeserializer implements DebeziumDeserializationSchema, when the customized serializer parses the timestamp type data, it needs to refer to the analysis of the timestamp type in RowDataDebeziumDeserializeSchema and use the given time zone information.

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

### Q3: Does MySQL CDC support listening to slave database? How to configure slave database?

Yes, the slave database needs to be configured with log slave updates = 1, so that the slave instance can also write the data synchronized from the master instance to the binlog file of the slave database. If the master database has enabled gtid mode, the slave database also needs to be enabled.

```
log-slave-updates = 1
gtid_mode = on 
enforce_gtid_consistency = on 
```

### Q4: I want to synchronize sub databases and sub tables. How should I configure them?

In the with parameter of MySQL CDC table, both table name and database name support regular configuration, such as 'table name ' = 'user_ '.' Can match table name  'user_ 1, user_ 2,user_ A ' table.

Note that any regular matching character is'. ' Instead of '*', where the dot represents any character, the asterisk represents 0 or more, and so does database name, that the shared table should be in the same schema.

### Q5: I want to skip the stock reading phase and only read binlog data. How to configure it?

In the with parameter of MySQL CDC table

```
'scan.startup.mode' = 'latest-offset'.
```

### Q6: I want to get DDL events in the database. What should I do? Is there a demo?

Flink CDC provides DataStream API `MysqlSource` since version 2.1. Users can configure includeschemachanges to indicate whether DDL events are required. After obtaining DDL events, they can write code for next processing.

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

### Q7: How to synchronize the whole MySQL database? Does Flink CDC support it?

The DataStream API provided in Q6 has enabled users to obtain DDL change events and data change events. On this basis, users need to develop DataStream jobs according to their own business logic and downstream storage.

### Q8: In the same MySQL instance, the table of one database cannot synchronize incremental data, but other databases works fine. Why?

Users can check Binlog_Ignore_DB and Binlog_Do_DB through the `show master status` command

```mysql
mysql> show master status;
+------------------+----------+--------------+------------------+----------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set    |
+------------------+----------+--------------+------------------+----------------------+
| mysql-bin.000006 |     4594 |              |                  | xxx:1-15             |
+------------------+----------+--------------+------------------+----------------------+
```

### Q9: The job reports an error the connector is trying to read binlog starting at GTIDs xxx and binlog file 'binlog.000064', pos=89887992, skipping 4 events plus 1 rows, but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed, What should I do?

This error occurs because the binlog file being read by the job has been cleaned up on the MySQL server. Generally, the expiration time of the binlog file retained on the MySQL server is too short. You can set this value higher, such as 7 days.

```mysql
mysql> show variables like 'expire_logs_days';
mysql> set global expire_logs_days=7;
```

In another case, the binlog consumption of the Flink CDC job is too slow. Generally, sufficient resources can be allocated.

### Q10: The job reports an error ConnectException: A slave with the same server_uuid/server_id as this slave has connected to the master,What should I do?

This error occurs because the server ID used in the job conflicts with the server ID used by other jobs or other synchronization tools. The server ID needs to be globally unique. The server ID is an int type integer. In CDC 2.x In version, each concurrency of the source requires a server ID. it is recommended to reasonably plan the server ID. for example, if the source of the job is set to four concurrency, you can configure 'server ID' = '5001-5004', so that each source task will not conflict.

### Q11: The job reports an error ConnectException: Received DML ‘…’ for processing, binlog probably contains events generated with statement or mixed based replication format,What should I do?

This error occurs because the MySQL server is not configured correctly. You need to check the binlog is format row? You can view it through the following command

```mysql
mysql> show variables like '%binlog_format%'; 
```

### Q12: The job reports an error Mysql8.0 Public Key Retrieval is not allowed,What should I do?

This is because the MySQL user configured by the user uses sha256 password authentication and requires TLS and other protocols to transmit passwords. A simple method is to allow MySQL users to support original password access.

```mysql
mysql> ALTER USER 'username'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
mysql> FLUSH PRIVILEGES; 
```

### Q13: The job reports an error EventDataDeserializationException: Failed to deserialize data of EventHeaderV4 .... Caused by: java.net.SocketException: Connection reset,What should I do?

This problem is generally caused by the network. First, check the network between the Flink cluster and the database, and then increase the network parameters of the MySQL server.

```mysql
mysql> set global slave_net_timeout = 120; 
mysql> set global thread_pool_idle_timeout = 120;
```

Or try to use the flink configuration as follows.

```
execution.checkpointing.interval=10min
execution.checkpointing.tolerable-failed-checkpoints=100
restart-strategy=fixed-delay
restart-strategy.fixed-delay.attempts=2147483647
restart-strategy.fixed-delay.delay= 30s
```

If there is bad back pressure in the job, this problem may happen too. Then you need to handle the back pressure in the job first.

### Q14: The job reports an error The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires,What should I do?

The reason for this problem is that the reading of the full volume phase of the job is too slow. After reading the full volume phase, the previously recorded gtid site at the beginning of the full volume phase has been cleared by mysql. This can increase the save time of binlog files on the MySQL server, or increase the concurrency of source to make the full volume phase read faster.

### Q15: How to config `tableList` option when build MySQL CDC source in DataStream API?

The `tableList` option requires table name with database name rather than table name in DataStream API. For MySQL CDC source, the `tableList` option value should like ‘my_db.my_table’.

## Postgres CDC FAQ

### Q1: It is found that the disk utilization rate of PG server is high. What is the reason why wal is not released?

Flink Postgres CDC will only update the LSN in the Postgres slot when the checkpoint is completed. Therefore, if you find that the disk utilization is high, please first confirm whether the checkpoint is turned on.

### Q2: Flink Postgres CDC returns null for decimal types exceeding the maximum precision (38, 18) in synchronous Postgres

In Flink, if the precision of the received data is greater than the precision of the type declared in Flink, the data will be processed as null. You can configure the corresponding 'debezium decimal. handling. Mode '='string' process the read data with string type

### Q3: Flink Postgres CDC prompts that toast data is not transmitted. What is the reason?

Please ensure that the replica identity is full first. The toast data is relatively large. In order to save the size of wal, if the toast data is not changed, the wal2json plugin will not bring toast data to the updated data. To avoid this problem, you can use 'debezium schema. refresh. mode'='columns_ diff_ exclude_ unchanged_ Toast 'to solve.

### Q4: The job reports an error replication slot "XXXX" is active. What should I do?

Currently, Flink Postgres CDC does not release the slot manually after the job exits. There are two ways to solve this problem

- Go to Postgres and manually execute the following command

```
select pg_drop_replication_slot('rep_slot');
    ERROR:  replication slot "rep_slot" is active for PID 162564
select pg_terminate_backend(162564); select pg_drop_replication_slot('rep_slot');
```

- Add 'debezium.slot.drop.on.stop'='true' to PG source with parameter to automatically clean up the slot after the job stops

### Q5: Jobs have dirty data, such as illegal dates. Are there parameters that can be configured and filtered?

Yes, you can add configure. In the with parameter of the Flink CDC table  'debezium.event.deserialization.failure.handling.mode'='warn' parameter, skip dirty data and print dirty data to warn log. You can also configure 'debezium.event.deserialization.failure.handling.mode'='ignore', skip dirty data directly and do not print dirty data to the log.

### Q6: How to config `tableList` option when build Postgres CDC source in DataStream API?

The `tableList` option requires table name with schema name rather than table name in DataStream API. For Postgres CDC source, the `tableList` option value should like ‘my_schema.my_table’.

## MongoDB CDC FAQ

### Q1: Does mongodb CDC support full + incremental read and read-only incremental?

Yes, the default is full + incremental reading; Use copy The existing = false parameter is set to read-only increment.

### Q2: Does mongodb CDC support recovery from checkpoint? What is the principle?

Yes, the checkpoint will record the resumetoken of the changestream. During recovery, the changestream can be restored through the resumetoken. Where resumetoken corresponds to oplog RS (mongodb change log collection), oplog RS is a fixed capacity collection. When the corresponding record of resumetoken is in oplog When RS does not exist, an exception of invalid resumetoken may occur. In this case, you can set the appropriate oplog Set size of RS to avoid oplog RS retention time is too short, you can refer to https://docs.mongodb.com/manual/tutorial/change-oplog-size/ In addition, the resumetoken can be refreshed through the newly arrived change record and heartbeat record.

### Q3: Does mongodb CDC support outputting - U (update_before) messages?

Mongodb original oplog RS has only insert, update, replace and delete operation types. It does not retain the information before update. It cannot output - U messages. It can only realize the update semantics in Flink. When using mongodbtablesource, Flink planner will automatically perform changelognormalize optimization, fill in the missing - U messages, and output complete + I, - u, + U, and - D messages. The cost of changelognormalize optimization is that the node will save the status of all previous keys. Therefore, if the DataStream job directly uses mongodbsource, without the optimization of Flink planner, changelognormalize will not be performed automatically, so - U messages cannot be obtained directly. To obtain the pre update image value, you need to manage the status yourself. If you don't want to manage the status yourself, you can convert mongodbtablesource to changelogstream or retractstream and supplement the pre update image value with the optimization ability of Flink planner. An example is as follows:

```
    tEnv.executeSql("CREATE TABLE orders ( ... ) WITH ( 'connector'='mongodb-cdc',... )");

    Table table = tEnv.from("orders")
            .select($("*"));

    tEnv.toChangelogStream(table)
            .print()
            .setParallelism(1);

    env.execute();
```



### Q4: Does mongodb CDC support subscribing to multiple collections?

Only the collection of the whole database can be subscribed, but some collection filtering functions are not supported. For example, if the database is configured as' mgdb 'and the collection is an empty string, all collections under the' mgdb 'database will be subscribed.

### Q5: Does mongodb CDC support setting multiple concurrent reads?

Not yet supported.

### Q6: What versions of mongodb are supported by mongodb CDC?

Mongodb CDC is implemented based on the changestream feature, which is a new feature launched by mongodb 3.6. Mongodb CDC theoretically supports versions above 3.6. It is recommended to run version > = 4.0. When executing versions lower than 3.6, an error will occur: unrecognized pipeline stage name: '$changestream'.

### Q7: What is the operation mode of mongodb supported by mongodb CDC?

Changestream requires mongodb to run in replica set or fragment mode. Local tests can use stand-alone replica set rs.initiate().

Errors occur in standalone mode : The $changestage is only supported on replica sets.

### Q8: Mongodb CDC reports an error. The user name and password are incorrect, but other components can connect normally with this user name and password. What is the reason?

If the user is creating a DB that needs to be connected, add 'connection' to the with parameter Options' ='authsource = DB where the user is located '.

### Q9: Does mongodb CDC support debezium related parameters?

The mongodb CDC connector is not supported because it is independently developed in the Flink CDC project and does not rely on the debezium project.

### Q10: In the mongodb CDC full reading phase, can I continue reading from the checkpoint after the job fails?

In the full reading phase, mongodb CDC does not do checkpoint until the full reading phase is completed. If it fails in the full reading phase, mongodb CDC will read the stock data again.

## Oracle CDC FAQ

### Q1: Oracle CDC's archive logs grow rapidly and read logs slowly?

The online mining mode can be used without writing the data dictionary to the redo log, but it cannot process DDL statements. The default policy of the production environment reads the log slowly, and the default policy will write the data dictionary information to the redo log, resulting in a large increase in the log volume. You can add the following debezium configuration items. " log. mining. strategy' = 'online_ catalog','log. mining. continuous. mine' = 'true'。 If you use SQL, you need to prefix the configuration item with 'debezium.', Namely:

```
'debezium.log.mining.strategy' = 'online_catalog',
'debezium.log.mining.continuous.mine' = 'true'
```


### Q2: Operation error caused by: io debezium. DebeziumException: Supplemental logging not configured for table xxx.  Use command: alter table XXX add supplementary log data (all) columns?

For Oracle version 11, debezium will set tableidcasesensitive to true by default, resulting in the table name being updated to lowercase. Therefore, the table completion log setting cannot be queried in Oracle, resulting in the false alarm of "supplementary logging not configured for table error".

If it is the DataStream API, add the configuration item of debezium 'database.tablename.case.insensitive' = 'false'. If the SQL API is used, add the configuration item 'debezium.database.tablename.case.insensitive' = 'false' in the option of the table

### Q3: How does Oracle CDC switch to XStream?

Add configuration item  'database.connection.adpter' = 'xstream', please use the configuration item 'debezium.database.connection.adpter' = 'xstream' if you're using SQL API.

### Q4: What are the database name and schema name of Oracle CDC

Database name is the name of the database example, that is, the SID of Oracle. Schema name is the schema corresponding to the table. Generally speaking, a user corresponds to a schema. The schema name of the user is equal to the user name and is used as the default schema of the user. Therefore, schema name is generally the user name for creating the table, but if a schema is specified when creating the table, the specified schema is schema name. For example, use create table AAAA If TestTable (XXXX) is successfully created, AAAA is schema name.
