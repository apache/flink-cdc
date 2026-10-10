---
title: "Oracle"
weight: 3
type: docs
aliases:
- /connectors/flink-sources/oracle-cdc
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

# Oracle CDC 连接器

Oracle CDC 连接器允许从 Oracle 数据库读取快照数据和增量数据。本文描述了如何设置 Oracle CDC 连接器来对 Oracle 数据库运行 SQL 查询。

依赖
------------

为了设置 Oracle CDC 连接器，下表提供了使用构建自动化工具（如 Maven 或 SBT ）和带有 SQL JAR 包的 SQL 客户端的两个项目的依赖关系信息。

### Maven dependency

{{< artifact flink-connector-oracle-cdc >}}

### SQL Client JAR

**下载链接仅适用于稳定版本。**

下载 [flink-sql-connector-oracle-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oracle-cdc) 到 `<FLINK_HOME>/lib/` 目录下。

**注意:** 参考 [flink-sql-connector-oracle-cdc](https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-oracle-cdc) 当前已发布的所有版本都可以在 Maven 中央仓库获取。


由于 Oracle Connector 采用的 FUTC 协议与 Flink CDC 项目不兼容，我们无法在 jar 包中提供 Oracle 连接器。
您可能需要手动配置以下依赖：

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
        <td><a href="https://mvnrepository.com/artifact/com.oracle.ojdbc/ojdbc8/19.3.0.0">com.oracle.ojdbc:ojdbc8:19.3.0.0</a></td>
        <td>用于连接到 Oracle 数据库。</td>
      </tr>
    </tbody>
    <tbody>
      <tr>
        <td><a href="https://mvnrepository.com/artifact/com.oracle.database.xml/xdb/19.3.0.0">com.oracle.database.xml:xdb:19.3.0.0</a></td>
        <td>用于存储 XML 文件。</td>
      </tr>
    </tbody>
</table>
</div>

设置 Oracle
----------------
你必须为 Oracle 数据库启用日志归档，并定义一个 Oracle 用户，该用户对 Debezium Oracle 连接器监视的所有数据库都应该具有相应的权限。

### 非 CDB 数据库

1. 启用日志归档 

   (1.1). 以 DBA 身份连接到数据库
    ```sql
    ORACLE_SID=SID
    export ORACLE_SID
    sqlplus /nolog
      CONNECT sys/password AS SYSDBA
    ```

   (1.2). 启用日志归档
    ```sql
    alter system set db_recovery_file_dest_size = 10G;
    alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
    shutdown immediate;
    startup mount;
    alter database archivelog;
    alter database open;
   ```
    **Note:**

    - Enable log archiving requires database restart, pay attention when try to do it
    - The archived logs will occupy a large amount of disk space, so consider clean the expired logs the periodically
   
    (1.3). Check whether log archiving is enabled
    ```sql
    -- Should now "Database log mode: Archive Mode"
    archive log list;
    ```
   **Note:**
   
   Supplemental logging must be enabled for captured tables or the database in order for data changes to capture the <em>before</em> state of changed database rows.
   The following illustrates how to configure this on the table/database level.
   ```sql
   -- Enable supplemental logging for a specific table:
   ALTER TABLE inventory.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   ```
   ```sql
   -- Enable supplemental logging for database
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ```

2. 创建具有权限的 Oracle 用户

   (2.1). 创建表空间
   ```sql
   sqlplus sys/password@host:port/SID AS SYSDBA;
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/SID/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit;
   ```

   (2.2). 创建用户并授予权限
   ```sql
   sqlplus sys/password@host:port/SID AS SYSDBA;
     CREATE USER flinkuser IDENTIFIED BY flinkpw DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
     GRANT CREATE SESSION TO flinkuser;
     GRANT SET CONTAINER TO flinkuser;
     GRANT SELECT ON V_$DATABASE to flinkuser;
     GRANT FLASHBACK ANY TABLE TO flinkuser;
     GRANT SELECT ANY TABLE TO flinkuser;
     GRANT SELECT_CATALOG_ROLE TO flinkuser;
     GRANT EXECUTE_CATALOG_ROLE TO flinkuser;
     GRANT SELECT ANY TRANSACTION TO flinkuser;
     GRANT LOGMINING TO flinkuser;
     GRANT ANALYZE ANY TO flinkuser;

     GRANT CREATE TABLE TO flinkuser;
     -- need not to execute if set scan.incremental.snapshot.enabled=true(default)
     GRANT LOCK ANY TABLE TO flinkuser;
     GRANT ALTER ANY TABLE TO flinkuser;
     GRANT CREATE SEQUENCE TO flinkuser;

     GRANT EXECUTE ON DBMS_LOGMNR TO flinkuser;
     GRANT EXECUTE ON DBMS_LOGMNR_D TO flinkuser;

     GRANT SELECT ON V_$LOG TO flinkuser;
     GRANT SELECT ON V_$LOG_HISTORY TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_LOGS TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_CONTENTS TO flinkuser;
     GRANT SELECT ON V_$LOGMNR_PARAMETERS TO flinkuser;
     GRANT SELECT ON V_$LOGFILE TO flinkuser;
     GRANT SELECT ON V_$ARCHIVED_LOG TO flinkuser;
     GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO flinkuser;
     exit;
   ```
   
### CDB 数据库

总体而言，配置 CDB 数据库的步骤与非 CDB 数据库非常相似，但命令可能有所不同。
1. 启用日志归档
   ```sql
   ORACLE_SID=ORCLCDB
   export ORACLE_SID
   sqlplus /nolog
     CONNECT sys/password AS SYSDBA
     alter system set db_recovery_file_dest_size = 10G;
     -- should exist
     alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
     shutdown immediate
     startup mount
     alter database archivelog;
     alter database open;
     -- Should show "Database log mode: Archive Mode"
     archive log list
     exit;
   ```
   **注意:**
   你也可以使用以下命令来启用补充日志：
   ```sql
   -- Enable supplemental logging for a specific table:
   ALTER TABLE inventory.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   -- Enable supplemental logging for database
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ```

2. 创建具有权限的 Oracle 用户
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLCDB as sysdba
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit
   ```
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLPDB1 as sysdba
     CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
     exit
   ```
   ```sql
   sqlplus sys/password@//localhost:1521/ORCLCDB as sysdba
     CREATE USER flinkuser IDENTIFIED BY flinkpw DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
     GRANT CREATE SESSION TO flinkuser CONTAINER=ALL;
     GRANT SET CONTAINER TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$DATABASE to flinkuser CONTAINER=ALL;
     GRANT FLASHBACK ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT_CATALOG_ROLE TO flinkuser CONTAINER=ALL;
     GRANT EXECUTE_CATALOG_ROLE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ANY TRANSACTION TO flinkuser CONTAINER=ALL;
     GRANT LOGMINING TO flinkuser CONTAINER=ALL;
     GRANT CREATE TABLE TO flinkuser CONTAINER=ALL;
     -- need not to execute if set scan.incremental.snapshot.enabled=true(default)
     GRANT LOCK ANY TABLE TO flinkuser CONTAINER=ALL;
     GRANT CREATE SEQUENCE TO flinkuser CONTAINER=ALL;

     GRANT EXECUTE ON DBMS_LOGMNR TO flinkuser CONTAINER=ALL;
     GRANT EXECUTE ON DBMS_LOGMNR_D TO flinkuser CONTAINER=ALL;

     GRANT SELECT ON V_$LOG TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOG_HISTORY TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_LOGS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_CONTENTS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGMNR_PARAMETERS TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$LOGFILE TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$ARCHIVED_LOG TO flinkuser CONTAINER=ALL;
     GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO flinkuser CONTAINER=ALL;
     exit
   ```
   
查看更多关于 [Setting up Oracle](https://debezium.io/documentation/reference/1.9/connectors/oracle.html#setting-up-oracle) 的信息

如何创建 Oracle CDC 表
----------------

Oracle CDC 表可以定义如下：

```sql 
-- register an Oracle table 'products' in Flink SQL
Flink SQL> CREATE TABLE products (
     ID INT NOT NULL,
     NAME STRING,
     DESCRIPTION STRING,
     WEIGHT DECIMAL(10, 3),
     PRIMARY KEY(id) NOT ENFORCED
     ) WITH (
     'connector' = 'oracle-cdc',
     'hostname' = 'localhost',
     'port' = '1521',
     'username' = 'flinkuser',
     'password' = 'flinkpw',
     'database-name' = 'ORCLCDB',
     'schema-name' = 'inventory',
     'table-name' = 'products');
  
-- read snapshot and redo logs from products table
Flink SQL> SELECT * FROM products;
```
**注意:**
在使用 CDB + PDB 模型时，需要在 Flink DDL 中添加一个额外的选项 `'debezium.database.pdb.name' = 'xxx'` 来指定要连接的 PDB 的名称。

**注意:**
虽然该连接器可能适用于多种 Oracle 版本和发行版，但仅对 Oracle 9i、10g、11g 和 12c 进行了测试。

连接器配置项
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
      <td>指定要使用的连接器, 这里应该是 <code>'oracle-cdc'</code>.</td>
    </tr>
    <tr>
      <td>hostname</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> Oracle 数据库服务器的 IP 地址或主机名。如果 url 不为空，则可以不配置 hostname，否则 hostname 不能为空</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 Oracle 数据库服务器时要使用的 Oracle 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 Oracle 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>database-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 Oracle 服务器的数据库名称。</td>
    </tr>
    <tr>
      <td>schema-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 Oracle 数据库的 schema 名称。</td>
    </tr>
    <tr>
      <td>table-name</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要监视的 Oracle 数据库的表名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1521</td>
      <td>Integer</td>
      <td> Oracle 数据库服务器的整数端口号。</td>
    </tr>
  <tr>
      <td>url</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">jdbc:oracle:thin:@{hostname}:{port}:{database-name}</td>
      <td>String</td>
      <td>Oracle 数据库服务器的 JdbcUrl。如果配置了 hostname 和 port 参数，默认情况下，URL 会按 SID 格式由 hostname、port、database-name 拼接而成。否则，你需要配置 URL 参数</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Oracle CDC 消费者可选的启动模式，
           合法的模式为 "initial"，"latest-offset"，"specific-offset"。 
           请查阅 <a href="#启动模式">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.scn</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在 "specific-offset" 启动模式下使用的可选 SCN</td>
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
                （3）在快照读取之前，Source 不需要获取 ROW SHARE MODE 锁。
          </td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">8096</td>
          <td>Integer</td>
          <td>表快照的分片大小（行数），读取表的快照时，捕获的表被拆分为多个分片。</td>
    </tr>
    <tr>
          <td>scan.snapshot.fetch.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">1024</td>
          <td>Integer</td>
          <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
          <td>connect.max-retries</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">3</td>
          <td>Integer</td>
          <td>连接器应重试以建立 Oracle 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
          <td>connection.pool.size</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">20</td>
          <td>Integer</td>
          <td>连接池大小。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 Oracle 服务器捕获数据更改。
          例如：<code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/oracle.html#oracle-connector-properties"> Debezium 的  Oracle 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束后关闭空闲的 Reader。 <br>
          当 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 设置为 true 时，要求 flink 版本大于等于 1.14。<br>
          如果 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 的默认值已变更为 true，
          因此不需要显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true'
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.metadata.release.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在 source 进入增量阶段后，释放 source coordinator 持有的快照分片元数据（已分配的分片、已完成分片的位点以及表结构），以降低快照分片数量非常大的作业的 JobManager 内存占用。默认关闭。与 scan.newly-added-table.enabled 不兼容：同时开启两者会导致作业启动失败，且已释放元数据的作业无法再开启动态加表功能。仅在成功完成一次 checkpoint 后才会释放；若未开启 checkpoint 或没有 checkpoint 完成，则会保留该元数据，因此该配置项在未开启 checkpoint 时不生效。开启该配置项后生成的 checkpoint 或 savepoint，无法在降级到 Flink CDC 3.6.0 及更早版本后用于恢复作业。</td>
    </tr>
    <tr>
          <td>scan.incremental.snapshot.chunk.key-column</td>
          <td>optional</td>
          <td style="word-wrap: break-word;">(none)</td>
          <td>String</td>
          <td>表快照的分片键，在读取表的快照时，被捕获的表会按分片键拆分为多个分片。
            默认情况下，分片键是 'ROWID'。可以使用非主键列作为分片键，但这可能会导致查询性能下降。
            <br>
            <b>警告：</b> 使用非主键列作为分片键可能会导致数据不一致。请参阅 <a href="#警告">警告</a> 了解详细信息。
          </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.unbounded-chunk-first.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>
        快照读取阶段是否先分配 UnboundedChunk。<br>
        这可能有助于降低 TaskManager 在对最大的 UnboundedChunk 执行快照时出现内存溢出 (OOM) 错误的风险。<br> 
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
        是否在快照读取阶段跳过 backfill 。<br> 
        如果跳过 backfill ，快照阶段捕获表的更改将在稍后的 change log 读取阶段被回放，而不是合并到快照中。<br>
        警告：跳过 backfill 可能会导致数据不一致，因为快照阶段发生的某些 change log 事件可能会被重放（仅保证 at-least-once ）。
        例如，更新快照阶段已更新的值，或删除快照阶段已删除的数据。这些重放的 change log 事件应进行特殊处理。
      </td>
    </tr>
    <tr>
      <td>records.per.second</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Double</td>
      <td>
        The maximum number of records emitted per second, the default value: -1, means no rate limiting.(only for flink2.x)<br>
        WARNING: Incremental/binlog phase: Rate limiting can cause the connector to fall behind the upstream change stream, risking binlog/WAL purging before the connector catches up (data loss for MySQL, replication slot issues for PostgreSQL).
      </td>
    </tr>
    </tbody>
</table>    
</div>

限制
--------

### 扫描表快照期间无法执行 checkpoint
在扫描数据库表快照期间，由于没有可恢复的位点，我们无法执行 checkpoint。为了不执行 checkpoint，Oracle CDC source 会让 checkpoint 一直等待直至超时。超时的 checkpoint 会被视为失败的 checkpoint，默认情况下，这将触发 Flink 作业的故障转移。因此，如果数据库表很大，建议添加以下 Flink 配置，以避免因 checkpoint 超时而触发故障转移：

```
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
```

可用的元数据
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
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的 schema 名称。</td>
    </tr>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>当前记录所属的库名称。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>当前记录表在数据库中更新的时间。 <br>如果从表的快照而不是更改流读取记录，该值将始终为0。</td>
    </tr>
  </tbody>
</table>

下述创建表示例展示元数据列的用法：
```sql
CREATE TABLE products (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    schema_name STRING METADATA FROM 'schema_name' VIRTUAL, 
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    ID INT NOT NULL,
    NAME STRING,
    DESCRIPTION STRING,
    WEIGHT DECIMAL(10, 3),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'oracle-cdc',
    'hostname' = 'localhost',
    'port' = '1521',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'ORCLCDB',
    'schema-name' = 'inventory',
    'table-name' = 'products',
    'debezium.log.mining.strategy' = 'online_catalog',
    'debezium.log.mining.continuous.mine' = 'true'
);
```

**注意** : Oracle 方言是大小写敏感的，如果字段名没有加引号，它会将字段名转换为大写，而 Flink SQL 不会转换字段名。因此，对于来自 Oracle 数据库的物理列，在 Flink SQL 中定义 `oracle-cdc` 表时，应该使用其在 Oracle 中转换后的字段名。

特性
--------

### Exactly-Once 处理

Oracle CDC 连接器是一个 Flink Source 连接器，它将首先读取数据库快照，然后继续读取变更事件，即使在处理时出现故障，也能**准确读取数据**。请参阅 [How the connector works](https://debezium.io/documentation/reference/1.9/connectors/oracle.html#how-the-oracle-connector-works)。

### 启动模式

配置选项`scan.startup.mode`指定 Oracle CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 redo log。
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照，仅读取
  连接器启动之后的数据更改。
- `specific-offset`：跳过快照阶段，使用 scn 从特定位点开始读取 redo log。

_注意：`scan.startup.mode` 选项的机制依赖于 Debezium 的 `snapshot.mode` 配置，因此请不要同时使用它们。如果在表 DDL 中同时指定 `scan.startup.mode` 和 `debezium.snapshot.mode` 选项，可能会导致 `scan.startup.mode` 失效。_

### 单线程读取

Oracle CDC source 无法以并行读取的方式工作，因为只有一个任务可以接收变更事件。

### 动态加表

**注意:** 该功能从 Flink CDC 3.1.0 版本开始支持。

动态加表功能使你可以为正在运行的作业添加新表进行监控。新添加的表将首先读取其快照数据,然后自动读取其 redo log。

想象一下这个场景:一开始,Flink 作业监控表 `[product, user, address]`,但几天后,我们希望这个作业还可以监控表 `[order, custom]`,这些表包含历史数据,我们需要作业仍然可以复用作业的已有状态。动态加表功能可以优雅地解决此问题。

以下操作显示了如何启用此功能来解决上述场景。使用现有的 Oracle CDC Source 作业,如下:

```java
    JdbcIncrementalSource<String> oracleSource = new OracleSourceBuilder()
        .hostname("yourHostname")
        .port(1521)
        .databaseList("ORCLCDB") // 设置捕获的数据库
        .schemaList("INVENTORY") // 设置捕获的 schema
        .tableList("INVENTORY.PRODUCT", "INVENTORY.USER", "INVENTORY.ADDRESS") // 设置捕获的表
        .username("yourUsername")
        .password("yourPassword")
        .scanNewlyAddedTableEnabled(true) // 启用扫描新添加的表功能
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();
   // 你的业务代码
```

如果我们想添加新表 `[INVENTORY.ORDER, INVENTORY.CUSTOM]` 到现有的 Flink 作业,只需更新作业的 `tableList()` 将新增表 `[INVENTORY.ORDER, INVENTORY.CUSTOM]` 加入并从已有的 savepoint 恢复作业。

_Step 1_: 使用 savepoint 停止现有的 Flink 作业。
```shell
$ ./bin/flink stop $Existing_Flink_JOB_ID
```
```shell
Suspending job "cca7bc1061d61cf15238e92312c2fc20" with a savepoint.
Savepoint completed. Path: file:/tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab
```
_Step 2_: 更新现有 Flink 作业的表列表选项。
1. 更新 `tableList()` 参数。
2. 编译更新后的作业,示例如下:
```java
    JdbcIncrementalSource<String> oracleSource = new OracleSourceBuilder()
        .hostname("yourHostname")
        .port(1521)
        .databaseList("ORCLCDB")
        .schemaList("INVENTORY")
        .tableList("INVENTORY.PRODUCT", "INVENTORY.USER", "INVENTORY.ADDRESS", "INVENTORY.ORDER", "INVENTORY.CUSTOM") // 设置捕获的表 [PRODUCT, USER, ADDRESS, ORDER, CUSTOM]
        .username("yourUsername")
        .password("yourPassword")
        .scanNewlyAddedTableEnabled(true)
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();
   // 你的业务代码
```
_Step 3_: 从 savepoint 还原更新后的 Flink 作业。
```shell
$ ./bin/flink run \
      --detached \
      --from-savepoint /tmp/flink-savepoints/savepoint-cca7bc-bb1e257f0dab \
      ./FlinkCDCExample.jar
```
**注意:** 请参考文档 [Restore the job from previous savepoint](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/#command-line-interface) 了解更多详细信息。

### DataStream Source

Oracle CDC 连接器也可以是一个数据流源。DataStream 源有两种模式：

- 基于增量快照，允许并行读取
- 基于 SourceFunction，仅支持单线程读取

#### 基于增量快照的 DataStream（实验性）

```java
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class OracleParallelSourceExample {

    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");

        JdbcIncrementalSource<String> oracleChangeEventSource =
                new OracleSourceBuilder()
                        .hostname("host")
                        .port(1521)
                        .databaseList("ORCLCDB")
                        .schemaList("DEBEZIUM")
                        .tableList("DEBEZIUM.PRODUCTS")
                        .username("username")
                        .password("password")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .splitSize(2)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000L);
        // set the source parallelism to 4
        env.fromSource(
                        oracleChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "OracleParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);
        env.execute("Print Oracle Snapshot + RedoLog");
    }
}
```

#### 基于 SourceFunction 的 DataStream

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.oracle.OracleSource;

public class OracleSourceExample {
  public static void main(String[] args) throws Exception {
     SourceFunction<String> sourceFunction = OracleSource.<String>builder()
             .url("jdbc:oracle:thin:@{hostname}:{port}:{database}")
             .port(1521)
             .database("ORCLCDB") // monitor XE database
             .schemaList("inventory") // monitor inventory schema
             .tableList("inventory.products") // monitor products table
             .username("flinkuser")
             .password("flinkpw")
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

### 可用的指标

指标系统能够帮助了解分片分发的进展， 下面列举出了支持的 Flink 指标 [Flink metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/):

| Group                  | Name                       | Type  | Description    |
|------------------------|----------------------------|-------|----------------|
| namespace.schema.table | isSnapshotting             | Gauge | 表是否在快照读取阶段     |     
| namespace.schema.table | isStreamReading            | Gauge | 表是否在增量读取阶段     |
| namespace.schema.table | numTablesSnapshotted       | Gauge | 已经被快照读取完成的表的数量 |
| namespace.schema.table | numTablesRemaining         | Gauge | 还没有被快照读取的表的数据  |
| namespace.schema.table | numSnapshotSplitsProcessed | Gauge | 正在处理的分片的数量     |
| namespace.schema.table | numSnapshotSplitsRemaining | Gauge | 还没有被处理的分片的数量   |
| namespace.schema.table | numSnapshotSplitsFinished  | Gauge | 已经处理完成的分片的数据   |
| namespace.schema.table | snapshotStartTime          | Gauge | 快照读取阶段开始的时间    |
| namespace.schema.table | snapshotEndTime            | Gauge | 快照读取阶段结束的时间    |

注意:
1. Group 名称是 `namespace.schema.table`，这里的 `namespace` 是实际的数据库名称， `schema` 是实际的 schema 名称， `table` 是实际的表名称。
2. 对于 Oracle，Group 的名称会类似于 `test_database.test_schema.test_table`。

### 关于无主键表

从3.4.0 版本开始支持无主键表，使用无主键表必须设置 `scan.incremental.snapshot.chunk.key-column`，且只能选择非空类型的一个字段。

在使用无主键表时，需要注意以下两种情况。

1. 配置 `scan.incremental.snapshot.chunk.key-column` 时，如果表中存在索引，请尽量使用索引中的列来加快 select 速度。
2. 无主键表的处理语义由 `scan.incremental.snapshot.chunk.key-column` 指定的列的行为决定：
* 如果指定的列不存在更新操作，此时可以保证 Exactly once 语义。
* 如果指定的列存在更新操作，此时只能保证 At least once 语义。但可以结合下游，通过指定下游主键，结合幂等性操作来保证数据的正确性。

#### 警告

在 Oracle 表中，若使用 **非主键列** 作为有主键表的 `scan.incremental.snapshot.chunk.key-column`，可能导致**数据不一致**。以下为可能出现的问题及其缓解方案。

#### 问题场景

- **表结构：**
   - **主键：** `id`
   - **分片键列 ：** `pid`（非主键）

- **快照分片 ：**
   - **分片 0:** `1 < pid <= 3`
   - **分片 1:** `3 < pid <= 5`

- **操作 ：**
   - 两个子任务并行读取 **分片 0** 和 **分片 1**。
   - 在读取过程中，发生了一次 **更新** 操作，使 `id=0` 的 `pid` 从 `2` 变为 `4`，在两个分片的**高低水位**间都包含此次变更，导致该更新操作在增量阶段不会被处理。

- **结果 ：**
   - **分片 0:** 记录 `[id=0, pid=2]`
   - **分片 1:** 记录 `[id=0, pid=4]`

由于**处理顺序**无法保证，最终 `id=0` 的 `pid` 可能为 `2` 或 `4`，从而导致数据不一致。

数据类型映射
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left"><a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html">Oracle type</a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 3
      </td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 5
      </td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 10
      </td>
      <td>INT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s < 19
      </td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), 19 <= p - s <= 38 <br>
      </td>
      <td>DECIMAL(p - s, 0)</td>
    </tr>
    <tr>
      <td>NUMBER(p, s > 0)
      </td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>NUMBER(p, s <= 0), p - s > 38
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td> 
        FLOAT<br>
        BINARY_FLOAT
      </td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>
        DOUBLE PRECISION<br>
        BINARY_DOUBLE
      </td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>NUMBER(1)</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>
        DATE<br>
        TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] WITH TIME ZONE</td>
      <td>TIMESTAMP [(p)] WITH TIME ZONE</td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)] WITH LOCAL TIME ZONE</td>
      <td>TIMESTAMP_LTZ [(p)]</td>
    </tr>
    <tr>
      <td>
        CHAR(n)<br>
        NCHAR(n)<br>
        NVARCHAR2(n)<br>
        VARCHAR(n)<br>
        VARCHAR2(n)<br>
        CLOB<br>
        NCLOB<br>
        XMLType<br>
        SYS.XMLTYPE
      </td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BLOB<br>
      ROWID
      </td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>
      INTERVAL DAY TO SECOND<br>
      INTERVAL YEAR TO MONTH
      </td>
      <td>BIGINT</td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}
