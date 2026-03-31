---
title: "Postgres 同步到 Fluss"
weight: 5
type: docs
aliases:
- /get-started/quickstart/postgres-to-fluss
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

# Streaming ELT 同步 Postgres 到 Fluss

这篇教程将展示如何基于 Flink CDC 快速构建 PostgreSQL 到 Fluss 的 Streaming ELT 作业，包含整库同步和表结构变更同步的功能。
本教程的演示都将在 Flink CDC CLI 中进行，无需一行 Java/Scala 代码，也无需安装 IDE。

## 准备阶段
准备一台已经安装了 Docker 的 Linux 或者 MacOS 电脑。

### 准备 Flink Standalone 集群
1. 下载 [Flink 2.2.0](https://archive.apache.org/dist/flink/flink-2.2.0/flink-2.2.0-bin-scala_2.12.tgz)，解压后得到 flink-2.2.0 目录。
   使用下面的命令跳转至 Flink 目录下，并且设置 FLINK_HOME 为 flink-2.2.0 所在目录。

   ```shell
   cd flink-2.2.0
   ```

2. 通过在 conf/config.yaml 配置文件追加下列参数开启 checkpoint，每隔 3 秒做一次 checkpoint。

   ```yaml
   execution:
     checkpointing:
       interval: 3s
   ```

3. 使用下面的命令启动 Flink 集群。

   ```shell
   ./bin/start-cluster.sh
   ```  

启动成功的话，可以在 [http://localhost:8081/](http://localhost:8081/) 访问到 Flink Web UI。

多次执行 `start-cluster.sh` 可以拉起多个 TaskManager。

### 准备 Docker 环境
接下来的教程将以 `docker-compose` 的方式准备所需要的组件。

使用下面的内容创建一个 `docker-compose.yml` 文件：

   ```yaml
   services:
     # Fluss 集群
     coordinator-server:
       image: apache/fluss:0.9.0-incubating
       command: coordinatorServer
       depends_on:
         - zookeeper
       environment:
         - |
           FLUSS_PROPERTIES=
           zookeeper.address: zookeeper:2181
           bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
           advertised.listeners: CLIENT://localhost:9123
           internal.listener.name: INTERNAL
           remote.data.dir: /tmp/fluss/remote-data
           security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
           security.sasl.enabled.mechanisms: PLAIN
           security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" ;
           super.users: User:admin
       ports:
         - "9123:9123"
     tablet-server:
       image: apache/fluss:0.9.0-incubating
       command: tabletServer
       depends_on:
         - coordinator-server
       environment:
         - |
           FLUSS_PROPERTIES=
           zookeeper.address: zookeeper:2181
           bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
           advertised.listeners: CLIENT://localhost:9124
           internal.listener.name: INTERNAL
           tablet-server.id: 0
           kv.snapshot.interval: 0s
           data.dir: /tmp/fluss/data
           remote.data.dir: /tmp/fluss/remote-data
           security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
           security.sasl.enabled.mechanisms: PLAIN
           security.sasl.plain.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_developer="developer-pass" ;
           super.users: User:admin
       ports:
         - "9124:9123"
     zookeeper:
       restart: always
       image: zookeeper:3.9.2
     # PostgreSQL
     postgres:
       image: postgres:14.5
       environment:
         POSTGRES_USER: root
         POSTGRES_PASSWORD: password
         POSTGRES_DB: postgres
       ports:
         - "5432:5432"
       volumes:
         - postgres_data:/var/lib/postgresql/data
       command:
         - "postgres"
         - "-c"
         - "wal_level=logical"
         - "-c"
         - "max_replication_slots=5"
         - "-c"
         - "max_wal_senders=5"
         - "-c"
         - "hot_standby=on"
   volumes:
     postgres_data:
   ```

该 Docker Compose 中包含的容器有：
- **Fluss**（coordinator-server, tablet-server, zookeeper）：目标数据湖仓
- **PostgreSQL**：源数据库，已开启逻辑复制（`wal_level=logical`）

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：

   ```shell
   docker-compose up -d
   ```

该命令将以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。你可以通过 `docker ps` 来观察上述的容器是否正常启动了。

#### 在 PostgreSQL 数据库中准备数据
1. 连接 PostgreSQL 数据库

   ```shell
   psql -h localhost -p 5432 -U root postgres
   ```
   密码为：`password`

2. 创建 `adb` 数据库并切换

   ```sql
   CREATE DATABASE adb;
   \c adb
   ```

3. 创建 Schema 和表，并插入数据

    ```sql
    -- 创建 Schema
    CREATE SCHEMA hr;
    CREATE SCHEMA sales;
   
    -- 创建表
    CREATE TABLE hr.employees(
       ID INT PRIMARY KEY NOT NULL,
       NAME TEXT NOT NULL,
       AGE INT NOT NULL,
       ADDRESS CHAR(50),
       SALARY REAL
    );
   
    CREATE TABLE sales.orders(
       ID INT PRIMARY KEY NOT NULL,
       PRODUCT TEXT NOT NULL,
       QUANTITY INT NOT NULL,
       REGION CHAR(50),
       AMOUNT REAL
    );
   
    -- 插入数据
    INSERT INTO hr.employees (ID, NAME, AGE, ADDRESS, SALARY)
    VALUES (1, 'Paul', 32, 'California', 20000.00);
   
    INSERT INTO sales.orders (ID, PRODUCT, QUANTITY, REGION, AMOUNT)
    VALUES (1, 'Laptop', 5, 'East', 49999.50);
    ```

## 通过 Flink CDC CLI 提交任务
1. 下载下面列出的二进制压缩包，并解压得到目录 `flink-cdc-{{< param Version >}}`；
   [flink-cdc-{{< param Version >}}-bin.tar.gz](https://www.apache.org/dyn/closer.lua/flink/flink-cdc-{{< param Version >}}/flink-cdc-{{< param Version >}}-bin.tar.gz)
   flink-cdc-{{< param Version >}} 下会包含 `bin`、`lib`、`log`、`conf` 四个目录。

2. 下载下面列出的 connector 包，并且移动到 `lib` 目录下；
   **下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地基于 master 或 release- 分支编译。**
   **请注意，您需要将 jar 移动到 Flink CDC Home 的 lib 目录，而非 Flink Home 的 lib 目录下。**
   - [flink-cdc-pipeline-connector-postgres](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-postgres)
   - [flink-cdc-pipeline-connector-fluss](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-fluss)

3. 编写任务配置 yaml 文件。
   下面给出了一个整库同步的示例文件 `postgres-to-fluss.yaml`：

   ```yaml
   ################################################################################
   # Description: Sync Postgres all tables to Fluss
   ################################################################################
   source:
     type: postgres
     hostname: localhost
     port: 5432
     username: root
     password: password
     tables: adb.\.*.\.*
     decoding.plugin.name: pgoutput
     slot.name: pgtest
     schema-change.enabled: true
   
   sink:
     type: fluss
     bootstrap.servers: localhost:9123
     properties.client.security.protocol: sasl
     properties.client.security.sasl.mechanism: PLAIN
     properties.client.security.sasl.username: developer
     properties.client.security.sasl.password: developer-pass
   
   pipeline:
     name: Postgres to Fluss Pipeline
     parallelism: 2
     schema.change.behavior: LENIENT
   
   ```

   其中：
   - source 中的 `tables: adb.\.*.\.*` 通过正则匹配同步 `adb` 数据库下所有 Schema 的所有表。所有表必须属于同一个数据库。
   - `schema-change.enabled: true` 开启基于 pgoutput Relation 消息的 Schema 变更推导。需要 `decoding.plugin.name` 设置为 `pgoutput`。
   - `schema.change.behavior: LENIENT` 必须显式设置，否则可能会被默认的 `conf.yaml` 配置覆盖。

4. 最后，通过命令行提交任务到 Flink Standalone cluster
   ```shell
   bash bin/flink-cdc.sh postgres-to-fluss.yaml
   ```
   提交成功后，返回信息如：
   ```shell
   Pipeline has been submitted to cluster.
   Job ID: ae30f4580f1918bebf16752d4963dc54
   Job Description: Postgres to Fluss Pipeline
   ```
   在 Flink Web UI，可以看到一个名为 `Postgres to Fluss Pipeline` 的任务正在运行。

### 在 Fluss 中查询数据
要查询已同步到 Fluss 的数据，需要配置 Flink SQL Client。

1. 下载 [fluss-flink-2.2-0.9.0-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-2.2/0.9.0-incubating/fluss-flink-2.2-0.9.0-incubating.jar) 并放入 Flink 的 `lib` 目录。

2. 启动 Flink SQL Client：
   ```shell
   bin/sql-client.sh
   ```

3. 创建 Fluss Catalog 并查询数据：
   ```sql
   SET 'execution.runtime-mode' = 'batch';
   SET 'sql-client.execution.result-mode' = 'tableau';
   
   CREATE CATALOG developer_catalog WITH (
       'type' = 'fluss',
       'bootstrap.servers' = 'localhost:9123',
       'client.security.protocol' = 'SASL',
       'client.security.sasl.mechanism' = 'PLAIN',
       'client.security.sasl.username' = 'developer',
       'client.security.sasl.password' = 'developer-pass'
   );
   
   USE CATALOG developer_catalog;
   SHOW DATABASES;
   +---------------+
   | database name |
   +---------------+
   |         fluss |
   |            hr |
   |         sales |
   +---------------+
   ```

4. 查询已同步的表：
   ```sql
   SELECT * FROM `developer_catalog`.`hr`.`employees` LIMIT 20;
    +----+------+-----+--------------------------------+---------+
    | id | name | age |                        address |  salary |
    +----+------+-----+--------------------------------+---------+
    |  1 | Paul |  32 | California                 ... | 20000.0 |
    +----+------+-----+--------------------------------+---------+
   ```

### 同步表结构变更
PostgreSQL 的表结构变更是**数据驱动**的 — DDL 变更不会立即被捕获，只有当下一个 DML 消息触发 pgoutput 插件发送 Relation 消息时才会被识别。

连接 PostgreSQL 数据库：

   ```shell
   psql -h localhost -p 5432 -U root adb
   ```

#### 新增列
在 PostgreSQL 侧新增列并插入数据：

   ```sql
   ALTER TABLE hr.employees
   ADD COLUMN EMAIL TEXT,
   ADD COLUMN DEPARTMENT TEXT;
   
   INSERT INTO hr.employees (ID, NAME, AGE, ADDRESS, SALARY, EMAIL, DEPARTMENT) VALUES
   (4, 'David', 32, 'Guangzhou', 8000.0, 'david@example.com', 'IT'),
   (5, 'Eva', 27, 'Hangzhou', 7100.0, 'eva@example.com', 'HR');
   ```

查询 Fluss，可以发现新的列已经创建了，而且存量数据对应列为 NULL：

   ```sql
   SELECT * FROM `developer_catalog`.`hr`.`employees` LIMIT 20;
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
    | id |  name | age |                        address |  salary |             email | department |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
    |  1 |  Paul |  32 | California                 ... | 20000.0 |            <NULL> |     <NULL> |
    |  4 | David |  32 | Guangzhou                  ... |  8000.0 | david@example.com |         IT |
    |  5 |   Eva |  27 | Hangzhou                   ... |  7100.0 |   eva@example.com |         HR |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
   ```

#### 删除列
在 PostgreSQL 侧删除列并插入数据：

   ```sql
   ALTER TABLE hr.employees
   DROP COLUMN ADDRESS;
   
   INSERT INTO hr.employees (ID, NAME, AGE, SALARY, EMAIL, DEPARTMENT) VALUES
   (6, 'Frank', 35, 9000.0, 'frank@example.com', 'Finance'),
   (7, 'Grace', 29, 7600.0, 'grace@example.com', 'Marketing');
   ```

查询 Fluss：

   ```sql
   SELECT * FROM `developer_catalog`.`hr`.`employees` LIMIT 20;
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
    | id |  name | age |                        address |  salary |             email | department |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
    |  1 |  Paul |  32 | California                 ... | 20000.0 |            <NULL> |     <NULL> |
    |  4 | David |  32 | Guangzhou                  ... |  8000.0 | david@example.com |         IT |
    |  5 |   Eva |  27 | Hangzhou                   ... |  7100.0 |   eva@example.com |         HR |
    |  6 | Frank |  35 |                         <NULL> |  9000.0 | frank@example.com |    Finance |
    |  7 | Grace |  29 |                         <NULL> |  7600.0 | grace@example.com |  Marketing |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+
   ```

#### 重命名列
在 PostgreSQL 侧重命名列并插入数据：

   ```sql
   ALTER TABLE hr.employees
   RENAME COLUMN EMAIL TO WORK_EMAIL;
   
   INSERT INTO hr.employees (ID, NAME, AGE, SALARY, WORK_EMAIL, DEPARTMENT) VALUES
   (8, 'Henry', 31, 8800.0, 'henry@example.com', 'Sales'),
   (9, 'Ivy', 26, 6900.0, 'ivy@example.com', 'Support');
   ```

查询 Fluss 查看重命名后的列：

   ```sql
   SELECT * FROM `developer_catalog`.`hr`.`employees` LIMIT 20;
    +----+-------+-----+--------------------------------+---------+-------------------+------------+-------------------+
    | id |  name | age |                        address |  salary |             email | department |        work_email |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+-------------------+
    |  1 |  Paul |  32 | California                 ... | 20000.0 |            <NULL> |     <NULL> |            <NULL> |
    |  4 | David |  32 | Guangzhou                  ... |  8000.0 | david@example.com |         IT |            <NULL> |
    |  5 |   Eva |  27 | Hangzhou                   ... |  7100.0 |   eva@example.com |         HR |            <NULL> |
    |  6 | Frank |  35 |                         <NULL> |  9000.0 | frank@example.com |    Finance |            <NULL> |
    |  7 | Grace |  29 |                         <NULL> |  7600.0 | grace@example.com |  Marketing |            <NULL> |
    |  8 | Henry |  31 |                         <NULL> |  8800.0 |            <NULL> |      Sales | henry@example.com |
    |  9 |   Ivy |  26 |                         <NULL> |  6900.0 |            <NULL> |    Support |   ivy@example.com |
    +----+-------+-----+--------------------------------+---------+-------------------+------------+-------------------+
   ```

## 环境清理
本教程结束后，在 `docker-compose.yml` 文件所在的目录下执行如下命令停止所有容器：

   ```shell
   docker-compose down -v
   ```
在 Flink 所在目录 `flink-2.2.0` 下执行如下命令停止 Flink 集群：

   ```shell
   ./bin/stop-cluster.sh
   ```

{{< top >}}
