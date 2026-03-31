---
title: "Postgres to Fluss"
weight: 5
type: docs
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

# Streaming ELT from Postgres to Fluss

This tutorial shows how to quickly build a Streaming ELT job from PostgreSQL to Fluss using Flink CDC, including
full-database synchronization and schema change evolution.
All exercises in this tutorial are performed in the Flink CDC CLI, and the entire process uses standard SQL syntax,
without a single line of Java/Scala code or IDE installation.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.

### Prepare Flink Standalone cluster
1. Download [Flink 1.20.3](https://archive.apache.org/dist/flink/flink-1.20.3/flink-1.20.3-bin-scala_2.12.tgz), unzip and get flink-1.20.3 directory.
   Use the following command to navigate to the Flink directory and set FLINK_HOME to the directory where flink-1.20.3 is located.

   ```shell
   cd flink-1.20.3
   ```

2. Enable checkpointing by appending the following parameters to the conf/config.yaml configuration file to perform a checkpoint every 3 seconds.

   ```yaml
   execution:
     checkpointing:
       interval: 3s
   ```

3. Start the Flink cluster using the following command.

   ```shell
   ./bin/start-cluster.sh
   ```  

If successfully started, you can access the Flink Web UI at [http://localhost:8081/](http://localhost:8081/).

Executing `start-cluster.sh` multiple times can start multiple `TaskManager`s.

### Prepare docker compose
The following tutorial will prepare the required components using `docker-compose`.

Create a `docker-compose.yml` file using the content provided below:

   ```yaml
   services:
     # Fluss cluster
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

The Docker Compose includes the following services:
- **Fluss** (coordinator-server, tablet-server, zookeeper): the target data lakehouse
- **PostgreSQL**: the source database with logical replication enabled (`wal_level=logical`)

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.

   ```shell
   docker-compose up -d
   ```

This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run `docker ps` to check whether these containers are running properly.

#### Prepare records for PostgreSQL
1. Connect to the PostgreSQL database

   ```shell
   psql -h localhost -p 5432 -U root postgres
   ```
   The password is: `password`

2. Create the `adb` database and switch to it

   ```sql
   CREATE DATABASE adb;
   \c adb
   ```

3. Create schemas and tables, then insert data

    ```sql
    -- Create schemas
    CREATE SCHEMA hr;
    CREATE SCHEMA sales;
   
    -- Create tables
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
   
    -- Insert data
    INSERT INTO hr.employees (ID, NAME, AGE, ADDRESS, SALARY)
    VALUES (1, 'Paul', 32, 'California', 20000.00);
   
    INSERT INTO sales.orders (ID, PRODUCT, QUANTITY, REGION, AMOUNT)
    VALUES (1, 'Laptop', 5, 'East', 49999.50);
    ```

## Submit job with Flink CDC CLI
1. Download the binary compressed packages listed below and extract them to the directory `flink-cdc-{{< param Version >}}`：    
   [flink-cdc-{{< param Version >}}-bin.tar.gz](https://www.apache.org/dyn/closer.lua/flink/flink-cdc-{{< param Version >}}/flink-cdc-{{< param Version >}}-bin.tar.gz)
   flink-cdc-{{< param Version >}} directory will contain four directories: `bin`, `lib`, `log`, and `conf`.

2. Download the connector packages listed below and move them to the `lib` directory    
   **Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release branches by yourself.**
   **Please note that you need to move the jar to the lib directory of Flink CDC Home, not to the lib directory of Flink Home.**
    - [flink-cdc-pipeline-connector-postgres](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-postgres)
    - [flink-cdc-pipeline-connector-fluss](https://mvnrepository.com/artifact/org.apache.flink/flink-cdc-pipeline-connector-fluss)

3. Write task configuration yaml file.
   Here is an example file for synchronizing the entire database `postgres-to-fluss.yaml`：

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
   
   Notice that:
   - `tables: adb.\.*.\.*` in source synchronizes all tables across all schemas in the `adb` database through regular expression matching. All tables must belong to the same database.
   - `schema-change.enabled: true` enables schema change inference based on pgoutput Relation messages. Requires `decoding.plugin.name` to be set to `pgoutput`.
   - `schema.change.behavior: LENIENT` must be explicitly set, otherwise it may be overridden by the default `conf.yaml` configuration.

4. Finally, submit the job to the Flink Standalone cluster using CLI.
   ```shell
   bash bin/flink-cdc.sh postgres-to-fluss.yaml
   ```
   After successful submission, the return information is as follows：
   ```shell
   Pipeline has been submitted to cluster.
   Job ID: ae30f4580f1918bebf16752d4963dc54
   Job Description: Postgres to Fluss Pipeline
   ```
   You can find a job named `Postgres to Fluss Pipeline` running through the Flink Web UI.

### Query data in Fluss
To query the synchronized data in Fluss, you need to set up the Flink SQL Client.

1. Download [fluss-flink-1.20-0.9.0-incubating.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.9.0-incubating/fluss-flink-1.20-0.9.0-incubating.jar) and place it in the Flink `lib` directory.

2. Start the Flink SQL Client:
   ```shell
   bin/sql-client.sh
   ```

3. Create a Fluss catalog and query data:
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

4. Query the synchronized table:
   ```sql
   SELECT * FROM `developer_catalog`.`hr`.`employees` LIMIT 20;
    +----+------+-----+--------------------------------+---------+
    | id | name | age |                        address |  salary |
    +----+------+-----+--------------------------------+---------+
    |  1 | Paul |  32 | California                 ... | 20000.0 |
    +----+------+-----+--------------------------------+---------+
   ```

### Synchronize Schema Changes
PostgreSQL schema changes are **data-driven** — a DDL change will not be captured until the next DML message triggers a Relation message from the pgoutput plugin.

Connect to the PostgreSQL database:

   ```shell
   psql -h localhost -p 5432 -U root adb
   ```

#### Add columns
Add new columns and insert data on the PostgreSQL side:

   ```sql
   ALTER TABLE hr.employees
   ADD COLUMN EMAIL TEXT,
   ADD COLUMN DEPARTMENT TEXT;
   
   INSERT INTO hr.employees (ID, NAME, AGE, ADDRESS, SALARY, EMAIL, DEPARTMENT) VALUES
   (4, 'David', 32, 'Guangzhou', 8000.0, 'david@example.com', 'IT'),
   (5, 'Eva', 27, 'Hangzhou', 7100.0, 'eva@example.com', 'HR');
   ```

Query Fluss — the new columns have been created, and existing rows show `NULL` for the new columns:

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

#### Drop column
Drop a column and insert data on the PostgreSQL side:

   ```sql
   ALTER TABLE hr.employees
   DROP COLUMN ADDRESS;
   
   INSERT INTO hr.employees (ID, NAME, AGE, SALARY, EMAIL, DEPARTMENT) VALUES
   (6, 'Frank', 35, 9000.0, 'frank@example.com', 'Finance'),
   (7, 'Grace', 29, 7600.0, 'grace@example.com', 'Marketing');
   ```

Query Fluss:

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

#### Rename column
Rename a column and insert data on the PostgreSQL side:

   ```sql
   ALTER TABLE hr.employees
   RENAME COLUMN EMAIL TO WORK_EMAIL;
   
   INSERT INTO hr.employees (ID, NAME, AGE, SALARY, WORK_EMAIL, DEPARTMENT) VALUES
   (8, 'Henry', 31, 8800.0, 'henry@example.com', 'Sales'),
   (9, 'Ivy', 26, 6900.0, 'ivy@example.com', 'Support');
   ```

Query Fluss to see the renamed column:

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

## Clean up
After finishing the tutorial, run the following command to stop all containers in the directory of `docker-compose.yml`:

   ```shell
   docker-compose down -v
   ```
Run the following command to stop the Flink cluster in the directory of Flink `flink-1.20.3`:

   ```shell
   ./bin/stop-cluster.sh
   ```

{{< top >}}
