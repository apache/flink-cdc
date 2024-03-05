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

# Streaming ELT from MySQL to StarRocks using Flink CDC 3.0

This tutorial is to show how to quickly build a Streaming ELT job from MySQL to StarRocks using Flink CDC 3.0，including the feature of sync all table of one database, schema change evolution and sync sharding tables into one table.  
All exercises in this tutorial are performed in the Flink CDC CLI, and the entire process uses standard SQL syntax, without a single line of Java/Scala code or IDE installation.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.

### Prepare Flink Standalone cluster
1. Download [Flink 1.18.0](https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz) ，unzip and get flink-1.18.0 directory.   
   Use the following command to navigate to the Flink directory and set FLINK_HOME to the directory where flink-1.18.0 is located.

   ```shell
   cd flink-1.18.0
   ```

2. Enable checkpointing by appending the following parameters to the conf/flink-conf.yaml configuration file to perform a checkpoint every 3 seconds.

   ```yaml
   execution.checkpointing.interval: 3000
   ```

3. Start the Flink cluster using the following command.

   ```shell
   ./bin/start-cluster.sh
   ```  

If successfully started, you can access the Flink Web UI at [http://localhost:8081/](http://localhost:8081/), as shown below.

![Flink UI](/_static/fig/mysql-starrocks-tutorial/flink-ui.png "Flink UI")

Executing `start-cluster.sh` multiple times can start multiple `TaskManager`s.

### Prepare docker compose
The following tutorial will prepare the required components using `docker-compose`.
Create a `docker-compose.yml` file using the content provided below:

   ```yaml
   version: '2.1'
   services:
      StarRocks:
         image: registry.starrocks.io/starrocks/allin1-ubuntu
         ports:
            - "8030:8030"
            - "8040:8040"
            - "9030:9030"
      MySQL:
         image: debezium/example-mysql:1.1
         ports:
            - "3306:3306"
         environment:
            - MYSQL_ROOT_PASSWORD=123456
            - MYSQL_USER=mysqluser
            - MYSQL_PASSWORD=mysqlpw
   ```

The Docker Compose should include the following services (containers):
- MySQL: include a database named `app_db`
- StarRocks: to store tables from MySQL

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.

   ```shell
   docker-compose up -d
   ```

This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run docker ps to check whether these containers are running properly. You can also visit [http://localhost:8030/](http://localhost:8030/) to check whether StarRocks is running.
#### Prepare records for MySQL
1. Enter MySQL container

   ```shell
   docker-compose exec mysql mysql -uroot -p123456
   ```

2. create `app_db` database and `orders`,`products`,`shipments` tables, then insert records

    ```sql
    -- create database
    CREATE DATABASE app_db;
   
    USE app_db;
   
   -- create orders table
   CREATE TABLE `orders` (
   `id` INT NOT NULL,
   `price` DECIMAL(10,2) NOT NULL,
   PRIMARY KEY (`id`)
   );
   
   -- insert records
   INSERT INTO `orders` (`id`, `price`) VALUES (1, 4.00);
   INSERT INTO `orders` (`id`, `price`) VALUES (2, 100.00);
   
   -- create shipments table
   CREATE TABLE `shipments` (
   `id` INT NOT NULL,
   `city` VARCHAR(255) NOT NULL,
   PRIMARY KEY (`id`)
   );
   
   -- insert records
   INSERT INTO `shipments` (`id`, `city`) VALUES (1, 'beijing');
   INSERT INTO `shipments` (`id`, `city`) VALUES (2, 'xian');
   
   -- create products table
   CREATE TABLE `products` (
   `id` INT NOT NULL,
   `product` VARCHAR(255) NOT NULL,
   PRIMARY KEY (`id`)
   );
   
   -- insert records
   INSERT INTO `products` (`id`, `product`) VALUES (1, 'Beer');
   INSERT INTO `products` (`id`, `product`) VALUES (2, 'Cap');
   INSERT INTO `products` (`id`, `product`) VALUES (3, 'Peanut');
    ```
   
## Submit job using FlinkCDC cli
1. Download the binary compressed packages listed below and extract them to the directory ` flink cdc-3.0.0 '`：    
   [flink-cdc-3.0.0-bin.tar.gz](https://github.com/ververica/flink-cdc-connectors/releases/download/release-3.0.0/flink-cdc-3.0.0-bin.tar.gz)
   flink-cdc-3.0.0 directory will contain four directory `bin`,`lib`,`log`,`conf`.

2. Download the connector package listed below and move it to the `lib` directory  
   **Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself.**
    - [MySQL pipeline connector 3.0.0](https://repo1.maven.org/maven2/com/ververica/flink-cdc-pipeline-connector-mysql/3.0.0/flink-cdc-pipeline-connector-mysql-3.0.0.jar)
    - [StarRocks pipeline connector 3.0.0](https://repo1.maven.org/maven2/com/ververica/flink-cdc-pipeline-connector-starrocks/3.0.0/flink-cdc-pipeline-connector-starrocks-3.0.0.jar)

3. Write task configuration yaml file.
   Here is an example file for synchronizing the entire database `mysql-to-starrocks.yaml`：

   ```yaml
   ################################################################################
   # Description: Sync MySQL all tables to StarRocks
   ################################################################################
   source:
     type: mysql
     hostname: localhost
     port: 3306
     username: root
     password: 123456
     tables: app_db.\.*
     server-id: 5400-5404
     server-time-zone: UTC
   
   sink:
     type: starrocks
     name: StarRocks Sink
     jdbc-url: jdbc:mysql://127.0.0.1:9030
     load-url: 127.0.0.1:8030
     username: root
     password: ""
     table.create.properties.replication_num: 1
   
   pipeline:
     name: Sync MySQL Database to StarRocks
     parallelism: 2
   
   ```

Notice that:  
* `tables: app_db.\.*` in source synchronize all tables in `app_db` through Regular Matching.   
* `table.create.properties.replication_num` in sink is because there is only one StarRocks BE node in the Docker image.

4. Finally, submit job to Flink Standalone cluster using Cli.

   ```shell
   bash bin/flink-cdc.sh mysql-to-starrocks.yaml
   ```
   
After successful submission, the return information is as follows：

   ```shell
   Pipeline has been submitted to cluster.
   Job ID: 02a31c92f0e7bc9a1f4c0051980088a0
   Job Description: Sync MySQL Database to StarRocks
   ```

We can find a job  named `Sync MySQL Database to StarRocks` is running through Flink Web UI.

![MySQL-to-StarRocks](/_static/fig/mysql-starrocks-tutorial/mysql-to-starrocks.png "MySQL-to-StarRocks")

Connect to jdbc through database connection tools such as Dbeaver using `mysql://127.0.0.1:9030`. You can view the data written to three tables in StarRocks.

![StarRocks-display-data](/_static/fig/mysql-starrocks-tutorial/starrocks-display-data.png "StarRocks-display-data")

### Synchronize Schema and Data changes
Enter MySQL container

    ```shell
    docker-compose exec mysql mysql -uroot -p123456
    ```

Then, modify schema and record in MySQL, and the tables of StarRocks will change the same in real time：
1. insert one record in `orders` from MySQL:

   ```sql
   INSERT INTO app_db.orders (id, price) VALUES (3, 100.00);
   ```

2. add one column in `orders` from MySQL:

   ```sql
   ALTER TABLE app_db.orders ADD amount varchar(100) NULL;
   ```   

3. update one record in `orders` from MySQL:

   ```sql
   UPDATE app_db.orders SET price=100.00, amount=100.00 WHERE id=1;
   ```
4. delete one record in `orders` from MySQL:

   ```sql
   DELETE FROM app_db.orders WHERE id=2;
   ```

Refresh the Dbeaver every time you execute a step, and you can see that the `orders` table displayed in StarRocks will be updated in real-time, like the following：

![StarRockss-display-result](/_static/fig/mysql-starrocks-tutorial/starrocks-display-result.png "StarRocks-display-result")

Similarly, by modifying the `shipments` and `products` tables, you can also see the results of synchronized changes in real-time in StarRocks.

### Route the changes
Flink CDC provides the configuration to route the table structure/data of the source table to other table names.   
With this ability, we can achieve functions such as table name, database name replacement, and whole database synchronization.
Here is an example file for using `route` feature:
   ```yaml
   ################################################################################
   # Description: Sync MySQL all tables to StarRocks
   ################################################################################
   source:
      type: mysql
      hostname: localhost
      port: 3306
      username: root
      password: 123456
      tables: app_db.\.*
      server-id: 5400-5404
      server-time-zone: UTC

   sink:
      type: starrocks
      jdbc-url: jdbc:mysql://127.0.0.1:9030
      load-url: 127.0.0.1:8030
      username: root
      password: ""
      table.create.properties.replication_num: 1

   route:
      - source-table: app_db.orders
        sink-table: ods_db.ods_orders
      - source-table: app_db.shipments
        sink-table: ods_db.ods_shipments
      - source-table: app_db.products
        sink-table: ods_db.ods_products

   pipeline:
      name: Sync MySQL Database to StarRocks
      parallelism: 2
   ```

Using the upper `route` configuration, we can synchronize the table schema and data of `app_db.orders` to `ods_db.ods_orders`, thus achieving the function of database migration.   
Specifically, `source-table` support regular expression matching with multiple tables to synchronize sharding databases and tables. like the following：

   ```yaml
   route:
     - source-table: app_db.order\.*
       sink-table: ods_db.ods_orders
   ```

In this way, we can synchronize sharding tables like `app_db.order01`、`app_db.order02`、`app_db.order03` into one ods_db.ods_orders tables.      
Warning that there is currently no support for scenarios where the same primary key data exists in multiple tables, which will be supported in future versions.

## Clean up
After finishing the tutorial, run the following command to stop all containers in the directory of `docker-compose.yml`:

   ```shell
   docker-compose down
   ```

Run the following command to stop the Flink cluster in the directory of Flink `flink-1.18.0`:

   ```shell
   ./bin/stop-cluster.sh
   ```

