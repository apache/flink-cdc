---
title: "MySQL to Kafka"
weight: 2
type: docs
aliases:
- /try-flink-cdc/pipeline-connectors/mysql-Kafka-pipeline-tutorial.html
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

# Streaming ELT from MySQL to Kafka

This tutorial is to show how to quickly build a Streaming ELT job from MySQL to StarRocks using Flink CDC, including the
feature of sync all table of one database, schema change evolution and sync sharding tables into one table.
All exercises in this tutorial are performed in the Flink CDC CLI, and the entire process uses standard SQL syntax,
without a single line of Java/Scala code or IDE installation.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.


### Prepare Flink Standalone cluster
1. Download [Flink 1.20.1](https://archive.apache.org/dist/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz) ，unzip and get flink-1.20.1 directory.
   Use the following command to navigate to the Flink directory and set FLINK_HOME to the directory where flink-1.20.1 is located.

   ```shell
   tar -zxvf flink-1.20.1-bin-scala_2.12.tgz
   exprot FLINK_HOME=$(pwd)/flink-1.20.1
   cd flink-1.20.1
   ```

2. Enable checkpointing by appending the following parameters to the conf/flink-conf.yaml configuration file to perform a checkpoint every 3 seconds.

   ```yaml
   execution:
       checkpointing:
           interval: 3000
   ```

3. Start the Flink cluster using the following command.

   ```shell
   ./bin/start-cluster.sh
   ```  

If successfully started, you can access the Flink Web UI at [http://localhost:8081/](http://localhost:8081/), as shown below.

{{< img src="/fig/mysql-Kafka-tutorial/flink-ui.png" alt="Flink UI" >}}

Executing `start-cluster.sh` multiple times can start multiple  TaskManager‘s.  

Note: If you are a cloud server and cannot access the local area, you need to change the localhost of rest.bd-address and rest.address in conf/config.yaml to 0.0.0.0, and then use the public IP address:8081 to access it.
### Prepare docker compose
The following tutorial will prepare the required components using `docker-compose`.
Create a `docker-compose.yml` file using the content provided below:

   ```yaml
   version: '2.1'
   services:
    Zookeeper:
        image: zookeeper:3.7.1
        ports:
            - "2181:2181"
        environment:
            - ALLOW_ANONYMOUS_LOGIN=yes
    Kafka:
        image: bitnami/kafka:2.8.1
        ports:
            - "9092:9092"
            - "9093:9093"
        environment:
            - ALLOW_PLAINTEXT_LISTENER=yes
            - KAFKA_LISTENERS=PLAINTEXT://:9092
            - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.67.2:9092
            - KAFKA_ZOOKEEPER_CONNECT=192.168.67.2:2181
     MySQL:
       image: debezium/example-mysql:1.1
       ports:
         - "3306:3306"
       environment:
         - MYSQL_ROOT_PASSWORD=123456
         - MYSQL_USER=mysqluser
         - MYSQL_PASSWORD=mysqlpw
   ```
Note: The 192.168.67.2 in the file is an internal network IP and can be found through ifconfig.
The Docker Compose should include the following services (containers):
- MySQL: include a database named `app_db`
- Kafka: Store the result table mapped from MySQL according to the rules
- Zookeeper：It is mainly used for Kafka cluster management and coordination

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.

   ```shell
   docker-compose up -d
   ```

This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run docker ps to check whether these containers are running properly.  
{{< img src="/fig/mysql-Kafka-tutorial/docker-ps.png" alt="Docker ps" >}}
#### Prepare records for MySQL
1. Enter MySQL container

   ```shell
   docker-compose exec MySQL mysql -uroot -p123456
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

## Submit job with Flink CDC CLI
1. Download the binary compressed packages listed below and extract them to the directory `flink cdc-{{< param Version >}}'`：
   [flink-cdc-{{< param Version >}}-bin.tar.gz](https://www.apache.org/dyn/closer.lua/flink/flink-cdc-{{< param Version >}}/flink-cdc-{{< param Version >}}-bin.tar.gz)
   flink-cdc-{{< param Version >}} directory will contain four directory: `bin`, `lib`, `log`, and `conf`.

2. Download the connector package listed below and move it to the `lib` directory
   **Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release branches by yourself.**
   **Please note that you need to move the jar to the lib directory of Flink CDC Home, not to the lib directory of Flink Home.**
   - [MySQL pipeline connector {{< param Version >}}](https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/{{< param Version >}}/flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar)
   - [Kafka pipeline connector {{< param Version >}}](https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/{{< param Version >}}/flink-cdc-pipeline-connector-kafka-{{< param Version >}}.jar)

   You also need to place MySQL connector into Flink `lib` folder or pass it with `--jar` argument, since they're no longer packaged with CDC connectors:
   - [MySQL Connector Java](https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)

3. Write task configuration yaml file.
   Here is an example file for synchronizing the entire database `mysql-to-kafka.yaml`：

   ```yaml
   ################################################################################
   # Description: Sync MySQL all tables to Kafka
   ################################################################################
   source:
     type: mysql
     hostname: 0.0.0.0
     port: 3306
     username: root
     password: 123456
     tables: app_db.\.*
     server-id: 5400-5404
     server-time-zone: UTC
   
   sink:
     type: kafka
     name: Kafka Sink
     properties.bootstrap.servers: 0.0.0.0:9092
     topic: yaml-mysql-kafka

   pipeline:
     name: MySQL to Kafka Pipeline
     parallelism: 1
   ```

Notice that:
* `tables: app_db.\.*` in source synchronize all tables in `app_db` through Regular Matching.

4. Finally, submit job to Flink Standalone cluster using Cli.

   ```shell
   bash bin/flink-cdc.sh mysql-to-kafka.yaml
   #For reference, some examples of custom paths are mainly used in situations such as multiple versions of flink and inconsistent mysql drivers, as follows
   #bash /root/flink-cdc-3.4.0/bin/flink-cdc.sh /root/flink-cdc-3.4.0/bin/mysql-to-kafka.yaml --flink-home /root/flink-1.20.1 --jar /root/flink-cdc-3.4.0/lib/mysql-connector-java-8.0.27.jar
   ```

After successful submission, the return information is as follows：

   ```shell
    Pipeline has been submitted to cluster.
    Job ID: 04fd88ccb96c789dce2bf0b3a541d626
    Job Description: MySQL to Kafka Pipeline
   ```

We can find a job  named `Sync MySQL Database to Kafka` is running through Flink Web UI.

{{< img src="/fig/mysql-Kafka-tutorial/mysql-to-Kafka.png" alt="MySQL-to-Kafka" >}}

The Topic situation can be viewed through the built-in client of kafka to obtain the content in debezium-json format:  
```shell
  docker-compose exec Kafka kafka-console-consumer.sh --bootstrap-server 192.168.67.2:9092 --topic yaml-mysql-kafka --from-beginning
```  
The debezium-json format contains several elements such as before,after,op, and source. The demonstration example is as follows:  
```json
{
    "before": null,
    "after": {
        "id": 1,
        "price": 4
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "orders"
    }
}
...
{
    "before": null,
    "after": {
        "id": 1,
        "product": "Beer"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "products"
    }
}
...
{
    "before": null,
    "after": {
        "id": 2,
        "city": "xian"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "shipments"
    }
}
```
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
By monitoring the topic through consumers, we can see that these changes are also taking place in real time on Kafka:
```json
{
    "before": {
        "id": 1,
        "price": 4,
        "amount": null
    },
    "after": {
        "id": 1,
        "price": 100,
        "amount": "100.00"
    },
    "op": "u",
    "source": {
        "db": "app_db",
        "table": "orders"
    }
}
```
Similarly, by modifying the `shipments`,`products` table, you can see the results of the synchronized changes in real time at the corresponding topic in Kafka.  

### Route the changes  
Flink CDC provides the configuration to route the table structure/data of the source table to other table names.
With this ability, we can achieve functions such as table name, database name replacement, and whole database synchronization.
Here is an example file for using `route` feature:  
   ```yaml
   ################################################################################
   # Description: Sync MySQL all tables to Kafka
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
     type: kafka
     name: Kafka Sink
     properties.bootstrap.servers: 0.0.0.0:9092
   pipeline:
     name: MySQL to Kafka Pipeline
     parallelism: 1
   route:
     - source-table: app_db.orders
       sink-table: kafka_ods_orders
     - source-table: app_db.shipments
       sink-table: kafka_ods_shipments
     - source-table: app_db.products
       sink-table: kafka_ods_products
   ```

Using the upper `route` configuration, we can synchronize the table schema and data of `app_db.orders` to `kafka_ods_orders`, thus achieving the function of database migration.
Specifically, `source-table` support regular expression matching with multiple tables to synchronize sharding databases and tables. like the following：

   ```yaml
      route:
        - source-table: app_db.order\.*
          sink-table: kafka_ods_orders
   ```

In this way, we can synchronize sharding tables like `app_db.order01`、`app_db.order02`、`app_db.order03` into one kafka_ods_orders tables.By using the built-in tools of kafka, you can view the successful establishment of the corresponding Topic. Data details can be queried using kafka-console-Consumer.sh:  


```shell
docker-compose exec Kafka kafka-topics.sh --bootstrap-server 192.168.67.2:9092 --list
```
The information of the newly created Kafka Topic is as follows:
```shell
    __consumer_offsets
    kafka_ods_orders
    kafka_ods_products
    kafka_ods_shipments
    yaml-mysql-kafka
```

Select the Topic "kafka_ods_orders" for query, and the returned data example is as follows:
```json
{
    "before": null,
    "after": {
        "id": 1,
        "price": 100,
        "amount": "100.00"
    },
    "op": "c",
    "source": {
        "db": null,
        "table": "kafka_ods_orders"
    }
}
```

### Write to multiple partitions
The partition.strategy parameter can be used to define the strategy for sending data to the Kafka partition. The options that can be set are:
 - `all-to-zero` (Send all data to partition 0), default value
 - `hash-by-key` (All data are distributed based on the hash value of the primary key.)

We define a row of partition.strategy: hash-by-key under the sink based on mysql-to-kafka.yaml
```yaml
source:
  ...
sink:
  ...
  topic: yaml-mysql-kafka-hash-by-key
  partition.strategy: hash-by-key
pipeline:
  ...
```
Meanwhile, we use the script of Kafka to create a 12-partition kafka Topic:
```shell
docker-compose exec Kafka kafka-topics.sh --create --topic yaml-mysql-kafka-hash-by-key --bootstrap-server 192.168.67.2:9092  --partitions 1
```
After submitting the yaml program, at this point, we should specify the partition consumption and check the data stored in each partition.
```shell
docker-compose exec Kafka kafka-console-consumer.sh --bootstrap-server=192.168.67.2:9092  --topic yaml-mysql-kafka-hash-by-key  --partition 0  --from-beginning
```
The details of some partition data are as follows:  
```json
# partition 0
{
    "before": null,
    "after": {
        "id": 1,
        "price": 100,
        "amount": "100.00"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "orders"
    }
}
# partition 4
{
    "before": null,
    "after": {
        "id": 2,
        "product": "Cap"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "products"
    }
}
{
    "before": null,
    "after": {
        "id": 1,
        "city": "beijing"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "shipments"
    }
}
```

### Output format
The value.format parameter is used to serialize the format of the value part data of Kafka messages. Optional fill in value including [debezium - json] (https://debezium.io/documentation/reference/stable/integrations/serdes.html) and [canal - json] (HTTPS: / / github.com/alibaba/canal/wiki), the default value is ` debezium - json `, currently does not support user-defined output format.  
- `debezium-json` The format will contain several elements such as before(data before change)/after(data after change)/op(change type)/source(metadata). The ts_ms field will not be included in the output structure by default (metadata.list needs to be specified in the Source to cooperate).
- `canal-json` Format will contain old/data/type/database/table/pkNames several elements, but ts does not included by default (for).  

The `value.format`: `canal-json` type can be defined in the sink of the YAML file to specify the output format as `canal-json` type:  
```yaml
source:
  ...

sink:
  ...
  topic: yaml-mysql-kafka-canal
  value.format: canal-json
pipeline:
  ...
```  
Query the data corresponding to the Topic, and the returned example is as follows:
```json
{
    "old": null,
    "data": [
        {
            "id": 1,
            "price": 100,
            "amount": "100.00"
        }
    ],
    "type": "INSERT",
    "database": "app_db",
    "table": "orders",
    "pkNames": [
        "id"
    ]
}
```


### The mapping relationship from the upstream table name to the downstream Topic name
The `sink.tableId-to-topic.mapping` parameter can be used to specify the mapping relationship from the upstream table name to the downstream Kafka Topic name. No route configuration is required. The difference from the previously introduced implementation through route lies in that configuring this parameter can set the Topic name to be written while retaining the table name information of the source table.  

In the previous YAML file, add `sink.tableId-to-topic.mapping` configuration to specify the mapping relationship, and each mapping relationship is composed of `;` The TableId of the upstream table and the Topic name of the downstream Kafka are separated by `:`.
  
```yaml
source:
  ...

sink:
  ...
  sink.tableId-to-topic.mapping: app_db.orders:yaml-mysql-kafka-orders;app_db.shipments:yaml-mysql-kafka-shipments;app_db.products:yaml-mysql-kafka-products
pipeline:
  ...
```  
After running, the following topics will be generated in Kafka:  
```
...
yaml-mysql-kafka-orders
yaml-mysql-kafka-products
yaml-mysql-kafka-shipments
```  
Details of some data in different topics of Kafka:
- yaml-mysql-kafka-orders

    ```json
    {
        "before": null,
        "after": {
            "id": 1,
            "price": 100,
            "amount": "100.00"
        },
        "op": "c",
        "source": {
            "db": "app_db",
            "table": "orders"
        }
    }
    ```
- yaml-mysql-kafka-products
    ```json
    {
    "before": null,
    "after": {
        "id": 2,
        "product": "Cap"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "products"
        }
    }
    ```
- yaml-mysql-kafka-shipments
    ```json
    {
    "before": null,
    "after": {
        "id": 2,
        "city": "xian"
    },
    "op": "c",
    "source": {
        "db": "app_db",
        "table": "shipments"
        }
    }
    ```

## Clean up
After finishing the tutorial, run the following command to stop all containers in the directory of `docker-compose.yml`:

   ```shell
   docker-compose down
   ```

Run the following command to stop the Flink cluster in the directory of Flink `flink-1.20.1`:

   ```shell
   ./bin/stop-cluster.sh
   ```

{{< top >}}