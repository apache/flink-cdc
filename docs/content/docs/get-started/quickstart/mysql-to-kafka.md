---
title: "MySQL to Kafka"
weight: 4
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

This tutorial is to show how to quickly build a Streaming ELT job from MySQL to Kafka using Flink CDC, including the
feature of sync all table of one database, schema change evolution and sync sharding tables into one table.
All exercises in this tutorial are performed in the Flink CDC CLI, and the entire process uses standard SQL syntax,
without a single line of Java/Scala code or IDE installation.

## Preparation

You need a Linux or macOS computer with Docker installed before starting.

### Prepare Flink Standalone cluster
1. Download [Flink 1.20.1](https://archive.apache.org/dist/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz), unzip it and enter `flink-1.20.1` directory.
   Use the following command to navigate to the Flink directory and set `FLINK_HOME` to the directory where flink-1.20.1 is located.

   ```shell
   tar -zxvf flink-1.20.1-bin-scala_2.12.tgz
   exprot FLINK_HOME=$(pwd)/flink-1.20.1
   cd flink-1.20.1
   ```

2. Enable checkpointing by appending the following parameters to the `conf/config.yaml` configuration file to perform a checkpoint every 3 seconds.

   ```yaml
   execution:
     checkpointing:
       interval: 3s
   ```

3. Start the Flink cluster using the following command.

   ```shell
   ./bin/start-cluster.sh
   ```  

After the cluster gets started, you can access the Flink Web UI at [http://localhost:8081/](http://localhost:8081/).

{{< img src="/fig/mysql-Kafka-tutorial/flink-ui.png" alt="Flink UI" >}}

Run `start-cluster.sh` multiple times to start more TaskManagers if necessary.  

Notice: If you are deploying your Flink cluster as a cloud service, you may need to configure `rest.bind-address` and `rest.address` in `conf/config.yaml` to `0.0.0.0`, and then use the public IP address to access the Flink Web UI.

### Prepare docker compose

First, let's create a `docker-compose.yml` file and write the following contents:

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
         - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://Kafka:9092
         - KAFKA_ZOOKEEPER_CONNECT=Zookeeper:2181
     MySQL:
       image: debezium/example-mysql:1.1
       ports:
         - "3306:3306"
       environment:
         - MYSQL_ROOT_PASSWORD=123456
         - MYSQL_USER=mysqluser
         - MYSQL_PASSWORD=mysqlpw
   ```

The Docker Compose will prepare the following containers for us:

- MySQL, acts as the pipeline source
- Kafka, acts as the pipeline sink
- Zookeeper, for Kafka cluster management and coordination

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.

   ```shell
   docker compose up -d
   ```

This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode.

Run `docker ps` to check whether these containers are running properly.  

{{< img src="/fig/mysql-Kafka-tutorial/docker-ps.png" alt="Executing docker ps command" >}}

#### Prepare records for MySQL

1. Enter MySQL container with the following command:

   ```shell
   docker compose exec MySQL mysql -uroot -p123456
   ```

2. Create `app_db` database and `orders`,`products`,`shipments` tables, then insert some data change records:

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

**Please note that the following download links are available only for stable releases.
You need to build your own SNAPSHOT versions based on master or release branches by yourself.**

1. Download the binary compressed packages listed below and extract them to the directory `flink cdc-{{< param Version >}}'`:
   [flink-cdc-{{< param Version >}}-bin.tar.gz](https://www.apache.org/dyn/closer.lua/flink/flink-cdc-{{< param Version >}}/flink-cdc-{{< param Version >}}-bin.tar.gz)
   flink-cdc-{{< param Version >}} directory will contain four directory: `bin`, `lib`, `log`, and `conf`.

2. Download the connector package listed below and move it to the `lib` directory:
   **Please note that you need to move the jar to the lib directory of Flink CDC Home, not to the lib directory of Flink Home.**
   - [MySQL pipeline connector {{< param Version >}}](https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/{{< param Version >}}/flink-cdc-pipeline-connector-mysql-{{< param Version >}}.jar)
   - [Kafka pipeline connector {{< param Version >}}](https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/{{< param Version >}}/flink-cdc-pipeline-connector-kafka-{{< param Version >}}.jar)

   You also need to place MySQL connector into Flink `lib` folder or pass it with `--jar` argument, since they're no longer packaged with CDC connectors:
   - [MySQL Connector Java](https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar)

3. Write a YAML pipeline definition file.
   Here is an example YAML file `mysql-to-kafka.yaml` for synchronizing MySQL database to Kafka:

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

4. Finally, submit job to Flink Standalone cluster using CDC CLI.

    ```shell
    bash bin/flink-cdc.sh mysql-to-kafka.yaml
    ```

    The following message should be printed to the console after submitting pipeline job to Flink cluster:

    ```
    Pipeline has been submitted to cluster.
    Job ID: 04fd88ccb96c789dce2bf0b3a541d626
    Job Description: MySQL to Kafka Pipeline
    ```

    A job named `Sync MySQL Database to Kafka` could be seen running in the Flink Web UI.

    {{< img src="/fig/mysql-Kafka-tutorial/mysql-to-Kafka.png" alt="MySQL-to-Kafka" >}}

    We can subscribe the sink kafka topic to monitor messages sent to Kafka with this command:  

    ```shell
    docker compose exec Kafka kafka-console-consumer.sh --bootstrap-server 0.0.0.0:9092 --topic yaml-mysql-kafka --from-beginning
    ```
   
    The debezium-json format encodes several fields including `before`, `after`, `op`, and `source`. An example:

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
    // ...
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
    // ...
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

1. First, we enter the MySQL container:  

    ```shell
    docker compose exec mysql mysql -uroot -p123456
    ```

2. Insert one record in `orders` from MySQL:  

    ```sql
    INSERT INTO app_db.orders (id, price) VALUES (3, 100.00);
    ```

3. Add one column in `orders` from MySQL:

    ```sql
    ALTER TABLE app_db.orders ADD amount varchar(100) NULL;
    ```

4. Update one record in `orders` from MySQL:

    ```sql
    UPDATE app_db.orders SET price=100.00, amount=100.00 WHERE id=1;
    ```

5. Delete one record in `orders` from MySQL:

    ```sql
    DELETE FROM app_db.orders WHERE id=2;
    ```

By monitoring the topic through consumers, we may expect these messages in Kafka topic:

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

Similarly, by modifying the `shipments` and `products` table, you can see the results of other changes synchronized in real-time in corresponding topics in Kafka.

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

With `route` rules, we can synchronize schema and data changes of tables in `app_db` to corresponding Kafka topics.


> `source-table` also supports matching multiple tables with Regular Expressions. Table schemas will be merged and synchronize to one single Kafka topic.

   ```yaml
      route:
        - source-table: app_db.order\.*
          sink-table: kafka_ods_orders
   ```

> In this way, we can synchronize sharding tables like `app_db.order01`, `app_db.order02`, `app_db.order03` into one kafka_ods_orders topic.

We can run the following command to peek topics created to Kafka:

```shell
docker compose exec Kafka kafka-topics.sh --bootstrap-server 0.0.0.0:9092 --list
```

* __consumer_offsets
* kafka_ods_orders
* kafka_ods_products
* kafka_ods_shipments
* yaml-mysql-kafka

By peeking messages sent to `kafka_ods_orders` topic, we may see the following output: 

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

### Writing to Multiple Partitions

The `partition.strategy` option can be used to configure the strategy of sending data to Kafka partitions. Available choices are:

 - `all-to-zero`: Send all data to Partition #0. This is the default behavior.
 - `hash-by-key`: Data changes will be distributed based on the hash value of the primary key.

For example, we may add the extra configuration here:

```yaml
source:
  # ...
sink:
  # ...
  topic: yaml-mysql-kafka-hash-by-key
  partition.strategy: hash-by-key
pipeline:
  # ...
```

And create a Kafka topic with 12 partitions:

```shell
docker compose exec Kafka kafka-topics.sh --create --topic yaml-mysql-kafka-hash-by-key --bootstrap-server 0.0.0.0:9092  --partitions 12
```

After submitting the pipeline job, we can peek messages from specific partitions:

```shell
docker compose exec Kafka kafka-console-consumer.sh --bootstrap-server=0.0.0.0:9092  --topic yaml-mysql-kafka-hash-by-key  --partition 0  --from-beginning
```

You may get the following output:

```json
// partition 0
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
// partition 4
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

The `value.format` could be used to configure the JSON serialization format sent to Kafka.

Available options are [debezium-json](https://debezium.io/documentation/reference/stable/integrations/serdes.html) (default) and [canal-json](https://github.com/alibaba/canal/wiki).
User-defined output format is not supported for now.

- `debezium-json` format will encode `before`, `after`, `op`, and `source` fields into JSON.
- `canal-json` format will encode `old`, `data`, `type`, `database`, `table`, `pkNames` fields into JSON.
- The `ts_ms` field will not be included in the output structure by default. You need to set MySQL source option `metadata.list` to expose extra metadata fields.

An example for `canal-json` format output would be like:

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

### Table-to-Topic Mapping

The `sink.tableId-to-topic.mapping` parameter can be used to specify the mapping rules from upstream tables to Kafka topics.

Unlike the `route` rules, table-to-topic mapping will not try to merge table schemas from upstream.
TableId from different tables will keep unchanged, they're just dispatched to different topics.

Add `sink.tableId-to-topic.mapping` configuration to specify the mapping relationship.
Topic mapping rules are separated by `;`. Each mapping rule is consisted by two parts: upstream Table ID (RegExp) and sink Kafka topic name, separated by `:`.

```yaml
source:
  # ...

sink:
  # ...
  sink.tableId-to-topic.mapping: app_db.orders:yaml-mysql-kafka-orders;app_db.shipments:yaml-mysql-kafka-shipments;app_db.products:yaml-mysql-kafka-products
pipeline:
  # ...
```  

With the configuration above, these topics will be created:

* yaml-mysql-kafka-orders
* yaml-mysql-kafka-products
* yaml-mysql-kafka-shipments

Each topic should contain the following records:

- `yaml-mysql-kafka-orders`

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
- `yaml-mysql-kafka-products`
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
- `yaml-mysql-kafka-shipments`
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

## Cleanup

After finishing the tutorial, you may run the following command to stop all containers in the directory of `docker-compose.yml`:

```shell
docker compose down
```

Run the following command to stop the Flink cluster in `FLINK_HOME`:

```shell
./bin/stop-cluster.sh
```

{{< top >}}