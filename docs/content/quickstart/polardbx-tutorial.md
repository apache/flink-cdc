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

# Demo: PolarDB-X CDC to Elasticsearch

This tutorial is to show how to quickly build streaming ETL for PolarDB-X with Flink CDC.

Assuming we are running an e-commerce business. The product and order data stored in PolarDB-X.
We want to enrich the orders using the product table, and then load the enriched orders to ElasticSearch in real time.

In the following sections, we will describe how to use Flink PolarDB-X CDC to implement it.
All exercises in this tutorial are performed in the Flink SQL CLI, and the entire process uses standard SQL syntax, without a single line of Java/Scala code or IDE installation.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.

### Starting components required
The components required in this demo are all managed in containers, so we will use `docker-compose` to start them.

Create `docker-compose.yml` file using following contents:
```
version: '2.1'
services:
  polardbx:
    polardbx:
    image: polardbx/polardb-x:2.0.1
    container_name: polardbx
    ports:
      - "8527:8527"
  elasticsearch:
    image: 'elastic/elasticsearch:7.6.0'
    container_name: elasticsearch
    environment:
      - cluster.name=docker-cluster
      - bootstrap.memory_lock=true
      - ES_JAVA_OPTS=-Xms512m -Xmx512m
      - discovery.type=single-node
    ports:
      - '9200:9200'
      - '9300:9300'
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
  kibana:
    image: 'elastic/kibana:7.6.0'
    container_name: kibana
    ports:
      - '5601:5601'
    volumes:
      - '/var/run/docker.sock:/var/run/docker.sock'
```
The Docker Compose environment consists of the following containers:
- PolarDB-X: the `products`,`orders` tables will be store in the database. They will be joined enrich the orders.
- Elasticsearch: mainly used as a data sink to store enriched orders.
- Kibana: used to visualize the data in Elasticsearch.

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run docker ps to check whether these containers are running properly.
We can also visit [http://localhost:5601/](http://localhost:5601/) to see if Kibana is running normally.

### Preparing Flink and JAR package required
1. Download [Flink 1.18.0](https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz) and unzip it to the directory `flink-1.18.0`
2. Download following JAR package required and put them under `flink-1.18.0/lib/`:

   **Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself.**
    - flink-sql-connector-mysql-cdc-2.5-SNAPSHOT.jar
    - [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
   
### Preparing data in databases
#### Preparing data in PolarDB-X
1. Enter PolarDB-X Database:
    ```shell
   mysql -h127.0.0.1 -P8527 -upolardbx_root -p"123456"
    ```
2. Create tables and populate data:
    ```sql
    -- PolarDB-X
   CREATE TABLE products (
   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
   name VARCHAR(255) NOT NULL,
   description VARCHAR(512)
   ) AUTO_INCREMENT = 101;
   
   INSERT INTO products
   VALUES (default,"scooter","Small 2-wheel scooter"),
   (default,"car battery","12V car battery"),
   (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3"),
   (default,"hammer","12oz carpenter's hammer"),
   (default,"hammer","14oz carpenter's hammer"),
   (default,"hammer","16oz carpenter's hammer"),
   (default,"rocks","box of assorted rocks"),
   (default,"jacket","water resistent black wind breaker"),
   (default,"spare tire","24 inch spare tire");
   
   
   CREATE TABLE orders (
   order_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
   order_date DATETIME NOT NULL,
   customer_name VARCHAR(255) NOT NULL,
   price DECIMAL(10, 5) NOT NULL,
   product_id INTEGER NOT NULL,
   order_status BOOLEAN NOT NULL -- Whether order has been placed
   ) AUTO_INCREMENT = 10001;
   
   INSERT INTO orders
   VALUES (default, '2020-07-30 10:08:22', 'Jark', 50.50, 102, false),
   (default, '2020-07-30 10:11:09', 'Sally', 15.00, 105, false),
   (default, '2020-07-30 12:00:30', 'Edward', 25.25, 106, false);
    ```
   
## Starting Flink cluster and Flink SQL CLI

1. Use the following command to change to the Flink directory:
    ```
    cd flink-1.18.0
    ```
   
2. Use the following command to start a Flink cluster:
    ```shell
    ./bin/start-cluster.sh
    ```

   Then we can visit [http://localhost:8081/](http://localhost:8081/) to see if Flink is running normally, and the web page looks like:

   ![Flink UI](/_static/fig/mysql-postgress-tutorial/flink-ui.png "Flink UI")

3. Use the following command to start a Flink SQL CLI:
    ```shell
    ./bin/sql-client.sh
    ```
   We should see the welcome screen of the CLI client.

   ![Flink SQL Client](/_static/fig/mysql-postgress-tutorial/flink-sql-client.png "Flink SQL Client")

## Creating tables using Flink DDL in Flink SQL CLI
First, enable checkpoints every 3 seconds
```sql
-- Flink SQL                   
Flink SQL> SET execution.checkpointing.interval = 3s;
```

Then, create tables that capture the change data from the corresponding database tables.
```sql
-- Flink SQL
Flink SQL> SET execution.checkpointing.interval = 3s;

-- create source table2 - orders
Flink SQL> CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
    'connector' = 'mysql-cdc',
	'hostname' = '127.0.0.1',
	'port' = '8527',
	'username' = 'polardbx_root',
	'password' = '123456',
	'database-name' = 'mydb',
	'table-name' = 'orders'
 );

-- create source table2 - products
CREATE TABLE products (
    id INT,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED
  ) WITH (
	'connector' = 'mysql-cdc',
	'hostname' = '127.0.0.1',
	'port' = '8527',
	'username' = 'polardbx_root',
	'password' = '123456',
	'database-name' = 'mydb',
	'table-name' = 'products'
);
```

Finally, create `enriched_orders` table that is used to load data to the Elasticsearch.
```sql
-- Flink SQL
-- create sink table - enrich_orders
Flink SQL> CREATE TABLE enriched_orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   product_name STRING,
   product_description STRING,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders'
 );
```

## Enriching orders and load to ElasticSearch
Use Flink SQL to join the `order` table with the `products` table to enrich orders and write to the Elasticsearch.
```sql
-- Flink SQL
Flink SQL> INSERT INTO enriched_orders
  SELECT o.order_id,
    o.order_date,
    o.customer_name,
    o.price,
    o.product_id,
    o.order_status,
    p.name,
    p.description
 FROM orders AS o
 LEFT JOIN products AS p ON o.product_id = p.id;
```
Now, the enriched orders should be shown in Kibana.
Visit [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) to create an index pattern `enriched_orders`.

![Create Index Pattern](/_static/fig/mysql-postgress-tutorial/kibana-create-index-pattern.png "Create Index Pattern")

Visit [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover) to find the enriched orders.

![Find enriched Orders](/_static/fig/mysql-postgress-tutorial/kibana-detailed-orders.png "Find enriched Orders")

Next, do some change in the databases, and then the enriched orders shown in Kibana will be updated after each step in real time.
1. Insert a new order in PolarDB-X
   ```sql
   --PolarDB-X
   INSERT INTO orders
   VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);
   ```
2. Update the order status in PolarDB-X
   ```sql
   --PolarDB-X
   UPDATE orders SET order_status = true WHERE order_id = 10004;
   ```
3. Delete the order in PolarDB-X
   ```sql
   --PolarDB-X
   DELETE FROM orders WHERE order_id = 10004;
   ```
   The changes of enriched orders in Kibana are as follows:
   ![Enriched Orders Changes](/_static/fig/mysql-postgress-tutorial/kibana-detailed-orders-changes.gif "Enriched Orders Changes")
   
## Clean up
After finishing the tutorial, run the following command to stop all containers in the directory of `docker-compose.yml`:
```shell
docker-compose down
```
Run the following command to stop the Flink cluster in the directory of Flink `flink-1.18.0`:
```shell
./bin/stop-cluster.sh
```
