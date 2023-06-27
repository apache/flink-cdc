# Demo: TiDB CDC to Elasticsearch

**First,we will start TiDB cluster with docker.**

```shell
$ git clone https://github.com/pingcap/tidb-docker-compose.git
```
**Next,replace `docker-compose.yml` file using following contents in directory `tidb-docker-compose`:**

```
version: "2.1"

services:
  pd:
    image: pingcap/pd:v5.3.1
    ports:
      - "2379:2379"
    volumes:
      - ./config/pd.toml:/pd.toml
      - ./logs:/logs
    command:
      - --client-urls=http://0.0.0.0:2379
      - --peer-urls=http://0.0.0.0:2380
      - --advertise-client-urls=http://pd:2379
      - --advertise-peer-urls=http://pd:2380
      - --initial-cluster=pd=http://pd:2380
      - --data-dir=/data/pd
      - --config=/pd.toml
      - --log-file=/logs/pd.log
    restart: on-failure

  tikv:
    image: pingcap/tikv:v5.3.1
    ports:
      - "20160:20160"
    volumes:
      - ./config/tikv.toml:/tikv.toml 
      - ./logs:/logs           
    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv:20160
      - --data-dir=/data/tikv
      - --pd=pd:2379
      - --config=/tikv.toml
      - --log-file=/logs/tikv.log
    depends_on:
      - "pd"
    restart: on-failure

  tidb:
    image: pingcap/tidb:v5.3.1
    ports:
      - "4000:4000"
    volumes:
      - ./config/tidb.toml:/tidb.toml
      - ./logs:/logs
    command:
      - --store=tikv
      - --path=pd:2379
      - --config=/tidb.toml
      - --log-file=/logs/tidb.log
      - --advertise-address=tidb
    depends_on:
      - "tikv"
    restart: on-failure
    
  elasticsearch:
     image: elastic/elasticsearch:7.6.0
     container_name: elasticsearch
     environment:
       - cluster.name=docker-cluster
       - bootstrap.memory_lock=true
       - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
       - discovery.type=single-node
     ports:
       - "9200:9200"
       - "9300:9300"
     ulimits:
       memlock:
         soft: -1
         hard: -1
       nofile:
         soft: 65536
         hard: 65536
         
  kibana:
     image: elastic/kibana:7.6.0
     container_name: kibana
     ports:
       - "5601:5601"
     volumes:
       - /var/run/docker.sock:/var/run/docker.sock
       
``` 
The Docker Compose environment consists of the following containers:
- TiDB cluster: tikv、pd、tidb.
- Elasticsearch: store the join result of the `orders` and `products` table.
- Kibana: mainly used to visualize the data in Elasticsearch.

Add `pd` and `tikv` mapping to `127.0.0.1` in `host` file.
To start all containers, run the following command in the directory that contains the docker-compose.yml file:
```shell
docker-compose up -d
mysql -h 127.0.0.1 -P 4000 -u root # Just test tidb cluster is ready,if you have install mysql local.
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode.
Run docker ps to check whether these containers are running properly. You can also visit http://localhost:5601/ to see if Kibana is running normally.

Don’t forget to run the following command to stop and remove all containers after you finished the tutorial:

```shell
docker-compose down
````

**Download following JAR package to `<FLINK_HOME>/lib`:**

*Download links are available only for stable releases, SNAPSHOT dependency need build by yourself. *

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-tidb-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-tidb-cdc/2.4.0/flink-sql-connector-tidb-cdc-2.4.0.jar)


**Preparing data in TiDB database**

Create databases/tables and populate data

 ```sql
-- TiDB
CREATE DATABASE mydb;
USE mydb;
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
**Launch a Flink cluster and start a Flink SQL CLI:**

```sql
-- Flink SQL
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET execution.checkpointing.interval = 3s;

Flink SQL> CREATE TABLE products (
    id INT,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED
  ) WITH (
    'connector' = 'tidb-cdc',
    'tikv.grpc.timeout_in_ms' = '20000',
    'pd-addresses' = '127.0.0.1:2379',
    'database-name' = 'mydb',
    'table-name' = 'products'
  );

Flink SQL> CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(3),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
    'connector' = 'tidb-cdc',
    'tikv.grpc.timeout_in_ms' = '20000',
    'pd-addresses' = '127.0.0.1:2379',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);

Flink SQL> CREATE TABLE enriched_orders (
   order_id INT,
   order_date DATE,
   customer_name STRING,
   order_status BOOLEAN,
   product_name STRING,
   product_description STRING,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders_1'
 );

Flink SQL> INSERT INTO enriched_orders
  SELECT o.order_id, o.order_date, o.customer_name, o.order_status, p.name, p.description
  FROM orders AS o
  LEFT JOIN products AS p ON o.product_id = p.id;
```

**Check result in Elasticsearch**

Check the data has been written to Elasticsearch successfully, you can visit [Kibana](http://localhost:5601/) to see the data.

**Make changes in TiDB and watch result in Elasticsearch**

Do some changes in the databases, and then the enriched orders shown in Kibana will be updated after each step in real time.

```sql
INSERT INTO orders
VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);

UPDATE orders SET order_status = true WHERE order_id = 10004;

DELETE FROM orders WHERE order_id = 10004;
```
