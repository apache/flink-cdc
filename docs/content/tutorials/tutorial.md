# Tutorial

1. Create `docker-compose.yml` file using following contents: 

```
version: '2.1'
services:
  postgres:
    image: debezium/example-postgres:1.1
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_PASSWORD=1234
      - POSTGRES_DB=postgres
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
  mysql:
    image: debezium/example-mysql:1.1
    ports:
      - "3306:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
      - MYSQL_USER=mysqluser
      - MYSQL_PASSWORD=mysqlpw
  elasticsearch:
    image: elastic/elasticsearch:7.6.0
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
    ports:
      - "5601:5601"
  zookeeper:
    image: wurstmeister/zookeeper:3.4.6
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka:2.12-2.2.1
    ports:
      - "9092:9092"
      - "9094:9094"
    depends_on:
      - zookeeper
    environment:
      - KAFKA_ADVERTISED_LISTENERS=INSIDE://:9094,OUTSIDE://localhost:9092
      - KAFKA_LISTENERS=INSIDE://:9094,OUTSIDE://:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
      - KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_CREATE_TOPICS="user_behavior:1:1"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

2. Enter mysql's container and initialize data: 

```
docker-compose exec mysql mysql -uroot -p123456
```

```sql
-- MySQL
CREATE DATABASE mydb;
USE mydb;
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512)
);
ALTER TABLE products AUTO_INCREMENT = 101;

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

3. Enter Postgres's container and initialize data:

```
docker-compose exec postgres psql -h localhost -U postgres
```

```sql
-- PG
CREATE TABLE shipments (
  shipment_id SERIAL NOT NULL PRIMARY KEY,
  order_id SERIAL NOT NULL,
  origin VARCHAR(255) NOT NULL,
  destination VARCHAR(255) NOT NULL,
  is_arrived BOOLEAN NOT NULL
);
ALTER SEQUENCE public.shipments_shipment_id_seq RESTART WITH 1001;
ALTER TABLE public.shipments REPLICA IDENTITY FULL;

INSERT INTO shipments
VALUES (default,10001,'Beijing','Shanghai',false),
       (default,10002,'Hangzhou','Shanghai',false),
       (default,10003,'Shanghai','Hangzhou',false);
```



4. Download following JAR package to `<FLINK_HOME>/lib/`:
- [flink-sql-connector-elasticsearch7_2.11-1.11.1.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.11.1/flink-sql-connector-elasticsearch7_2.11-1.11.1.jar)
- [flink-sql-connector-mysql-cdc-1.0.0.jar](https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-mysql-cdc/1.0.0/flink-sql-connector-mysql-cdc-1.0.0.jar)
- [flink-sql-connector-postgres-cdc-1.0.0.jar](https://repo1.maven.org/maven2/com/alibaba/ververica/flink-sql-connector-postgres-cdc/1.0.0/flink-sql-connector-postgres-cdc-1.0.0.jar)

5. Launch a Flink cluster, then start a Flink SQL CLI and execute following SQL statements inside: 

```sql
-- Flink SQL
CREATE TABLE products (
  id INT,
  name STRING,
  description STRING
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'mydb',
  'table-name' = 'products'
);

CREATE TABLE orders (
  order_id INT,
  order_date TIMESTAMP(0),
  customer_name STRING,
  price DECIMAL(10, 5),
  product_id INT,
  order_status BOOLEAN
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'mydb',
  'table-name' = 'orders'
);

CREATE TABLE shipments (
  shipment_id INT,
  order_id INT,
  origin STRING,
  destination STRING,
  is_arrived BOOLEAN
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'shipments'
);

CREATE TABLE enriched_orders (
  order_id INT,
  order_date TIMESTAMP(0),
  customer_name STRING,
  price DECIMAL(10, 5),
  product_id INT,
  order_status BOOLEAN,
  product_name STRING,
  product_description STRING,
  shipment_id INT,
  origin STRING,
  destination STRING,
  is_arrived BOOLEAN,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://localhost:9200',
    'index' = 'enriched_orders'
);

INSERT INTO enriched_orders
SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived
FROM orders AS o
LEFT JOIN products AS p ON o.product_id = p.id
LEFT JOIN shipments AS s ON o.order_id = s.order_id;
```

6. Make some changes in MySQL and Postgres, then check the result in Elasticsearch: 

```sql
--MySQL
INSERT INTO orders
VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);

--PG
INSERT INTO shipments
VALUES (default,10004,'Shanghai','Beijing',false);

--MySQL
UPDATE orders SET order_status = true WHERE order_id = 10004;

--PG
UPDATE shipments SET is_arrived = true WHERE shipment_id = 1004;

--MySQL
DELETE FROM orders WHERE order_id = 10004;
```

7. Kafka Changelog JSON format

Execute following SQL in Flink SQL CLI: 

```sql
-- Flink SQL
CREATE TABLE kafka_gmv (
  day_str STRING,
  gmv DECIMAL(10, 5)
) WITH (
    'connector' = 'kafka',
    'topic' = 'kafka_gmv',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'changelog-json'
);

INSERT INTO kafka_gmv
SELECT DATE_FORMAT(order_date, 'yyyy-MM-dd') as day_str, SUM(price) as gmv
FROM orders
WHERE order_status = true
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM-dd');

-- Consumer changelog data from Kafka, and check the result of materialized view: 
SELECT * FROM kafka_gmv;
```
To consumer records in Kafka using `kafka-console-consumer`:

```
docker-compose exec kafka bash -c 'kafka-console-consumer.sh --topic kafka_gmv --bootstrap-server kafka:9094 --from-beginning'
```

Update orders data and check the output in Flink SQL CLI and Kafka console consumer: 
```sql
-- MySQL
UPDATE orders SET order_status = true WHERE order_id = 10001;
UPDATE orders SET order_status = true WHERE order_id = 10002;
UPDATE orders SET order_status = true WHERE order_id = 10003;

INSERT INTO orders
VALUES (default, '2020-07-30 17:33:00', 'Timo', 50.00, 104, true);

UPDATE orders SET price = 40.00 WHERE order_id = 10005;

DELETE FROM orders WHERE order_id = 10005;
```