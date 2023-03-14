# Demo: OceanBase CDC to ElasticSearch

## Video tutorial

- [YouTube](https://www.youtube.com/watch?v=ODGE-73Dntg&t=2s)
- [Bilibili](https://www.bilibili.com/video/BV1Zg411a7ZB/?spm_id_from=333.999.0.0)

### Preparation

#### Configure and start the components

Create `docker-compose.yml`.

```yaml
version: '2.1'
services:
  observer:
    image: oceanbase/oceanbase-ce:3.1.4
    container_name: observer
    environment:
      - 'OB_ROOT_PASSWORD=pswd'
    network_mode: "host"
  oblogproxy:
    image: whhe/oblogproxy:1.0.3
    container_name: oblogproxy
    environment:
      - 'OB_SYS_USERNAME=root'
      - 'OB_SYS_PASSWORD=pswd'
    network_mode: "host"
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

Execute the following command in the directory where `docker-compose.yml` is located.

```shell
docker-compose up -d
```

### Create data for reading snapshot

Open the CLI:

```shell
docker-compose exec observer obclient -h127.0.0.1 -P2881 -uroot -ppswd
```

Insert data:

```sql
CREATE DATABASE ob;
USE ob;

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

### Download the libraries required

```Download links are only available for stable releases.```

- [flink-sql-connector-elasticsearch7-1.16.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/1.16.0/flink-sql-connector-elasticsearch7-1.16.0.jar)
- [flink-sql-connector-oceanbase-cdc-2.3.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.3.0/flink-sql-connector-oceanbase-cdc-2.3.0.jar)

### Use Flink DDL to create dynamic table in Flink SQL CLI

```sql
-- checkpoint every 3000 milliseconds                     
Flink SQL> SET execution.checkpointing.interval = 3s;

-- set local time zone as Asia/Shanghai
Flink SQL> SET table.local-time-zone = Asia/Shanghai;

-- create orders table
Flink SQL> CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
    'connector' = 'oceanbase-cdc',
    'scan.startup.mode' = 'initial',
    'username' = 'root',
    'password' = 'pswd',
    'tenant-name' = 'sys',
    'database-name' = '^ob$',
    'table-name' = '^orders$',
    'hostname' = 'localhost',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = 'localhost',
    'logproxy.port' = '2983',
    'working-mode' = 'memory'
 );

-- create products table
Flink SQL> CREATE TABLE products (
    id INT,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED
  ) WITH (
    'connector' = 'oceanbase-cdc',
    'scan.startup.mode' = 'initial',
    'username' = 'root',
    'password' = 'pswd',
    'tenant-name' = 'sys',
    'database-name' = '^ob$',
    'table-name' = '^products$',
    'hostname' = 'localhost',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = 'localhost',
    'logproxy.port' = '2983',
    'working-mode' = 'memory'
 );

-- create flat table enriched_orders
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
     'index' = 'enriched_orders');

-- Start the reading and writing job
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

### Check data on Kibana

Open  [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) and create index pattern `enriched_orders`, then go to [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover), and you will see the data of `enriched_orders`.

### Check data changes

Execute the following sql in OceanBase under `ob` database, you will find records in Kibana be updated after each step in real time.

```sql
INSERT INTO orders VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);
UPDATE orders SET order_status = true WHERE order_id = 10004;
DELETE FROM orders WHERE order_id = 10004;
```

### Clean up

Execute the following command to stop all containers in the directory where `docker-compose.yml` is located.

```shell
docker-compose down
```

Stop the flink cluster by following command.

```shell
./bin/stop-cluster.sh
```
