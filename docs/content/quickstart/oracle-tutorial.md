# Demo: Oracle CDC to Elasticsearch

**1. Create `docker-compose.yml` file using following contents:**

```
version: '2.1'
services:
  oracle:
    image: yuxialuo/oracle-xe-11g-r2-cdc-demo:v1.0
    ports:
      - "1521:1521"
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
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```
The Docker Compose environment consists of the following containers:
- Oracle: Oracle 11g and a pre-populated `products` and `orders` table in the database.
- Elasticsearch: store the join result of the `orders` and `products` table.
- Kibana: mainly used to visualize the data in Elasticsearch

To start all containers, run the following command in the directory that contains the docker-compose.yml file.
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. 
Run docker ps to check whether these containers are running properly. You can also visit http://localhost:5601/ to see if Kibana is running normally.

Don’t forget to run the following command to stop all containers after you finished the tutorial:
```shell
docker-compose down
```

**2. Download following JAR package to `<FLINK_HOME>/lib`**

*Download links are available only for stable releases, SNAPSHOT dependency need build by yourself. *

- [flink-sql-connector-elasticsearch7-1.16.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/1.16.0/flink-sql-connector-elasticsearch7-1.16.0.jar)
- [flink-sql-connector-oracle-cdc-2.3.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oracle-cdc/2.3.0/flink-sql-connector-oracle-cdc-2.3.0.jar)

**3. Launch a Flink cluster and start a Flink SQL CLI**

Execute following SQL statements in the Flink SQL CLI:

```sql
-- Flink SQL
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET execution.checkpointing.interval = 3s;

Flink SQL> CREATE TABLE products (
    ID INT,
    NAME STRING,
    DESCRIPTION STRING,
    PRIMARY KEY (ID) NOT ENFORCED
  ) WITH (
    'connector' = 'oracle-cdc',
    'hostname' = 'localhost',
    'port' = '1521',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'XE',
    'schema-name' = 'flinkuser',  
    'table-name' = 'products'
  );

Flink SQL> CREATE TABLE orders (
   ORDER_ID INT,
   ORDER_DATE TIMESTAMP_LTZ(3),
   CUSTOMER_NAME STRING,
   PRICE DECIMAL(10, 5),
   PRODUCT_ID INT,
   ORDER_STATUS BOOLEAN
 ) WITH (
   'connector' = 'oracle-cdc',
   'hostname' = 'localhost',
   'port' = '1521',
   'username' = 'flinkuser',
   'password' = 'flinkpw',
   'database-name' = 'XE',
   'schema-name' = 'flinkuser',  
   'table-name' = 'orders'
 );

Flink SQL> CREATE TABLE enriched_orders (
   ORDER_ID INT,
   ORDER_DATE TIMESTAMP_LTZ(3),
   CUSTOMER_NAME STRING,
   PRICE DECIMAL(10, 5),
   PRODUCT_ID INT,
   ORDER_STATUS BOOLEAN,
   PRODUCT_NAME STRING,
   PRODUCT_DESCRIPTION STRING,
   PRIMARY KEY (ORDER_ID) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders_1'
 );

Flink SQL> INSERT INTO enriched_orders
 SELECT o.*, p.NAME, p.DESCRIPTION
 FROM orders AS o
 LEFT JOIN products AS p ON o.PRODUCT_ID = p.ID;
```

**4. Check result in Elasticsearch**

Check the data has been written to Elasticsearch successfully, you can visit [Kibana](http://localhost:5601/) to see the data.

**5. Make changes in Oracle and watch result in Elasticsearch**

Enter Oracle's container to make some changes in Oracle, then you can see the result in Elasticsearch will change after executing every SQL statement:

```shell
docker-compose exec sqlplus flinkuser/flinkpw
```

```sql
INSERT INTO flinkuser.orders VALUES (10004, to_date('2020-07-30 15:22:00', 'yyyy-mm-dd hh24:mi:ss'), 'Jark', 29.71, 104, 0);

UPDATE flinkuser.orders SET ORDER_STATUS = 1 WHERE ORDER_ID = 10004;

DELETE FROM flinkuser.orders WHERE ORDER_ID = 10004;
```