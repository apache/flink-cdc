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

# Demo: Oracle CDC to Elasticsearch

**Create `docker-compose.yml` file using following contents:**

```
version: '2.1'
services:
  oracle:
    image: goodboy008/oracle-19.3.0-ee:non-cdb
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
- Oracle: Oracle 19c database.
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

**Download following JAR package to `<FLINK_HOME>/lib`**

**Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release-branches by yourself.**

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-oracle-cdc-2.5-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oracle-cdc/2.5-SNAPSHOT/flink-sql-connector-oracle-cdc-2.5-SNAPSHOT.jar)


**Preparing data in Oracle database**

Introduce the tables in Oracle:
```shell
docker-compose exec oracle sqlplus debezium/dbz@localhost:1521/ORCLCDB
```
```sql
BEGIN
EXECUTE IMMEDIATE 'DROP TABLE DEBEZIUM.PRODUCTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;
/

CREATE TABLE DEBEZIUM.PRODUCTS (
  ID NUMBER(9, 0) NOT NULL,
  NAME VARCHAR(255) NOT NULL,
  DESCRIPTION VARCHAR(512),
  WEIGHT FLOAT,
  PRIMARY KEY(ID)
);

BEGIN
EXECUTE IMMEDIATE 'DROP TABLE DEBEZIUM.ORDERS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;
/

CREATE TABLE DEBEZIUM.ORDERS (
  ID NUMBER(9, 0) NOT NULL,
  ORDER_DATE TIMESTAMP(3) NOT NULL,
  PURCHASER VARCHAR(255) NOT NULL,
  QUANTITY NUMBER(9, 0) NOT NULL,
  PRODUCT_ID NUMBER(9, 0) NOT NULL,
  PRIMARY KEY(ID)
);

ALTER TABLE DEBEZIUM.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE DEBEZIUM.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.PRODUCTS VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (102, 'car battery', '12V car battery', 8.1);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (104, 'hammer', '12oz carpenter''s hammer', 0.75);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (105, 'hammer', '14oz carpenter''s hammer', 0.875);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (106, 'hammer', '16oz carpenter''s hammer', 1.0);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (107, 'rocks', 'box of assorted rocks', 5.3);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (108, 'jacket', 'water resistent black wind breaker', 0.1);
INSERT INTO DEBEZIUM.PRODUCTS VALUES (109, 'spare tire', '24 inch spare tire', 22.2);

INSERT INTO DEBEZIUM.ORDERS VALUES (1001, TO_TIMESTAMP('2020-07-30 10:08:22.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Jark', 1, 101);
INSERT INTO DEBEZIUM.ORDERS VALUES (1002, TO_TIMESTAMP('2020-07-30 10:11:09.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Sally', 2, 102);
INSERT INTO DEBEZIUM.ORDERS VALUES (1003, TO_TIMESTAMP('2020-07-30 12:00:30.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Edward', 2, 103);
INSERT INTO DEBEZIUM.ORDERS VALUES (1004, TO_TIMESTAMP('2020-07-30 15:22:00.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Jark', 1, 104);
```

**Launch a Flink cluster and start a Flink SQL CLI**

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
    'username' = 'dbzuser',
    'password' = 'dbz',
    'database-name' = 'ORCLCDB',
    'schema-name' = 'DEBEZIUM',  
    'table-name' = 'products'
  );

Flink SQL> CREATE TABLE orders (
   ID INT,
   ORDER_DATE TIMESTAMP(3),
   PURCHASER STRING,
   QUANTITY INT,
   PRODUCT_ID INT,
   ORDER_STATUS BOOLEAN
 ) WITH (
   'connector' = 'oracle-cdc',
   'hostname' = 'localhost',
   'port' = '1521',
   'username' = 'dbzuser',
   'password' = 'dbz',
   'database-name' = 'ORCLCDB',
   'schema-name' = 'DEBEZIUM',  
   'table-name' = 'orders'
 );

Flink SQL> CREATE TABLE enriched_orders (
   ORDER_ID INT,
   ORDER_DATE TIMESTAMP(3),
   PURCHASER STRING,
   QUANTITY INT,
   PRODUCT_NAME STRING,
   PRODUCT_DESCRIPTION STRING,
   PRIMARY KEY (ORDER_ID) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders_1'
 );

Flink SQL> INSERT INTO enriched_orders
 SELECT o.ID,o.ORDER_DATE,o.PURCHASER,o.QUANTITY, p.NAME, p.DESCRIPTION
 FROM orders AS o
 LEFT JOIN products AS p ON o.PRODUCT_ID = p.ID;
```

**Check result in Elasticsearch**

Check the data has been written to Elasticsearch successfully, you can visit [Kibana](http://localhost:5601/) to see the data.

**Make changes in Oracle and watch result in Elasticsearch**

Enter Oracle's container to make some changes in Oracle, then you can see the result in Elasticsearch will change after executing every SQL statement:

```shell
docker-compose exec oracle sqlplus debezium/dbz@localhost:1521/ORCLCDB
```

```sql
INSERT INTO DEBEZIUM.ORDERS VALUES (1005, TO_TIMESTAMP('2020-07-30 15:22:00.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Jark', 5, 105);

UPDATE DEBEZIUM.ORDERS SET QUANTITY = 10 WHERE ID = 1002;

DELETE FROM DEBEZIUM.ORDERS WHERE ID = 1004;
```