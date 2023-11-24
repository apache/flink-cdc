# 演示: Oracle CDC 导入 Elasticsearch

**创建`docker-compose.yml`文件，内容如下所示:**

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
该 Docker Compose 中包含的容器有:
- Oracle: Oracle 19c 数据库
- Elasticsearch: `orders` 表将和 `products` 表进行join，join的结果写入Elasticsearch中
- Kibana: 可视化 Elasticsearch 中的数据

在 docker-compose.yml 所在目录下运行如下命令以启动所有容器：
```shell
docker-compose up -d
```
该命令会以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。
你可以通过 docker ps 来观察上述的容器是否正常启动了。 也可以访问 http://localhost:5601/ 来查看 Kibana 是否运行正常。
另外可以通过如下命令停止所有的容器：

```shell
docker-compose down
````

**下载以下 jar 包到 `<FLINK_HOME>/lib/`:**

*下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地编译*

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-oracle-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oracle-cdc/2.4.0/flink-sql-connector-oracle-cdc-2.4.0.jar)


**在 Oracle 数据库中准备数据**

创建数据库和表 `products`，`orders`，并插入数据：

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

**然后启动 Flink 集群，再启动 SQL CLI:**

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

**检查 ElasticSearch 中的结果**

检查最终的结果是否写入ElasticSearch中, 可以在[Kibana](http://localhost:5601/)看到ElasticSearch中的数据

**在 Oracle 制造一些变更，观察 ElasticSearch 中的结果**

进入Oracle容器中并通过如下的SQL语句对Oracle数据库进行一些修改, 然后就可以看到每执行一条SQL语句，Elasticsearch中的数据都会实时更新。

```shell
docker-compose exec oracle sqlplus debezium/dbz@localhost:1521/ORCLCDB
```

```sql
INSERT INTO DEBEZIUM.ORDERS VALUES (1005, TO_TIMESTAMP('2020-07-30 15:22:00.001000', 'YYYY-MM-DD HH24:MI:SS.FF'), 'Jark', 5, 105);

UPDATE DEBEZIUM.ORDERS SET QUANTITY = 10 WHERE ID = 1002;

DELETE FROM DEBEZIUM.ORDERS WHERE ID = 1004;
```