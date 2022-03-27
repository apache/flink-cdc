# 演示: Oracle CDC 导入 Elasticsearch

**1. 创建`docker-compose.yml`文件，内容如下所示:**

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
该 Docker Compose 中包含的容器有:
- Oracle: Oracle 11g, 已经预先创建了 `products` 和 `orders`表，并插入了一些数据. 
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

**2. 下载以下 jar 包到 `<FLINK_HOME>/lib/`:**

*下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地编译*

- [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
- [flink-sql-connector-oracle-cdc-2.2.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oracle-cdc/2.2.0/flink-sql-connector-oracle-cdc-2.2.0.jar)

**3. 然后启动 Flink 集群，再启动 SQL CLI:**

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

**4. 检查 ElasticSearch 中的结果**

检查最终的结果是否写入ElasticSearch中, 可以在[Kibana](http://localhost:5601/)看到ElasticSearch中的数据

**5. 在 Oracle 制造一些变更，观察 ElasticSearch 中的结果**

进入Oracle容器中并通过如下的SQL语句对Oracle数据库进行一些修改, 然后就可以看到每执行一条SQL语句，Elasticsearch中的数据都会实时更新。

```shell
docker-compose exec sqlplus flinkuser/flinkpw
```

```sql
INSERT INTO flinkuser.orders VALUES (10004, to_date('2020-07-30 15:22:00', 'yyyy-mm-dd hh24:mi:ss'), 'Jark', 29.71, 104, 0);

UPDATE flinkuser.orders SET ORDER_STATUS = 1 WHERE ORDER_ID = 10004;

DELETE FROM flinkuser.orders WHERE ORDER_ID = 10004;
```