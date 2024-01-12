# 演示: TiDB CDC 导入 Elasticsearch

**首先我们得通过 docker 来启动 TiDB 集群。**

```shell
$ git clone https://github.com/pingcap/tidb-docker-compose.git
```
**其次替换目录 `tidb-docker-compose` 里面的 `docker-compose.yml` 文件，内容如下所示：**

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
该 Docker Compose 中包含的容器有：
- TiDB 集群: tikv、pd、tidb。
- Elasticsearch：`orders` 表将和 `products` 表进行 join，join 的结果写入 Elasticsearch 中。
- Kibana：可视化 Elasticsearch 中的数据。

本机添加 host 映射 `pd` 和 `tikv` 映射 `127.0.0.1`。
在 docker-compose.yml 所在目录下运行如下命令以启动所有容器：
```shell
docker-compose up -d
mysql -h 127.0.0.1 -P 4000 -u root # Just test tidb cluster is ready,if you have install mysql local.
```
该命令会以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。
你可以通过 docker ps 来观察上述的容器是否正常启动了。 也可以访问 http://localhost:5601/ 来查看 Kibana 是否运行正常。

另外可以通过如下命令停止并删除所有的容器：

```shell
docker-compose down
````

**下载以下 jar 包到 `<FLINK_HOME>/lib/`：**

```下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地基于 master 或 release- 分支编译```

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-tidb-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-tidb-cdc/3.0-SNAPSHOT/flink-sql-connector-tidb-cdc-3.0-SNAPSHOT.jar)


**在 TiDB 数据库中准备数据**

创建数据库和表 `products`，`orders`，并插入数据：

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
**然后启动 Flink 集群，再启动 SQL CLI：**

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

**检查 ElasticSearch 中的结果**

检查最终的结果是否写入 ElasticSearch 中，可以在 [Kibana](http://localhost:5601/) 看到 ElasticSearch 中的数据。

**在 TiDB 制造一些变更，观察 ElasticSearch 中的结果**

通过如下的 SQL 语句对 TiDB 数据库进行一些修改，然后就可以看到每执行一条 SQL 语句，Elasticsearch 中的数据都会实时更新。

```sql
INSERT INTO orders
VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);

UPDATE orders SET order_status = true WHERE order_id = 10004;

DELETE FROM orders WHERE order_id = 10004;
```

