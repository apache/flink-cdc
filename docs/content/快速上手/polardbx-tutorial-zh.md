# 演示: PolarDB-X CDC 导入 Elasticsearch

本示例我们通过演示 PolarDB-X 借助 Flink-CDC 将数据导入至 Elasticsearch 来介绍 PolarDB-X 的增量订阅能力，你可以前往:[PolarDB-X](https://github.com/ApsaraDB/galaxysql) 了解更多细节。

### 准备教程所需要的组件
我们假设你运行在一台 MacOS 或者 Linux 机器上，并且已经安装 docker.

#### 配置并启动容器

配置 `docker-compose.yml`。

```yaml
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
该 Docker Compose 中包含的容器有：
- PolarDB-X: 商品表 `products` 和 订单表 `orders` 将存储在该数据库中， 这两张表将进行关联，得到一张包含更多信息的订单表 `enriched_orders`
- Elasticsearch: 最终的订单表 `enriched_orders` 将写到 Elasticsearch
- Kibana: 用来可视化 ElasticSearch 的数据

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：

```shell
docker-compose up -d
```
该命令将以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。你可以通过 docker ps 来观察上述的容器是否正常启动了，也可以通过访问 [http://localhost:5601/](http://localhost:5601/) 来查看 Kibana 是否运行正常


### 准备数据：

使用已创建的用户名和密码进行登陆PolarDB-X。

```shell
mysql -h127.0.0.1 -P8527 -upolardbx_root -p"123456"
```

```sql
CREATE DATABASE mydb;
USE mydb;

-- 创建一张产品表，并写入一些数据
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


-- 创建一张订单表，并写入一些数据
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

### 下载 Flink 和所需要的依赖包
1. 下载 [Flink 1.13.2](https://archive.apache.org/dist/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.11.tgz) 并将其解压至目录 `flink-1.13.2`
2. 下载下面列出的依赖包，并将它们放到目录 `flink-1.13.2/lib/` 下

```下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地编译```
- 用于订阅PolarDB-X Binlog: [flink-sql-connector-mysql-cdc-2.2.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.2.0/flink-sql-connector-mysql-cdc-2.2.0.jar)
- 用于写入Elasticsearch: [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
3. 启动flink服务:
```shell
./bin/start-cluster.sh
```

我们可以访问 [http://localhost:8081/](http://localhost:8081/) 看到Flink正常运行:

![Flink UI](/_static/fig/mysql-postgress-tutorial/flink-ui.png "Flink UI")


4. 启动Flink SQL CLI:
```shell
./bin/sql-client.sh
```

### 在 Flink SQL CLI 中使用 Flink DDL 创建表


```sql
-- 设置间隔时间为3秒                       
Flink SQL> SET execution.checkpointing.interval = 3s;

-- 创建source1 -订单表
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

-- 创建source2 -产品表
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

-- 创建sink - 关联后的结果表
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

-- 执行读取和写入   
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

### 在 Kibana 中查看数据

访问  [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) 

创建 index pattern `enriched_orders`，之后可以在 [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover) 看到写入的数据了。

### 修改监听表数据，查看增量数据变动

在PolarDB-X中依次执行如下修改操作，每执行一步就刷新一次 Kibana，可以看到 Kibana 中显示的订单数据将实时更新。

```sql
INSERT INTO orders VALUES (default, '2020-07-30 15:22:00', 'Jark', 29.71, 104, false);

UPDATE orders SET order_status = true WHERE order_id = 10004;

DELETE FROM orders WHERE order_id = 10004;
```

### 环境清理

在 `docker-compose.yml` 文件所在的目录下执行如下命令停止所有容器：

```shell
docker-compose down
```

进入Flink的部署目录，停止 Flink 集群：

```shell
./bin/stop-cluster.sh
```
