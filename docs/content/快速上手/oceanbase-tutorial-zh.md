# 演示: OceanBase CDC 导入 Elasticsearch

### 准备教程所需要的组件

#### 配置并启动容器

配置 `docker-compose.yml`。

```yaml
version: '2.1'
services:
  observer:
    image: whhe/obce-mini:3.1.2_CE
    container_name: observer
    environment:
      - 'OB_ROOT_PASSWORD=pswd'
    network_mode: "host"
  oblogproxy:
    image: whhe/oblogproxy:1.0.0
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

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：

```shell
docker-compose up -d
```

### 准备数据：

使用新创建的用户名和密码进行登陆。

```shell
docker-compose exec observer obclient -h127.0.0.1 -P2881 -uroot -ppswd
```

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

### 下载所需要的依赖包

```下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地编译```

- [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
- [flink-sql-connector-oceanbase-cdc-2.2.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-oceanbase-cdc/2.2.0/flink-sql-connector-oceanbase-cdc-2.2.0.jar)

### 在 Flink SQL CLI 中使用 Flink DDL 创建表

```sql
-- 设置间隔时间为3秒                       
Flink SQL> SET execution.checkpointing.interval = 3s;

-- 设置本地时区为 Asia/Shanghai
Flink SQL> SET table.local-time-zone = Asia/Shanghai;

-- 创建订单表
Flink SQL> CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status TINYINT,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
    'connector' = 'oceanbase-cdc',
    'scan.startup.mode' = 'initial',
    'username' = 'root',
    'password' = 'pswd',
    'tenant-name' = 'sys',
    'database-name' = 'ob',
    'table-name' = 'orders',
    'hostname' = 'localhost',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = 'localhost',
    'logproxy.port' = '2983');

-- 创建商品表 
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
    'database-name' = 'ob',
    'table-name' = 'products',
    'hostname' = 'localhost',
    'port' = '2881',
    'rootserver-list' = '127.0.0.1:2882:2881',
    'logproxy.host' = 'localhost',
    'logproxy.port' = '2983');

-- 创建关联后的订单数据表
Flink SQL> CREATE TABLE enriched_orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status TINYINT,
   product_name STRING,
   product_description STRING,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders');

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

访问  [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) 创建 index pattern `enriched_orders`，之后可以在 [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover) 看到写入的数据了。

### 修改监听表数据，查看增量数据变动

在OceanBase中依次执行如下修改操作，每执行一步就刷新一次 Kibana，可以看到 Kibana 中显示的订单数据将实时更新。

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
