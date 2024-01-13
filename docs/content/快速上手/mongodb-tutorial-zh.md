# 演示: MongoDB CDC 导入 Elasticsearch

1. 下载 `docker-compose.yml`

```
version: '2.1'
services:
  mongo:
    image: "mongo:4.0-xenial"
    command: --replSet rs0 --smallfiles --oplogSize 128
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_ROOT_USERNAME=mongouser
      - MONGO_INITDB_ROOT_PASSWORD=mongopw
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
```

2. 进入 MongoDB 容器，初始化副本集和数据:
```
docker-compose exec mongo /usr/bin/mongo -u mongouser -p mongopw
```

```javascript
// 1. 初始化副本集
rs.initiate();
rs.status();

// 2. 切换数据库
use mgdb;

// 3. 初始化数据
db.orders.insertMany([
  {
    order_id: 101,
    order_date: ISODate("2020-07-30T10:08:22.001Z"),
    customer_id: 1001,
    price: NumberDecimal("50.50"),
    product: {
      name: 'scooter',
      description: 'Small 2-wheel scooter'
    },
    order_status: false
  },
  {
    order_id: 102, 
    order_date: ISODate("2020-07-30T10:11:09.001Z"),
    customer_id: 1002,
    price: NumberDecimal("15.00"),
    product: {
      name: 'car battery',
      description: '12V car battery'
    },
    order_status: false
  },
  {
    order_id: 103,
    order_date: ISODate("2020-07-30T12:00:30.001Z"),
    customer_id: 1003,
    price: NumberDecimal("25.25"),
    product: {
      name: 'hammer',
      description: '16oz carpenter hammer'
    },
    order_status: false
  }
]);

db.customers.insertMany([
  { 
    customer_id: 1001, 
    name: 'Jark', 
    address: 'Hangzhou' 
  },
  { 
    customer_id: 1002, 
    name: 'Sally',
    address: 'Beijing'
  },
  { 
    customer_id: 1003,
    name: 'Edward',
    address: 'Shanghai'
  }
]);
```

3. 下载以下 jar 包到 `<FLINK_HOME>/lib/`:

```下载链接只对已发布的版本有效, SNAPSHOT 版本需要本地基于 master 或 release- 分支编译```

 - [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
 - [flink-sql-connector-mongodb-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/3.0-SNAPSHOT/flink-sql-connector-mongodb-cdc-3.0-SNAPSHOT.jar)

4. 然后启动 Flink 集群，再启动 SQL CLI.

```sql
-- Flink SQL
-- 设置间隔时间为3秒                       
Flink SQL> SET execution.checkpointing.interval = 3s;

-- 设置本地时区为 Asia/Shanghai
Flink SQL> SET table.local-time-zone = Asia/Shanghai;

Flink SQL> CREATE TABLE orders (
   _id STRING,
   order_id INT,
   order_date TIMESTAMP_LTZ(3),
   customer_id INT,
   price DECIMAL(10, 5),
   product ROW<name STRING, description STRING>,
   order_status BOOLEAN,
   PRIMARY KEY (_id) NOT ENFORCED
 ) WITH (
   'connector' = 'mongodb-cdc',
   'hosts' = 'localhost:27017',
   'username' = 'mongouser',
   'password' = 'mongopw',
   'database' = 'mgdb',
   'collection' = 'orders'
 );
 
 Flink SQL> CREATE TABLE customers (
   _id STRING,
   customer_id INT,
   name STRING,
   address STRING,
   PRIMARY KEY (_id) NOT ENFORCED
 ) WITH (
   'connector' = 'mongodb-cdc',
   'hosts' = 'localhost:27017',
   'username' = 'mongouser',
   'password' = 'mongopw',
   'database' = 'mgdb',
   'collection' = 'customers'
 );

Flink SQL> CREATE TABLE enriched_orders (
   order_id INT,
   order_date TIMESTAMP_LTZ(3),
   customer_id INT,
   price DECIMAL(10, 5),
   product ROW<name STRING, description STRING>,
   order_status BOOLEAN,
   customer_name STRING,
   customer_address STRING,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders'
 );

Flink SQL> INSERT INTO enriched_orders
 SELECT o.order_id,
        o.order_date,
        o.customer_id,
        o.price,
        o.product,
        o.order_status,
        c.name,
        c. address
   FROM orders AS o
   LEFT JOIN customers AS c ON o.customer_id = c.customer_id;
```

5. 修改 MongoDB 里面的数据，观察 elasticsearch 里的结果。

```javascript
db.orders.insert({ 
  order_id: 104, 
  order_date: ISODate("2020-07-30T12:00:30.001Z"),
  customer_id: 1004,
  price: NumberDecimal("25.25"),
  product: { 
    name: 'rocks',
    description: 'box of assorted rocks'
  },
  order_status: false
});

db.customers.insert({ 
  customer_id: 1004,
  name: 'Jacob', 
  address: 'Shanghai' 
});

db.orders.updateOne(
  { order_id: 104 },
  { $set: { order_status: true } }
);

db.orders.deleteOne(
  { order_id : 104 }
);
```
