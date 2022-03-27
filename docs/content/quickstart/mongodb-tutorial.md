# Demo: MongoDB CDC to Elasticsearch

1. Create `docker-compose.yml` file using following contents: 

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

2. Enter Mongodb's container and initialize replica set and data:
```
docker-compose exec mongo /usr/bin/mongo -u mongouser -p mongopw
```

```javascript
// 1. initialize replica set
rs.initiate();
rs.status();

// 2. switch database
use mgdb;

// 3. initialize data
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

3. Download following JAR package to `<FLINK_HOME>/lib/`:

```Download links are available only for stable releases, SNAPSHOT dependency need build by yourself. ```

 - [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
 - [flink-sql-connector-mongodb-cdc-2.2.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/2.2.0/flink-sql-connector-mongodb-cdc-2.2.0.jar)

4. Launch a Flink cluster, then start a Flink SQL CLI and execute following SQL statements inside: 

```sql
-- Flink SQL
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET execution.checkpointing.interval = 3s;

-- set local time zone as Asia/Shanghai
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

5. Make some changes in MongoDB, then check the result in Elasticsearch: 

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
