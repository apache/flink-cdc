# Demo: SqlServer CDC to Elasticsearch

**Create `docker-compose.yml` file using following contents:**

```
version: '2.1'
services:
   sqlserver:
     image: mcr.microsoft.com/mssql/server:2019-latest
     container_name: sqlserver
     ports:
       - "1433:1433"
     environment:
       - "MSSQL_AGENT_ENABLED=true"
       - "MSSQL_PID=Standard"
       - "ACCEPT_EULA=Y"
       - "SA_PASSWORD=Password!"
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
The Docker Compose environment consists of the following containers:
- SqlServer: SqlServer database.
- Elasticsearch: store the join result of the `orders` and `products` table.
- Kibana: mainly used to visualize the data in Elasticsearch.

To start all containers, run the following command in the directory that contains the docker-compose.yml file:
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode.
Run docker ps to check whether these containers are running properly. You can also visit http://localhost:5601/ to see if Kibana is running normally.

Don’t forget to run the following command to stop and remove all containers after you finished the tutorial:

```shell
docker-compose down
````

**Download following JAR package to `<FLINK_HOME>/lib`:**

**Download links are available only for stable releases, SNAPSHOT dependencies need to be built based on master or release- branches by yourself.**

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-sqlserver-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-sqlserver-cdc/3.0-SNAPSHOT/flink-sql-connector-sqlserver-cdc-3.0-SNAPSHOT.jar)


**Preparing data in SqlServer database**

Create databases/tables and populate data

 ```sql
 -- Sqlserver
 CREATE DATABASE inventory;
 GO
 USE inventory;
 EXEC sys.sp_cdc_enable_db;
 
 -- Create and populate our products using a single insert with many rows
 CREATE TABLE products (
 id INTEGER IDENTITY(101,1) NOT NULL PRIMARY KEY,
 name VARCHAR(255) NOT NULL,
 description VARCHAR(512),
 weight FLOAT
 );
 INSERT INTO products(name,description,weight)
 VALUES ('scooter','Small 2-wheel scooter',3.14);
 INSERT INTO products(name,description,weight)
 VALUES ('car battery','12V car battery',8.1);
 INSERT INTO products(name,description,weight)
 VALUES ('12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
 INSERT INTO products(name,description,weight)
 VALUES ('hammer','12oz carpenter''s hammer',0.75);
 INSERT INTO products(name,description,weight)
 VALUES ('hammer','14oz carpenter''s hammer',0.875);
 INSERT INTO products(name,description,weight)
 VALUES ('hammer','16oz carpenter''s hammer',1.0);
 INSERT INTO products(name,description,weight)
 VALUES ('rocks','box of assorted rocks',5.3);
 INSERT INTO products(name,description,weight)
 VALUES ('jacket','water resistent black wind breaker',0.1);
 INSERT INTO products(name,description,weight)
 VALUES ('spare tire','24 inch spare tire',22.2);
 EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0;
 -- Create some very simple orders
 CREATE TABLE orders (
 id INTEGER IDENTITY(10001,1) NOT NULL PRIMARY KEY,
 order_date DATE NOT NULL,
 purchaser INTEGER NOT NULL,
 quantity INTEGER NOT NULL,
 product_id INTEGER NOT NULL,
 FOREIGN KEY (product_id) REFERENCES products(id)
 );
 INSERT INTO orders(order_date,purchaser,quantity,product_id)
 VALUES ('16-JAN-2016', 1001, 1, 102);
 INSERT INTO orders(order_date,purchaser,quantity,product_id)
 VALUES ('17-JAN-2016', 1002, 2, 105);
 INSERT INTO orders(order_date,purchaser,quantity,product_id)
 VALUES ('19-FEB-2016', 1002, 2, 106);
 INSERT INTO orders(order_date,purchaser,quantity,product_id)
 VALUES ('21-FEB-2016', 1003, 1, 107);
 EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0;
 GO
 ```
**Launch a Flink cluster and start a Flink SQL CLI:**

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
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'table-name' = 'dbo.products'
  );

Flink SQL> CREATE TABLE orders (
   id INT,
   order_date DATE,
   purchaser INT,
   quantity INT,
   product_id INT,
   PRIMARY KEY (id) NOT ENFORCED
 ) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = 'localhost',
    'port' = '1433',
    'username' = 'sa',
    'password' = 'Password!',
    'database-name' = 'inventory',
    'table-name' = 'dbo.orders'
);

Flink SQL> CREATE TABLE enriched_orders (
   order_id INT,
   order_date DATE,
   purchaser INT,
   quantity INT,
   product_name STRING,
   product_description STRING,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_orders_1'
 );

Flink SQL> INSERT INTO enriched_orders
  SELECT o.id,o.order_date,o.purchaser,o.quantity, p.name, p.description
  FROM orders AS o
  LEFT JOIN products AS p ON o.product_id = p.id;
```

**Check result in Elasticsearch**

Check the data has been written to Elasticsearch successfully, you can visit [Kibana](http://localhost:5601/) to see the data.

**Make changes in SqlServer and watch result in Elasticsearch**

Do some changes in the databases, and then the enriched orders shown in Kibana will be updated after each step in real time.

```sql
INSERT INTO orders(order_date,purchaser,quantity,product_id) VALUES ('22-FEB-2016', 1006, 22, 107);
GO

UPDATE orders SET quantity = 11 WHERE id = 10001;
GO

DELETE FROM orders WHERE id = 10004;
GO
```
