# 演示: SqlServer CDC 导入 Elasticsearch

**创建 `docker-compose.yml` 文件，内容如下所示：**

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
该 Docker Compose 中包含的容器有：
- SqlServer：SqlServer 数据库。
- Elasticsearch：`orders` 表将和 `products` 表进行 join，join 的结果写入 Elasticsearch 中。
- Kibana：可视化 Elasticsearch 中的数据。

在 docker-compose.yml 所在目录下运行如下命令以启动所有容器：
```shell
docker-compose up -d
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
- [flink-sql-connector-sqlserver-cdc-3.0-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-sqlserver-cdc/3.0-SNAPSHOT/flink-sql-connector-sqlserver-cdc-3.0-SNAPSHOT.jar)


**在 SqlServer 数据库中准备数据**

创建数据库和表 `products`，`orders`，并插入数据：

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

**检查 ElasticSearch 中的结果**

检查最终的结果是否写入 ElasticSearch 中，可以在 [Kibana](http://localhost:5601/) 看到 ElasticSearch 中的数据。

**在 SqlServer 制造一些变更，观察 ElasticSearch 中的结果**

通过如下的 SQL 语句对 SqlServer 数据库进行一些修改，然后就可以看到每执行一条 SQL 语句，Elasticsearch 中的数据都会实时更新。

```sql
INSERT INTO orders(order_date,purchaser,quantity,product_id) VALUES ('22-FEB-2016', 1006, 22, 107);
GO

UPDATE orders SET quantity = 11 WHERE id = 10001;
GO

DELETE FROM orders WHERE id = 10004;
GO
```
