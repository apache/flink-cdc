# Demo: Db2 CDC to Elasticsearch

**1. Create `docker-compose.yml` file using following contents:**

```
version: '2.1'
services:
  db2:
    image: ruanhang/db2-cdc-demo:v1
    privileged: true
    ports:
      - 50000:50000
    environment: 
      - LICENSE=accept
      - DB2INSTANCE=db2inst1
      - DB2INST1_PASSWORD=admin
      - DBNAME=testdb    
      - ARCHIVE_LOGS=true
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
- Db2: db2 server and a pre-populated `products` table in the database `testdb`.
- Elasticsearch: store the result of the `products` table.
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

**2. Download following JAR package to `<FLINK_HOME>/lib`**

*Download links are available only for stable releases, SNAPSHOT dependency need build by yourself. *

- [flink-sql-connector-elasticsearch7-3.0.1-1.17.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar)
- [flink-sql-connector-db2-cdc-2.4.0.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-db2-cdc/2.4.0/flink-sql-connector-db2-cdc-2.4.0.jar)

**3. Launch a Flink cluster and start a Flink SQL CLI**

Execute following SQL statements in the Flink SQL CLI:

```sql
-- Flink SQL
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET execution.checkpointing.interval = 3s;

Flink SQL> CREATE TABLE products (
    ID INT NOT NULL,
    NAME STRING,
    DESCRIPTION STRING,
    WEIGHT DECIMAL(10,3),
    PRIMARY KEY (ID) NOT ENFORCED
  ) WITH (
    'connector' = 'db2-cdc',
    'hostname' = 'localhost',
    'port' = '50000',
    'username' = 'db2inst1',
    'password' = 'admin',
    'database-name' = 'testdb',
    'schema-name' = 'DB2INST1',  
    'table-name' = 'PRODUCTS'
  );
  
Flink SQL> CREATE TABLE es_products (
    ID INT NOT NULL,
    NAME STRING,
    DESCRIPTION STRING,
    WEIGHT DECIMAL(10,3),
    PRIMARY KEY (ID) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'enriched_products_1'
 );

Flink SQL> INSERT INTO es_products SELECT * FROM products;
```

**4. Check result in Elasticsearch**

Check the data has been written to Elasticsearch successfully, you can visit [Kibana](http://localhost:5601/) to see the data.

**5. Make changes in Db2 and watch result in Elasticsearch**

Enter Db2's container to make some changes in Db2, then you can see the result in Elasticsearch will change after 
executing every SQL statement:
```shell
docker exec -it ${containerId} /bin/bash

su db2inst1

db2 connect to testdb

# enter db2 and execute sqls
db2
```

```sql
UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106;

INSERT INTO DB2INST1.PRODUCTS VALUES (default,'jacket','water resistent white wind breaker',0.2);

INSERT INTO DB2INST1.PRODUCTS VALUES (default,'scooter','Big 2-wheel scooter ',5.18);

DELETE FROM DB2INST1.PRODUCTS WHERE ID=111;
```