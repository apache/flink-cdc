# Streaming ETL for sharding table with Flink CDC
For OLTP databases, to deal with a huge number of data in a single table, we usually do database and table sharding to get faster performance. But sometimes, we need to load them into one table for convenient analysis.

This tutorial will show how to use Flink CDC to do streaming ETL for such a scenario.
The following sections will take the pipeline from MySQL to ElasticSearch as an example.
The overview of the architecture is as follows:

![Flink CDC Streaming ETL with sharding tables](/_static/fig/working-with-sharding-table-tutorial/work-with-sharding-table-architecture.png "architecture of working with sharding tables")

You can also use other data sources and sinks to construct your own pipeline.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.
### Starting components required
The components required in this tutorial are all managed in containers, so we will use `docker-compose` to start them.

Create `docker-compose.yml` file using following contents:
```
version: '2.1'
services:
  mysql:
    image: debezium/example-mysql:1.1
    ports:
      - "3306:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
      - MYSQL_USER=mysqluser
      - MYSQL_PASSWORD=mysqlpw
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
The Docker Compose environment consists of the following containers:
- MySQL: mainly used as a data source to store the sharding tables.
- Elasticsearch: mainly used as a data sink.
- Kibana: used to visualize the data in Elasticsearch.

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file.
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run `docker ps` to check whether these containers are running properly.
We can also visit [http://localhost:5601/](http://localhost:5601/) to see if Kibana is running normally.

### Preparing Flink and JAR package required
1. Download [Flink 1.13.2](https://downloads.apache.org/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.11.tgz) and unzip it to the directory `flink-1.13.2`
2. Download following JAR package required and put them under `flink-1.13.2/lib/`:

   **Download links are available only for stable releases.**
    - [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
    - [flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.1-SNAPSHOT/flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar)

### Preparing data in databases
1. Enter mysql's container:
    ```shell
    docker-compose exec mysql mysql -uroot -p123456
    ```
2. Create databases/tables and populate data: 
   
   create a logical sharding `user` table sharded in different databases and tables physically, and the tables' columns aren't identical exactly.
   ```sql
    CREATE DATABASE user_db_1;
    USE user_db_1;
    CREATE TABLE user_table_1_1 (
      id INTEGER NOT NULL PRIMARY KEY,
      name VARCHAR(255) NOT NULL DEFAULT 'flink',
      address VARCHAR(1024),
      phone_number VARCHAR(512),
      email VARCHAR(255)
    );
    INSERT INTO user_table_1_1 VALUES (11100,"user_11100","Shanghai","123567891234","user_11100@foo.com");
   
    CREATE TABLE user_table_1_2 (
      id INTEGER NOT NULL PRIMARY KEY,
      name VARCHAR(255) NOT NULL DEFAULT 'flink',
      address VARCHAR(1024),
      phone_number VARCHAR(512)
    );
   INSERT INTO user_table_1_2 VALUES (12100,"user_12100","Shanghai","123567891234");
   ```
   ```sql
   CREATE DATABASE user_db_2;
   USE user_db_2;
   CREATE TABLE user_table_2_1 (
     id INTEGER NOT NULL PRIMARY KEY,
     name VARCHAR(255) NOT NULL DEFAULT 'flink',
     address VARCHAR(1024),
     phone_number VARCHAR(512)
   );
   INSERT INTO user_table_2_1 VALUES (21100,"user_21100","Shanghai","123567891234");

   CREATE TABLE user_table_2_2 (
     id INTEGER NOT NULL PRIMARY KEY,
     name VARCHAR(255) NOT NULL DEFAULT 'flink',
     address VARCHAR(1024),
     phone_number VARCHAR(512),
     age INTEGER
   );
   INSERT INTO user_table_2_2 VALUES (221,"user_221","Shanghai","123567891234", 18);
   ```
## Starting Flink cluster and Flink SQL CLI

1. Use the following command to change to the Flink directory:
    ```
    cd flink-1.13.2
    ```
2. Use the following command to start a Flink cluster:
    ```shell
    ./bin/start-cluster.sh
    ```
   Then we can visit [http://localhost:8081/](http://localhost:8081/) to see if Flink is running normally, and the web page looks like:

   ![Flink UI](/_static/fig/working-with-sharding-table-tutorial/flink-ui.png "Flink UI")

3. Use the following command to start a Flink SQL CLI:
    ```shell
    ./bin/sql-client.sh
    ```
   We should see the welcome screen of the CLI client.

   ![Flink SQL Client](/_static/fig/working-with-sharding-table-tutorial/flink-sql-client.png  "Flink SQL Client" )

## Creating tables using Flink DDL in Flink SQL CLI
First, enable checkpoints every 3 seconds
```sql
-- Flink SQL                   
Flink SQL> SET execution.checkpointing.interval = 3s;
```

Then, create a table that captures the change data from the sharding `user` table. Here, we use regex to match all the tables physically. 
The table `user_source` contains all the columns, and the column's value will be null if the record from the underlying table misses the column. 
Also, the table defines metadata column to identify which database/table the record comes from.
```sql
-- Flink SQL
Flink SQL> CREATE TABLE user_source (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA VIRTUAL,
    `id` DECIMAL(20, 0) NOT NULL,
    name STRING,
    address STRING,
    phone_number STRING,
    email STRING,
    age INT,
    name STRING,
    primary key (`id`) not enforced
  ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'user_db_.*',
    'table-name' = 'user_table_.*'
  );
```

Finally, create `all_users_sink` table used to load data to the Elasticsearch.
```sql
-- Flink SQL
Flink SQL> CREATE TABLE all_users_sink (
    database_name STRING,
    table_name STRING,
    `id` DECIMAL(20, 0) NOT NULL,
    name STRING,
    address STRING,
    phone_number STRING,
    email STRING,
    age INT,
    name STRING,
    primary key (database_name, table_name, `id`) not enforced
  ) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://localhost:9200',
    'index' = 'users'  
  );
```
## Load to ElasticSearch
Use Flink SQL to load data from MySQL to ElasticSearch
```sql
-- Flink SQL
Flink SQL> INSERT INTO all_users_sink select * from user_source;
```
Now, the user data from all tables should be shown in Kibana.
Visit [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) to create an index pattern `users`.

![Create Index Pattern](/_static/fig/working-with-sharding-table-tutorial/kibana-create-index-pattern.png "Create Index Pattern")

Visit [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover) to find the user data.

![Find user data](/_static/fig/working-with-sharding-table-tutorial/kibana-users.png "Find All User Data")

Next, do some change in the databases, and then the user data shown in Kibana will be updated after each step in real time.
1. Insert a new user in table `user_db_1.user_table_1_1`
   ```sql
   --- user_db_1
   INSERT INTO user_table_1_1 VALUES (11101,"user_11101","Shanghai","123567891234","user_11101@foo.com");
   ```

2. update a user in table `user_db_1.user_table_1_2`
   ```sql
   --- user_db_1
   UPDATE user_table_1_2 SET address='Beijing' WHERE id=12100;
   ```
   
3. delete a user in table `user_db_2.user_table_2_1`
   ```sql
   --- user_db_2
   DELETE FROM user_table_2_1 WHERE id=21100;
   ```
The changes of the user data in Kibana are as follows:
![User Data Changes](/_static/fig/working-with-sharding-table-tutorial/user_data_changes.gif "User Data Changes")

## Clean up
After finishing the tutorial, run the following command to stop all containers in the directory of `docker-compose.yml`:
```shell
docker-compose down
```
Run the following command to stop the Flink cluster in the directory of Flink `flink-1.13.2`:
```shell
./bin/stop-cluster.sh
```


