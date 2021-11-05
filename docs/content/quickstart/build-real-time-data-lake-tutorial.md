# Building a real-time data lake to synchronize data from MySQL sharding table with Flink CDC
For OLTP databases, to deal with a huge number of data in a single table, we usually do database and table sharding to get faster performance. 
But sometimes, for convenient analysis, we need to merge them into one table when loading them to data warehouse or data lake.

This tutorial will show how to use Flink CDC to build a real-time data lake for such a scenario.
You can walk through the tutorial easily for the environment is built with docker, and the entire process uses standard SQL syntax without a single line of Java/Scala code or IDE installation.

The following sections will take the pipeline from MySQL to Iceberg as an example. The overview of the architecture is as follows:

![Real-time data lake with Flink CDC](/_static/fig/real-time-data-lake-tutorial/real-time-data-lake-tutorial.png "architecture of real-time data lake")

You can also use other data sources like Oracle/Postgres and sinks like Hudi/Doris to build your own pipeline.

## Preparation
Prepare a Linux or MacOS computer with Docker installed.

### Starting components required
The components required in this tutorial are all managed in containers, so we will use `docker-compose` to start them.

Create `docker-compose.yml` file using following contents:
```
version: '2.1'
services:
  sql-client:
    image: yuxialuo/flink-sql-client:1.13.2.v1 
    depends_on:
      - jobmanager
      - mysql
    environment:
      FLINK_JOBMANAGER_HOST: jobmanager
      MYSQL_HOST: mysql
    volumes:
      - /Users/yuxia/demo/iceberg:/home/iceberg
  jobmanager:
    image: flink:1.13.2-scala_2.11
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - /Users/yuxia/demo/iceberg:/home/iceberg    
  taskmanager:
    image: flink:1.13.2-scala_2.11
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
    volumes:
      - /Users/yuxia/demo/iceberg:/home/iceberg    
  mysql:
    image: debezium/example-mysql:1.1
    ports:
      - "3306:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
      - MYSQL_USER=mysqluser
      - MYSQL_PASSWORD=mysqlpw
```

The Docker Compose environment consists of the following containers:
- SQL-Client: Flink SQL Client, used to submit queries and visualize their results.
- Flink Cluster: a Flink JobManager and a Flink TaskManager container to execute queries.
- MySQL: mainly used as a data source to store the sharding table.


***Note:***
1. To simply this tutorial, the jar packages required has been packaged into the SQL-Client container.
If you want to run with your own Flink environment, remember to download the following packages and then put them to `Flink_HOME/lib/`.
   
   **Download links are available only for stable releases.**
   - [flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.1-SNAPSHOT/flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar)
   - [iceberg-flink-runtime-0.12.0.jar](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/0.12.0/iceberg-flink-runtime-0.12.0.jar)
   - [flink-shaded-hadoop-2-uber-2.7.5-10.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar)
2. This tutorial will use the local filesystem as the backend of Iceberg. To make it can be accessed by the containers, 
   we mount the volume `/Users/yuxia/demo/iceberg` in the local filesystem into `/home/iceberg` for the containers in this `docker-compose.yml` file.
   You can change `/Users/yuxia/demo/iceberg` to any other directory you like in your local file system.
3. All the following commands for entering the container should be executed in the directory of the `docker-compose.yml` file.

To start all containers, run the following command in the directory that contains the `docker-compose.yml` file:
```shell
docker-compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in a detached mode. Run `docker ps` to check whether these containers are running properly.
We can also visit [http://localhost:8081/](http://localhost:8081/) to see if Flink is running normally.
![Flink UI](/_static/fig/real-time-data-lake-tutorial/flink-ui.png "Flink UI")

### Preparing data in databases
1. Enter mysql's container:
    ```shell
    docker-compose exec mysql mysql -uroot -p123456
    ```
2. Create databases/tables and populate data:

   Create a logical sharding table `user` sharded in different databases and tables physically, and one of them misses a column.
   ```sql
    CREATE DATABASE user_db_1;
    USE user_db_1;
    CREATE TABLE user_table_1 (
      id INTEGER NOT NULL PRIMARY KEY,
      name VARCHAR(255) NOT NULL DEFAULT 'flink',
      address VARCHAR(1024),
      phone_number VARCHAR(512),
      email VARCHAR(255)
    );
    INSERT INTO user_table_1 VALUES (11100,"user_11100","Shanghai","123567891234","user_11100@foo.com");
   
    CREATE TABLE user_table_2 (
      id INTEGER NOT NULL PRIMARY KEY,
      name VARCHAR(255) NOT NULL DEFAULT 'flink',
      address VARCHAR(1024),
      phone_number VARCHAR(512),
      email VARCHAR(255)
    );
   INSERT INTO user_table_2 VALUES (12100,"user_12100","Shanghai","123567891234","user_12100@foo.com");
   ```
   ```sql
   CREATE DATABASE user_db_2;
   USE user_db_2;
   CREATE TABLE user_table_1 (
     id INTEGER NOT NULL PRIMARY KEY,
     name VARCHAR(255) NOT NULL DEFAULT 'flink',
     address VARCHAR(1024),
     phone_number VARCHAR(512)
   );
   INSERT INTO user_table_1 VALUES (21100,"user_21100","Shanghai","123567891234");

   CREATE TABLE user_table_2 (
     id INTEGER NOT NULL PRIMARY KEY,
     name VARCHAR(255) NOT NULL DEFAULT 'flink',
     address VARCHAR(1024),
     phone_number VARCHAR(512),
     email VARCHAR(255)
   );
   INSERT INTO user_table_2 VALUES (22100,"user_22100","Shanghai","123567891234","user_22100@foo.com");
   ```

## Creating tables using Flink DDL in Flink SQL CLI
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker-compose exec sql-client ./sql-client
```

We should see the welcome screen of the CLI client:
![Flink SQL Client](/_static/fig/real-time-data-lake-tutorial/flink-sql-client.png  "Flink SQL Client" )

Then do the following steps in Flink SQL CLI:

1. Enable checkpoints every 3 seconds
   ```sql
   -- Flink SQL                   
   Flink SQL> SET execution.checkpointing.interval = 3s;
   ```
2. Create source table 

   Create a source table that captures the data from the logical sharding table `user`. Here, we use regex to match all the physical tables.
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
       primary key (`id`) not enforced
     ) WITH (
       'connector' = 'mysql-cdc',
       'hostname' = 'mysql',
       'port' = '3306',
       'username' = 'root',
       'password' = '123456',
       'database-name' = 'user_db_[0-9]+',
       'table-name' = 'user_table_[0-9]+'
     );
   ```
3. Create sink table 

   Create a sink table `all_users_sink` used to load data to Iceberg:
   ```sql
   -- FLink SQL
   Flink SQL> CREATE TABLE all_users_sink (
       db_name STRING,
       table_name    STRING,
       `id`          DECIMAL(20, 0) NOT NULL,
       name          STRING,
       address       STRING,
       phone_number  STRING,
       email         STRING,
       primary key (db_name, table_name, `id`) not enforced
     ) WITH (
       'connector'='iceberg',
       'catalog-name'='iceberg_catalog',
       'catalog-type'='hadoop',  
       'warehouse'='file:///home/iceberg/warehouse',
       'format-version'='2'
     );
   ```

## Streaming to Iceberg
1. Streaming write data from MySQL to Iceberg using the following Flink SQL:
   ```sql
   -- Flink SQL
   Flink SQL> INSERT INTO all_users_sink select * from user_source;
   ```

2. Use the following Flink SQL to query the data written to `all_users_sink`:
   ```sql
   -- Flink SQL
   Flink SQL> SELECT * FROM all_users_sink;
   ```
   We can see the data queried in the Flink SQL CLI:
   ![Data in Iceberg](/_static/fig/real-time-data-lake-tutorial/data_in_iceberg.png "Data in Iceberg")

   Also, we can find the data files in our local filesystem.

3. Make some changes in the MySQL databases, and then the data in Iceberg table `all_users_sink` will also change in real time.
   
   (3.1) Insert a new user in table `user_db_1.user_table_1`
   ```sql
   --- user_db_1
   INSERT INTO user_db_1.user_table_1 VALUES (11101,"user_11101","Shanghai","123567891234","user_11101@foo.com");
   ```

   (3.2) Update a user in table `user_db_1.user_table_2`
   ```sql
   --- user_db_1
   UPDATE user_db_1.user_table_2 SET address='Beijing' WHERE id=12100;
   ```

   (3.3) Delete a user in table `user_db_2.user_table_1`
   ```sql
   --- user_db_2
   DELETE FROM user_db_2.user_table_1 WHERE id=21100;
   ```

   After executing each step, we can query the table `all_users_sink` using `SELECT * FROM all_users_sink` in Flink SQL CLI to see the changes.
   
   The overall changes are as following:
   ![Data Changes in Iceberg](/_static/fig/real-time-data-lake-tutorial/data-changes-in-iceberg.gif "Data Changes in Iceberg")
   
## Clean up
After finishing the tutorial, run the following command in the directory of `docker-compose.yml` to stop all containers:
```shell
docker-compose down
```


