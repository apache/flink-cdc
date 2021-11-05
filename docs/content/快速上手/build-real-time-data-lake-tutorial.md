# 基于 Flink CDC 同步 MySQL 分库分表构建实时数据湖
在 OLTP 系统中，为了解决单表数据量大的问题，通常采用分库分表的方式将单个大表进行拆分以提高系统的性能。
但是为了方便数据分析，通常需要将分库分表拆分出的表在同步到数据仓库、数据湖时，再合并成一个大表。

这篇教程将展示如何使用 Flink CDC 构建实时数据湖来应对这种场景，本教程的演示基于 Docker，只涉及 SQL，无需一行 Java/Scala 代码，也无需安装 IDE，你可以很方便地在自己的电脑上完成本教程的全部内容。

接下来将以将数据从 MySQL 同步到 Iceberg 为例展示整个流程，架构图如下所示：

![Architecture of Real-Time Data Lake](/_static/fig/real-time-data-lake-tutorial/real-time-data-lake-tutorial.png "architecture of real-time data lake")

你也可以使用不同的 source 比如 Oracle/Postgres 和 sink 比如 Doris/Hudi 来构建自己的 ETL 流程。

## 准备阶段
准备一台已经安装了 Docker 的 Linux 或者 MacOS 电脑。

### 准备教程所需要的组件
接下来的教程将以 `docker-compose` 的方式准备所需要的组件。

使用下面的内容创建一个 `docker-compose.yml` 文件：
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

该 Docker Compose 中包含的容器有：
- SQL-Client: Flink SQL Client, 用来提交 SQL 查询和查看 SQL 的执行结果
- Flink Cluster：包含 Flink JobManager 和 Flink TaskManager，用来执行 Flink SQL  
- MySQL：作为分库分表的数据源，存储本教程的 `user` 表

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：
```shell
docker-compose up -d
```
该命令将以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。你可以通过 docker ps 来观察上述的容器是否正常启动了，也可以通过访问 [http://localhost:8081/](http://localhost:8081//) 来查看 Flink 是否运行正常。
![Flink UI](/_static/fig/real-time-data-lake-tutorial/flink-ui.png "Flink UI")

***注意：***
1. 为了简化整个教程，本教程需要的 jar 包都已经被打包进 SQL-Client 容器中了，如果你想要在自己的 Flink 环境运行本教程，需要下载下面列出的包并且把它们放在 Flink 所在目录的 lib 目录下，即 `Flink_HOME/lib/`。
   
    **下载链接只在已发布的版本上可用**

    - [flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.1-SNAPSHOT/flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar)
    - [iceberg-flink-runtime-0.12.0.jar](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/0.12.0/iceberg-flink-runtime-0.12.0.jar)
    - [flink-shaded-hadoop-2-uber-2.7.5-10.0.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar)
2. 该教程使用本地文件系统作为 Iceberg 的后端存储，为了让本地的文件可被容器访问，需要将其挂载至容器中。本教程使用本地的文件目录 `/Users/yuxia/demo/iceberg`, 你可以在这个 `docker-compose.yml` 文件中将其改成你机器上的其它目录
3. 本教程接下来用到的进入容器的命令都需要在 `docker-compose.yml` 所在目录下执行

### 准备数据
1. 进入 MySQL 容器中
    ```shell
    docker-compose exec mysql mysql -uroot -p123456
    ```
2. 创建数据和表，并填充数据
   
   创建两个不同的数据库，并在每个数据库中创建两个表，作为 `user` 表分库分表下拆分出的表，并且有一个表少了一列。
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

## 在 Flink SQL CLI 中使用 Flink DDL 创建表
首先，使用如下的命令进入 Flink SQL CLI 容器中：
```shell
docker-compose exec sql-client ./sql-client
```
我们可以看到如下界面：
![Flink SQL Client](/_static/fig/real-time-data-lake-tutorial/flink-sql-client.png  "Flink SQL Client" )

然后，进行如下步骤：
1. 开启 checkpoint，每隔3秒做一次 checkpoint
   ```sql
   -- Flink SQL                   
   Flink SQL> SET execution.checkpointing.interval = 3s;
   ```
2. 创建 source 表
   
   创建 source 表 `user_source` 来捕获MySQL中所有 `user` 表，在表的配置项 `database-name` , `table-name` 使用正则表达式来匹配这些表。 
   `user_source` 表包含所有的列，如果数据库表中不存在该列，则对应 `user_source` 表该列的值为 null。
   此外，`user_source` 表也定义了 metadata 列来区分数据是来自哪个数据库和表。
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
3. 创建 sink 表
   
   创建 sink 表`all_users_sink`，用来将数据加载至 Iceberg 中
   ```sql
   -- Flink SQL
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
## 流式写入 Iceberg   

1. 使用下面的 Flink SQL 语句将数据从 MySQL 写入 Iceberg 中
   ```sql
   -- Flink SQL
   Flink SQL> INSERT INTO all_users_sink select * from user_source;
   ```

2. 使用下面的 FLink SQL 语句查询表 `all_users_sink` 中的数据，验证是否成功写入了 Iceberg
   ```sql
   -- Flink SQL
   Flink SQL> SELECT * FROM all_users_sink;
   ```
   在 Flink SQL CLI 中我们可以看到如下查询结果：
   ![Data in Iceberg](/_static/fig/real-time-data-lake-tutorial/data_in_iceberg.png "Data in Iceberg")
   同时，我们也可以在我们的本地文件系统看到已经写入了一些数据文件。

3. 修改 MySQL 中表的数据，Iceberg 中的表 `all_users_sink` 中的数据也将实时更新：
   
   (3.1) 在 `user_db_1.user_table_1_1` 表中插入新的一行
   ```sql
   --- user_db_1
   INSERT INTO user_db_1.user_table_1 VALUES (11101,"user_11101","Shanghai","123567891234","user_11101@foo.com");
   ```
   (3.2) 更新 `user_db_1.user_table_1_2` 表的数据
    ```sql
    --- user_db_1
    UPDATE user_db_1.user_table_2 SET address='Beijing' WHERE id=12100;
    ```
   (3.3) 在 `user_db_2.user_table_2_1` 表中删除一行
    ```sql
    --- user_db_2
    DELETE FROM user_db_2.user_table_1 WHERE id=21100;
    ```
    每执行一步，我们就可以在 Flink Client CLI 中使用 `SELECT * FROM all_users_sink` 查询表 `all_users_sink` 来看到数据的变化。
    
    整体的数据变化如下所示：
    ![Data Changes in Iceberg](/_static/fig/real-time-data-lake-tutorial/data-changes-in-iceberg.gif "Data Changes in Iceberg")

## 环境清理
本教程结束后，在 `docker-compose.yml` 文件所在的目录下执行如下命令停止所有容器：
```shell
docker-compose down
```