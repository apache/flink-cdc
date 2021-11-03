# 基于 Flink CDC 构建分库分表场景下的 Streaming ETL
在 OLTP 系统中，为了解决单表数据量大的问题，通常采用分库分表的方式将单个大表进行拆分以提高系统的性能。
但是有的时候，为了方便分析，又需要将分库分表拆分出的表合并成一个大表。

这篇教程将展示如何采用 Flink CDC 构建流式 ETL 来应对这种场景。
接下来将以 MySQL 写到 ElasticSearch 为例展示整个 ETL 流程，架构图如下所示：
![Flink CDC Streaming ETL with sharding tables](/_static/fig/working-with-sharding-table-tutorial/work-with-sharding-table-architecture.png "architecture of working with sharding tables")

你也可以使用不同的 source 和 sink 来构建自己的ETL。

## 准备阶段
准备一台已经安装了 Docker 的 Linux 或者 MacOS 电脑。
### 准备教程所需要的组件
接下来的教程将以 `docker-compose` 的方式准备所需要的组件。

使用下面的内容创建一个 `docker-compose.yml` 文件：
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
该 Docker Compose 中包含的容器有：
- MySQL：作为分库分表的数据源，存储本教程的 `user` 表
- ElasticSearch：MySQL 中的数据将写到 ElasticSearch
- Kibana：用来可视化 ElasticSearch 的数据

在 `docker-compose.yml` 所在目录下执行下面的命令来启动本教程需要的组件：
```shell
docker-compose up -d
```
该命令将以 detached 模式自动启动 Docker Compose 配置中定义的所有容器。你可以通过 docker ps 来观察上述的容器是否正常启动了，也可以通过访问 [http://localhost:5601/](http://localhost:5601/) 来查看 Kibana 是否运行正常。

### 下载 Flink 和所需要的依赖包
1. 下载 [Flink 1.13.2](https://downloads.apache.org/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.11.tgz) 并将其解压至目录 `flink-1.13.2`
2. 下载下面列出的依赖包，并将它们放到目录 `flink-1.13.2/lib/` 下：
   
   **下载链接只在已发布的版本上可用**
    - [flink-sql-connector-elasticsearch7_2.11-1.13.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.11/1.13.2/flink-sql-connector-elasticsearch7_2.11-1.13.2.jar)
    - [flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.1-SNAPSHOT/flink-sql-connector-mysql-cdc-2.1-SNAPSHOT.jar)

### 准备数据
1. 进入 MySQL 容器中
    ```shell
    docker-compose exec mysql mysql -uroot -p123456
    ```
2. 创建数据和表，并填充数据
   
   创建两个不同的数据库，并在每个数据库中创建两个表，作为 `user` 表分库分表下拆分出的表，并且这些表的列不完全一致
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

### 启动 Flink 集群和 Flink SQL CLI

1. 使用下面的命令跳转至 Flink 目录下
   ```
    cd flink-1.13.2
   ```
2. 使用下面的命令启动 Flink 集群
   ```shell
    ./bin/start-cluster.sh
   ```
    启动成功的话，可以在 [http://localhost:8081/](http://localhost:8081/) 访问到 Flink Web UI，如下所示：
    ![Flink UI](/_static/fig/working-with-sharding-table-tutorial/flink-ui.png "Flink UI")
   
3. 使用下面的命令启动 Flink SQL CLI
   ```shell
    ./bin/sql-client.sh
   ```
   启动成功后，可以看到如下的页面：
   
   ![Flink SQL Client](/_static/fig/working-with-sharding-table-tutorial/flink-sql-client.png  "Flink SQL Client" )

## 在 Flink SQL CLI 中使用 Flink DDL 创建表
首先，开启 checkpoint，每隔3秒做一次 checkpoint
```sql
-- Flink SQL                   
Flink SQL> SET execution.checkpointing.interval = 3s;
```

然后，创建 `user_source` 表来捕获MySQL中所有 `user` 表，在表的配置项 `database-name` , `table-name` 使用正则表达式来匹配这些表。 
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

最后，创建 `all_users_sink` 表，用来将数据加载至 Elasticsearch 中
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
## 将数据写入 ElasticSearch 中
使用下面的 Flink SQL 语句将数据从 MySQL 写入 ElasticSearch 中
```sql
-- Flink SQL
Flink SQL> INSERT INTO all_users_sink select * from user_source;
```
现在，就可以在 Kibana 中看到 MySQL 中所有表的数据。

首先访问 [http://localhost:5601/app/kibana#/management/kibana/index_pattern](http://localhost:5601/app/kibana#/management/kibana/index_pattern) 创建 index pattern `all_users` 。

![Create Index Pattern](/_static/fig/working-with-sharding-table-tutorial/kibana-create-index-pattern.png "Create Index Pattern")


然后就可以在 [http://localhost:5601/app/kibana#/discover](http://localhost:5601/app/kibana#/discover) 看到写入的数据。

![Find user data](/_static/fig/working-with-sharding-table-tutorial/kibana-users.png "Find All User Data")
接下来，修改 MySQL 中表的数据，Kibana 中显示的数据也将实时更新：
1. 在 `user_db_1.user_table_1_1` 表中插入新的一行
   ```sql
   --- user_db_1
   INSERT INTO user_table_1_1 VALUES (11101,"user_11101","Shanghai","123567891234","user_11101@foo.com");
   ```
2. 更新 `user_db_1.user_table_1_2` 表的数据
   ```sql
   --- user_db_1
   UPDATE user_table_1_2 SET address='Beijing' WHERE id=12100;
   ```
3. 在 `user_db_2.user_table_2_1` 表中删除一行
   ```sql
   --- user_db_2
   DELETE FROM user_table_2_1 WHERE id=21100;
   ```
Kibana 中显示的数据将实时更新，如下所示：
![User Data Changes](/_static/fig/working-with-sharding-table-tutorial/user_data_changes.gif "User Data Changes")

## 环境清理
本教程结束后，在 `docker-compose.yml` 文件所在的目录下执行如下命令停止所有容器：
```shell
docker-compose down
```
在 Flink 所在目录 `flink-1.13.2` 下执行如下命令停止 Flink 集群：
```shell
./bin/stop-cluster.sh
```