# GaussDB CDC Connector 手动端到端测试手册

本手册详细介绍了如何通过**手动执行命令**的方式验证 GaussDB Flink CDC Connector 的功能。  
本指南适合小白用户，所有命令均可直接复制粘贴执行，包含**集中式**和**分布式**两种完整场景。

> **重要提示**：请根据您的实际环境替换以下占位符：
> - `<GAUSSDB_IP>`: GaussDB 服务器 IP 地址
> - `<GAUSSDB_PORT>`: GaussDB 端口号
> - `<USERNAME>`: 数据库用户名
> - `<PASSWORD>`: 数据库密码

---

## 第一部分：环境准备

### 1.1 编译 Connector JAR

1. **放置 JDBC 驱动**  
   将 `gaussdbjdbc.jar` 放入以下目录：
   ```
   flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/lib/
   ```

2. **执行 Maven 编译**  
   分两步编译 Connector（确保编译最新代码）：
   ```bash
   # 定义模块路径变量
   CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc"
   SQL_CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc"

   # 步骤1：编译基础 Connector
   mvn clean install -DskipTests \
       -Drat.skip \
       -Dspotless.skip=true \
       -Dspotless.check.skip=true \
       -Dspotless.apply.skip=true \
       -Dcheckstyle.skip=true \
       -pl $CONNECTOR_MODULE \
       -am

   # 步骤2：编译 SQL Connector（最终产物）
   mvn clean install -DskipTests \
       -Drat.skip \
       -Dspotless.skip=true \
       -Dspotless.check.skip=true \
       -Dspotless.apply.skip=true \
       -Dcheckstyle.skip=true \
       -pl $SQL_CONNECTOR_MODULE \
       -am
   ```

3. **确认产物**  
   编译产物位于：
   ```
   flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar
   ```

---

### 1.2 部署 Docker 测试环境

创建 `docker-compose.yml` 文件，内容如下：

```yaml
version: '3.8'
services:
  jobmanager:
    image: flink:1.18.1-scala_2.12-java11
    container_name: flink-jobmanager
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        execution.checkpointing.interval: 60000
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - flink-network

  taskmanager:
    image: flink:1.18.1-scala_2.12-java11
    container_name: flink-taskmanager
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - flink-network

  mysql-sink:
    image: mysql:8.0
    container_name: mysql-sink
    ports:
      - "3307:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpw
      MYSQL_DATABASE: inventory
      MYSQL_USER: flinkuser
      MYSQL_PASSWORD: flinkpw
    networks:
      - flink-network
    command: --default-authentication-plugin=mysql_native_password

networks:
  flink-network:
    driver: bridge
```

启动容器：
```bash
docker-compose up -d
```

---

### 1.3 分发 JAR 包到 Flink 容器

```bash
# 定义变量（请根据实际路径修改）
JAR_PATH="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar"
JDBC_PATH="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/lib/gaussdbjdbc.jar"

# 拷贝到 JobManager
docker cp $JAR_PATH flink-jobmanager:/opt/flink/lib/
docker cp $JDBC_PATH flink-jobmanager:/opt/flink/lib/

# 拷贝到 TaskManager
docker cp $JAR_PATH flink-taskmanager:/opt/flink/lib/
docker cp $JDBC_PATH flink-taskmanager:/opt/flink/lib/

# 重启 Flink 容器以加载新 JAR
docker restart flink-jobmanager flink-taskmanager

# 等待容器启动（约 30 秒）
sleep 30
```

---

### 1.4 初始化 MySQL Sink 表

```bash
docker exec -it mysql-sink mysql -uflinkuser -pflinkpw inventory
```

在 MySQL 命令行中执行：
```sql
CREATE TABLE IF NOT EXISTS products_sink (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10,2),
    stock INT
);

-- 清空历史数据（可选）
TRUNCATE TABLE products_sink;
```

输入 `exit` 退出 MySQL 命令行。

---

## 第二部分：集中式场景测试

适用于**单节点 GaussDB** 或 **通过统一入口访问的集群**。

### 2.1 GaussDB 侧准备

使用 `psql` 连接到 GaussDB：
```bash
psql -h <GAUSSDB_IP> -p <GAUSSDB_PORT> -U <USERNAME> -d db1
```

执行建表 SQL（如果表不存在）：
```sql
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0
);
```

---

### 2.2 创建 Flink SQL 任务文件

在本地创建文件 `centralized_sync.sql`，内容如下：

```sql
-- =====================================================
-- Flink SQL: 集中式 GaussDB -> MySQL 同步任务
-- =====================================================

-- 优化配置
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- 1. 定义 GaussDB 数据源
CREATE TABLE products_source (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<GAUSSDB_IP>',
    'port' = '<GAUSSDB_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_slot_centralized',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- 2. 定义 MySQL Sink
CREATE TABLE products_sink (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql-sink:3306/inventory?characterEncoding=UTF-8&useUnicode=true',
    'table-name' = 'products_sink',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 3. 启动同步作业
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock
FROM products_source;
```

> **请务必替换** `<GAUSSDB_IP>`, `<GAUSSDB_PORT>`, `<USERNAME>`, `<PASSWORD>` 为实际值。

---

### 2.3 提交 Flink 任务

```bash
# 拷贝 SQL 文件到 JobManager 容器
docker cp centralized_sync.sql flink-jobmanager:/opt/flink/

# 提交任务
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/centralized_sync.sql
```

---

### 2.4 验证同步功能

#### 测试 INSERT
在 GaussDB 中执行：
```sql
INSERT INTO products VALUES (1001, '测试手机', '电子产品', 5999.00, 100);
INSERT INTO products VALUES (1002, '测试电脑', '电子产品', 8999.00, 50);
```

验证 MySQL（等待约 10 秒后执行）：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT * FROM products_sink WHERE product_id IN (1001, 1002);"
```

#### 测试 UPDATE
在 GaussDB 中执行：
```sql
UPDATE products SET price = 4999.00 WHERE product_id = 1001;
```

验证 MySQL：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT product_id, price FROM products_sink WHERE product_id = 1001;"
```
预期结果：`price` 应为 `4999.00`。

#### 测试 DELETE
在 GaussDB 中执行：
```sql
DELETE FROM products WHERE product_id = 1002;
```

验证 MySQL：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT COUNT(*) FROM products_sink WHERE product_id = 1002;"
```
预期结果：`COUNT(*)` 应为 `0`。

---

## 第三部分：分布式场景测试

适用于**多 Data Node (DN)** 的分布式 GaussDB 环境。

### 3.1 分布式环境说明

分布式 GaussDB 通常包含：
- **Coordinator Node (CN)**：用于执行 DDL 和分布式 DML（端口通常为 `8000`）
- **Data Node (DN)**：存储实际数据的分片节点（每个 DN 有独立端口，如 `40000`, `40020`, `40040`）

> Flink CDC 需要连接到**每个 DN** 才能捕获完整的数据变更。

---

### 3.2 GaussDB 侧准备 (通过 CN 执行)

连接到 CN：
```bash
psql -h <CN_IP> -p 8000 -U <USERNAME> -d db1
```

创建分布式表：
```sql
DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0
) DISTRIBUTE BY HASH(product_id);
```

---

### 3.3 创建 Flink SQL 任务文件

在本地创建文件 `distributed_sync.sql`，内容如下：

```sql
-- =====================================================
-- Flink SQL: 分布式 GaussDB (3 DN) -> MySQL 同步任务
-- =====================================================

-- 优化配置（重要！防止 DELETE 事件丢失）
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- =====================================================
-- DN1 数据源
-- =====================================================
CREATE TABLE products_source_dn1 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN1_IP>',
    'port' = '<DN1_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_slot_dn1',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- DN2 数据源
-- =====================================================
CREATE TABLE products_source_dn2 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN2_IP>',
    'port' = '<DN2_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_slot_dn2',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- DN3 数据源
-- =====================================================
CREATE TABLE products_source_dn3 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN3_IP>',
    'port' = '<DN3_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_slot_dn3',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- MySQL Sink
-- =====================================================
CREATE TABLE products_sink (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql-sink:3306/inventory?characterEncoding=UTF-8&useUnicode=true',
    'table-name' = 'products_sink',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- =====================================================
-- 启动同步作业（合并所有 DN 数据）
-- =====================================================
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock FROM products_source_dn1
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn2
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn3;
```

> **请务必替换**：
> - `<DN1_IP>`, `<DN1_PORT>` (例如 `10.250.0.30`, `40000`)
> - `<DN2_IP>`, `<DN2_PORT>` (例如 `10.250.0.181`, `40020`)
> - `<DN3_IP>`, `<DN3_PORT>` (例如 `10.250.0.157`, `40040`)
> - `<USERNAME>`, `<PASSWORD>`

---

### 3.4 提交 Flink 任务

```bash
# 拷贝 SQL 文件到 JobManager 容器
docker cp distributed_sync.sql flink-jobmanager:/opt/flink/

# 提交任务
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/distributed_sync.sql
```

---

### 3.5 验证同步功能

#### 测试 INSERT (通过 CN 执行，数据会自动分片到各 DN)
```sql
INSERT INTO products VALUES (2001, '分布式测试1', '测试', 100.00, 10);
INSERT INTO products VALUES (2002, '分布式测试2', '测试', 200.00, 20);
INSERT INTO products VALUES (2003, '分布式测试3', '测试', 300.00, 30);
```

验证 MySQL：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT * FROM products_sink WHERE product_id >= 2001;"
```
预期：应看到 3 条记录。

#### 测试 UPDATE
```sql
UPDATE products SET price = 150.00 WHERE product_id = 2001;
```

验证：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT price FROM products_sink WHERE product_id = 2001;"
```
预期：`150.00`

#### 测试 DELETE
```sql
DELETE FROM products WHERE product_id >= 2001;
```

验证：
```bash
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory -e "SELECT COUNT(*) FROM products_sink WHERE product_id >= 2001;"
```
预期：`0`

---

## 第四部分：常见问题排查

### 问题 1：数据没有同步到 MySQL
**排查步骤**：
1. 查看 Flink TaskManager 日志：
   ```bash
   docker logs -f flink-taskmanager
   ```
2. 搜索关键字 `ERROR` 或 `Exception`。

### 问题 2：DELETE 操作没有生效
**原因**：Flink 状态过期导致删除事件丢失。
**解决**：确保 SQL 中包含以下配置：
```sql
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
```

### 问题 3：复制槽冲突
**现象**：报错 `replication slot already exists`。
**解决**：在 GaussDB 中删除旧槽：
```sql
SELECT pg_drop_replication_slot('flink_cdc_slot_dn1');
```

### 问题 4：网络不通
**排查**：在 Flink 容器内测试连接：
```bash
docker exec -it flink-taskmanager bash
nc -zv <GAUSSDB_IP> <GAUSSDB_PORT>
```

---

## 附录：快速参考表

| 配置项 | 推荐值 | 说明 |
|--------|--------|------|
| `table.exec.state.ttl` | `1 h` | 状态保留时间，防止 DELETE 丢失 |
| `table.exec.sink.upsert-materialize` | `NONE` | 禁用物化，直接下发 DELETE |
| `slot.name` | 唯一值 | 每个 DN 必须使用不同的槽名 |
| `decoding.plugin.name` | `mppdb_decoding` | GaussDB 推荐解码插件 |

---

## 第五部分：GaussDB 作为 Sink 端

本章介绍如何将 GaussDB 作为目标数据库（Sink），实现 **GaussDB → GaussDB** 的数据同步。

### 5.1 背景说明

GaussDB 与 PostgreSQL 兼容，但不完全支持 PostgreSQL 的 `ON CONFLICT` 语法用于 upsert 操作。为解决此问题，我们实现了**自定义 GaussDB JDBC Dialect**，使用 GaussDB 兼容的 `MERGE INTO` 语法。

#### 技术实现

| 组件 | 文件 | 功能 |
|------|------|------|
| 自定义方言 | `GaussDBJdbcDialect.java` | 使用 `MERGE INTO` 语法替代 `ON CONFLICT` |
| 方言工厂 | `GaussDBJdbcDialectFactory.java` | 识别 `jdbc:gaussdb://` URL 并创建方言实例 |

#### MERGE INTO 语法示例

```sql
MERGE INTO products_sink t 
USING (SELECT :product_id AS "product_id", :product_name AS "product_name", ...) AS s 
ON t."product_id" = s."product_id" 
WHEN MATCHED THEN UPDATE SET "product_name" = s."product_name", ... 
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

---

### 5.2 环境准备（GaussDB Sink）

#### 5.2.1 确保自定义方言已编译

方言类位于 `flink-connector-gaussdb-cdc` 模块中，编译步骤同 1.1 节。

#### 5.2.2 准备修改版 JDBC Connector

由于 Flink JDBC Connector 使用 SPI 机制发现方言，需要将自定义方言类注入到 `flink-connector-jdbc.jar` 中：

```bash
# 定义变量
PROJECT_ROOT=$(pwd)
CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc"
JDBC_MOD_DIR="/tmp/jdbc_mod_gaussdb"

# 提取原始 JDBC Connector
rm -rf "$JDBC_MOD_DIR" && mkdir -p "$JDBC_MOD_DIR" && cd "$JDBC_MOD_DIR"
cp /path/to/flink-connector-jdbc.jar .
unzip -q -o flink-connector-jdbc.jar -d extracted

# 添加 GaussDB 方言工厂到 SPI 文件
echo "org.apache.flink.cdc.connectors.gaussdb.jdbc.GaussDBJdbcDialectFactory" >> \
    extracted/META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory

# 拷贝方言类
mkdir -p extracted/org/apache/flink/cdc/connectors/gaussdb/jdbc
cp "$PROJECT_ROOT/$CONNECTOR_MODULE/target/classes/org/apache/flink/cdc/connectors/gaussdb/jdbc/"*.class \
    extracted/org/apache/flink/cdc/connectors/gaussdb/jdbc/

# 重新打包
cd extracted && jar -cf ../flink-connector-jdbc-gaussdb.jar . && cd ..

echo "修改版 JDBC Connector 已生成: $JDBC_MOD_DIR/flink-connector-jdbc-gaussdb.jar"
```

#### 5.2.3 分发 JAR 包到 Flink 容器

```bash
JAR_PATH="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar"
JDBC_DRIVER="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/lib/gaussdbjdbc.jar"
MODIFIED_JDBC_CONNECTOR="$JDBC_MOD_DIR/flink-connector-jdbc-gaussdb.jar"

for container in flink-jobmanager flink-taskmanager; do
    # 清理旧 JAR
    docker exec $container rm -f /opt/flink/lib/flink-connector-jdbc*.jar
    
    # 拷贝新 JAR
    docker cp $JAR_PATH $container:/opt/flink/lib/
    docker cp $JDBC_DRIVER $container:/opt/flink/lib/
    docker cp $MODIFIED_JDBC_CONNECTOR $container:/opt/flink/lib/flink-connector-jdbc.jar
done

# 重启容器
docker restart flink-jobmanager flink-taskmanager
sleep 30
```

---

### 5.3 初始化 GaussDB Sink 表

通过 CN 连接到目标 GaussDB：

```bash
psql -h <CN_IP> -p 8000 -U <USERNAME> -d db1
```

创建 Sink 表：

```sql
DROP TABLE IF EXISTS products_sink CASCADE;

CREATE TABLE products_sink (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER
);
```

---

### 5.4 创建 Flink SQL 任务文件

创建 `gaussdb_to_gaussdb.sql` 文件：

```sql
-- =====================================================
-- Flink SQL: 分布式 GaussDB -> GaussDB (via CN) 同步任务
-- =====================================================

-- 优化配置
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- =====================================================
-- DN1 数据源
-- =====================================================
CREATE TABLE products_source_dn1 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN1_IP>',
    'port' = '<DN1_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn1',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- DN2 数据源
-- =====================================================
CREATE TABLE products_source_dn2 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN2_IP>',
    'port' = '<DN2_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn2',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- DN3 数据源
-- =====================================================
CREATE TABLE products_source_dn3 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '<DN3_IP>',
    'port' = '<DN3_PORT>',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn3',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- GaussDB Sink (通过 CN 写入)
-- 使用自定义 GaussDB JDBC Dialect (支持 MERGE INTO)
-- =====================================================
CREATE TABLE products_sink (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://<CN_IP>:8000/db1?currentSchema=public',
    'table-name' = 'products_sink',
    'username' = '<USERNAME>',
    'password' = '<PASSWORD>',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- =====================================================
-- 启动同步作业（合并所有 DN 数据）
-- =====================================================
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock FROM products_source_dn1
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn2
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn3;
```

> **关键配置说明**：
> - `url`: 使用 `jdbc:gaussdb://` 协议以触发自定义 GaussDB 方言
> - `driver`: 使用 `com.huawei.gaussdb.jdbc.Driver` (来自 gaussdbjdbc.jar)

---

### 5.5 提交 Flink 任务

```bash
# 拷贝 SQL 文件到 JobManager 容器
docker cp gaussdb_to_gaussdb.sql flink-jobmanager:/opt/flink/

# 提交任务
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/gaussdb_to_gaussdb.sql
```

---

### 5.6 验证同步功能

#### 测试 INSERT

在 GaussDB 中执行（通过 CN）：
```sql
INSERT INTO products VALUES (6001, 'G2G 产品A', '测试', 100.00, 10);
INSERT INTO products VALUES (6002, 'G2G 产品B', '测试', 200.00, 20);
```

验证 Sink 表：
```bash
psql -h <CN_IP> -p 8000 -U <USERNAME> -d db1 -c "SELECT * FROM products_sink WHERE product_id >= 6000;"
```

预期：应看到 2 条记录。

#### 测试 UPDATE

```sql
UPDATE products SET price = 150.00, stock = 15 WHERE product_id = 6001;
```

验证：
```bash
psql -h <CN_IP> -p 8000 -U <USERNAME> -d db1 -c "SELECT * FROM products_sink WHERE product_id = 6001;"
```

预期：`price` 为 `150.00`，`stock` 为 `15`。

#### 测试 DELETE

```sql
DELETE FROM products WHERE product_id = 6002;
```

验证：
```bash
psql -h <CN_IP> -p 8000 -U <USERNAME> -d db1 -c "SELECT COUNT(*) FROM products_sink WHERE product_id = 6002;"
```

预期：`0`

---

### 5.7 一键测试脚本

项目提供了一键测试脚本，可自动完成部署和验证：

```bash
# 运行完整的 GaussDB -> GaussDB 测试
bash test_gaussdb_to_gaussdb_cdc.sh
```

该脚本会自动：
1. 编译并打包连接器
2. 创建修改版 JDBC Connector（包含 GaussDB 方言）
3. 部署到 Flink 集群
4. 初始化 Sink 表
5. 提交 Flink SQL 任务

---

### 5.8 GaussDB Sink 常见问题

#### 问题 1：报错 `Could not find any jdbc dialect factory that can handle url 'jdbc:gaussdb://...'`

**原因**：自定义 GaussDB 方言未正确加载。

**解决**：
1. 确认方言类已编译到目标目录
2. 确认已将方言工厂添加到 SPI 文件
3. 确认修改版 JDBC Connector 已部署到 `/opt/flink/lib/`

#### 问题 2：报错 `SQL statement must not contain ? character`

**原因**：方言未被正确调用，使用了默认的 PostgreSQL 方言。

**解决**：
1. 确认 `url` 使用 `jdbc:gaussdb://` 协议
2. 确认 SPI 文件包含 `GaussDBJdbcDialectFactory`
3. 重启 Flink 容器

#### 问题 3：报错 `ON CONFLICT is supported only in PG-format database`

**原因**：使用了错误的 JDBC URL 或驱动。

**解决**：
- 确保 Sink 配置中：
  - `url` = `jdbc:gaussdb://...` (而不是 `jdbc:postgresql://`)
  - `driver` = `com.huawei.gaussdb.jdbc.Driver`

#### 问题 4：ClassNotFoundException: com.huawei.gaussdb.jdbc.Driver

**原因**：`gaussdbjdbc.jar` 未正确部署。

**解决**：
```bash
docker cp gaussdbjdbc.jar flink-taskmanager:/opt/flink/lib/
docker restart flink-taskmanager
```

---

## 附录 B：GaussDB Sink 配置参考

| 配置项 | 值 | 说明 |
|--------|---|------|
| `connector` | `jdbc` | 使用 Flink JDBC Connector |
| `url` | `jdbc:gaussdb://<host>:<port>/<db>` | 必须使用 `jdbc:gaussdb://` 协议 |
| `driver` | `com.huawei.gaussdb.jdbc.Driver` | GaussDB JDBC 驱动类 |
| `table-name` | 表名 | 目标表名称 |
| `username` | 用户名 | 数据库用户 |
| `password` | 密码 | 数据库密码 |

