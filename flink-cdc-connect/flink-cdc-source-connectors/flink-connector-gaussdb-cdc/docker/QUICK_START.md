# GaussDB CDC Connector Quick Start 测试指南

本文档详细介绍如何在 Docker 环境中测试验证 GaussDB CDC Connector 的功能。

## 目录

1. [环境准备](#1-环境准备)
2. [GaussDB 配置](#2-gaussdb-配置)
3. [Flink 环境部署](#3-flink-环境部署)
4. [Connector JAR 部署](#4-connector-jar-部署)
5. [测试场景](#5-测试场景)
6. [故障排查](#6-故障排查)

---

## 1. 环境准备

### 1.1 前置条件

- Docker 和 Docker Compose 已安装
- 可访问的 GaussDB 数据库实例
- Maven 3.6+ (用于构建 Connector)
- JDK 8 或 11

### 1.2 目录结构

```
docker/
├── docker-compose.yml      # Flink 集群配置
├── jars/                   # Connector JAR 文件目录
├── sql/                    # SQL 脚本目录
├── flink-checkpoints/      # Checkpoint 存储目录
├── flink-savepoints/       # Savepoint 存储目录
└── log/                    # 日志目录
```

### 1.3 构建 Connector JAR

```bash
# 在项目根目录执行
cd /path/to/flink-cdc

# 构建 shaded JAR
mvn clean package -pl flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc -am -DskipTests

# 复制 JAR 到 docker/jars 目录
cp flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar \
   flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker/jars/
```

---

## 2. GaussDB 配置

### 2.1 检查 GaussDB 配置

连接到 GaussDB 数据库，执行以下检查：

```sql
-- 检查 wal_level (必须为 logical)
SHOW wal_level;

-- 检查 max_replication_slots (建议 >= 4)
SHOW max_replication_slots;

-- 检查 max_wal_senders (建议 >= 4)
SHOW max_wal_senders;
```

如果 `wal_level` 不是 `logical`，需要修改配置：

```sql
-- 修改 wal_level (需要重启数据库)
ALTER SYSTEM SET wal_level = 'logical';
```

### 2.2 创建测试数据库和用户

```sql
-- 创建测试数据库
CREATE DATABASE cdc_test;

-- 创建测试用户 (如果需要)
CREATE USER cdc_user WITH PASSWORD 'cdc_password';

-- 授予权限
GRANT ALL PRIVILEGES ON DATABASE cdc_test TO cdc_user;
ALTER USER cdc_user REPLICATION;

-- 连接到测试数据库
\c cdc_test
```

### 2.3 创建测试表

```sql
-- 创建订单表
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    order_status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建商品表
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建用户表
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入初始测试数据
INSERT INTO orders (customer_name, product_name, price, quantity, order_status) VALUES
('张三', 'iPhone 15', 7999.00, 1, 'COMPLETED'),
('李四', 'MacBook Pro', 14999.00, 1, 'PENDING'),
('王五', 'AirPods Pro', 1999.00, 2, 'SHIPPED');

INSERT INTO products (product_name, category, price, stock) VALUES
('iPhone 15', '手机', 7999.00, 100),
('MacBook Pro', '电脑', 14999.00, 50),
('AirPods Pro', '配件', 1999.00, 200);

INSERT INTO users (username, email, phone, status) VALUES
('zhangsan', 'zhangsan@example.com', '13800138001', 'ACTIVE'),
('lisi', 'lisi@example.com', '13800138002', 'ACTIVE'),
('wangwu', 'wangwu@example.com', '13800138003', 'INACTIVE');
```

### 2.4 创建复制槽 (可选)

```sql
-- 手动创建复制槽 (Connector 也会自动创建)
SELECT * FROM pg_create_logical_replication_slot('flink_cdc_orders', 'mppdb_decoding');

-- 查看已创建的复制槽
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;
```

---

## 3. Flink 环境部署

### 3.1 启动 Flink 集群

```bash
# 进入 docker 目录
cd flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker

# 启动 Flink 集群
docker-compose up -d

# 查看容器状态
docker-compose ps

# 查看日志
docker-compose logs -f jobmanager
```

### 3.2 验证 Flink 集群

1. 打开浏览器访问 Flink Web UI: http://localhost:8081
2. 确认 TaskManager 已注册 (Available Task Slots: 4)

### 3.3 进入 Flink SQL Client

```bash
# 进入 JobManager 容器
docker exec -it flink-jobmanager /bin/bash

# 启动 SQL Client
./bin/sql-client.sh

# 或者直接执行
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

---

## 4. Connector JAR 部署

### 4.1 确认 JAR 文件

```bash
# 检查 JAR 文件是否存在
ls -la docker/jars/

# 应该看到:
# flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar
```

### 4.2 验证 JAR 加载

在 Flink SQL Client 中执行：

```sql
-- 查看已加载的 Connector
SHOW JARS;

-- 如果 JAR 未自动加载，手动添加
ADD JAR '/opt/flink/usrlib/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar';
```

---

## 5. 测试场景

### 5.1 场景一：基础 CDC 功能测试

#### 5.1.1 创建 CDC 源表

在 Flink SQL Client 中执行：

```sql
-- 设置 checkpoint 间隔
SET 'execution.checkpointing.interval' = '10s';

-- 创建 GaussDB CDC 源表
CREATE TABLE orders_source (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    quantity INT,
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',          -- 替换为实际 GaussDB 地址
    'port' = '8000',
    'username' = 'cdc_user',              -- 替换为实际用户名
    'password' = 'cdc_password',          -- 替换为实际密码
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_orders',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);
```

#### 5.1.2 查询 CDC 数据

```sql
-- 查询源表数据 (会持续输出变更)
SELECT * FROM orders_source;
```

#### 5.1.3 在 GaussDB 中执行变更操作

打开另一个终端，连接 GaussDB 执行：

```sql
-- 插入新订单
INSERT INTO orders (customer_name, product_name, price, quantity, order_status)
VALUES ('赵六', 'iPad Pro', 8999.00, 1, 'PENDING');

-- 更新订单状态
UPDATE orders SET order_status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP
WHERE order_id = 2;

-- 删除订单
DELETE FROM orders WHERE order_id = 1;
```

#### 5.1.4 验证结果

在 Flink SQL Client 中观察输出，应该能看到：
- INSERT 操作产生 `+I` 记录
- UPDATE 操作产生 `-U` 和 `+U` 记录
- DELETE 操作产生 `-D` 记录

---

### 5.2 场景二：Print Sink 测试

#### 5.2.1 创建 Print Sink 表

```sql
-- 创建 Print Sink 表
CREATE TABLE orders_print (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    quantity INT,
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- 将 CDC 数据写入 Print Sink
INSERT INTO orders_print SELECT * FROM orders_source;
```

#### 5.2.2 查看输出

```bash
# 查看 TaskManager 日志
docker logs -f flink-taskmanager
```

---

### 5.3 场景三：元数据列测试

#### 5.3.1 创建带元数据的源表

```sql
-- 创建带元数据列的源表
CREATE TABLE orders_with_metadata (
    -- 元数据列
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    schema_name STRING METADATA FROM 'schema_name' VIRTUAL,
    table_name STRING METADATA FROM 'table_name' VIRTUAL,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    -- 业务列
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    order_status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_orders_meta',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- 查询带元数据的数据
SELECT db_name, schema_name, table_name, op_ts, order_id, customer_name, order_status
FROM orders_with_metadata;
```

---

### 5.4 场景四：多表同步测试

#### 5.4.1 创建多个源表

```sql
-- 商品表 CDC 源
CREATE TABLE products_source (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    created_at TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_products',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- 用户表 CDC 源
CREATE TABLE users_source (
    user_id INT,
    username STRING,
    email STRING,
    phone STRING,
    status STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'users',
    'slot.name' = 'flink_cdc_users',
    'decoding.plugin.name' = 'mppdb_decoding'
);
```

#### 5.4.2 多表联合查询

```sql
-- 创建 Print Sink
CREATE TABLE multi_table_print (
    source_table STRING,
    record_id INT,
    record_name STRING,
    record_info STRING
) WITH (
    'connector' = 'print'
);

-- 合并多表数据
INSERT INTO multi_table_print
SELECT 'orders' as source_table, order_id, customer_name, order_status FROM orders_source
UNION ALL
SELECT 'products' as source_table, product_id, product_name, category FROM products_source
UNION ALL
SELECT 'users' as source_table, user_id, username, status FROM users_source;
```

---

### 5.5 场景五：Checkpoint 和故障恢复测试

#### 5.5.1 启动带 Checkpoint 的作业

```sql
-- 设置 Checkpoint 配置
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60s';

-- 创建源表和 Sink 表
CREATE TABLE orders_cdc (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    order_status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_checkpoint_test',
    'decoding.plugin.name' = 'mppdb_decoding'
);

CREATE TABLE orders_sink (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    order_status STRING
) WITH (
    'connector' = 'print'
);

-- 启动同步作业
INSERT INTO orders_sink SELECT * FROM orders_cdc;
```

#### 5.5.2 验证 Checkpoint

1. 打开 Flink Web UI: http://localhost:8081
2. 进入 Running Jobs -> 选择作业 -> Checkpoints
3. 确认 Checkpoint 正常完成

#### 5.5.3 模拟故障恢复

```bash
# 停止 TaskManager
docker stop flink-taskmanager

# 等待几秒后重启
docker start flink-taskmanager

# 观察作业是否自动恢复
```

---

### 5.6 场景六：数据类型测试

#### 5.6.1 创建包含多种数据类型的测试表

在 GaussDB 中执行：

```sql
-- 创建数据类型测试表
CREATE TABLE data_types_test (
    id SERIAL PRIMARY KEY,
    -- 数值类型
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_numeric NUMERIC(10, 2),
    -- 字符类型
    col_char CHAR(10),
    col_varchar VARCHAR(100),
    col_text TEXT,
    -- 日期时间类型
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    -- 布尔类型
    col_boolean BOOLEAN,
    -- 二进制类型
    col_bytea BYTEA,
    -- JSON 类型
    col_json JSON,
    col_jsonb JSONB
);

-- 插入测试数据
INSERT INTO data_types_test (
    col_smallint, col_integer, col_bigint, col_real, col_double, col_numeric,
    col_char, col_varchar, col_text,
    col_date, col_time, col_timestamp, col_timestamptz,
    col_boolean, col_bytea, col_json, col_jsonb
) VALUES (
    32767, 2147483647, 9223372036854775807, 3.14, 3.141592653589793, 12345.67,
    'CHAR      ', 'VARCHAR测试', 'TEXT长文本测试',
    '2024-01-15', '12:30:45', '2024-01-15 12:30:45', '2024-01-15 12:30:45+08',
    true, E'\\xDEADBEEF', '{"key": "value"}', '{"name": "test", "count": 100}'
);
```

#### 5.6.2 创建 Flink CDC 表

```sql
-- 创建数据类型测试 CDC 表
CREATE TABLE data_types_source (
    id INT,
    col_smallint SMALLINT,
    col_integer INT,
    col_bigint BIGINT,
    col_real FLOAT,
    col_double DOUBLE,
    col_numeric DECIMAL(10, 2),
    col_char CHAR(10),
    col_varchar VARCHAR(100),
    col_text STRING,
    col_date DATE,
    col_time TIME(0),
    col_timestamp TIMESTAMP(6),
    col_timestamptz TIMESTAMP_LTZ(6),
    col_boolean BOOLEAN,
    col_bytea BYTES,
    col_json STRING,
    col_jsonb STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'data_types_test',
    'slot.name' = 'flink_cdc_datatypes',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- 查询验证数据类型
SELECT * FROM data_types_source;
```

---

## 6. 故障排查

### 6.1 常见问题

#### 问题 1: 连接 GaussDB 失败

**错误信息**: `Connection refused` 或 `Connection timed out`

**解决方案**:
```bash
# 1. 检查 GaussDB 是否可访问
docker exec -it flink-jobmanager ping 10.250.0.51

# 2. 检查端口是否开放
docker exec -it flink-jobmanager nc -zv 10.250.0.51 8000

# 3. 检查 GaussDB pg_hba.conf 配置
# 确保允许来自 Docker 网络的连接
```

#### 问题 2: 复制槽创建失败

**错误信息**: `ERROR: logical decoding requires wal_level >= logical`

**解决方案**:
```sql
-- 在 GaussDB 中执行
ALTER SYSTEM SET wal_level = 'logical';
-- 重启 GaussDB
```

#### 问题 3: 权限不足

**错误信息**: `ERROR: must be superuser or replication role to use replication slots`

**解决方案**:
```sql
-- 授予 REPLICATION 权限
ALTER USER cdc_user REPLICATION;
```

#### 问题 4: JAR 未加载

**错误信息**: `Could not find any factory for identifier 'gaussdb-cdc'`

**解决方案**:
```bash
# 1. 检查 JAR 文件是否存在
ls -la docker/jars/

# 2. 检查 JAR 是否正确挂载
docker exec -it flink-jobmanager ls -la /opt/flink/usrlib/

# 3. 重启 Flink 集群
docker-compose restart
```

#### 问题 5: Checkpoint 失败

**错误信息**: `Checkpoint expired before completing`

**解决方案**:
```sql
-- 增加 Checkpoint 超时时间
SET 'execution.checkpointing.timeout' = '120s';

-- 减少 Checkpoint 间隔
SET 'execution.checkpointing.interval' = '30s';
```

### 6.2 日志查看

```bash
# 查看 JobManager 日志
docker logs -f flink-jobmanager

# 查看 TaskManager 日志
docker logs -f flink-taskmanager

# 查看特定时间段的日志
docker logs --since 10m flink-taskmanager

# 进入容器查看日志文件
docker exec -it flink-jobmanager cat /opt/flink/log/flink-*-jobmanager-*.log
```

### 6.3 清理资源

```bash
# 停止 Flink 集群
docker-compose down

# 清理复制槽 (在 GaussDB 中执行)
SELECT pg_drop_replication_slot('flink_cdc_orders');
SELECT pg_drop_replication_slot('flink_cdc_products');
SELECT pg_drop_replication_slot('flink_cdc_users');

# 清理 Checkpoint 目录
rm -rf docker/flink-checkpoints/*
rm -rf docker/flink-savepoints/*
```

---

## 附录

### A. 完整测试脚本

将以下内容保存为 `docker/sql/test_all.sql`:

```sql
-- GaussDB CDC Connector 完整测试脚本

-- 1. 设置环境
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 2. 创建 CDC 源表
CREATE TABLE orders_source (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    quantity INT,
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '${GAUSSDB_HOST}',
    'port' = '${GAUSSDB_PORT}',
    'username' = '${GAUSSDB_USER}',
    'password' = '${GAUSSDB_PASSWORD}',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_test',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- 3. 创建 Print Sink
CREATE TABLE orders_print (
    order_id INT,
    customer_name STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    quantity INT,
    order_status STRING
) WITH (
    'connector' = 'print'
);

-- 4. 启动同步作业
INSERT INTO orders_print
SELECT order_id, customer_name, product_name, price, quantity, order_status
FROM orders_source;
```

### B. 环境变量配置

创建 `docker/.env` 文件:

```bash
# GaussDB 连接配置
GAUSSDB_HOST=10.250.0.51
GAUSSDB_PORT=8000
GAUSSDB_USER=cdc_user
GAUSSDB_PASSWORD=cdc_password
GAUSSDB_DATABASE=cdc_test
```

### C. 性能调优建议

1. **增加并行度**: 修改 `parallelism.default` 配置
2. **调整内存**: 增加 TaskManager 内存
3. **优化 Checkpoint**: 调整 Checkpoint 间隔和超时时间
4. **网络优化**: 确保 Flink 和 GaussDB 之间网络延迟低

---

## 版本信息

- Flink: 1.18.1
- GaussDB CDC Connector: 3.6-SNAPSHOT
- GaussDB: 5.x+
- Docker Compose: 3.8

---

**文档版本**: 1.0
**最后更新**: 2024-12-17
