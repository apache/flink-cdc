-- Flink SQL CDC 测试脚本
-- 在 Flink SQL Client 中执行

-- ============================================
-- 1. 环境配置
-- ============================================

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '60s';
SET 'sql-client.execution.result-mode' = 'changelog';

-- ============================================
-- 2. 创建 CDC 源表
-- ============================================

-- 订单表 CDC 源
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
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'your_username',
    'password' = 'your_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_cdc_orders',
    'decoding.plugin.name' = 'mppdb_decoding'
);

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
    'username' = 'your_username',
    'password' = 'your_password',
    'database-name' = 'cdc_test',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_products',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- ============================================
-- 3. 创建 Sink 表
-- ============================================

-- Print Sink (用于调试)
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

-- ============================================
-- 4. 测试查询
-- ============================================

-- 查询订单数据
-- SELECT * FROM orders_source;

-- 查询商品数据
-- SELECT * FROM products_source;

-- 启动同步作业
-- INSERT INTO orders_print
-- SELECT order_id, customer_name, product_name, price, quantity, order_status
-- FROM orders_source;
