-- GaussDB 初始化脚本
-- 在 GaussDB 中执行此脚本来准备测试环境

-- 创建测试数据库 (如果不存在)
-- CREATE DATABASE cdc_test;

-- 连接到测试数据库后执行以下内容
-- \c cdc_test

-- ============================================
-- 1. 创建测试表
-- ============================================

-- 订单表
DROP TABLE IF EXISTS orders CASCADE;
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

-- 商品表
DROP TABLE IF EXISTS products CASCADE;
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 用户表
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 数据类型测试表
DROP TABLE IF EXISTS data_types_test CASCADE;
CREATE TABLE data_types_test (
    id SERIAL PRIMARY KEY,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_numeric NUMERIC(10, 2),
    col_char CHAR(10),
    col_varchar VARCHAR(100),
    col_text TEXT,
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    col_boolean BOOLEAN,
    col_bytea BYTEA,
    col_json JSON,
    col_jsonb JSONB
);

-- ============================================
-- 2. 插入初始测试数据
-- ============================================

-- 订单数据
INSERT INTO orders (customer_name, product_name, price, quantity, order_status) VALUES
('张三', 'iPhone 15', 7999.00, 1, 'COMPLETED'),
('李四', 'MacBook Pro', 14999.00, 1, 'PENDING'),
('王五', 'AirPods Pro', 1999.00, 2, 'SHIPPED'),
('赵六', 'iPad Pro', 8999.00, 1, 'PENDING'),
('钱七', 'Apple Watch', 3999.00, 1, 'COMPLETED');

-- 商品数据
INSERT INTO products (product_name, category, price, stock) VALUES
('iPhone 15', '手机', 7999.00, 100),
('MacBook Pro', '电脑', 14999.00, 50),
('AirPods Pro', '配件', 1999.00, 200),
('iPad Pro', '平板', 8999.00, 80),
('Apple Watch', '手表', 3999.00, 150);

-- 用户数据
INSERT INTO users (username, email, phone, status) VALUES
('zhangsan', 'zhangsan@example.com', '13800138001', 'ACTIVE'),
('lisi', 'lisi@example.com', '13800138002', 'ACTIVE'),
('wangwu', 'wangwu@example.com', '13800138003', 'INACTIVE'),
('zhaoliu', 'zhaoliu@example.com', '13800138004', 'ACTIVE'),
('qianqi', 'qianqi@example.com', '13800138005', 'ACTIVE');

-- 数据类型测试数据
INSERT INTO data_types_test (
    col_smallint, col_integer, col_bigint, col_real, col_double, col_numeric,
    col_char, col_varchar, col_text,
    col_date, col_time, col_timestamp, col_timestamptz,
    col_boolean, col_bytea, col_json, col_jsonb
) VALUES (
    32767, 2147483647, 9223372036854775807, 3.14, 3.141592653589793, 12345.67,
    'CHAR      ', 'VARCHAR测试', 'TEXT长文本测试内容',
    '2024-01-15', '12:30:45', '2024-01-15 12:30:45', '2024-01-15 12:30:45+08',
    true, E'\\xDEADBEEF', '{"key": "value"}', '{"name": "test", "count": 100}'
);

-- ============================================
-- 3. 验证数据
-- ============================================

SELECT 'orders' as table_name, COUNT(*) as row_count FROM orders
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'users', COUNT(*) FROM users
UNION ALL
SELECT 'data_types_test', COUNT(*) FROM data_types_test;

-- ============================================
-- 4. 检查复制槽 (可选)
-- ============================================

-- 查看已存在的复制槽
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;

-- 如需清理旧的复制槽
-- SELECT pg_drop_replication_slot('flink_cdc_orders');

COMMIT;
