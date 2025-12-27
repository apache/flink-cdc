-- Flink SQL Job to sync Distributed GaussDB to GaussDB (Sink via CN)
-- 分布式 GaussDB -> GaussDB 同步任务

-- 设置状态保留时间为 1 小时
SET 'table.exec.state.ttl' = '1 h';
-- 禁用 Upsert Materialize，因为数据是按 ID 分片的，不存在多节点更新同一行的乱序问题
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- =====================================================
-- 1. Source Table DN1 (GaussDB)
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
    'hostname' = '10.250.0.30',
    'port' = '40000',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn1',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- 2. Source Table DN2 (GaussDB)
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
    'hostname' = '10.250.0.181',
    'port' = '40020',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn2',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- 3. Source Table DN3 (GaussDB)
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
    'hostname' = '10.250.0.157',
    'port' = '40040',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_g2g_dn3',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- =====================================================
-- 4. Sink Table (GaussDB via CN)
-- 使用自定义 GaussDB JDBC Dialect (支持 MERGE INTO)
-- 使用 gaussdbjdbc.jar 驱动
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
    'url' = 'jdbc:gaussdb://10.250.0.30:8000/db1?currentSchema=public',
    'table-name' = 'products_sink',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver'
);

-- =====================================================
-- 5. Sync Job (Union all DNs)
-- =====================================================
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock FROM products_source_dn1
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn2
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn3;
