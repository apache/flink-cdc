-- Flink SQL Job to sync Distributed GaussDB to GaussDB (Sink via CN)
-- 分布式 GaussDB -> GaussDB 同步任务 (性能优化版)

-- =====================================================
-- Performance Tuning Settings
-- =====================================================
SET 'table.exec.state.ttl' = '1 h';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'parallelism.default' = '4';
SET 'pipeline.object-reuse' = 'true';
SET 'execution.checkpointing.interval' = '5min';

-- =====================================================
-- 1. Source Table DN1 (GaussDB) - with performance tuning
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
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.snapshot.fetch.size' = '10000',
    'scan.incremental.snapshot.chunk.size' = '50000'
);

-- =====================================================
-- 2. Source Table DN2 (GaussDB) - with performance tuning
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
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.snapshot.fetch.size' = '10000',
    'scan.incremental.snapshot.chunk.size' = '50000'
);

-- =====================================================
-- 3. Source Table DN3 (GaussDB) - with performance tuning
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
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.snapshot.fetch.size' = '10000',
    'scan.incremental.snapshot.chunk.size' = '50000'
);

-- =====================================================
-- 4. Sink Table (GaussDB via CN)
-- 使用自定义 GaussDB JDBC Dialect (支持 MERGE INTO)
-- 性能优化: 批量写入配置
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
    'driver' = 'com.huawei.gaussdb.jdbc.Driver',
    -- Performance tuning: batch write settings
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3',
    'sink.parallelism' = '3'
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

