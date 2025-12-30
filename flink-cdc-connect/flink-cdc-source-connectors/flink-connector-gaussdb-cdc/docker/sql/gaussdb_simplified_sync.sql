-- Simplified Flink SQL Job for GaussDB to GaussDB Synchronization
-- Recommended standard approach after JDBC Dialect UPSERT refactor

-- =====================================================
-- Performance Tuning Settings
-- =====================================================
SET 'parallelism.default' = '1';
SET 'checkpoint.interval' = '10s';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.optimizer.source.reuse-enabled' = 'true';

-- =====================================================
-- 1. Source Tables (Distributed GaussDB)
-- =====================================================
CREATE TABLE source_dn1 (
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
    'slot.name' = 'flink_cdc_simplified_dn1',
    'decoding.plugin.name' = 'mppdb_decoding'
);

CREATE TABLE source_dn2 (
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
    'slot.name' = 'flink_cdc_simplified_dn2',
    'decoding.plugin.name' = 'mppdb_decoding'
);

CREATE TABLE source_dn3 (
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
    'slot.name' = 'flink_cdc_simplified_dn3',
    'decoding.plugin.name' = 'mppdb_decoding'
);

-- =====================================================
-- 2. Simplified Sink Table
-- Single Sink is now efficient for both Snapshot and Incremental phases
-- =====================================================
CREATE TABLE products_sink (
    product_id INT PRIMARY KEY NOT ENFORCED,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://10.250.0.30:8000/db1?currentSchema=public',
    'table-name' = 'products_sink',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver',
    -- Batch settings for overall performance
    'sink.buffer-flush.max-rows' = '2000',
    'sink.buffer-flush.interval' = '1s'
);

-- =====================================================
-- 3. Unified Sync (Transparent UPSERT)
-- =====================================================
-- =====================================================
-- 3. Unified Sync (Independent Pipelines for Stability)
-- =====================================================
BEGIN STATEMENT SET;
INSERT INTO products_sink SELECT product_id, product_name, category, price, stock FROM source_dn1;
INSERT INTO products_sink SELECT product_id, product_name, category, price, stock FROM source_dn2;
INSERT INTO products_sink SELECT product_id, product_name, category, price, stock FROM source_dn3;
END;
