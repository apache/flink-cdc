-- Optimized Flink SQL Job for GaussDB to GaussDB Synchronization
-- Uses dual-sink routing to optimize snapshot phase with INSERT INTO

-- =====================================================
-- Performance Tuning Settings
-- =====================================================
SET 'parallelism.default' = '4';
SET 'table.exec.state.ttl' = '36h';
SET 'table.optimizer.source.reuse-enabled' = 'true';
SET 'table.exec.sink.upsert-materialize' = 'AUTO';
SET 'checkpoint.interval' = '10s';
SET 'pipeline.object-reuse' = 'true';
SET 'execution.checkpointing.interval' = '10s';

-- =====================================================
-- 1. Source Table DN1 (GaussDB)
-- =====================================================
CREATE TABLE source_dn1 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    is_snapshot BOOLEAN METADATA FROM 'is_snapshot' VIRTUAL,
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
-- 2. Source Table DN2 (GaussDB)
-- =====================================================
CREATE TABLE source_dn2 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    is_snapshot BOOLEAN METADATA FROM 'is_snapshot' VIRTUAL,
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
-- 3. Source Table DN3 (GaussDB)
-- =====================================================
CREATE TABLE source_dn3 (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    is_snapshot BOOLEAN METADATA FROM 'is_snapshot' VIRTUAL,
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
-- 4. Fast Sink (Snapshot phase)
-- No primary key defined in Flink -> Uses INSERT INTO
-- =====================================================
CREATE TABLE products_sink_fast (
    product_id INT PRIMARY KEY NOT ENFORCED,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:gaussdb://10.250.0.30:8000/db1?currentSchema=public',
    'table-name' = 'products_sink_FAST_INSERT_ONLY',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'driver' = 'com.huawei.gaussdb.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '1s'
);

-- =====================================================
-- 5. Upsert Sink (Incremental phase)
-- Primary key defined -> Uses MERGE INTO (GaussDB Dialect)
-- =====================================================
CREATE TABLE products_sink_upsert (
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
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s'
);

-- =====================================================
-- 6. Unified Source View
-- =====================================================
CREATE TEMPORARY VIEW unified_source AS
SELECT product_id, product_name, category, price, stock, is_snapshot FROM source_dn1
UNION ALL
SELECT product_id, product_name, category, price, stock, is_snapshot FROM source_dn2
UNION ALL
SELECT product_id, product_name, category, price, stock, is_snapshot FROM source_dn3;

-- =====================================================
-- 7. Routing Statement Set
-- =====================================================
BEGIN STATEMENT SET;

-- Snapshot records route to fast sink (using blind INSERT)
INSERT INTO products_sink_fast
SELECT product_id, product_name, category, price, stock
FROM unified_source 
WHERE is_snapshot = true;

-- Incremental records route to upsert sink (using MERGE INTO)
INSERT INTO products_sink_upsert
SELECT product_id, product_name, category, price, stock
FROM unified_source 
WHERE is_snapshot = false;

END;
