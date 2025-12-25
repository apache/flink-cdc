-- Flink SQL Job to sync Distributed GaussDB to MySQL

-- 设置状态保留时间为 1 小时
SET 'table.exec.state.ttl' = '1 h';
-- 禁用 Upsert Materialize，因为数据是按 ID 分片的，不存在多节点更新同一行的乱序问题
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- 1. Source Table DN1 (GaussDB)
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
    'slot.name' = 'flink_cdc_dn1',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- 2. Source Table DN2 (GaussDB)
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
    'slot.name' = 'flink_cdc_dn2',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);

-- 3. Source Table DN3 (GaussDB)
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
    'slot.name' = 'flink_cdc_dn3',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true'
);


-- 4. Sink Table (MySQL)
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

-- 5. Sync Job (Union all DNs)
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock FROM products_source_dn1
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn2
UNION ALL
SELECT product_id, product_name, category, price, stock FROM products_source_dn3;
