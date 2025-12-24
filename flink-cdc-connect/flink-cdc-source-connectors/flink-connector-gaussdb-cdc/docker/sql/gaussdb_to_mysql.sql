-- Flink SQL Job to sync GaussDB to MySQL

-- 1. Source Table (GaussDB)
CREATE TABLE products_source (
    product_id INT NOT NULL,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'gaussdb-cdc',
    'hostname' = '10.250.0.51',
    'port' = '8000',
    'username' = 'tom',
    'password' = 'Gauss_235',
    'database-name' = 'db1',
    'schema-name' = 'public',
    'table-name' = 'products',
    'slot.name' = 'flink_cdc_e2e_test',
    'decoding.plugin.name' = 'mppdb_decoding',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.newly-added-table.enabled' = 'false'
);


-- 2. Sink Table (MySQL)
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

-- 3. Sync Job
INSERT INTO products_sink
SELECT product_id, product_name, category, price, stock
FROM products_source;
