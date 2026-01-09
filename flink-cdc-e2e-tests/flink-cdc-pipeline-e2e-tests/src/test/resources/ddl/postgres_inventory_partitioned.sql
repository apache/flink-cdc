-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Partitioned table schema for PostgreSQL 10+ partition routing tests.
-- In PG10, primary keys are defined on child partitions, not the parent table.

DROP SCHEMA IF EXISTS inventory_partitioned CASCADE;
CREATE SCHEMA inventory_partitioned;
SET search_path TO inventory_partitioned;

-- Create partitioned parent table (no PK on parent in PG10 style)
CREATE TABLE products (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    description VARCHAR(512),
    weight FLOAT,
    country VARCHAR(20) NOT NULL
) PARTITION BY LIST(country);

ALTER SEQUENCE products_id_seq RESTART WITH 101;

-- Create child partitions with primary keys
CREATE TABLE products_uk PARTITION OF products
    FOR VALUES IN ('uk');
ALTER TABLE products_uk ADD PRIMARY KEY (id, country);
ALTER TABLE products_uk REPLICA IDENTITY FULL;

CREATE TABLE products_us PARTITION OF products
    FOR VALUES IN ('us');
ALTER TABLE products_us ADD PRIMARY KEY (id, country);
ALTER TABLE products_us REPLICA IDENTITY FULL;

-- Insert initial data
INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14, 'us'),
       (default,'car battery','12V car battery',8.1, 'us'),
       (default,'hammer','12oz carpenter''s hammer',0.75, 'us'),
       (default,'hammer','16oz carpenter''s hammer',1.0, 'uk'),
       (default,'rocks','box of assorted rocks',5.3, 'uk'),
       (default,'jacket','water resistent black wind breaker',0.1, 'uk');
