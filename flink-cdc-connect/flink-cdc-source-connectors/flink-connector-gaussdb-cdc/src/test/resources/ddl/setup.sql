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

-- One-shot initialization for GaussDB CDC connector integration tests.
-- This script is a convenience wrapper that (re)creates all test schemas/tables/data
-- from customer.sql, datatypes.sql and replica_identity.sql.

-- --------------------------------------------------------------------------------------------
-- customer.sql
-- --------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS customer CASCADE;
CREATE SCHEMA customer;

CREATE TABLE customer.customers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    phone_number VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE customer.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_name VARCHAR(100),
    quantity INT,
    price NUMERIC(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);

INSERT INTO customer.customers VALUES
    (101, 'user_1', 'Shanghai', '123567891234', 'user1@example.com', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
    (102, 'user_2', 'Shanghai', '123567891234', 'user2@example.com', '2024-01-01 11:00:00', '2024-01-01 11:00:00'),
    (103, 'user_3', 'Shanghai', '123567891234', 'user3@example.com', '2024-01-01 12:00:00', '2024-01-01 12:00:00'),
    (109, 'user_4', 'Shanghai', '123567891234', 'user4@example.com', '2024-01-01 13:00:00', '2024-01-01 13:00:00');

INSERT INTO customer.orders VALUES
    (10001, 101, 'Product A', 2, 99.99, '2024-01-02 10:00:00', 'pending'),
    (10002, 102, 'Product B', 1, 149.99, '2024-01-02 11:00:00', 'shipped'),
    (10003, 103, 'Product C', 3, 299.99, '2024-01-02 12:00:00', 'delivered');

-- --------------------------------------------------------------------------------------------
-- datatypes.sql
-- --------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS test_types CASCADE;
CREATE SCHEMA test_types;

CREATE TABLE test_types.numeric_types (
    id INT PRIMARY KEY,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    decimal_col DECIMAL(10, 2),
    numeric_col NUMERIC(15, 5),
    real_col REAL,
    double_col DOUBLE PRECISION
);

CREATE TABLE test_types.string_types (
    id INT PRIMARY KEY,
    char_col CHAR(10),
    varchar_col VARCHAR(100),
    text_col TEXT
);

CREATE TABLE test_types.temporal_types (
    id INT PRIMARY KEY,
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMP WITH TIME ZONE
);

CREATE TABLE test_types.special_types (
    id INT PRIMARY KEY,
    boolean_col BOOLEAN,
    uuid_col UUID,
    json_col JSON,
    array_col INTEGER[]
);

INSERT INTO test_types.numeric_types VALUES
    (1, 100, 10000, 1000000000, 123.45, 12345.67890, 3.14, 2.718281828);

INSERT INTO test_types.string_types VALUES
    (1, 'test', 'variable length', 'This is a long text field for testing');

INSERT INTO test_types.temporal_types VALUES
    (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00', '2024-01-15 14:30:00+08');

INSERT INTO test_types.special_types VALUES
    (1, true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"key": "value"}', ARRAY[1,2,3,4,5]);

-- --------------------------------------------------------------------------------------------
-- replica_identity.sql
-- --------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS replication_test CASCADE;
CREATE SCHEMA replication_test;

CREATE TABLE replication_test.full_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);
ALTER TABLE replication_test.full_replica REPLICA IDENTITY FULL;

CREATE TABLE replication_test.default_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);

CREATE TABLE replication_test.nothing_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);
ALTER TABLE replication_test.nothing_replica REPLICA IDENTITY NOTHING;

INSERT INTO replication_test.full_replica VALUES (1, 'test1', 100);
INSERT INTO replication_test.default_replica VALUES (2, 'test2', 200);
INSERT INTO replication_test.nothing_replica VALUES (3, 'test3', 300);

