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

-- 创建 schema
DROP SCHEMA IF EXISTS customer CASCADE;
CREATE SCHEMA customer;

-- 客户表（包含常见数据类型）
CREATE TABLE customer.customers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    phone_number VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 订单表（测试关联关系）
CREATE TABLE customer.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_name VARCHAR(100),
    quantity INT,
    price NUMERIC(10, 2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);

-- 初始化测试数据
INSERT INTO customer.customers VALUES
    (101, 'user_1', 'Shanghai', '123567891234', 'user1@example.com', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
    (102, 'user_2', 'Shanghai', '123567891234', 'user2@example.com', '2024-01-01 11:00:00', '2024-01-01 11:00:00'),
    (103, 'user_3', 'Shanghai', '123567891234', 'user3@example.com', '2024-01-01 12:00:00', '2024-01-01 12:00:00'),
    (109, 'user_4', 'Shanghai', '123567891234', 'user4@example.com', '2024-01-01 13:00:00', '2024-01-01 13:00:00');

INSERT INTO customer.orders VALUES
    (10001, 101, 'Product A', 2, 99.99, '2024-01-02 10:00:00', 'pending'),
    (10002, 102, 'Product B', 1, 149.99, '2024-01-02 11:00:00', 'shipped'),
    (10003, 103, 'Product C', 3, 299.99, '2024-01-02 12:00:00', 'delivered');

