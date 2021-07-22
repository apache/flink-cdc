-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  customer
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our users using a single insert with many rows
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);

INSERT INTO customers
VALUES (101,"user_1","Shanghai","123567891234"),
       (102,"user_2","Shanghai","123567891234"),
       (103,"user_3","Shanghai","123567891234"),
       (109,"user_4","Shanghai","123567891234"),
       (110,"user_5","Shanghai","123567891234"),
       (111,"user_6","Shanghai","123567891234"),
       (118,"user_7","Shanghai","123567891234"),
       (121,"user_8","Shanghai","123567891234"),
       (123,"user_9","Shanghai","123567891234"),
       (1009,"user_10","Shanghai","123567891234"),
       (1010,"user_11","Shanghai","123567891234"),
       (1011,"user_12","Shanghai","123567891234"),
       (1012,"user_13","Shanghai","123567891234"),
       (1013,"user_14","Shanghai","123567891234"),
       (1014,"user_15","Shanghai","123567891234"),
       (1015,"user_16","Shanghai","123567891234"),
       (1016,"user_17","Shanghai","123567891234"),
       (1017,"user_18","Shanghai","123567891234"),
       (1018,"user_19","Shanghai","123567891234"),
       (1019,"user_20","Shanghai","123567891234"),
       (2000,"user_21","Shanghai","123567891234");

-- table has same name prefix with 'customers.*'
CREATE TABLE customers_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);

INSERT INTO customers_1
VALUES (101,"user_1","Shanghai","123567891234"),
       (102,"user_2","Shanghai","123567891234"),
       (103,"user_3","Shanghai","123567891234"),
       (109,"user_4","Shanghai","123567891234"),
       (110,"user_5","Shanghai","123567891234"),
       (111,"user_6","Shanghai","123567891234"),
       (118,"user_7","Shanghai","123567891234"),
       (121,"user_8","Shanghai","123567891234"),
       (123,"user_9","Shanghai","123567891234"),
       (1009,"user_10","Shanghai","123567891234"),
       (1010,"user_11","Shanghai","123567891234"),
       (1011,"user_12","Shanghai","123567891234"),
       (1012,"user_13","Shanghai","123567891234"),
       (1013,"user_14","Shanghai","123567891234"),
       (1014,"user_15","Shanghai","123567891234"),
       (1015,"user_16","Shanghai","123567891234"),
       (1016,"user_17","Shanghai","123567891234"),
       (1017,"user_18","Shanghai","123567891234"),
       (1018,"user_19","Shanghai","123567891234"),
       (1019,"user_20","Shanghai","123567891234"),
       (2000,"user_21","Shanghai","123567891234");

-- table has combined primary key
CREATE TABLE customer_card (
  card_no BIGINT NOT NULL,
  level VARCHAR(10) NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  note VARCHAR(1024),
  PRIMARY KEY(card_no, level)
);

insert into customer_card
VALUES (20001, 'LEVEL_4', 'user_1', 'user with level 4'),
       (20002, 'LEVEL_4', 'user_2', 'user with level 4'),
       (20003, 'LEVEL_4', 'user_3', 'user with level 4'),
       (20004, 'LEVEL_4', 'user_4', 'user with level 4'),
       (20004, 'LEVEL_1', 'user_4', 'user with level 4'),
       (20004, 'LEVEL_2', 'user_4', 'user with level 4'),
       (20004, 'LEVEL_3', 'user_4', 'user with level 4'),
       (30006, 'LEVEL_3', 'user_5', 'user with level 3'),
       (30007, 'LEVEL_3', 'user_6', 'user with level 3'),
       (30008, 'LEVEL_3', 'user_7', 'user with level 3'),
       (30009, 'LEVEL_3', 'user_8', 'user with level 3'),
       (30009, 'LEVEL_2', 'user_8', 'user with level 3'),
       (30009, 'LEVEL_1', 'user_8', 'user with level 3'),
       (40001, 'LEVEL_2', 'user_9', 'user with level 2'),
       (40002, 'LEVEL_2', 'user_10', 'user with level 2'),
       (40003, 'LEVEL_2', 'user_11', 'user with level 2'),
       (50001, 'LEVEL_1', 'user_12', 'user with level 1'),
       (50002, 'LEVEL_1', 'user_13', 'user with level 1'),
       (50003, 'LEVEL_1', 'user_14', 'user with level 1');

-- table has single line
CREATE TABLE customer_card_single_line (
  card_no BIGINT NOT NULL,
  level VARCHAR(10) NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  note VARCHAR(1024),
  PRIMARY KEY(card_no, level)
);

insert into customer_card_single_line
VALUES (20001, 'LEVEL_1', 'user_1', 'user with level 1');


-- table has combined primary key
CREATE TABLE shopping_cart (
  product_no INT NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(user_id, product_no, product_kind)
);

insert into shopping_cart
VALUES (101, 'KIND_001', 'user_1', 'my shopping cart'),
       (101, 'KIND_002', 'user_1', 'my shopping cart'),
       (102, 'KIND_007', 'user_1', 'my shopping cart'),
       (102, 'KIND_008', 'user_1', 'my shopping cart'),
       (501, 'KIND_100', 'user_2', 'my shopping list'),
       (701, 'KIND_999', 'user_3', 'my shopping list'),
       (801, 'KIND_010', 'user_4', 'my shopping list'),
       (600, 'KIND_009', 'user_4', 'my shopping list'),
       (401, 'KIND_002', 'user_5', 'leo list'),
       (401, 'KIND_007', 'user_5', 'leo list'),
       (404, 'KIND_008', 'user_5', 'leo list'),
       (600, 'KIND_009', 'user_6', 'my shopping cart');

-- table has bigint unsigned primary key
CREATE TABLE shopping_cart_big (
  product_no BIGINT UNSIGNED NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(product_no)
);

insert into shopping_cart_big
VALUES (9223372036854773807, 'KIND_001', 'user_1', 'my shopping cart'),
       (9223372036854774807, 'KIND_002', 'user_1', 'my shopping cart'),
       (9223372036854775807, 'KIND_003', 'user_1', 'my shopping cart');

-- table has decimal primary key
CREATE TABLE shopping_cart_dec (
  product_no DECIMAL(10, 4) NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(product_no)
);

insert into shopping_cart_dec
VALUES (123456.123, 'KIND_001', 'user_1', 'my shopping cart'),
       (124456.456, 'KIND_002', 'user_1', 'my shopping cart'),
       (125489.6789, 'KIND_003', 'user_1', 'my shopping cart');
