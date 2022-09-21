-- Copyright 2022 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
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

-- create table whose split key is evenly distributed
CREATE TABLE customers_even_dist (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL ,
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);
INSERT INTO customers_even_dist
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (104,'user_4','Shanghai','123567891234'),
       (105,'user_5','Shanghai','123567891234'),
       (106,'user_6','Shanghai','123567891234'),
       (107,'user_7','Shanghai','123567891234'),
       (108,'user_8','Shanghai','123567891234'),
       (109,'user_9','Shanghai','123567891234'),
       (110,'user_10','Shanghai','123567891234');

-- create table whose split key is evenly distributed and sparse
CREATE TABLE customers_sparse_dist (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL ,
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);
INSERT INTO customers_sparse_dist
VALUES (2,'user_1','Shanghai','123567891234'),
       (4,'user_2','Shanghai','123567891234'),
       (6,'user_3','Shanghai','123567891234'),
       (8,'user_4','Shanghai','123567891234'),
       (10,'user_5','Shanghai','123567891234'),
       (16,'user_6','Shanghai','123567891234'),
       (17,'user_7','Shanghai','123567891234'),
       (18,'user_8','Shanghai','123567891234'),
       (20,'user_9','Shanghai','123567891234'),
       (22,'user_10','Shanghai','123567891234');

-- create table whose split key is evenly distributed and dense
CREATE TABLE customers_dense_dist (
 id1 INTEGER NOT NULL,
 id2 VARCHAR(255) NOT NULL ,
 address VARCHAR(1024),
 phone_number VARCHAR(512),
 PRIMARY KEY(id1, id2)
);
INSERT INTO customers_dense_dist
VALUES (1,'user_1','Shanghai','123567891234'),
       (1,'user_2','Shanghai','123567891234'),
       (1,'user_3','Shanghai','123567891234'),
       (1,'user_4','Shanghai','123567891234'),
       (2,'user_5','Shanghai','123567891234'),
       (2,'user_6','Shanghai','123567891234'),
       (2,'user_7','Shanghai','123567891234'),
       (3,'user_8','Shanghai','123567891234'),
       (3,'user_9','Shanghai','123567891234'),
       (3,'user_10','Shanghai','123567891234');

CREATE TABLE customers_no_pk (
   id INTEGER NOT NULL,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512)
);

INSERT INTO customers_no_pk
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

-- table has combined primary key and one of the primary key is evenly
CREATE TABLE evenly_shopping_cart (
  product_no INT NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(product_kind, product_no, user_id)
);

insert into evenly_shopping_cart
VALUES (101, 'KIND_001', 'user_1', 'my shopping cart'),
       (102, 'KIND_002', 'user_1', 'my shopping cart'),
       (103, 'KIND_007', 'user_1', 'my shopping cart'),
       (104, 'KIND_008', 'user_1', 'my shopping cart'),
       (105, 'KIND_100', 'user_2', 'my shopping list'),
       (105, 'KIND_999', 'user_3', 'my shopping list'),
       (107, 'KIND_010', 'user_4', 'my shopping list'),
       (108, 'KIND_009', 'user_4', 'my shopping list'),
       (109, 'KIND_002', 'user_5', 'leo list'),
       (111, 'KIND_007', 'user_5', 'leo list'),
       (111, 'KIND_008', 'user_5', 'leo list'),
       (112, 'KIND_009', 'user_6', 'my shopping cart');

-- table has bigint unsigned auto increment primary key
CREATE TABLE shopping_cart_big (
  product_no BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY(product_no)
);

insert into shopping_cart_big
VALUES (default, 'KIND_001', 'user_1', 'my shopping cart'),
       (default, 'KIND_002', 'user_1', 'my shopping cart'),
       (default, 'KIND_003', 'user_1', 'my shopping cart');

-- table has decimal primary key
CREATE TABLE shopping_cart_dec (
  product_no DECIMAL(10, 4) NOT NULL,
  product_kind VARCHAR(255),
  user_id VARCHAR(255) NOT NULL,
  description VARCHAR(255) DEFAULT 'flink',
  PRIMARY KEY(product_no)
);

insert into shopping_cart_dec
VALUES (123456.123, 'KIND_001', 'user_1', 'my shopping cart'),
       (123457.456, 'KIND_002', 'user_2', 'my shopping cart'),
       (123458.6789, 'KIND_003', 'user_3', 'my shopping cart'),
       (123459.1234, 'KIND_004', 'user_4', null);

-- create table whose primary key are produced by snowflake algorithm
CREATE TABLE address (
  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  country VARCHAR(255) NOT NULL,
  city VARCHAR(255) NOT NULL,
  detail_address VARCHAR(1024)
);

INSERT INTO address
VALUES (416874195632735147, 'China', 'Beijing', 'West Town address 1'),
       (416927583791428523, 'China', 'Beijing', 'West Town address 2'),
       (417022095255614379, 'China', 'Beijing', 'West Town address 3'),
       (417111867899200427, 'America', 'New York', 'East Town address 1'),
       (417271541558096811, 'America', 'New York', 'East Town address 2'),
       (417272886855938987, 'America', 'New York', 'East Town address 3'),
       (417420106184475563, 'Germany', 'Berlin', 'West Town address 1'),
       (418161258277847979, 'Germany', 'Berlin', 'West Town address 2');

CREATE TABLE default_value_test (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number INTEGER DEFAULT ' 123 '
);
INSERT INTO default_value_test
VALUES (1,'user1','Shanghai',123567),
       (2,'user2','Shanghai',123567);
