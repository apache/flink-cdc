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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  oceanbase_ddl_test
-- ----------------------------------------------------------------------------------------------------------------

-- Create orders table with single primary key
create table orders (
 id bigint not null auto_increment,
 seller_id varchar(30) DEFAULT NULL,
 order_id varchar(30) DEFAULT NULL,
 buyer_id varchar(30) DEFAULT NULL,
 create_time datetime DEFAULT NULL,
 primary key(id),
 INDEX `i_seller`(`seller_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by key(id) partitions 3;

-- insert some orders for testing
INSERT INTO orders
VALUES (1, 1001, 1, 102, '2022-01-16'),
       (2, 1002, 2, 105, '2022-01-16'),
       (3, 1004, 3, 109, '2022-01-16'),
       (4, 1002, 2, 106, '2022-01-16'),
       (5, 1003, 1, 107, '2022-01-16');

-- Create orders with multi primary keys
create table orders_with_multi_pks (
 id bigint not null auto_increment,
 seller_id varchar(30) DEFAULT NULL,
 order_id varchar(30) NOT NULL,
 buyer_id varchar(30) DEFAULT NULL,
 create_time datetime DEFAULT NULL,
 primary key(id, order_id),
 INDEX `g_mi_seller`(`seller_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by key(id, order_id) partitions 3;

-- insert some orders for testing
INSERT INTO orders_with_multi_pks
VALUES (1, 1001, 1, 102, '2022-01-16'),
       (2, 1002, 2, 105, '2022-01-16'),
       (3, 1004, 3, 109, '2022-01-16'),
       (4, 1002, 2, 106, '2022-01-16'),
       (5, 1003, 1, 107, '2022-01-16');


-- create table with full types
CREATE TABLE oceanbase_full_types (
    id INT AUTO_INCREMENT,
    tiny_c TINYINT,
    tiny_un_c TINYINT UNSIGNED,
    small_c SMALLINT,
    small_un_c SMALLINT UNSIGNED,
    medium_c MEDIUMINT,
    medium_un_c MEDIUMINT UNSIGNED,
    int_c INTEGER ,
    int_un_c INTEGER UNSIGNED,
    int11_c INT(11) DEFAULT 0,
    big_c BIGINT,
    big_un_c BIGINT UNSIGNED,
    varchar_c VARCHAR(255) DEFAULT '1',
    char_c CHAR(3) DEFAULT '',
    real_c REAL,
    float_c FLOAT,
    double_c DOUBLE,
    decimal_c DECIMAL(8, 4),
    numeric_c NUMERIC(6, 0),
    big_decimal_c DECIMAL(65, 1),
    bit1_c BIT,
    tiny1_c TINYINT(1),
    boolean_c BOOLEAN,
    date_c DATE,
    time_c TIME(0),
    datetime3_c DATETIME(3),
    datetime6_c DATETIME(6),
    timestamp_c TIMESTAMP,
    file_uuid BINARY(16),
    bit_c BIT(64),
    text_c TEXT,
    tiny_blob_c TINYBLOB,
    blob_c BLOB,
    medium_blob_c MEDIUMBLOB,
    long_blob_c LONGBLOB,
    year_c YEAR,
    enum_c enum('red', 'white') default 'red',
    set_c SET('a', 'b'),
    json_c JSON,
    point_c POINT,
    geometry_c GEOMETRY,
    linestring_c LINESTRING,
    polygon_c POLYGON,
    multipoint_c MULTIPOINT,
    multiline_c MULTILINESTRING,
    multipolygon_c MULTIPOLYGON,
    geometrycollection_c GEOMETRYCOLLECTION,
    PRIMARY KEY (id),
    INDEX `g_mit_seller`(`int_c`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by Hash(id) partitions 3;

INSERT INTO oceanbase_full_types VALUES (
    DEFAULT, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 2147483647, 9223372036854775807,
    18446744073709551615,
    'Hello World', 'abc', 123.102, 123.102, 404.4443, 123.4567, 345.6, 34567892.1, 0, 1, true,
    '2020-07-17', '18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22',
    unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400','-','')), b'0000010000000100000001000000010000000100000001000000010000000100',
    'text',UNHEX(HEX(16)),UNHEX(HEX(16)),UNHEX(HEX(16)),UNHEX(HEX(16)), 2021,
    'red', 'a,b,a', '{"key1": "value1", "key2": "value2", "num1": 1.6708304E7, "num2": 16708305}',
    ST_GeomFromText('POINT(1 1)'),
    ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
    ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),
    ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
    ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
    ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
    ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),
    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')
);

-- Create orders_sink for testing the sink of flink-jdbc-connector
create table orders_sink (
 id bigint not null auto_increment,
 seller_id varchar(30) DEFAULT NULL,
 order_id varchar(30) DEFAULT NULL,
 buyer_id varchar(30) DEFAULT NULL,
 create_time datetime DEFAULT NULL,
 primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(id) partitions 3;


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


CREATE TABLE customers_no_pk (
   id INTEGER,
   name VARCHAR(255) DEFAULT 'flink',
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