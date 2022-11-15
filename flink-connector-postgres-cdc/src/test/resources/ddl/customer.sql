-- Copyright 2023 Ververica Inc.
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

DROP SCHEMA IF EXISTS customer CASCADE;
CREATE SCHEMA customer;
SET search_path TO customer;

-- Create and populate our users using a single insert with many rows
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);
ALTER TABLE customers REPLICA IDENTITY FULL;

INSERT INTO customers
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');

-- table has same name prefix with 'customers.*'
CREATE TABLE customers_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);
ALTER TABLE customers_1 REPLICA IDENTITY FULL;

INSERT INTO customers_1
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');

CREATE TABLE customers_no_pk (
   id INTEGER NOT NULL,
   name VARCHAR(255) NOT NULL DEFAULT 'flink',
   address VARCHAR(1024),
   phone_number VARCHAR(512)
);

INSERT INTO customers_no_pk
VALUES (101,'user_1','Shanghai','123567891234'),
       (102,'user_2','Shanghai','123567891234'),
       (103,'user_3','Shanghai','123567891234'),
       (109,'user_4','Shanghai','123567891234'),
       (110,'user_5','Shanghai','123567891234'),
       (111,'user_6','Shanghai','123567891234'),
       (118,'user_7','Shanghai','123567891234'),
       (121,'user_8','Shanghai','123567891234'),
       (123,'user_9','Shanghai','123567891234'),
       (1009,'user_10','Shanghai','123567891234'),
       (1010,'user_11','Shanghai','123567891234'),
       (1011,'user_12','Shanghai','123567891234'),
       (1012,'user_13','Shanghai','123567891234'),
       (1013,'user_14','Shanghai','123567891234'),
       (1014,'user_15','Shanghai','123567891234'),
       (1015,'user_16','Shanghai','123567891234'),
       (1016,'user_17','Shanghai','123567891234'),
       (1017,'user_18','Shanghai','123567891234'),
       (1018,'user_19','Shanghai','123567891234'),
       (1019,'user_20','Shanghai','123567891234'),
       (2000,'user_21','Shanghai','123567891234');
