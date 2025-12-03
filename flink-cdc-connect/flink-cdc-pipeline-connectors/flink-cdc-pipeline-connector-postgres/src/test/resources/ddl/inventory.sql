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

-- Create the schema that we'll use to populate data and watch the effect in the WAL
DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id SERIAL NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight FLOAT(38)
);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14),
       (default,'car battery','12V car battery',8.1),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),
       (default,'hammer','12oz carpenter''s hammer',0.75),
       (default,'hammer','14oz carpenter''s hammer',0.875),
       (default,'hammer','16oz carpenter''s hammer',1.0),
       (default,'rocks','box of assorted rocks',5.3),
       (default,'jacket','water resistent black wind breaker',0.1),
       (default,'spare tire','24 inch spare tire',22.2);

-- Create customers table
CREATE TABLE customers (
                           id SERIAL PRIMARY KEY,
                           first_name VARCHAR(255) NOT NULL,
                           last_name VARCHAR(255) NOT NULL,
                           email VARCHAR(255) NOT NULL UNIQUE
);

ALTER SEQUENCE customers_id_seq RESTART WITH 1001;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert data into customers table
INSERT INTO customers (first_name, last_name, email)
VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com'),
       ('George', 'Bailey', 'gbailey@foobar.com'),
       ('Edward', 'Walker', 'ed@walker.com'),
       ('Anne', 'Kretchmar', 'annek@noanswer.org');

-- Create orders table
CREATE TABLE orders (
                        order_number SERIAL PRIMARY KEY,
                        order_date DATE NOT NULL,
                        purchaser INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        CONSTRAINT fk_customer FOREIGN KEY (purchaser) REFERENCES customers(id),
                        CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert data into orders table
INSERT INTO orders (order_date, purchaser, quantity, product_id)
VALUES ('2016-01-16', 1001, 1, 102),
       ('2016-01-17', 1002, 2, 105),
       ('2016-02-18', 1004, 3, 109),
       ('2016-02-19', 1002, 2, 106),
       ('2016-02-21', 1003, 1, 107);

-- Create multi_max_table
CREATE TABLE multi_max_table (
                                 order_id VARCHAR(128) NOT NULL,
                                 index INT NOT NULL,
                                 description VARCHAR(512) NOT NULL,
                                 PRIMARY KEY (order_id, index)
);

-- Insert data into multi_max_table
INSERT INTO multi_max_table (order_id, index, description)
VALUES ('', 0, 'flink'),
       ('', 1, 'flink'),
       ('', 2, 'flink'),
       ('a', 0, 'flink'),
       ('b', 0, 'flink'),
       ('c', 0, 'flink'),
       ('d', 0, 'flink'),
       ('E', 0, 'flink'),
       ('E', 1, 'flink'),
       ('E', 2, 'flink'),
       ('e', 4, 'flink'),
       ('E', 3, 'flink');
