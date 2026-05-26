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
                          name VARCHAR(255) NOT NULL,
                          description VARCHAR(512),
                          weight FLOAT(24),
                          created_at TIMESTAMPTZ
);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

INSERT INTO products
VALUES (default,'scooter','Small 2-wheel scooter',3.14,'2024-01-01 10:00:00+00'),
       (default,'car battery','12V car battery',8.1,'2024-01-02 11:30:00+00'),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8,'2024-01-03 12:00:00+00'),
       (default,'hammer','12oz carpenter''s hammer',0.75,'2024-01-04 13:15:00+00'),
       (default,'hammer','14oz carpenter''s hammer',0.875,'2024-01-05 14:20:00+00'),
       (default,'hammer','16oz carpenter''s hammer',1.0,'2024-01-06 15:30:00+00'),
       (default,'rocks','box of assorted rocks',5.3,'2024-01-07 16:45:00+00'),
       (default,'jacket','water resistent black wind breaker',0.1,'2024-01-08 17:00:00+00'),
       (default,'spare tire','24 inch spare tire',22.2,'2024-01-09 18:00:00+00');

-- Create customers table
CREATE TABLE customers (
                           id SERIAL PRIMARY KEY,
                           first_name VARCHAR(255) NOT NULL,
                           last_name VARCHAR(255) NOT NULL,
                           email VARCHAR(255) NOT NULL UNIQUE
);

ALTER SEQUENCE customers_id_seq RESTART WITH 101;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert data into customers table
INSERT INTO customers (first_name, last_name, email)
VALUES ('Sally', 'Thomas', 'sally.thomas@acme.com'),
       ('George', 'Bailey', 'gbailey@foobar.com'),
       ('Edward', 'Walker', 'ed@walker.com'),
       ('Anne', 'Kretchmar', 'annek@noanswer.org');