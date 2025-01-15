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
-- DATABASE:  mysql_inventory
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight FLOAT,
  enum_c enum('red', 'white') default 'red',  -- test some complex types as well,
  json_c JSON,                                -- because we use additional dependencies to deserialize complex types.
  point_c POINT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"One",   "Alice",   3.202, 'red', '{"key1": "value1"}', null),
       (default,"Two",   "Bob",     1.703, 'white', '{"key2": "value2"}', null),
       (default,"Three", "Cecily",  4.105, 'red', '{"key3": "value3"}', null),
       (default,"Four",  "Derrida", 1.857, 'white', '{"key4": "value4"}', null),
       (default,"Five",  "Evelyn",  5.211, 'red', '{"K": "V", "k": "v"}', null),
       (default,"Six",   "Ferris",  9.813, null, null, null),
       (default,"Seven", "Grace",   2.117, null, null, null),
       (default,"Eight", "Hesse",   6.819, null, null, null),
       (default,"Nine",  "IINA",    5.223, null, null, null);

-- Create and populate our customers using a single insert with many rows
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
       (104,"user_4","Shanghai","123567891234");
