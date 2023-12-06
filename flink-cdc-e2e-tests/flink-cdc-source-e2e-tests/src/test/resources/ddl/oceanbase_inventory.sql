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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
-- Create and populate our products using a single insert with many rows
DROP DATABASE IF EXISTS inventory;

CREATE DATABASE inventory;

USE inventory;

CREATE TABLE products_source (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    description VARCHAR(512),
    weight DECIMAL(10, 3),
    enum_c enum('red', 'white') default 'red',
    json_c JSON
);
ALTER TABLE products_source AUTO_INCREMENT = 101;

INSERT INTO products_source
VALUES (default,"scooter","Small 2-wheel scooter",3.14, 'red', '{"key1": "value1"}'),
       (default,"car battery","12V car battery",8.1, 'white', '{"key2": "value2"}'),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8, 'red', '{"key3": "value3"}'),
       (default,"hammer","12oz carpenter's hammer",0.75, 'white', '{"key4": "value4"}'),
       (default,"hammer","14oz carpenter's hammer",0.875, 'red', '{"k1": "v1", "k2": "v2"}'),
       (default,"hammer","16oz carpenter's hammer",1.0, null, null),
       (default,"rocks","box of assorted rocks",5.3, null, null),
       (default,"jacket","water resistent black wind breaker",0.1, null, null),
       (default,"spare tire","24 inch spare tire",22.2, null, null);
