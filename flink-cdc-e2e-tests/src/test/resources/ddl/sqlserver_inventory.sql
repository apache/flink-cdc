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
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------
-- Create the inventory database
CREATE
DATABASE inventory;

USE
inventory;
EXEC sys.sp_cdc_enable_db;

-- Create and populate our products using a single insert with many rows
CREATE TABLE products
(
    id          INTEGER IDENTITY(101,1) NOT NULL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      FLOAT
);
INSERT INTO products(name,description,weight)
VALUES ('scooter','Small 2-wheel scooter',3.14);
INSERT INTO products(name,description,weight)
VALUES ('car battery','12V car battery',8.1);
INSERT INTO products(name,description,weight)
VALUES ('12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
INSERT INTO products(name,description,weight)
VALUES ('hammer','12oz carpenter''s hammer',0.75);
INSERT INTO products(name,description,weight)
VALUES ('hammer','14oz carpenter''s hammer',0.875);
INSERT INTO products(name,description,weight)
VALUES ('hammer','16oz carpenter''s hammer',1.0);
INSERT INTO products(name,description,weight)
VALUES ('rocks','box of assorted rocks',5.3);
INSERT INTO products(name,description,weight)
VALUES ('jacket','water resistent black wind breaker',0.1);
INSERT INTO products(name,description,weight)
VALUES ('spare tire','24 inch spare tire',22.2);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0;
