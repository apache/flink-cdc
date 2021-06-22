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
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE HR.products (
  id INTEGER NOT NULL,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight FLOAT,
  PRIMARY KEY(id)
);
ALTER TABLE HR.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- Auto-generated SQL script #202106141039

INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (101,'scooter','Small 2-wheel scooter',3.14);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (102,'car battery','12V car battery',8.1);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (103,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (104,'hammer','12oz carpenters hammer',0.75);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (105,'hammer','14oz carpenters hammer',0.875);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (106,'hammer','16oz carpenters hammer',1.0);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (107,'rocks','box of assorted rocks',5.3);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (108,'jacket','water resistent black wind breaker',0.1);
INSERT INTO HR.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (109,'spare tire','24 inch spare tire',22.2);