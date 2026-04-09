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
-- DATABASE:  mysql_case_inventory
-- ----------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS mixed_case_customer;
CREATE TABLE mixed_case_customer (
  `ID` INTEGER NOT NULL PRIMARY KEY,
  `Name` VARCHAR(255),
  `phone_Number` VARCHAR(255)
);

INSERT INTO mixed_case_customer
VALUES (101, "Alice", "13900000001"),
       (102, "Bob", "13900000002");

DROP TABLE IF EXISTS upper_case_customer;
CREATE TABLE upper_case_customer (
  `ID` INTEGER NOT NULL PRIMARY KEY,
  `NAME` VARCHAR(255),
  `PHONE_NUMBER` VARCHAR(255)
);

INSERT INTO upper_case_customer
VALUES (201, "Carol", "13900000003"),
       (202, "Dave", "13900000004");

DROP TABLE IF EXISTS lower_case_customer;
CREATE TABLE lower_case_customer (
  `id` INTEGER NOT NULL PRIMARY KEY,
  `name` VARCHAR(255),
  `phone_number` VARCHAR(255)
);

INSERT INTO lower_case_customer
VALUES (301, "Eve", "13900000005"),
       (302, "Frank", "13900000006");

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
  `id` INTEGER NOT NULL PRIMARY KEY,
  `NAME` VARCHAR(255),
  `age` INTEGER,
  `address` VARCHAR(255)
);

INSERT INTO customer
VALUES (401, "Grace", 18, "Shanghai"),
       (402, "Heidi", 19, "Beijing");
