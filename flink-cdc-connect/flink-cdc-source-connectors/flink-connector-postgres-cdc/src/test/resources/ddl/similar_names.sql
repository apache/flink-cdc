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

-- FLINK-38965: Test case for similar table names with underscore or percent characters
-- This tests the fix for PostgreSQL LIKE wildcard matching issue:
-- - underscore '_' matches any single character
-- - percent '%' matches any sequence of characters
-- For example, 'user_sink' may match 'userbsink' (due to '_')
-- and 'user%sink' may match 'user_test_sink' (due to '%')

DROP SCHEMA IF EXISTS similar_names CASCADE;
CREATE SCHEMA similar_names;
SET search_path TO similar_names;

-- Table 1: ndi_pg_user_sink_1 (target table)
CREATE TABLE ndi_pg_user_sink_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE ndi_pg_user_sink_1 REPLICA IDENTITY FULL;

INSERT INTO ndi_pg_user_sink_1
VALUES (1, 'user_1', 'Shanghai'),
       (2, 'user_2', 'Beijing'),
       (3, 'user_3', 'Hangzhou');

-- Table 2: ndi_pg_userbsink_1 (similar name - only difference is 'b' instead of '_')
-- This table name would match the LIKE pattern for 'ndi_pg_user_sink_1' 
-- because '_' acts as a wildcard
CREATE TABLE ndi_pg_userbsink_1 (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE ndi_pg_userbsink_1 REPLICA IDENTITY FULL;

INSERT INTO ndi_pg_userbsink_1
VALUES (101, 'userb_1', 'Guangzhou'),
       (102, 'userb_2', 'Shenzhen'),
       (103, 'userb_3', 'Chengdu');

-- Table 3: user%data (tests '%' wildcard scenario)
-- The table name contains '%' character which acts as a wildcard in LIKE pattern.
-- When querying for table 'user%data', the LIKE pattern may also match
-- 'user_test_data' because '%' matches any sequence of characters.
CREATE TABLE "user%data" (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE "user%data" REPLICA IDENTITY FULL;

INSERT INTO "user%data"
VALUES (201, 'percent_1', 'Tianjin'),
       (202, 'percent_2', 'Dalian'),
       (203, 'percent_3', 'Qingdao');

-- Table 4: user_test_data (similar to 'user%data' when % is treated as wildcard)
-- This table name would match the LIKE pattern for 'user%data'
-- because '%' matches '_test_' sequence.
CREATE TABLE user_test_data (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE user_test_data REPLICA IDENTITY FULL;

INSERT INTO user_test_data
VALUES (301, 'test_1', 'Harbin'),
       (302, 'test_2', 'Changchun'),
       (303, 'test_3', 'Shenyang');

-- Cross-schema variant: the schema name is also passed to getColumns as a LIKE pattern,
-- so a wildcard in the schema name matches other schemas too. 'sch_test' (where '_' matches any
-- single character) also matches 'schxtest'. Both schemas contain a table with the SAME name, so
-- a TABLE_NAME-only filter cannot tell them apart and the snapshot would mix in columns from the
-- look-alike schema. The schema name must be checked as well.
DROP SCHEMA IF EXISTS sch_test CASCADE;
CREATE SCHEMA sch_test;
DROP SCHEMA IF EXISTS schxtest CASCADE;
CREATE SCHEMA schxtest;

-- Target schema: sch_test.cross_schema_tbl
CREATE TABLE sch_test.cross_schema_tbl (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE sch_test.cross_schema_tbl REPLICA IDENTITY FULL;

INSERT INTO sch_test.cross_schema_tbl
VALUES (1001, 'sch_test_1', 'Shanghai'),
       (1002, 'sch_test_2', 'Beijing'),
       (1003, 'sch_test_3', 'Hangzhou');

-- Look-alike schema: schxtest.cross_schema_tbl (matches 'sch_test' because '_' matches 'x')
CREATE TABLE schxtest.cross_schema_tbl (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024)
);
ALTER TABLE schxtest.cross_schema_tbl REPLICA IDENTITY FULL;

INSERT INTO schxtest.cross_schema_tbl
VALUES (2001, 'schxtest_1', 'Guangzhou'),
       (2002, 'schxtest_2', 'Shenzhen'),
       (2003, 'schxtest_3', 'Chengdu');
