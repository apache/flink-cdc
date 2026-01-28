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

-- FLINK-38965: Test case for similar table names with underscore
-- This tests the fix for PostgreSQL LIKE wildcard matching issue
-- where underscore '_' matches any single character, causing
-- 'ndi_pg_user_sink_1' to also match 'ndi_pg_userbsink_1'

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
