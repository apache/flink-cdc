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
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------
-- Generate a number of tables to cover as many of the PG types as possible
DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
-- postgis is installed into public schema
SET search_path TO inventory, public;


CREATE TABLE cn_column_test
(
    "测试id"     INTEGER NOT NULL,
    "测试name"   VARCHAR NOT NULL,
    PRIMARY KEY ("测试Id")
);

ALTER TABLE inventory.cn_column_test
    REPLICA IDENTITY FULL;

INSERT INTO inventory.cn_column_test
VALUES (1, 'testName');