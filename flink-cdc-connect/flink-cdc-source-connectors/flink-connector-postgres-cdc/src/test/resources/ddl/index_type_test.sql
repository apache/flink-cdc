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
-- DATABASE:  key_type_test
-- ----------------------------------------------------------------------------------------------------------------
-- Generate a number of tables to cover as many of the PG index situation (primary key, unique index) as possible
DROP SCHEMA IF EXISTS indexes CASCADE;
CREATE SCHEMA indexes;
SET search_path TO indexes;

-- Generate a table without primary key but a functional unique index
CREATE TABLE functional_unique_index
(
    id                  INTEGER NOT NULL,
    char_c              CHAR
);
create unique index test_tbl_idx
    on functional_unique_index(id, COALESCE(char_c, ''::text));

ALTER TABLE functional_unique_index
    REPLICA IDENTITY FULL;

INSERT INTO functional_unique_index
VALUES (1, 'a');
