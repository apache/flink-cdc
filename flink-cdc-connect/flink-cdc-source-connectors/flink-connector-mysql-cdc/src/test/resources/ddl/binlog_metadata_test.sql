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

-- create a table with that number of numeric columns x meets the condition 8|(x + 32), so the table contains
-- 8 numeric columns. Which mean any miscalculation in numeric columns will result in the wrong number of bytes
-- read when parsing the signedness table metadata.

CREATE TABLE binlog_metadata
(
    id                   SERIAL,
    tiny_c               TINYINT,
    tiny_un_c            TINYINT UNSIGNED,
    tiny_un_z_c          TINYINT UNSIGNED ZEROFILL,
    small_c              SMALLINT,
    small_un_c           SMALLINT UNSIGNED,
    small_un_z_c         SMALLINT UNSIGNED ZEROFILL,
    year_c YEAR,
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO binlog_metadata
VALUES (DEFAULT, 127, 255, 255, 32767, 65535, 65535, 2023);