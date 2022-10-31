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
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE DB2INST1.FULL_TYPES (
    ID INTEGER NOT NULL,
    SMALL_C SMALLINT,
    INT_C INTEGER,
    BIG_C BIGINT,
    REAL_C REAL,
    DOUBLE_C DOUBLE,
    NUMERIC_C NUMERIC(10, 5),
    DECIMAL_C DECIMAL(10, 1),
    VARCHAR_C VARCHAR(200),
    CHAR_C CHAR,
    CHARACTER_C CHAR(3),
    TIMESTAMP_C TIMESTAMP,
    DATE_C DATE,
    TIME_C TIME,
    DEFAULT_NUMERIC_C NUMERIC,
    PRIMARY KEY (ID)
);


INSERT INTO DB2INST1.FULL_TYPES VALUES (
    1, 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443,
    'Hello World', 'a', 'abc', '2020-07-17 18:00:22.123', '2020-07-17', '18:00:22', 500);
