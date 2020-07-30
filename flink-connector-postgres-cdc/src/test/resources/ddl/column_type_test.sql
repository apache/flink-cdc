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
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE full_types (
    id INTEGER NOT NULL,
    bytea_c BYTEA,
    small_c SMALLINT,
    int_c INTEGER,
    big_c BIGINT,
    real_c REAL,
    double_precision DOUBLE PRECISION,
    numeric_c NUMERIC(10, 5),
    decimal_c DECIMAL(10, 1),
    boolean_c BOOLEAN,
    text_c TEXT,
    char_c CHAR,
    character_c CHARACTER(3),
    character_varying_c CHARACTER VARYING(20),
    timestamp3_c TIMESTAMP(3),
    timestamp6_c TIMESTAMP(6),
    date_c DATE,
    time_c TIME(0),
    default_numeric_c NUMERIC,
    PRIMARY KEY (id)
);

ALTER TABLE full_types REPLICA IDENTITY FULL;

INSERT INTO full_types VALUES (
    1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
    'Hello World', 'a', 'abc', 'abcd..xyz',  '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
    '2020-07-17', '18:00:22', 500);